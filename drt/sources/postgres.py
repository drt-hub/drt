"""PostgreSQL source implementation.

Requires: pip install drt-core[postgres]

Example ~/.drt/profiles.yml:
    pg:
      type: postgres
      host: localhost
      port: 5432
      dbname: analytics
      user: analyst
      password_env: PG_PASSWORD   # export PG_PASSWORD=secret
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import PostgresProfile, ProfileConfigLike, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry


class PostgresSource:
    """Extract records from a PostgreSQL database."""

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Transient — the server or the link to it was momentarily unavailable,
        and the identical query may well succeed on a second attempt:

        - ``OperationalError`` — connection refused, ``server closed the
          connection unexpectedly``, ``terminating connection due to
          administrator command`` (failover, restart, idle-timeout reaper).
        - ``InterfaceError`` — the driver's own connection object went bad.

        Permanent — ``ProgrammingError`` (bad SQL, missing relation, denied
        privilege), ``DataError``, ``IntegrityError``. Retrying these only
        delays an error the user has to fix anyway.

        Matched against ``OperationalError`` / ``InterfaceError`` specifically,
        not the shared base: psycopg2 makes ``OperationalError``,
        ``ProgrammingError``, ``DataError`` and ``IntegrityError`` all siblings
        under ``DatabaseError`` (PEP 249's hierarchy), so testing against the
        base would happily retry a typo in the user's SQL three times.

        ``OperationalError`` *does* have subclasses, and not all of them are
        transient — ``psycopg2.errors.InvalidPassword`` and
        ``InvalidAuthorizationSpecification`` live under it. Retrying a wrong
        password is not just wasted work: three rapid attempts can trip an
        account lockout policy and turn a config typo into an outage. They are
        excluded both by class and by SQLSTATE class ``28``, because neither
        signal is always present — ``pgcode`` is only populated on errors the
        server actually raised.

        ``psycopg2`` is imported inside the method: it is an optional extra,
        and this class is imported unconditionally by the connector registry.
        """
        try:
            import psycopg2
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        if not isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            return False
        # Exclude authentication failures. psycopg2 files these *under*
        # OperationalError, so the isinstance above lets them through.
        # Matched two ways because either can be absent: by class (works for
        # any exception the driver constructs) and by SQLSTATE class 28,
        # invalid_authorization_specification (set only on server-raised
        # errors, but authoritative when present).
        auth_errors = (
            psycopg2.errors.InvalidAuthorizationSpecification,
            psycopg2.errors.InvalidPassword,
        )
        if isinstance(exc, auth_errors):
            return False
        pgcode = getattr(exc, "pgcode", None)
        return not (pgcode and str(pgcode).startswith("28"))

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** Opening
        the connection, executing the query and fetching the result set are
        wrapped in exponential backoff, so a Postgres restart or a dropped
        connection on the way in no longer fails the whole sync.

        A failure **after the first row has been yielded is not retried** and
        propagates. By then the engine has already handed those rows to the
        destination; re-running the query would re-emit them, and skipping
        them would need a stable ordering the query does not promise. That is
        a checkpointing problem, not a retry problem — see #766.

        **Streaming (#765).** Rows arrive through a *named* (server-side)
        cursor in ``fetch_size`` batches rather than a single ``fetchall()``,
        so peak memory tracks the batch instead of the result set — an order of
        magnitude lower on a 300k-row extract. Measured figures live in
        ``docs/research/extraction-memory.md``, which is the single source for
        them. A plain ``cursor()`` would not do — psycopg2 buffers the whole
        result set client-side unless the cursor is named.

        The lifecycle is the subtle part. A server-side cursor lives only as
        long as its connection, so #766's ``finally: conn.close()`` *inside*
        the retried unit is no longer possible — closing there would invalidate
        the cursor before the first row is read. Instead:

        - the retried unit connects and executes, and closes its own
          connection on failure, so a retried attempt leaks nothing;
        - the yield loop owns the connection afterwards and closes it in a
          ``finally``, which also runs on ``GeneratorExit`` when a consumer
          abandons the iterator (``--limit`` / ``--fail-fast``, #775/#774).

        That last case is a real leak, not a theoretical one: verified against
        a live Postgres that dropping the cursor reference and forcing a GC
        leaves the server-side cursor open in ``pg_cursors``.

        ``query_tags`` is unused — Postgres has no session/job-level tagging
        primitive drt can reach, so the SQL comment the engine already
        prepended to ``query`` is this connector's only attribution (#768).
        """
        assert isinstance(config, PostgresProfile)

        def _connect_and_execute() -> tuple[Any, Any]:
            conn = self._connect(config)
            try:
                # Named cursor => server-side. The name only has to be unique
                # within the session, and each extract() gets its own
                # connection, so a fixed name is safe.
                cur = conn.cursor(name="drt_extract")
                cur.itersize = config.fetch_size
                cur.execute(query)
                return conn, cur
            except BaseException:
                # The failed attempt cleans up after itself — with the close
                # moved out of `finally`, nothing else would.
                conn.close()
                raise

        conn, cur = with_retry(_connect_and_execute, RetryConfig(), retry_on=self._is_transient)

        # Iteration sits outside the retry: once a row is yielded it cannot be
        # un-sent, so re-running is not safe. The finally also fires on
        # GeneratorExit, so an abandoned generator still closes the connection.
        try:
            # ``cur.description`` is None until the first batch actually
            # arrives — a named cursor has not touched the server at DECLARE
            # time, unlike a plain one where execute() populates it
            # immediately. So columns are read inside the loop, on the first
            # iteration, rather than up front.
            columns: list[str] = []
            for row in cur:
                if not columns:
                    columns = [desc[0] for desc in cur.description]
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, PostgresProfile)
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def _connect(self, config: PostgresProfile) -> Any:
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError("PostgreSQL support requires: pip install drt-core[postgres]") from e

        password = resolve_env(config.password, config.password_env)
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=password,
        )

    # --- ManagedTableCapable (#960, ADR 0005 step 3) ------------------------
    #
    # Uses its own plain cursor + explicit commit() — deliberately not the
    # named (server-side) cursor extract() uses, since a named cursor cannot
    # run DDL (see extract()'s docstring), and nothing on this connection
    # sets autocommit.

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        assert isinstance(config, PostgresProfile)
        from psycopg2 import sql as _pgsql

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Probe first (#695 discipline): a locked-down user with no
            # CREATE privilege, but an admin-pre-provisioned schema, must
            # never have the CREATE statement issued at all.
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (config.managed_schema,),
            )
            if cur.fetchone() is not None:
                return
            cur.execute(
                _pgsql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    _pgsql.Identifier(config.managed_schema)
                )
            )
            conn.commit()
        finally:
            conn.close()

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        assert isinstance(config, PostgresProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"{config.managed_schema}.{table_name}",),
            )
            return cur.fetchone()[0] is not None
        finally:
            conn.close()

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        assert isinstance(config, PostgresProfile)
        from psycopg2 import sql as _pgsql

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                _pgsql.SQL("DROP TABLE IF EXISTS {}").format(
                    _pgsql.Identifier(config.managed_schema, table_name)
                )
            )
            conn.commit()
        finally:
            conn.close()
