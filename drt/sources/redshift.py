"""Amazon Redshift source implementation.

Requires: pip install drt-core[redshift]

Redshift is PostgreSQL-compatible (based on PartiQL/Postgres 8.x),
so this connector reuses psycopg2 with Redshift-specific defaults.

Example ~/.drt/profiles.yml:
    redshift_prod:
      type: redshift
      host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
      port: 5439
      dbname: analytics
      user: analyst
      password_env: REDSHIFT_PASSWORD
      schema: public
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfigLike, RedshiftProfile, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry


class RedshiftSource:
    """Extract records from an Amazon Redshift cluster.

    Redshift uses PostgreSQL wire protocol, so we connect via psycopg2.
    The main differences from vanilla Postgres:
      - Default port is 5439
      - Schema support is important for Redshift's multi-schema warehouses
      - Connection string uses same parameters
    """

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Same psycopg2 classification as the Postgres source — Redshift speaks
        the Postgres wire protocol through the same driver. Transient:
        ``OperationalError`` (connection refused, cluster failover, a paused
        serverless workgroup resuming, the WLM dropping an idle session) and
        ``InterfaceError``. Permanent: ``ProgrammingError`` / ``DataError`` /
        ``IntegrityError``.

        Matched against ``OperationalError`` / ``InterfaceError`` rather than
        the shared ``DatabaseError`` base — under PEP 249 the permanent
        classes are siblings of ``OperationalError``, so a base-class check
        would retry bad SQL.

        Authentication failures are excluded even though psycopg2 files them
        *under* ``OperationalError`` (``InvalidPassword``,
        ``InvalidAuthorizationSpecification`` — SQLSTATE class ``28``).
        Retrying a wrong credential three times in quick succession can trip
        an account lockout, turning a config typo into an outage.
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
        """Execute query and yield records as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.

        **Streaming (#765): a named (server-side) cursor in ``fetch_size``
        batches**, so peak memory tracks the batch rather than the result set.
        The connection is therefore held for the whole load and closed in a
        ``finally`` that also covers ``GeneratorExit`` — see the Postgres
        source for the lifecycle rationale, which is identical.

        One Redshift-specific wrinkle: ``SET search_path`` runs on a **plain**
        cursor, not the named one. psycopg2 implements a named cursor as
        ``DECLARE <name> CURSOR WITHOUT HOLD FOR <query>``, and ``DECLARE …
        FOR SET`` is a syntax error — a named cursor can only wrap something
        that returns rows. Running it on a plain cursor first is equivalent
        anyway: ``search_path`` is session state, so it is still in force when
        the named cursor is declared on the same connection (verified against
        a live server).

        ``query_tags`` is unused — Redshift (psycopg2, same as Postgres) has
        no session/job-level tagging primitive drt can reach, so the SQL
        comment the engine already prepended to ``query`` is this
        connector's only attribution (#768).
        """
        assert isinstance(config, RedshiftProfile)

        def _connect_and_execute() -> tuple[Any, Any]:
            conn = self._connect(config)
            try:
                if config.schema:
                    # Plain cursor: this is session state, and DECLARE ... FOR
                    # SET is not valid SQL.
                    with conn.cursor() as setup:
                        setup.execute("SET search_path TO %s", (config.schema,))
                cur = conn.cursor(name="drt_extract")
                cur.itersize = config.fetch_size
                cur.execute(query)
                return conn, cur
            except BaseException:
                # A failed attempt cleans up after itself: the close no longer
                # lives in a `finally` here, since the cursor must outlive it.
                conn.close()
                raise

        conn, cur = with_retry(_connect_and_execute, RetryConfig(), retry_on=self._is_transient)

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        try:
            # ``description`` is None until the first batch arrives on a named
            # cursor (DECLARE does not touch the server), so columns are read
            # on the first iteration rather than after execute(). See the
            # Postgres source — same driver, same trap.
            columns: list[str] = []
            for row in cur:
                if not columns:
                    columns = [desc[0] for desc in cur.description]
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfigLike) -> bool:
        """Test if the Redshift cluster is reachable."""
        assert isinstance(config, RedshiftProfile)
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def _connect(self, config: RedshiftProfile) -> Any:
        """Create a connection to Redshift using psycopg2."""
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError("Redshift support requires: pip install drt-core[redshift]") from e

        password = resolve_env(config.password, config.password_env)
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=password,
        )
