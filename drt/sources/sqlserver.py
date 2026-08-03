"""SQL Server source using pymssql.

Requires: pip install drt-core[sqlserver]

Example ~/.drt/profiles.yml:
    sqlserver_prod:
      type: sqlserver
      host: db.example.com
      port: 1433
      database: analytics
      user: drt_reader
      password_env: SQLSERVER_PASSWORD
      schema: dbo
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfig, SQLServerProfile, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry


class SQLServerSource:
    """Extract records from a SQL Server database."""

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Transient: ``OperationalError`` (connection refused, the server
        restarting, an Azure SQL failover moving the database between
        replicas) and ``InterfaceError`` (the driver's connection object went
        bad).

        Permanent: ``ProgrammingError`` (bad SQL, missing table),
        ``DataError``, ``IntegrityError``, ``NotSupportedError``.

        pymssql follows PEP 249 exactly — verified against its
        ``exceptions.py``: ``InterfaceError`` subclasses ``Error``, while
        ``OperationalError``, ``ProgrammingError``, ``DataError``,
        ``IntegrityError``, ``InternalError`` and ``NotSupportedError`` are
        all siblings under ``DatabaseError``. Matching the two specific
        classes rather than the base keeps a SQL typo from being retried.

        **Login failures are excluded.** That hierarchy is a red herring here:
        ``pymssql.connect()`` wraps *every* ``_mssql.MSSQLDatabaseException``
        into ``OperationalError``, so SQL Server error 18456 ("Login failed for
        user") arrives as an otherwise perfectly retryable class. Retrying a
        bad credential three times in quick succession can trip an AD account
        lockout policy, turning a config typo into an outage — the same hazard
        already excluded on Postgres/Redshift (SQLSTATE class 28) and MySQL
        (errno 1045).

        Matched on the message rather than the error number because the number
        does not survive translation: ``connect()`` re-raises with
        ``e.args[0]`` alone, discarding the exception object that carried
        ``.number``. Substring rather than equality, since the text often
        arrives with the server's own prefix ("Adaptive Server connection
        failed …") ahead of the login message.
        """
        try:
            import pymssql  # type: ignore[import-untyped]
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        if not isinstance(exc, (pymssql.OperationalError, pymssql.InterfaceError)):
            return False
        return "login failed for user" not in str(exc).lower()

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.

        **Streaming (#765).** Rows arrive by iterating the cursor in
        ``fetch_size`` batches rather than through a single ``fetchall()``, so
        peak memory tracks the batch instead of the result set. Measured
        figures live in ``docs/research/extraction-memory.md``, which is the
        single source for them.

        The cursor is opened with ``as_dict=True``, so each row already arrives
        as a mapping and no column list is needed.

        The connection is held for the whole load. The cursor is closed before
        the connection, both in a ``finally`` that also fires on
        ``GeneratorExit`` — so an abandoned iterator (``--limit`` /
        ``--fail-fast``, #775/#774) still tears down.

        ``query_tags`` is unused — SQL Server (pymssql) has no session/job-
        level tagging primitive drt can reach, so the SQL comment the engine
        already prepended to ``query`` is this connector's only attribution
        (#768).
        """
        assert isinstance(config, SQLServerProfile)

        def _connect_and_execute() -> tuple[Any, Any]:
            conn = self._connect(config)
            try:
                cur = conn.cursor(as_dict=True)
                cur.arraysize = config.fetch_size
                cur.execute(query)
                return conn, cur
            except BaseException:
                # The failed attempt cleans up after itself — with the close
                # moved out of `finally`, nothing else would.
                conn.close()
                raise

        conn, cur = with_retry(_connect_and_execute, RetryConfig(), retry_on=self._is_transient)

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        try:
            for row in cur:
                yield dict(row)
        finally:
            cur.close()
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, SQLServerProfile)
        conn = None
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                return True
            finally:
                cur.close()
        except Exception:
            return False
        finally:
            if conn:
                conn.close()

    def _connect(self, config: SQLServerProfile) -> Any:
        password = resolve_env(config.password, config.password_env) or ""

        try:
            import pymssql
        except ImportError as e:
            raise ImportError("SQL Server support requires: pip install drt-core[sqlserver]") from e

        return pymssql.connect(
            server=config.host,
            # pymssql's own stub types `port` as `str`, though the driver
            # accepts an int at runtime (verified: an int port fails on the
            # connection, not on the argument). Passing the string satisfies
            # the stub without changing behaviour — the alternative was a
            # `type: ignore` for a third-party stub inaccuracy, which is
            # noisier and would outlive the stub being fixed.
            port=str(config.port),
            user=config.user,
            password=password,
            database=config.database,
        )
