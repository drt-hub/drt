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

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.
        """
        assert isinstance(config, SQLServerProfile)

        def _connect_and_fetch() -> list[Any]:
            conn = self._connect(config)
            try:
                cur = conn.cursor(as_dict=True)
                try:
                    cur.execute(query)
                    return list(cur.fetchall())
                finally:
                    cur.close()
            finally:
                conn.close()

        rows = with_retry(_connect_and_fetch, RetryConfig(), retry_on=self._is_transient)

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        for row in rows:
            yield dict(row)

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
            port=config.port,
            user=config.user,
            password=password,
            database=config.database,
        )
