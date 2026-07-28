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

import os
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import PostgresProfile, ProfileConfig
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

        Matched by **exact class**, not with a base-class ``isinstance``:
        psycopg2 makes ``OperationalError``, ``ProgrammingError``,
        ``DataError`` and ``IntegrityError`` all siblings under
        ``DatabaseError`` (PEP 249's hierarchy), so testing against the base
        would happily retry a typo in the user's SQL three times.
        ``OperationalError`` and ``InterfaceError`` have no subclasses in
        psycopg2, so ``isinstance`` against them specifically is exact.

        ``psycopg2`` is imported inside the method: it is an optional extra,
        and this class is imported unconditionally by the connector registry.
        """
        try:
            import psycopg2
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
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
        """
        assert isinstance(config, PostgresProfile)

        def _connect_and_fetch() -> tuple[list[str], list[Any]]:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                return columns, cur.fetchall()
            finally:
                # Close inside the retried unit: a failed attempt must not
                # leak its half-open connection while we back off.
                conn.close()

        columns, rows = with_retry(_connect_and_fetch, RetryConfig(), retry_on=self._is_transient)

        # Iteration sits outside the retry: see the docstring: once a row is
        # yielded it cannot be un-sent, so re-running is not safe.
        for row in rows:
            yield dict(zip(columns, row))

    def test_connection(self, config: ProfileConfig) -> bool:
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

        password = config.password or (
            os.environ.get(config.password_env) if config.password_env else None
        )
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=password,
        )
