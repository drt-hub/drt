"""ClickHouse source implementation.

Requires: pip install drt-core[clickhouse]

Example ~/.drt/profiles.yml:
    ch:
      type: clickhouse
      host: localhost
      port: 8123
      database: default
      user: default
      password_env: CLICKHOUSE_PASSWORD   # export CLICKHOUSE_PASSWORD=secret
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ClickHouseProfile, ProfileConfig
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry


class ClickHouseSource:
    """Extract records from a ClickHouse database."""

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Transient: ``OperationalError`` ("an unexpected disconnect occurs")
        and ``InterfaceError`` ("errors related to the database interface
        rather than the database itself") — both quoting
        ``clickhouse_connect.driver.exceptions``.

        Permanent: ``ProgrammingError`` (table not found, syntax error),
        ``DataError``, ``IntegrityError``, ``NotSupportedError``.

        clickhouse-connect follows PEP 249 — verified against its
        ``exceptions.py``: ``InterfaceError`` and ``DatabaseError`` both
        subclass ``Error``, with ``OperationalError`` / ``ProgrammingError`` /
        ``DataError`` / ``IntegrityError`` / ``InternalError`` /
        ``NotSupportedError`` as siblings under ``DatabaseError``. Note
        ``StreamClosedError`` subclasses ``ProgrammingError``, so it is
        correctly treated as permanent.

        ClickHouse's HTTP interface means raw ``httpx`` exceptions can surface
        instead of a driver class. Those need no handling here — ``with_retry``
        catches ``httpx.TransportError`` and retryable status codes natively,
        and ``retry_on`` is purely additive to that.
        """
        try:
            from clickhouse_connect.driver import exceptions as ch_exc
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        return isinstance(exc, (ch_exc.OperationalError, ch_exc.InterfaceError))

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.
        """
        assert isinstance(config, ClickHouseProfile)

        def _connect_and_fetch() -> tuple[list[str], list[Any]]:
            client = self._connect(config)
            try:
                result = client.query(query)
                # clickhouse_connect puts column names in result.column_names
                # and rows in result.result_rows
                return result.column_names, result.result_rows
            finally:
                client.close()

        columns, rows = with_retry(_connect_and_fetch, RetryConfig(), retry_on=self._is_transient)

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        for row in rows:
            yield dict(zip(columns, row))

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, ClickHouseProfile)
        try:
            client = self._connect(config)
            client.query("SELECT 1")
            client.close()
            return True
        except Exception:
            return False

    def _connect(self, config: ClickHouseProfile) -> Any:
        try:
            import clickhouse_connect
        except ImportError as e:
            raise ImportError(
                "ClickHouse support requires: pip install drt-core[clickhouse]"
            ) from e

        from drt.config.credentials import resolve_env

        password = resolve_env(config.password, config.password_env) or ""

        return clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            database=config.database,
            username=config.user,
            password=password,
        )
