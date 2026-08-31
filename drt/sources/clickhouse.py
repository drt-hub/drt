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

from drt.config.credentials import ClickHouseProfile, ProfileConfigLike
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

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.

        **Streaming (#765): ``query_rows_stream``** rather than ``query``.
        ``client.query()`` materialises the whole result set into
        ``result.result_rows`` before returning, so peak memory tracked the
        table; the streaming variant hands back rows as blocks arrive.
        Measured figures live in ``docs/research/extraction-memory.md``, which
        is the single source for them.

        The reduction here is ~1.5x — a far smaller win than the Postgres or
        MySQL legs, and the reason is worth knowing before anyone tries to improve
        it: the remainder is clickhouse-connect buffering the HTTP response
        internally, not drt holding rows. ``max_block_size`` does not move it
        (measured identical at 8192, 65536 and the default), so there is no
        ``fetch_size`` knob here — there is nothing for it to control.

        Column names come off ``stream.source.column_names``, which is
        populated before iteration begins (unlike psycopg2's named cursor).
        On an empty result it is an empty tuple, so nothing here may assume
        columns exist — the ClickHouse form of the same trap.

        The stream is a context manager and is exited *inside* the ``finally``
        that closes the client, so an abandoned iterator
        (``--limit`` / ``--fail-fast``, #775/#774) does not leave an
        unconsumed HTTP response on a client about to be closed under it.

        ``query_tags`` is unused. ClickHouse does have a native
        ``log_comment`` query setting that lands in ``system.query_log`` —
        a real follow-up — but #768 scoped native tagging to BigQuery /
        Snowflake / Databricks only, so this connector gets the SQL comment
        the engine already prepended to ``query`` and nothing more for now.
        """
        assert isinstance(config, ClickHouseProfile)

        def _connect_and_open_stream() -> tuple[Any, Any]:
            client = self._connect(config)
            try:
                stream = client.query_rows_stream(query)
                return client, stream
            except BaseException:
                # A failed attempt cleans up after itself: the close no longer
                # lives in a `finally` here, since the stream must outlive it.
                client.close()
                raise

        client, stream = with_retry(
            _connect_and_open_stream, RetryConfig(), retry_on=self._is_transient
        )

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        try:
            with stream:
                columns = stream.source.column_names
                for row in stream:
                    yield dict(zip(columns, row))
        finally:
            client.close()

    def test_connection(self, config: ProfileConfigLike) -> bool:
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
