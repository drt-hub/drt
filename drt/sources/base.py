"""Source Protocol — the interface all sources must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
Future PyO3 bindings will implement this same protocol.
"""

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable

from drt.config.credentials import ProfileConfig


@runtime_checkable
class Source(Protocol):
    """Extract records from a data warehouse or database."""

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield records one at a time from the source.

        ``query_tags`` (#768) is the cost-attribution payload (``app`` /
        ``sync`` / ``run_id`` / project ``extra``) — ``None`` when
        ``query_tagging.enabled`` is false. The universal SQL-comment
        fallback is already baked into ``query`` by the engine before this
        is called, so implementations with no warehouse-native tagging
        mechanism (BigQuery job labels, Snowflake ``QUERY_TAG``, Databricks
        session tags) can ignore the parameter entirely and still be tagged.
        Keyword-only and default-``None`` so every existing implementation
        and test caller keeps working unchanged.

        Raises:
            Exception: connection or query failure. Not caught by the
                engine (``yield from source.extract(...)``); propagates and
                aborts the sync.
        """
        ...

    def test_connection(self, config: ProfileConfig) -> bool:
        """Return True if the source is reachable, False otherwise.

        Deliberately the opposite contract from
        ``destinations.base.ConnectionTestable.test_connection`` (which
        raises rather than returns a bool). The two never meet at a shared
        call site (sources vs. destinations), so this asymmetry is frozen
        as-is rather than unified — see ADR 0007.

        Every implementation catches connection/query failures and returns
        False for them. It is NOT guaranteed to never raise: several
        implementations (MySQL, Databricks, Snowflake, SQL Server) close
        the connection in a ``finally`` block outside the surrounding
        ``except``, so a failure during that cleanup step still propagates.
        Callers (``drt profile test``, the MCP test-profile tool) already
        catch exceptions from this method defensively for that reason.
        """
        ...


@runtime_checkable
class IncrementalSource(Protocol):
    """Optional source capability — receive the resolved watermark directly (#767).

    SQL sources consume the cursor through the rendered ``WHERE`` clause in
    ``query``; API-shaped sources have no SQL to carry it, so for
    ``mode: incremental`` syncs the engine calls ``extract_incremental``
    with the watermark value instead of ``extract``. ``cursor_value`` is
    ``None`` on a first run with no stored watermark and no
    ``watermark.default_value``. Same optional-Protocol pattern as
    ``ConnectionTestable`` / ``StagedDestination``.
    """

    def extract_incremental(
        self,
        query: str,
        config: ProfileConfig,
        cursor_value: str | None,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield records, filtering server-side from ``cursor_value`` when possible.

        ``query_tags`` — see :meth:`Source.extract`.

        Raises:
            Exception: see :meth:`Source.extract` — same propagation
                contract.
        """
        ...
