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

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Yield records one at a time from the source."""
        ...

    def test_connection(self, config: ProfileConfig) -> bool:
        """Return True if the source is reachable."""
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
        self, query: str, config: ProfileConfig, cursor_value: str | None
    ) -> Iterator[dict[str, Any]]:
        """Yield records, filtering server-side from ``cursor_value`` when possible."""
        ...
