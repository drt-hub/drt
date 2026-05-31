"""``FakeSource`` — in-memory ``Source`` Protocol implementation for tests.

Closes [#364](https://github.com/drt-hub/drt/issues/364).

The real ``Source`` implementations (BigQuery, Postgres, DuckDB, etc.)
all require a live connection, which makes engine + destination tests
either slow (real DB), fragile (mock DB), or skipped entirely. The
``FakeSource`` shipped here yields configurable in-memory records
without touching the network, so engine integration tests, destination
contract tests, and behavioural assertions about cursor handling /
batch sizing become straightforward to write.

Not registered in ``drt/connectors/registry.py`` — this is test
infrastructure only, not a user-facing Source. End users who want an
in-memory source should look at the DuckDB Source instead (which also
supports in-memory tables).

Examples:
    Yield three rows verbatim::

        from drt.sources.fake import FakeSource

        source = FakeSource(
            rows=[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        )
        list(source.extract("SELECT *", profile))
        # [{'id': 1, 'name': 'alice'}, {'id': 2, 'name': 'bob'}]

    Inspect the queries the engine actually issued::

        source = FakeSource(rows=[{"id": 1}])
        list(source.extract("SELECT * FROM t WHERE id > 0", profile))
        assert source.queries_executed == ["SELECT * FROM t WHERE id > 0"]

    Simulate an unreachable source::

        source = FakeSource(connection_ok=False)
        assert source.test_connection(profile) is False
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig


@dataclass
class FakeSource:
    """In-memory ``Source`` implementation for tests.

    Args:
        rows: Records to yield from :meth:`extract`, in order.
        connection_ok: Return value of :meth:`test_connection`.

    Attributes:
        queries_executed: Ordered list of queries passed to
            :meth:`extract` — useful for asserting that the engine
            issued the expected query template (e.g., with the
            interpolated cursor value).
    """

    rows: list[dict[str, Any]] = field(default_factory=list)
    connection_ok: bool = True
    queries_executed: list[str] = field(default_factory=list, init=False, repr=False)

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Yield each configured row in order, recording the query."""
        self.queries_executed.append(query)
        yield from self.rows

    def test_connection(self, config: ProfileConfig) -> bool:
        """Return ``connection_ok``."""
        return self.connection_ok
