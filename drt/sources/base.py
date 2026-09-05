"""Source Protocol — the interface all sources must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
Future PyO3 bindings will implement this same protocol.
"""

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable

from drt.config.profiles import ProfileConfigLike


@runtime_checkable
class Source(Protocol):
    """Extract records from a data warehouse or database.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).
    """

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
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

    def test_connection(self, config: ProfileConfigLike) -> bool:
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

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

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
        config: ProfileConfigLike,
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


@runtime_checkable
class ManagedTableCapable(Protocol):
    """Optional source capability: create-if-absent drt-owned bookkeeping
    tables in the source warehouse (#960, ADR 0005 step 3).

    A new, separate Protocol rather than an addition to ``Source`` itself —
    per ADR 0007, adding a required method to an already-shipped Protocol
    breaks every existing implementer, so new capability goes here instead
    (same pattern as ``IncrementalSource`` above and
    ``destinations.base.QueryableDestination``).

    Stability: New in #960 — not yet frozen. Will be reviewed for stability
    at the same time as the rest of the Protocol surface (ADR 0007's
    two-minor deprecation window applies once this is declared stable).

    **Scope note, deliberately narrow.** This Protocol owns only the
    namespace-and-table *existence* half — locating drt's managed schema,
    checking whether a specific table already exists in it, and dropping one
    cleanly. It does **not** own DDL bodies (column definitions, types,
    indexes): each consumer (#755, #920, #1099, #1100) creates its own
    table's exact schema when it lands, using ``ensure_managed_schema``'s
    result. Caller-supplied column definitions were deliberately rejected —
    interpolating caller-controlled column names into DDL text is exactly
    the unquoted-identifier surface #1064/#1090 found and fixed on the
    diff-preview read path; keeping DDL bodies hardcoded per dialect (the
    same choice tracked mirror's ``_create_state_table`` hook already makes,
    ``destinations/sql_base.py``) keeps that fix's guarantee intact here too.

    A destination-side equivalent is intentionally out of scope: #760
    (managed *destination* tables) is a different feature with a different
    audience (user data tables, not drt's own bookkeeping) and depends only
    on the already-shipped ``introspect_schema()`` (#317), not on this
    Protocol.

    **Naming convention across dialects**, documented once here so a future
    Snowflake/Databricks/BigQuery implementation doesn't drift: each source
    profile's own field for the managed-schema name is called
    ``managed_schema`` (``PostgresProfile.managed_schema``,
    ``drt/config/profiles.py``) — never reused from an existing
    ``schema``-named field (Snowflake/Databricks already have one, meaning
    their query-execution default schema, a different concept this
    deliberately avoids colliding with). The default value is never
    ``public`` (or a dialect's equivalent catch-all default) — every
    reverse-ETL vendor researched for ADR 0005's 2026-09 amendment
    (RudderStack's ``_rudderstack``, Segment's ``__segment_reverse_etl``,
    Hightouch's ``hightouch_planner``) isolates its bookkeeping tables from
    user data for exactly this blast-radius reason.
    """

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        """Create drt's managed schema if it does not already exist.

        A no-op when the schema is already present — including when an
        operator pre-provisioned it by hand and granted the sync user no
        CREATE privilege at all (the escape hatch tracked mirror's own
        ``docs/connectors/postgres.md`` documents; #960 follows the same
        probe-before-DDL discipline, never skipping the existence check).

        Raises:
            Exception: connection failure, or a CREATE attempt that fails
                for a reason other than "already exists" (e.g. genuinely no
                CREATE privilege and no pre-provisioned schema either) —
                propagates so the caller can degrade or fail loudly rather
                than silently proceeding without the schema it needs.
        """
        ...

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        """Does ``table_name`` already exist in the managed schema?

        Pure probe, no side effect — mirrors tracked mirror's
        ``_state_table_exists`` hook. Callers use this before issuing their
        own ``CREATE TABLE IF NOT EXISTS`` for whichever table they own.
        """
        ...

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        """Drop ``table_name`` from the managed schema if present.

        A no-op if the table does not exist. This is the reversibility half
        of ADR 0005 Decision 4 — turning a warehouse-backed feature back off
        must be a clean, symmetric undo of whatever ``ensure_managed_schema``
        plus a consumer's own ``CREATE TABLE`` did, not a one-way migration.
        Does **not** drop the managed schema itself, even if this was the
        last table in it — multiple #960 consumers can share one schema, and
        dropping it out from under another feature's table would silently
        break that feature. Schema-level cleanup, if ever wanted, is a
        separate, explicit operation.
        """
        ...
