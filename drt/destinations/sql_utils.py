"""Shared utilities for SQL destinations.

Identifier quoting, row-count capability discovery, and mirror-mode guard
messages — factored out so the SQL destinations don't each hand-roll them.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from drt.config.models import DestinationConfig


def backtick_quote_ident(table: str) -> str:
    """Backtick-quote a (possibly qualified) identifier.

    ``mydb.scores`` -> ``\\`mydb\\`.\\`scores\\``` ; ``scores`` -> ``\\`scores\\```.

    Shared by the MySQL and ClickHouse destinations, whose quoting rules are
    identical.
    """
    if "." in table:
        return "`" + "`.`".join(table.split(".")) + "`"
    return f"`{table}`"


@runtime_checkable
class RowCountable(Protocol):
    """A destination that can report its current table row count.

    Capability is discovered structurally (``isinstance(dest, RowCountable)``)
    rather than enumerated, so a new SQL destination that implements
    ``get_row_count`` is picked up automatically.
    """

    def get_row_count(self, config: Any) -> int: ...


def get_row_count_for_destination(
    destination: Any,
    config: DestinationConfig,
) -> int | None:
    """Get the current row count from a SQL destination table.

    Args:
        destination: Destination instance.
        config: Destination configuration with table name.

    Returns:
        Row count, or ``None`` when the destination can't report one
        (e.g. REST API, Slack — anything without ``get_row_count``).

    Raises:
        Exception: If connection or query fails (should be caught by caller).
    """
    if isinstance(destination, RowCountable):
        return int(destination.get_row_count(config))
    return None


# Mirror-mode guard messages — centralized so the wording stays identical
# across every SQL destination that raises them (the tests assert these
# strings, so a per-file copy would silently drift).
MIRROR_UPSERT_KEY_MSG = (
    "sync.mode: mirror requires destination.upsert_key "
    "(needed to identify which rows to DELETE)."
)


def unsupported_tracked_scope_msg(dialect: str) -> str:
    """Message for ``mirror.strategy: tracked`` / ``mirror.scope`` on a
    destination that doesn't support them yet (#686/#687, extended per
    dialect by #692).

    Every SQL destination that implements ``sync.mode: mirror`` at all
    (Postgres, MySQL, Snowflake, ClickHouse, Databricks) now supports
    tracked/scope, so this only fires for a future dialect that adds mirror
    mode without tracked/scope support from day one."""
    return (
        f"mirror.strategy: tracked / mirror.scope are not yet supported on {dialect} "
        "(supported: postgres, mysql, snowflake, clickhouse, databricks)."
    )


def scope_not_subset_of_upsert_key_msg(scope: list[str], upsert_key: list[str] | None) -> str:
    return (
        "mirror.scope columns must be part of destination.upsert_key when combined "
        f"with strategy: tracked: {scope} not in upsert_key {upsert_key} (#694 derives "
        "scope values from the tracked key rather than storing them separately)."
    )


def check_scope_subset_of_upsert_key(config: Any, sync_options: Any) -> None:
    """``mirror.scope`` + ``strategy: tracked`` (#694) requires ``scope`` to be
    a subset of ``upsert_key`` — shared by every dialect implementing tracked
    mirror (Postgres/MySQL via ``BaseSqlDestination._validate_mirror_scope``,
    Snowflake via ``check_mirror_supported`` below), so the constraint and its
    wording can't drift between them.

    No-op unless both ``strategy: tracked`` and ``scope`` are set — the
    stateless destination-strategy scope (#687) has no state to derive from
    and isn't constrained by this.
    """
    if (
        sync_options.mode == "mirror"
        and sync_options.mirror is not None
        and sync_options.mirror.scope
        and sync_options.mirror.strategy == "tracked"
    ):
        extra = [c for c in sync_options.mirror.scope if c not in (config.upsert_key or [])]
        if extra:
            raise ValueError(
                scope_not_subset_of_upsert_key_msg(extra, config.upsert_key)
            )


def check_mirror_supported(
    config: Any, sync_options: Any, dialect: str, *, supports_tracked_scope: bool = False
) -> None:
    """Fail fast on a ``sync.mode: mirror`` config a SQL destination can't serve.

    - mirror requires an ``upsert_key`` (to know which rows to DELETE)
    - ``mirror.strategy: tracked`` / ``mirror.scope`` are opt-in per dialect
      (``supports_tracked_scope``) — reject them where unsupported rather
      than silently falling back to the (co-writer-unsafe) destination diff.
    - where supported, ``scope`` + ``strategy: tracked`` additionally requires
      ``scope ⊆ upsert_key`` (#694).

    No-op for non-mirror syncs. Callers holding an open connection should close
    it before re-raising (``try: check_mirror_supported(...) except ValueError:
    conn.close(); raise``).
    """
    if sync_options.mode != "mirror":
        return
    if not config.upsert_key:
        raise ValueError(MIRROR_UPSERT_KEY_MSG)
    if not supports_tracked_scope and sync_options.mirror is not None and (
        sync_options.mirror.strategy == "tracked" or sync_options.mirror.scope
    ):
        raise ValueError(unsupported_tracked_scope_msg(dialect))
    check_scope_subset_of_upsert_key(config, sync_options)
