"""Shared utilities for SQL destinations.

Identifier quoting, row-count capability discovery, mirror-mode guard
messages, and query tagging — factored out so the SQL destinations don't
each hand-roll them.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from drt.config.models import DestinationConfig, SyncOptions
from drt.config.query_tags import render_comment_header


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

    def get_row_count(self, config: Any) -> int:
        """Return the destination table's current row count.

        Raises:
            Exception: connection or query failure. Not caught by
                ``get_row_count_for_destination``; the caller of that
                helper is expected to catch it rather than let it abort
                the wider operation.
        """
        ...


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


class TaggedCursor:
    """Cursor wrapper that prepends the query-tagging comment header (#768)
    to every statement — used by every SQL destination that has no
    warehouse-native tagging mechanism of its own (Postgres, MySQL,
    ClickHouse) or that needs the comment in addition to its native
    mechanism (Snowflake, Databricks — session-level tags don't retroactively
    label queries issued before they were set).

    Wrapping once, where a dialect obtains its cursor, tags every subsequent
    ``execute()`` on it without touching each individual call site.

    Handles both plain strings (every dialect here except Postgres's DDL) and
    ``psycopg2.sql.Composable`` objects (``+`` against a plain string raises
    ``TypeError``).

    Explicitly implements ``__enter__``/``__exit__`` rather than relying on
    ``__getattr__`` to find them on the wrapped cursor: Python looks up
    dunder methods used by implicit protocols (``with``, ``len()``, ...) on
    the *type*, bypassing instance-level ``__getattr__`` entirely. Snowflake's
    destination uses ``with conn.cursor() as cur:``; without this override
    that would raise ``TypeError: ... does not support the context manager
    protocol`` on a wrapped cursor.
    """

    def __init__(self, cursor: Any, comment: str) -> None:
        self._cursor = cursor
        self._comment = comment

    def __enter__(self) -> TaggedCursor:
        # DB-API cursors conventionally return ``self`` from ``__enter__``,
        # but nothing guarantees it — honour whatever comes back rather than
        # assuming identity, same as any other context-manager consumer would.
        self._cursor = self._cursor.__enter__()
        return self

    def __exit__(self, *exc_info: Any) -> Any:
        return self._cursor.__exit__(*exc_info)

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(query, str):
            tagged = f"{self._comment}\n{query}"
        else:
            from psycopg2 import sql as _pgsql

            tagged = _pgsql.SQL(f"{self._comment}\n") + query
        return self._cursor.execute(tagged, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


def tagged_cursor(cur: Any, sync_options: SyncOptions) -> Any:
    """Wrap ``cur`` with :class:`TaggedCursor` when query tags are present;
    returns ``cur`` unchanged (no wrapper overhead) when tagging is off.

    ``getattr`` rather than direct attribute access: plenty of existing
    white-box tests build a bare ``SimpleNamespace(mode=..., mirror=...)``
    in place of a real ``SyncOptions`` (they only need the fields their own
    dialect hook reads), and ``_query_tags`` is a private attr those fakes
    were never told about. Treating it as optional keeps this backward
    compatible with every fake already in the test suite rather than
    requiring each one to grow a field it doesn't otherwise care about.
    """
    tags = getattr(sync_options, "_query_tags", None)
    if not tags:
        return cur
    return TaggedCursor(cur, render_comment_header(tags))


def tag_query(query: str, sync_options: SyncOptions) -> str:
    """Prepend the query-tagging comment header (#768) to a plain SQL string.

    For destinations with no cursor ``execute()`` to wrap wholesale —
    ClickHouse's ``client.command()`` / ``client.query()`` take a string
    directly, so there's no single seam to intercept the way there is for
    ``BaseSqlDestination``'s shared ``cur = conn.cursor()``. Returns ``query``
    unchanged when tagging is off. ``getattr`` for the same reason as
    :func:`tagged_cursor` above.
    """
    tags = getattr(sync_options, "_query_tags", None)
    if not tags:
        return query
    return f"{render_comment_header(tags)}\n{query}"
