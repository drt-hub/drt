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
    destination that doesn't support them yet (Postgres/MySQL only, #686)."""
    return (
        f"mirror.strategy: tracked / mirror.scope are not yet supported on {dialect} "
        "(supported: postgres, mysql — see #686 follow-ups)."
    )


def check_mirror_supported(config: Any, sync_options: Any, dialect: str) -> None:
    """Fail fast on a ``sync.mode: mirror`` config a SQL destination can't serve.

    - mirror requires an ``upsert_key`` (to know which rows to DELETE)
    - ``mirror.strategy: tracked`` / ``mirror.scope`` are Postgres/MySQL-only, so
      reject them on ``dialect`` rather than silently falling back to the
      (co-writer-unsafe) destination diff.

    No-op for non-mirror syncs. Callers holding an open connection should close
    it before re-raising (``try: check_mirror_supported(...) except ValueError:
    conn.close(); raise``).
    """
    if sync_options.mode != "mirror":
        return
    if not config.upsert_key:
        raise ValueError(MIRROR_UPSERT_KEY_MSG)
    if sync_options.mirror is not None and (
        sync_options.mirror.strategy == "tracked" or sync_options.mirror.scope
    ):
        raise ValueError(unsupported_tracked_scope_msg(dialect))


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
