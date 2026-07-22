"""Shared base for host-based SQL destinations (Postgres, MySQL).

Holds the *dialect-agnostic* orchestration that was duplicated verbatim across
the concrete SQL destinations: per-sync mutable state, the Layer-3 schema-cache
resolution (#317), and the ``sync.mode: mirror`` bookkeeping (#340 / #687).

Dialect-specific SQL construction — identifier quoting, INSERT / UPSERT / MERGE
builders, ``_connect`` — stays on the subclasses, because Postgres composes
``psycopg2.sql`` objects while MySQL builds backtick-quoted strings. Unifying
that layer is tracked separately; this base only lifts the parts that are
byte-identical and carry no dialect.

``config`` is typed ``Any`` in the helpers below on purpose: the fields they
read (``table`` / ``upsert_key`` / ``json_columns``) live on the concrete
subclass configs, not on ``BaseSqlDestinationConfig``, and each caller has
already narrowed the type via ``assert isinstance(config, ...)`` at ``load``
entry.
"""

from __future__ import annotations

from typing import Any

from drt.config.models import SyncOptions
from drt.destinations.base import SyncResult


class BaseSqlDestination:
    """Dialect-agnostic state + mirror/schema helpers for SQL destinations."""

    def __init__(self) -> None:
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None
        # sync.mode: mirror (#340) — accumulates upsert_key tuples seen across
        # batches so finalize_sync can DELETE missing rows. ``None`` means
        # mirror mode hasn't engaged yet (no batch with records); finalize_sync
        # treats that as "skip DELETE" — safety against deleting everything
        # when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None
        # mirror.scope (#687) — distinct scope-column value tuples observed
        # across batches; the destination-strategy DELETE is restricted to
        # rows whose scope values are in this set.
        self._mirror_scopes: set[tuple[Any, ...]] | None = None
        # Layer 3 (#317): INFORMATION_SCHEMA map, fetched once per table per
        # sync. ``None`` value = introspection ran but is unavailable; the key
        # being absent = not yet fetched.
        self._schema_cache: dict[str, dict[str, str] | None] = {}

    def _resolve_schema(self, config: Any) -> dict[str, str] | None:
        """Column → type-category map for the target table, cached per sync.

        Returns ``None`` (Layer 3 inactive) when ``introspect_schema`` is off,
        ``json_columns`` is set (Layer 2 wins), or introspection isn't
        available.
        """
        if not config.introspect_schema or config.json_columns is not None:
            return None
        if config.table not in self._schema_cache:
            from drt.destinations.schema import describe_columns

            self._schema_cache[config.table] = describe_columns(config)
        return self._schema_cache[config.table]

    def _validate_mirror_scope(
        self,
        records: list[dict[str, Any]],
        sync_options: SyncOptions,
    ) -> None:
        """mirror.scope (#687): a scope column absent from the model output is a
        config error — fail fast before any row is written."""
        if (
            sync_options.mode == "mirror"
            and sync_options.mirror is not None
            and sync_options.mirror.scope
        ):
            missing = [c for c in sync_options.mirror.scope if c not in records[0]]
            if missing:
                raise ValueError(
                    "mirror.scope columns missing from the model output: "
                    f"{missing} (available: {sorted(records[0].keys())})"
                )

    def _accumulate_mirror_state(
        self,
        records: list[dict[str, Any]],
        result: SyncResult,
        config: Any,
        sync_options: SyncOptions,
    ) -> None:
        """sync.mode: mirror (#340) — accumulate the ``upsert_key`` tuples (and,
        for mirror.scope #687, the scope-value tuples) of successfully-loaded
        records, so ``finalize_sync`` can DELETE the rows the source no longer
        produces. Failed records don't count as observed source state.
        """
        if not config.upsert_key:
            from drt.destinations.sql_utils import MIRROR_UPSERT_KEY_MSG

            raise ValueError(MIRROR_UPSERT_KEY_MSG)
        if self._mirror_keys is None:
            self._mirror_keys = []
        failed_indices = {err.batch_index for err in result.row_errors}
        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        if scope_cols and self._mirror_scopes is None:
            self._mirror_scopes = set()
        for idx, record in enumerate(records):
            if idx in failed_indices:
                continue
            self._mirror_keys.append(tuple(record.get(k) for k in config.upsert_key))
            if scope_cols:
                assert self._mirror_scopes is not None
                self._mirror_scopes.add(tuple(record.get(c) for c in scope_cols))
