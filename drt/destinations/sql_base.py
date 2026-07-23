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

import json
from typing import Any

from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


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

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Dialect-agnostic write template (#719 phase 2a).

        Validates mirror.scope, opens a dialect connection, dispatches to the
        replace-swap / replace / upsert write path, and — for ``mode: mirror``
        — accumulates the observed ``upsert_key`` (and scope) tuples for the
        ``finalize_sync`` DELETE. The concrete SQL construction lives in the
        subclass ``_load_replace_swap`` / ``_load_replace`` / ``_load_upsert``
        hooks, which each narrow the config type internally.
        """
        if not records:
            return SyncResult()

        # ``config`` is the ``DestinationConfig`` union (the Protocol signature);
        # the fields read below (``table``) live on the concrete SQL subclass
        # configs, so narrow via ``Any`` — the same convention the other base
        # helpers use. The ``_dialect_connect`` / ``_load_*`` hooks each assert
        # the concrete config type internally.
        cfg: Any = config
        self._validate_mirror_scope(records, sync_options)

        conn = self._dialect_connect(config)
        result = SyncResult()

        try:
            cur = conn.cursor()
            columns = list(records[0].keys())

            if sync_options.mode == "replace":
                if sync_options.replace_strategy == "swap":
                    result = self._load_replace_swap(
                        conn,
                        cur,
                        records,
                        columns,
                        cfg.table,
                        sync_options,
                        config,
                    )
                else:
                    result = self._load_replace(
                        conn,
                        cur,
                        records,
                        columns,
                        cfg.table,
                        sync_options,
                        config,
                    )
            else:
                result = self._load_upsert(
                    conn,
                    cur,
                    records,
                    columns,
                    config,
                    sync_options,
                )
                # sync.mode: mirror (#340 / #687) — record the observed
                # upsert_key (and scope) tuples for the finalize_sync DELETE.
                if sync_options.mode == "mirror":
                    self._accumulate_mirror_state(records, result, config, sync_options)
        finally:
            conn.close()

        return result

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

    def _record_row_error(
        self, result: SyncResult, i: int, record: dict[str, Any], exc: Exception
    ) -> None:
        """Append the standard per-row ``RowError``. This is the failure-recording
        block that was byte-identical across every SQL ``_load_*`` path (#722 seam).
        Callers keep their own success-count and error-recovery logic — only the
        ``result.failed += 1`` + ``row_errors.append(RowError(...))`` pair moved here.
        """
        result.failed += 1
        result.row_errors.append(
            RowError(
                batch_index=i,
                record_preview=json.dumps(record, default=str)[:200],
                http_status=None,
                error_message=str(exc),
            )
        )

    def test_connection(self, config: Any) -> None:
        """Connectivity check: open a connection and run ``SELECT 1``.

        Dialect-agnostic — the connection comes from the ``_dialect_connect``
        hook, which each subclass narrows the config type inside.
        """
        conn = self._dialect_connect(config)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        finally:
            conn.close()

    # --- dialect hooks (subclasses implement) -----------------------------
    def _dialect_connect(self, config: Any) -> Any:
        """Return a live DB connection (psycopg2 / pymysql) for this config."""
        raise NotImplementedError

    def _qualify_ident(self, name: str) -> Any:
        """Quote/qualify an identifier. Returns a psycopg2 Composable (PG)
        or a backtick-quoted str (MySQL) — both accepted by cursor.execute."""
        raise NotImplementedError

    def _load_replace_swap(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
        config: Any,
    ) -> SyncResult:
        """Zero-downtime replace: build a shadow table this batch; the atomic
        rename happens in ``finalize_sync``. Dialect-specific SQL."""
        raise NotImplementedError

    def _load_replace(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
        config: Any,
    ) -> SyncResult:
        """TRUNCATE-then-INSERT replace within a single transaction.
        Dialect-specific SQL."""
        raise NotImplementedError

    def _load_upsert(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: Any,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Idempotent upsert (INSERT ... ON CONFLICT / ON DUPLICATE KEY).
        Dialect-specific SQL."""
        raise NotImplementedError
