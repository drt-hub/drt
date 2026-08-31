"""Shared base for SQL destinations.

Holds the *dialect-agnostic* orchestration shared by the concrete SQL
destinations: per-sync mutable state, the Layer-3 schema-cache resolution
(#317), ``sync.mode: mirror`` bookkeeping (#340 / #687), and the
``finalize_sync`` dispatch/connection template (#1030).

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
from contextlib import nullcontext
from typing import Any

from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.destinations.sql_utils import tagged_cursor as _tagged_cursor


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

    def supported_modes(self) -> frozenset[str]:
        """Declare the advanced sync modes implemented by the SQL template."""
        return frozenset({"replace", "mirror"})

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
        self._validate_mirror_scope(records, cfg, sync_options)

        conn = self._dialect_connect(config, getattr(sync_options, "_query_tags", None))
        result = SyncResult()
        cur = None

        try:
            cur = _tagged_cursor(conn.cursor(), sync_options)
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
            if cur is not None:
                cur.close()
            conn.close()

        return result

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """End-of-sync hook: swap-finalize for replace, DELETE-missing for mirror.

        - ``mode=mirror`` (#340): delegate to ``_finalize_mirror`` and reset
          mirror state so a re-run starts fresh.
        - ``mode=replace, replace_strategy=swap``: complete the staged replace
          through ``_complete_swap``. PostgreSQL/MySQL rename tables,
          Snowflake uses ``SWAP WITH``, and Databricks uses ``INSERT
          OVERWRITE``.

        The swap guard, shadow-name computation, connection/cursor lifecycle,
        and dispatch live here. ``_complete_swap`` and
        ``_reset_swap_state_after_completion`` jointly preserve the exact
        state-reset timing because recovery differs by dialect:
        PostgreSQL/MySQL/Databricks clear state after connection close even
        when completion raises; Snowflake keeps it after a failed ``SWAP`` but
        clears it before the post-swap cleanup ``DROP``.
        ``_reset_swap_state_after_noop`` preserves dialect-only state such as
        the first-run direct-write flag without adding that flag to dialects
        that do not use it.
        """
        if sync_options.mode == "mirror":
            result = self._finalize_mirror(config, sync_options)
            # Reset mirror state regardless of result so a re-run starts fresh.
            self._mirror_keys = None
            self._mirror_scopes = None
            return result

        if not self._swap_shadow_created or self._swap_table is None:
            self._reset_swap_state_after_noop()
            return None

        table = self._swap_table
        shadow = self._shadow_name(table)

        conn = self._dialect_connect(config, getattr(sync_options, "_query_tags", None))
        try:
            with self._swap_cursor_context(conn, sync_options) as cur:
                self._complete_swap(conn, cur, table, shadow)
        finally:
            conn.close()
            self._reset_swap_state_after_completion()

        return SyncResult()

    def _finalize_mirror(
        self,
        config: Any,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass (#340 / #687).

        Deletes destination rows whose ``upsert_key`` tuple is not in the set
        of keys observed across all batches. Memory-bound to the source key
        cardinality; for tables larger than a few million keys, the temp-table
        strategy (#340 follow-up) will be more appropriate.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` — treats
        "no batch with records was ever observed" as a signal to skip the
        DELETE entirely, so a transient empty source doesn't wipe the
        destination.

        Dialect-agnostic (#719 phase 2b): the whole DELETE statement — and
        with it the placeholder-expansion strategy, which is irreducibly
        dialect-specific (psycopg2 auto-expands a tuple against one ``%s``;
        pymysql needs an explicit ``%s`` list) — comes from the
        ``_build_mirror_delete`` hook.
        """
        if not self._mirror_keys:
            return None

        # mirror.strategy: tracked (#686) — state-based diff instead of the
        # destination-table diff below. Shares the empty-source guard above,
        # so a transient empty source also keeps the tracked baseline intact.
        if (
            sync_options.mirror is not None
            and sync_options.mirror.strategy == "tracked"
        ):
            return self._finalize_mirror_tracked(config, sync_options)

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        upsert_cols = config.upsert_key

        # mirror.scope (#687) — prepend "scope IN (observed)" so the diff
        # only touches rows under parents this run actually saw. Rows under
        # unobserved parents (other pipelines / the application) stay put.
        scope_cols = (
            sync_options.mirror.scope if sync_options.mirror is not None else None
        )
        # list(), not sorted() — scope values may include None (unorderable).
        scopes = list(self._mirror_scopes or set()) if scope_cols else None

        conn = self._dialect_connect(config, getattr(sync_options, "_query_tags", None))
        try:
            cur = _tagged_cursor(conn.cursor(), sync_options)
            stmt, params = self._build_mirror_delete(
                self._mirror_table_ident(config),
                upsert_cols,
                keys,
                scope_cols,
                scopes,
                negate=True,
            )
            cur.execute(stmt, params)
            self._commit_mirror(conn)
        finally:
            conn.close()

        # SyncResult has no dedicated `deleted` field; future work tracks
        # this separately. Returning a bare SyncResult signals "finalize
        # ran successfully" to the engine without inflating success/failed.
        return SyncResult()

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
        config: Any,
        sync_options: SyncOptions,
    ) -> None:
        """mirror.scope (#687): a scope column absent from the model output is a
        config error — fail fast before any row is written.

        ``scope`` + ``strategy: tracked`` (#694) has a second constraint,
        checked via the shared ``check_scope_subset_of_upsert_key`` (also
        used by Snowflake's ``check_mirror_supported``, #692): ``scope`` must
        be a subset of ``upsert_key``. Scope values for a tracked key are
        derived positionally from the already-persisted ``key_json`` (see
        ``_finalize_mirror_tracked``) rather than stored in a separate
        state-table column, so a scope column drt never observed as part of
        the tracked key has nothing to derive from.
        """
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
            from drt.destinations.sql_utils import check_scope_subset_of_upsert_key

            check_scope_subset_of_upsert_key(config, sync_options)

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
        conn = self._dialect_connect(config, None)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        finally:
            conn.close()

    # --- dialect hooks (subclasses implement) -----------------------------
    def _dialect_connect(
        self, config: Any, query_tags: dict[str, str] | None = None
    ) -> Any:
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

    def _shadow_name(self, table: str) -> str:
        """Return the per-sync shadow table name for ``table``.

        PostgreSQL must preserve the schema while suffixing only the relation;
        the other dialects suffix their fully-qualified string. The result is
        passed unchanged to ``_complete_swap``.
        """
        raise NotImplementedError

    def _swap_cursor_context(self, conn: Any, sync_options: SyncOptions) -> Any:
        """Return the context used for swap-completion cursor access.

        PostgreSQL/MySQL historically left finalization cursors to connection
        close, so the default wraps their tagged cursor in ``nullcontext``.
        Snowflake/Databricks override this hook to retain their drivers'
        cursor context-manager entry/exit behavior.
        """
        return nullcontext(_tagged_cursor(conn.cursor(), sync_options))

    def _complete_swap(
        self, conn: Any, cur: Any, table: str, shadow: str
    ) -> None:
        """Complete a staged replace and reset swap state at the dialect's
        recovery-safe point.

        PostgreSQL/MySQL rename ``table`` to a transient old name and
        ``shadow`` over it; Databricks overwrites ``table`` from ``shadow``;
        Snowflake exchanges the two table objects. Implementations own commit
        boundaries and cleanup DDL. Snowflake additionally resets state inside
        this hook, after ``SWAP WITH`` succeeds but before cleanup; the other
        dialects use ``_reset_swap_state_after_completion`` so they retain
        their unconditional-reset behavior.
        """
        raise NotImplementedError

    def _reset_swap_state(self) -> None:
        """Clear the common shadow-table state after swap completion.

        Dialects with additional per-sync swap state override this helper,
        call ``super()``, and clear their own fields. The completion/reset
        hooks call it at the recovery-safe point defined by that dialect.
        """
        self._swap_shadow_created = False
        self._swap_table = None

    def _reset_swap_state_after_completion(self) -> None:
        """Clear swap state after the shared template closes its connection.

        This default preserves PostgreSQL/MySQL/Databricks behavior: state is
        cleared whether completion SQL succeeds or raises, but not when
        connection close itself raises. Snowflake overrides this with a no-op
        because its ``_complete_swap`` resets only after a successful exchange.
        """
        self._reset_swap_state()

    def _reset_swap_state_after_noop(self) -> None:
        """Reset dialect-only state when the shared swap guard returns early.

        The base implementation deliberately leaves the common fields alone,
        matching PostgreSQL/MySQL's prior no-op behavior. Snowflake and
        Databricks override this to clear their first-run direct-write flag.
        """

    def _build_mirror_delete(
        self,
        table: str,
        upsert_cols: list[str],
        keys: list[tuple[Any, ...]],
        scope_cols: list[str] | None = None,
        scopes: list[tuple[Any, ...]] | None = None,
        negate: bool = True,
    ) -> tuple[Any, Any]:
        """Build the mirror ``DELETE`` for ``keys`` as an ``(stmt, params)``
        pair ready to hand straight to ``cursor.execute``.

        ``negate=True`` produces the destination-strategy "delete what the
        source no longer has" form (``NOT IN``); ``negate=False`` the
        tracked-strategy "delete exactly these keys" form (``IN``).
        ``scope_cols``/``scopes`` prepend a ``scope IN (...) AND`` restriction
        (#687).

        This is the placeholder-expansion seam and cannot be lifted: Postgres
        returns ``(psycopg2.sql.Composed, tuple)`` and leans on psycopg2
        expanding a tuple (or tuple-of-tuples) against a single ``%s``, while
        MySQL returns ``(str, list)`` with an explicitly built ``%s`` list and
        flattened params. Both shapes are accepted by ``cursor.execute``.
        """
        raise NotImplementedError

    def _mirror_table_ident(self, config: Any) -> str:
        """Target-table identifier handed to ``_build_mirror_delete``.

        Host-based destinations already carry their complete target name in
        ``config.table``. Cloud warehouses whose config splits catalog/schema
        from table override this hook to preserve their fully-qualified SQL.
        """
        return str(config.table)

    def _commit_mirror(self, conn: Any) -> None:
        """Commit mirror finalization for transactional dialects."""
        conn.commit()

    def _state_table_ident(self, config: Any) -> tuple[Any, Any, Any]:
        """Locate the drt-managed ``_drt_synced_keys`` state table for ``config``.

        Returns ``(ident, scope, raw)`` where ``ident`` is the identifier ready
        to embed in SQL (psycopg2 ``Composed`` on PG, backtick-quoted ``str`` on
        MySQL), ``scope`` is the namespace the existence probe filters on
        (PG schema / MySQL database, ``None`` when the target is unqualified),
        and ``raw`` is the unquoted qualified name the probe binds as a
        parameter. Dialect-specific.
        """
        raise NotImplementedError

    def _state_table_exists(self, cur: Any, scope: Any, raw: str) -> bool:
        """Pre-provisioning probe (#695): does the state table already exist?

        Postgres asks ``to_regclass``; MySQL counts ``information_schema.tables``
        (falling back to ``DATABASE()`` when ``scope`` is ``None``). Kept as a
        hook rather than inlined because a locked-down destination user with no
        CREATE privilege must never see the DDL at all.
        """
        raise NotImplementedError

    def _create_state_table(self, cur: Any, ident: Any) -> None:
        """Issue the ``CREATE TABLE IF NOT EXISTS`` for the state table.
        Body is identical across dialects; only the identifier type differs
        (``Composable`` vs ``str``), which is why this is a hook."""
        raise NotImplementedError

    def _state_scope_columns_exist(self, cur: Any, scope: Any, raw: str) -> bool:
        """Do the #890 ``scope_spec`` / ``scope_key`` columns exist yet?

        Separate probe from ``_state_table_exists`` because a state table
        created before #890 is perfectly valid and must keep working — it just
        cannot be filtered server-side until the columns are added. Dialect
        hook for the same reason the table probe is one: a locked-down user
        must never see DDL it has no privilege for.
        """
        raise NotImplementedError

    def _add_state_scope_columns(self, cur: Any, ident: Any) -> None:
        """``ALTER TABLE ... ADD COLUMN`` for the #890 scope columns.

        Both nullable and without a default, which is a metadata-only change on
        every engine drt targets — no rewrite of an existing state table
        however large. May legitimately fail (no ALTER privilege, the #695
        family); the caller treats that as "stay on the Python-only filter".
        """
        raise NotImplementedError

    def _state_sql(self, template: str, ident: Any) -> Any:
        """Bind ``ident`` into a single-``{}`` SQL ``template``, returning
        something ``cursor.execute`` accepts.

        This is the ``Composed``-vs-``str`` translation seam. The state-table
        SELECT / DELETE / INSERT statements are byte-identical across dialects
        *as text*, but Postgres must compose them through ``psycopg2.sql.SQL(
        ...).format(Composed)`` while MySQL can use plain ``str.format``. Only
        the binding step differs, so only the binding step is dialect code —
        the templates themselves live in ``_finalize_mirror_tracked``.
        """
        raise NotImplementedError

    def _state_params(self, *values: Any) -> Any:
        """Build execute parameters for shared tracked-state statements."""
        return tuple(values)

    def _try_add_state_scope_columns(self, cur: Any, ident: Any) -> bool:
        """Attempt the #890 scope-column migration without poisoning the tx."""
        cur.execute("SAVEPOINT drt_scope_cols")
        try:
            self._add_state_scope_columns(cur, ident)
        except Exception:  # noqa: BLE001 — no ALTER privilege is a supported state
            cur.execute("ROLLBACK TO SAVEPOINT drt_scope_cols")
            return False
        cur.execute("RELEASE SAVEPOINT drt_scope_cols")
        return True

    def _stage_mirror_keys(
        self,
        cur: Any,
        config: Any,
        rows: list[tuple[str, str]],
    ) -> tuple[bool, str]:
        """Stage current tracked keys, returning availability and table name."""
        from drt.destinations._mirror_state import DIFF_STAGING_TABLE

        cur.execute("SAVEPOINT drt_diff_keys")
        try:
            cur.execute(
                f"CREATE TEMPORARY TABLE {DIFF_STAGING_TABLE} "
                "(key_hash VARCHAR(64), key_json TEXT)"
            )
            if rows:
                cur.executemany(
                    f"INSERT INTO {DIFF_STAGING_TABLE} "
                    "(key_hash, key_json) VALUES (%s, %s)",
                    rows,
                )
        except Exception:  # noqa: BLE001 — no temporary-table privilege is supported
            cur.execute("ROLLBACK TO SAVEPOINT drt_diff_keys")
            return False, DIFF_STAGING_TABLE
        cur.execute("RELEASE SAVEPOINT drt_diff_keys")
        return True, DIFF_STAGING_TABLE

    def reset_tracked_state(self, config: Any, sync_name: str) -> int:
        """Clear one sync's rows from ``_drt_synced_keys`` (#776).

        The destination-side half of ``drt state reset --tracked-mirror``.
        Returns the number of rows removed so the CLI can say "nothing to
        reset" rather than implying it cleared something.

        This is the most dangerous of the three reset levels and the only one
        that writes to the destination, so three properties are pinned by
        tests rather than left to review:

        * **Scoped to one sync.** ``sync_name`` is part of the state table's
          primary key, and it is *bound*, never interpolated.
        * **Never touches the target table.** The only DELETE issued names
          ``_drt_synced_keys``. Deleting user data is explicitly out of scope
          for #776 ("destination data deletion: never").
        * **No DDL.** A sync that never ran tracked mirror has no state table,
          and reset must not create one just to empty it — that would also
          fail for the locked-down, no-CREATE-privilege user #695 supports.

        What the *next* run does after this is the part worth understanding:
        it re-baselines. Keys are recorded, nothing is deleted, and a warning
        is emitted — identical to first-run/lost-state semantics. Rows the
        application wrote therefore become part of drt's tracked set, and so
        become deletion candidates on subsequent passes. That is why this is
        opt-in per level and never folded into ``--full-refresh``.
        """
        from drt.destinations._mirror_state import STATE_TABLE  # noqa: F401

        state_ident, state_scope, state_raw = self._state_table_ident(config)

        conn = self._dialect_connect(config, None)
        try:
            cur = conn.cursor()
            if not self._state_table_exists(cur, state_scope, state_raw):
                return 0  # never ran tracked mirror — nothing to clear
            cur.execute(
                self._state_sql("DELETE FROM {} WHERE sync_name = %s", state_ident),
                self._state_params(sync_name),
            )
            removed = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            self._commit_mirror(conn)
            return int(removed)
        finally:
            conn.close()

    def _finalize_mirror_tracked(
        self, config: Any, sync_options: SyncOptions
    ) -> SyncResult | None:
        """``mirror.strategy: tracked`` (#686) — delete only rows drt synced.

        Reads the previously-synced key set for this sync from the drt-managed
        ``_drt_synced_keys`` table (created lazily next to the target table),
        deletes ``previous - current`` from the target, and rewrites the state
        to the current key set. Transactional dialects commit the target delete
        and state rewrite together; Snowflake preserves its existing autocommit
        behavior through ``_commit_mirror``.

        First run (or lost state) baselines: record keys, delete nothing, WARN
        — matching Census semantics ("the first sync will be an upsert for all
        records; the second and following account for deletions"). Rows the
        application wrote are never candidates for deletion because they were
        never in the tracked set.

        Dialect-agnostic (#719 phase 2b / #720 phase 3). The core dialect seams
        are the state identifier (``_state_table_ident``), the pre-provisioning
        probe (``_state_table_exists``), the DDL (``_create_state_table``), and the
        ``Composed``/``str`` binding of the state statements (``_state_sql``).
        The target DELETE reuses ``_build_mirror_delete`` in its
        ``negate=False`` ("delete exactly these keys") form.

        ``mirror.scope`` + ``strategy: tracked`` (#694 part 1): prunes both
        the state *read* (the diff only ever considers previously-tracked
        keys whose scope is one this run actually observed) and the state
        *rewrite* (a scope this run never touched keeps its previously
        tracked keys untouched). Scope values come from ``key_json``
        positionally — validated as a subset of ``upsert_key`` by
        ``_validate_mirror_scope`` — so no new state-table column and no
        migration story for tables ``CREATE TABLE IF NOT EXISTS`` already
        created before #694.

        SQL-side diff (#694 part 2): the v0.7.10 implementation SELECTed
        every tracked key for this sync into Python and diffed it against
        ``current`` with a Python set — fine at hundreds of keys, but it
        means a state table with millions of rows gets read into memory in
        full just to compute a diff that's typically tiny (most runs only
        change a handful of rows). This version stages ``current`` into a
        scratch table and lets the database compute ``previous - current``
        via ``NOT EXISTS`` — only the (small) diff ever reaches Python **for
        unscoped tracked mirror**. The old "read every untouched row so it
        can be reinserted unchanged" step for the scope-preserved rows is
        gone too: since untouched rows are simply never selected as part of
        the diff, not touching them at all has the same effect as reading
        and reinserting them unchanged, without ever having to.

        Caveat for ``mirror.scope`` + ``strategy: tracked``: the diff query
        below filters only by ``sync_name``, not by scope (scope isn't a
        separate state-table column — see above), so when ``current``
        covers one scope out of many historically-tracked ones, the SQL
        diff's result is close to the *entire* previous state rather than a
        small diff. The final ``to_delete`` set is still correct (scope
        membership and current-run membership are independent, so filtering
        the diff by scope in Python afterward is equivalent to filtering
        the full previous set by scope first) — this is a missed
        performance win for the scoped case, not a correctness bug. Tracked
        as #890.
        """
        import logging

        from drt.destinations._mirror_state import (
            SCOPE_BACKFILL_PER_RUN,
            STATE_TABLE,
            decode_key,
            key_hash,
            key_json,
            scope_key_json,
            scope_spec_json,
        )

        sync_name = sync_options._sync_name or config.table
        current = list({tuple(k) for k in self._mirror_keys or []})
        upsert_cols = config.upsert_key
        state_ident, state_scope, state_raw = self._state_table_ident(config)

        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        scope_positions = [upsert_cols.index(c) for c in scope_cols] if scope_cols else None
        observed_scopes = set(self._mirror_scopes or set()) if scope_positions else None

        conn = self._dialect_connect(config, getattr(sync_options, "_query_tags", None))
        try:
            cur = _tagged_cursor(conn.cursor(), sync_options)
            # Pre-provisioning (#695): check existence before issuing DDL so a
            # locked-down destination user (no CREATE privilege) can run against
            # a state table an admin created ahead of time. Only CREATE when the
            # table is genuinely absent — the IF NOT EXISTS guard stays for the
            # concurrent-first-run race.
            fresh_table = not self._state_table_exists(cur, state_scope, state_raw)
            if fresh_table:
                self._create_state_table(cur, state_ident)

            # #890: the scope columns let the diff be filtered server-side.
            # Only ever probed for a scoped run — an unscoped sync has nothing
            # to gain and must not pay a probe, let alone DDL, for it.
            scope_spec = scope_spec_json(list(scope_cols)) if scope_cols else None
            scope_sql = False
            if scope_positions is not None:
                if fresh_table:
                    scope_sql = True  # created with the columns
                elif self._state_scope_columns_exist(cur, state_scope, state_raw):
                    scope_sql = True
                else:
                    # A state table from before #890. Adding the columns is
                    # metadata-only, but the privilege to do it is not
                    # guaranteed (#695) — so the dialect hook handles the
                    # refusal and simply leaves the run on the Python-only
                    # filter, permanently and without an error. Transactional
                    # dialects use a savepoint; Snowflake autocommits.
                    if self._try_add_state_scope_columns(cur, state_ident):
                        scope_sql = True

            # Baseline check (#694 part 2): a cheap existence probe, never a
            # full read — the only thing this needs to know is "has this sync
            # ever tracked anything at all", to tell a genuine first run
            # (WARN) apart from a run that's simply the first to touch this
            # particular scope (silent — see below).
            cur.execute(
                self._state_sql(
                    "SELECT 1 FROM {} WHERE sync_name = %s LIMIT 1", state_ident
                ),
                self._state_params(sync_name),
            )
            previous_exists = cur.fetchone() is not None

            # `TEMPORARY` (not `TEMP`) is the one spelling both Postgres and
            # MySQL accept — session-scoped on both, so no manual DROP is
            # strictly required, but one is issued anyway for clarity and to
            # keep the connection reusable if pooling is ever introduced.
            # Unqualified/unquoted: DIFF_STAGING_TABLE is a fixed constant,
            # never user-configured, so it needs no Composable-safe quoting
            # the way `state_ident` (schema-qualified from `config.table`)
            # does.
            existing_key_hashes: set[str] = set()
            staging_available, diff_table = self._stage_mirror_keys(
                cur,
                config,
                [(key_hash(k), key_json(k)) for k in current],
            )

            # #890: narrow `previous` to the observed scopes *in SQL* when the
            # columns are available. Three branches, and the first two are what
            # keep this a purely *coarse* filter — every row they let through is
            # re-checked exactly by the Python filter below:
            #   scope_key IS NULL   → written before the columns existed
            #   scope_spec <> ...   → written under a different `mirror.scope`,
            #                         so its frozen scope_key means nothing here
            # Without the second branch, editing `mirror.scope` would strand
            # every previously written row: no observed scope matches its stale
            # scope_key, so it drops out of the diff and stops being a deletion
            # candidate silently and forever.
            if staging_available:
                projection = (
                    "s.key_hash, s.key_json, s.scope_key" if scope_sql else "s.key_hash, s.key_json"
                )
                diff_sql = (
                    f"SELECT {projection} FROM {{}} s WHERE s.sync_name = %s "
                    f"AND NOT EXISTS (SELECT 1 FROM {diff_table} c "
                    "WHERE c.key_hash = s.key_hash)"
                )
                diff_params = self._state_params(sync_name)
                if scope_sql and observed_scopes:
                    observed_json = sorted(key_json(sc) for sc in observed_scopes)
                    placeholders = ", ".join(["%s"] * len(observed_json))
                    diff_sql += (
                        " AND (s.scope_key IS NULL OR s.scope_spec <> %s "
                        f"OR s.scope_key IN ({placeholders}))"
                    )
                    diff_params = self._state_params(
                        sync_name, scope_spec, *observed_json
                    )
                cur.execute(self._state_sql(diff_sql, state_ident), diff_params)
                fetched = cur.fetchall()
            else:
                projection = "key_hash, key_json, scope_key" if scope_sql else "key_hash, key_json"
                cur.execute(
                    self._state_sql(
                        f"SELECT {projection} FROM {{}} WHERE sync_name = %s",
                        state_ident,
                    ),
                    self._state_params(sync_name),
                )
                previous = cur.fetchall()
                existing_key_hashes = {row[0] for row in previous}
                current_key_hashes = {key_hash(k) for k in current}
                fetched = [row for row in previous if row[0] not in current_key_hashes]
            stale_scope = [(h, kj) for h, kj, sk in fetched if sk is None] if scope_sql else []
            raw_diff = [row[:2] for row in fetched]

            # Scope-filtering the (small) diff after the SQL-side subtraction
            # is equivalent to filtering `previous` by scope *before*
            # diffing (the #694 part 1 approach): scope membership doesn't
            # depend on `current`, and `current`-membership doesn't depend on
            # scope, so `(previous ∩ scope) - current == (previous - current)
            # ∩ scope`. This is what lets the scope check stay in Python
            # without ever materialising `previous` — it now only ever sees
            # the rows that are actually about to be deleted.
            if scope_positions is not None and observed_scopes is not None:
                to_delete = [
                    decode_key(kj)
                    for _h, kj in raw_diff
                    if tuple(decode_key(kj)[p] for p in scope_positions) in observed_scopes
                ]
            else:
                to_delete = [decode_key(kj) for _h, kj in raw_diff]

            # #890 backfill. Rows tracked before the scope columns existed are
            # never rewritten by the state pass — #694 part 2 deliberately
            # leaves an already-tracked row alone, which is what makes the diff
            # cheap — so without this they would keep their NULL scope forever
            # and keep falling through to the Python filter. On an upgraded
            # state table that means the optimisation never engages at all.
            #
            # The rows are already here: fetched, decoded, and their scope
            # already computed for the filter above. Healing them costs one
            # UPDATE. Capped per run because the expand/contract guidance is to
            # backfill in batches rather than in one pass inside the hot path,
            # and a sync run is the hot path — the table converges over a few
            # runs instead of one run paying for the whole history.
            #
            # Rows about to be deleted are skipped: writing a scope onto a row
            # that goes away two statements later is pure waste.
            if stale_scope and scope_positions is not None:
                doomed = {key_hash(k) for k in to_delete}
                heal = [(h, kj) for h, kj in stale_scope if h not in doomed][
                    :SCOPE_BACKFILL_PER_RUN
                ]
                if heal:
                    cur.executemany(
                        self._state_sql(
                            "UPDATE {} SET scope_spec = %s, scope_key = %s "
                            "WHERE sync_name = %s AND key_hash = %s",
                            state_ident,
                        ),
                        [
                            (
                                scope_spec,
                                scope_key_json(decode_key(kj), scope_positions),
                                sync_name,
                                h,
                            )
                            for h, kj in heal
                        ],
                    )

            if to_delete:
                stmt, params = self._build_mirror_delete(
                    self._mirror_table_ident(config),
                    upsert_cols,
                    to_delete,
                    None,
                    None,
                    negate=False,
                )
                cur.execute(stmt, params)
            elif not previous_exists:
                # No prior state at all for this sync (never run, or lost
                # state) — baseline. Prior state existing for *other* scopes
                # only (previous_exists True, to_delete empty) is not a
                # baseline situation — it's simply the first run to touch
                # this particular scope, and needs no warning.
                logging.getLogger(__name__).warning(
                    "tracked mirror: no prior state for sync %r in %s — "
                    "baselining this run's %d key(s); no deletes this run.",
                    sync_name,
                    STATE_TABLE,
                    len(current),
                )

            # Rewrite this sync's state: remove exactly the stale rows just
            # identified, add exactly the current keys that weren't already
            # tracked. A row whose key_hash is already present never needs
            # touching — key_hash is a pure function of key_json, so a
            # matching hash guarantees the stored key_json is already
            # correct. Untouched rows (other scopes, other syncs' keys under
            # the same sync_name — there are none, but also rows under
            # scopes this run never observed) are simply never selected by
            # either step, which is the #694 part 2 replacement for
            # `preserved`: not reading and not touching a row is equivalent
            # to reading it and reinserting it unchanged.
            if to_delete:
                cur.executemany(
                    self._state_sql(
                        "DELETE FROM {} WHERE sync_name = %s AND key_hash = %s",
                        state_ident,
                    ),
                    [(sync_name, key_hash(k)) for k in to_delete],
                )
            if staging_available:
                cur.execute(
                    self._state_sql(
                        "SELECT c.key_hash, c.key_json FROM "
                        f"{diff_table} c WHERE NOT EXISTS "
                        "(SELECT 1 FROM {} s WHERE s.sync_name = %s "
                        "AND s.key_hash = c.key_hash)",
                        state_ident,
                    ),
                    self._state_params(sync_name),
                )
                to_insert = cur.fetchall()
            else:
                to_insert = [
                    (key_hash(k), key_json(k))
                    for k in current
                    if key_hash(k) not in existing_key_hashes
                ]
            if to_insert and scope_sql and scope_positions is not None:
                cur.executemany(
                    self._state_sql(
                        "INSERT INTO {} (sync_name, key_hash, key_json, scope_spec, scope_key) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        state_ident,
                    ),
                    [
                        (
                            sync_name,
                            h,
                            kj,
                            scope_spec,
                            scope_key_json(decode_key(kj), scope_positions),
                        )
                        for h, kj in to_insert
                    ],
                )
            elif to_insert:
                # Unscoped sync, or scope columns unavailable — the columns stay
                # NULL, which the predicate above always lets through.
                cur.executemany(
                    self._state_sql(
                        "INSERT INTO {} (sync_name, key_hash, key_json) "
                        "VALUES (%s, %s, %s)",
                        state_ident,
                    ),
                    [(sync_name, h, kj) for h, kj in to_insert],
                )
            if staging_available:
                cur.execute(f"DROP TABLE {diff_table}")
            self._commit_mirror(conn)
        finally:
            conn.close()

        return SyncResult()
