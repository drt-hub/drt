"""ClickHouse destination — insert rows into a ClickHouse table.

Uses clickhouse-connect for HTTP-based batch inserts. An ambiguously failed
batch is retried once with the same ClickHouse deduplication token before it
is replayed record by record for row-level error tracking.

Application-level deduplication is handled by ClickHouse's ReplacingMergeTree
engine at merge time. The insert token is a best-effort mitigation for the
retry specifically, **not a guarantee**: ``insert_deduplication_token`` only
takes effect if the target table's insert-deduplication window is actually
enabled server-side — on by default for ``Replicated*MergeTree`` tables
(``replicated_deduplication_window``), but **off by default** for a plain,
non-replicated ``MergeTree``/``ReplacingMergeTree`` table unless the operator
has explicitly set ``non_replicated_deduplication_window`` > 0. drt does not
verify or configure this setting — doing so would need an extra round trip
and table-level assumptions out of scope for a batching perf fix. On a table
without an enabled dedup window, an ambiguous HTTP failure (the request
committed server-side but the response was lost) can still result in a
duplicated batch on retry, or duplicated rows if it takes two ambiguous
failures to reach the row-by-row fallback. Operators who need a hard
guarantee against this should enable the appropriate deduplication window on
their table.

Supports ``sync.mode: replace`` (TRUNCATE TABLE → INSERT) and
``replace_strategy: swap`` (zero-downtime: build a shadow table via
``CREATE TABLE ... AS ...``, INSERT into the shadow, then atomically
``EXCHANGE TABLES`` in :meth:`finalize_sync`).

Also supports ``sync.mode: mirror`` (#340 Step 3): INSERT every source
row, then in :meth:`finalize_sync` issue a single ``ALTER TABLE ...
DELETE WHERE <upsert_key> NOT IN (<observed>)`` mutation that removes
destination rows whose key was not in the source. The mutation runs
with ``mutations_sync=1`` so it completes before the call returns.
Mutations rewrite affected parts and are expensive — mirror mode is
appropriate for small/medium reference tables, not for high-volume
fact tables.

Requires: pip install drt-core[clickhouse]

Example sync YAML:

    destination:
      type: clickhouse
      host_env: TARGET_CH_HOST
      database_env: TARGET_CH_DATABASE
      user_env: TARGET_CH_USER
      password_env: TARGET_CH_PASSWORD
      table: analytics_scores
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import ClickHouseDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.destinations.sql_utils import tag_query


class ClickHouseDestination:
    """Insert records into a ClickHouse table.

    Implements ConnectionTestable via test_connection().
    """

    def __init__(self) -> None:
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None
        # sync.mode: mirror (#340 Step 3) — accumulates upsert_key tuples seen
        # across batches so finalize_sync can DELETE missing rows.
        # ``None`` means mirror mode hasn't engaged yet (no batch with
        # records); finalize_sync treats that as "skip DELETE" — safety
        # against deleting everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None
        # mirror.scope (#692, mirroring #687) — distinct scope-column value
        # tuples observed across batches; the finalize DELETE (destination or
        # tracked strategy) is restricted to rows whose scope values are in
        # this set.
        self._mirror_scopes: set[tuple[Any, ...]] | None = None

    def supported_modes(self) -> frozenset[str]:
        """Declare the advanced sync modes implemented by ClickHouse."""
        return frozenset({"replace", "mirror"})

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, ClickHouseDestinationConfig)
        if not records:
            return SyncResult()

        client = self._connect(config)
        result = SyncResult()

        try:
            columns = list(records[0].keys())

            if sync_options.mode == "replace" and sync_options.replace_strategy == "swap":
                result = self._load_replace_swap(
                    client,
                    records,
                    columns,
                    config.table,
                    sync_options,
                )
            else:
                if sync_options.mode == "replace" and not self._replace_truncated:
                    client.command(
                        tag_query(f"TRUNCATE TABLE {self._quote_ident(config.table)}", sync_options)
                    )
                    self._replace_truncated = True

                # sync.mode: mirror (#340 Step 3) — validate upsert_key
                # before any INSERT so a misconfigured sync fails fast
                # rather than after partially populating the table.
                # Reject an unserveable mirror config (missing upsert_key, or
                # a scope+tracked composition where scope isn't a subset of
                # upsert_key, #694) before any INSERT. tracked/scope
                # themselves are supported on ClickHouse since #692.
                from drt.destinations.sql_utils import check_mirror_supported

                check_mirror_supported(
                    config, sync_options, "clickhouse", supports_tracked_scope=True
                )
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

                # clickhouse-connect's client.insert(table=...) interpolates
                # the table raw into "INSERT INTO {table} ..." with no quoting
                # (see clickhouse_connect/driver/insert.py), so pre-quote here.
                table_q = self._quote_ident(config.table)

                if not self._insert_batched(
                    client,
                    table_q,
                    records,
                    columns,
                    sync_options,
                    result,
                ):
                    return result

                # sync.mode: mirror (#340 Step 3) — accumulate upsert_key
                # tuples for the finalize_sync DELETE pass. Only keys from
                # successfully-loaded records are tracked (failed records
                # don't count as "source state").
                if sync_options.mode == "mirror":
                    assert config.upsert_key  # guarded above
                    if self._mirror_keys is None:
                        self._mirror_keys = []
                    scope_cols = (
                        sync_options.mirror.scope if sync_options.mirror is not None else None
                    )
                    if scope_cols and self._mirror_scopes is None:
                        self._mirror_scopes = set()
                    failed_indices = {re.batch_index for re in result.row_errors}
                    for idx, record in enumerate(records):
                        if idx in failed_indices:
                            continue
                        self._mirror_keys.append(tuple(record.get(k) for k in config.upsert_key))
                        if scope_cols:
                            assert self._mirror_scopes is not None
                            self._mirror_scopes.add(tuple(record.get(c) for c in scope_cols))
        finally:
            client.close()

        return result

    def _insert_batched(
        self,
        client: Any,
        table_q: str,
        records: list[dict[str, Any]],
        columns: list[str],
        sync_options: SyncOptions,
        result: SyncResult,
        *,
        base_index: int = 0,
    ) -> bool:
        """Insert batches, falling back to rows for exact error attribution."""
        for start in range(0, len(records), sync_options.batch_size):
            record_batch = records[start : start + sync_options.batch_size]

            # Keep a one-record batch on the existing row path. If it fails,
            # replaying the same insert would both add a round trip and change
            # the previous single-attempt semantics.
            if len(record_batch) > 1:
                rows = [[record.get(c) for c in columns] for record in record_batch]
                token = str(uuid.uuid4())
                settings = {"insert_deduplication_token": token}

                # HTTP failure is ambiguous: ClickHouse may have committed the
                # batch before the response was lost. Retry the identical batch
                # once with the same server-side dedup token before attributing
                # errors row by row. Per-row tokens cannot extend this safety:
                # the token belongs to the whole INSERT and would not match the
                # original batch token.
                #
                # Best-effort, not a guarantee: insert_deduplication_token only
                # takes effect if the table's dedup window is enabled server-
                # side (on by default for Replicated*MergeTree, off by default
                # for a plain non-replicated table) -- see the module docstring.
                # drt does not verify this, so on a table without it enabled, an
                # ambiguous failure can still duplicate rows on retry.
                for _attempt in range(2):
                    try:
                        client.insert(
                            table_q,
                            rows,
                            column_names=columns,
                            settings=settings,
                        )
                        result.success += len(record_batch)
                        break
                    except Exception:
                        pass
                else:
                    # Two failed identical batch requests fall back to the
                    # existing row path for exact RowError attribution.
                    for i, record in enumerate(record_batch, start=base_index + start):
                        try:
                            row = [[record.get(c) for c in columns]]
                            client.insert(table_q, row, column_names=columns)
                            result.success += 1
                        except Exception as e:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=i,
                                    record_preview=json.dumps(record, default=str)[:200],
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )
                            if sync_options.on_error == "fail":
                                return False
                    continue

                continue

            # Existing per-row insert/error path for singleton batches.
            record = record_batch[0]
            try:
                row = [[record.get(c) for c in columns]]
                client.insert(table_q, row, column_names=columns)
                result.success += 1
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=base_index + start,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )
                if sync_options.on_error == "fail":
                    return False

        return True

    def _load_replace_swap(
        self,
        client: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Build a shadow table per sync; atomic EXCHANGE happens in finalize_sync.

        ClickHouse's ``CREATE TABLE shadow AS original`` clones the engine,
        partitioning, ORDER BY, and column definitions. INSERTs go to the shadow
        until :meth:`finalize_sync` runs ``EXCHANGE TABLES`` (atomic since 21.8).
        """
        result = SyncResult()
        shadow = f"{table}__drt_swap"
        shadow_q = self._quote_ident(shadow)
        table_q = self._quote_ident(table)

        if not self._swap_shadow_created:
            client.command(tag_query(f"DROP TABLE IF EXISTS {shadow_q}", sync_options))
            client.command(tag_query(f"CREATE TABLE {shadow_q} AS {table_q}", sync_options))
            self._swap_shadow_created = True
            self._swap_table = table

        if not self._insert_batched(
            client,
            shadow_q,
            records,
            columns,
            sync_options,
            result,
        ):
            # Drop the partial shadow + reset state so finalize_sync() cannot
            # EXCHANGE partial data into the live table. try/finally guarantees
            # state reset even if DROP fails; at worst we leave an orphan
            # shadow (tracked by #433).
            try:
                client.command(tag_query(f"DROP TABLE IF EXISTS {shadow_q}", sync_options))
            finally:
                self._swap_shadow_created = False
                self._swap_table = None

        return result

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """End-of-sync hook: EXCHANGE for swap-replace, ALTER DELETE for mirror.

        - ``mode=replace, replace_strategy=swap``: atomic ``EXCHANGE TABLES``
          (existing behaviour). After the exchange the shadow table holds the
          OLD data, so we drop it. ``EXCHANGE TABLES`` is atomic in
          ClickHouse 21.8+.
        - ``mode=mirror`` (#340 Step 3): ``ALTER TABLE ... DELETE WHERE
          <upsert_key> NOT IN (<observed>)`` mutation that removes
          destination rows whose key was not in the source. Skipped if the
          source produced no batches with records — treats "no observation"
          as "don't delete anything" for safety.
        """
        if sync_options.mode == "mirror":
            result = self._finalize_mirror(config, sync_options)
            # Reset mirror state regardless of result so a re-run starts fresh.
            self._mirror_keys = None
            self._mirror_scopes = None
            return result

        if not self._swap_shadow_created or self._swap_table is None:
            return None

        assert isinstance(config, ClickHouseDestinationConfig)
        table = self._swap_table
        shadow = f"{table}__drt_swap"
        table_q = self._quote_ident(table)
        shadow_q = self._quote_ident(shadow)

        client = self._connect(config)
        try:
            client.command(tag_query(f"EXCHANGE TABLES {table_q} AND {shadow_q}", sync_options))
            # Shadow now contains the OLD data — drop it.
            client.command(tag_query(f"DROP TABLE {shadow_q}", sync_options))
        finally:
            client.close()
            self._swap_shadow_created = False
            self._swap_table = None

        return SyncResult()

    def _build_mirror_delete(
        self,
        table_q: str,
        upsert_cols: list[str],
        keys: list[tuple[Any, ...]],
        scope_cols: list[str] | None,
        scopes: list[tuple[Any, ...]] | None,
        negate: bool,
    ) -> tuple[str, dict[str, Any]]:
        """Build an ``ALTER TABLE ... DELETE`` mutation (#340 Step 3 / #687 / #692).

        Uses clickhouse_connect's native ``{name:Type}`` parameter
        substitution with ``Array(String)`` (single column) or
        ``Array(Tuple(String, ...))`` (composite). Both column references
        and parameter values are coerced with ``toString()`` so the
        comparison works regardless of the source column type — at the
        cost of skipping any index on the upsert_key column.

        ``scope_cols``/``scopes`` (#692, mirroring Postgres/MySQL/Snowflake's
        #687 handling) prepend a ``scope IN {scope_keys:...} AND`` clause in
        the same shape. ``negate`` selects destination-strategy (``NOT IN``,
        delete what's absent) vs. tracked-strategy (``IN``, delete exactly
        these keys). Caller runs the returned statement with
        ``settings={"mutations_sync": 1}`` so the call blocks until the
        mutation finishes.
        """
        op = "NOT IN" if negate else "IN"
        scope_clause = ""
        params: dict[str, Any] = {}
        if scope_cols and scopes:
            if len(scope_cols) == 1:
                scope_col_q = f"toString(`{scope_cols[0]}`)"
                scope_clause = f"{scope_col_q} IN {{scope_keys:Array(String)}} AND "
                params["scope_keys"] = [str(s[0]) for s in scopes]
            else:
                scope_col_tuple = "(" + ", ".join(f"toString(`{c}`)" for c in scope_cols) + ")"
                scope_tuple_type = "Tuple(" + ", ".join(["String"] * len(scope_cols)) + ")"
                scope_clause = f"{scope_col_tuple} IN {{scope_keys:Array({scope_tuple_type})}} AND "
                params["scope_keys"] = [tuple(str(v) for v in s) for s in scopes]

        if len(upsert_cols) == 1:
            col_q = f"toString(`{upsert_cols[0]}`)"
            sql = (
                f"ALTER TABLE {table_q} DELETE "
                f"WHERE {scope_clause}{col_q} {op} {{keys:Array(String)}}"
            )
            params["keys"] = [str(k[0]) for k in keys]
        else:
            col_tuple = "(" + ", ".join(f"toString(`{c}`)" for c in upsert_cols) + ")"
            tuple_type = "Tuple(" + ", ".join(["String"] * len(upsert_cols)) + ")"
            sql = (
                f"ALTER TABLE {table_q} DELETE "
                f"WHERE {scope_clause}{col_tuple} {op} {{keys:Array({tuple_type})}}"
            )
            params["keys"] = [tuple(str(v) for v in k) for k in keys]
        return sql, params

    def _finalize_mirror(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass (#340 Step 3 / #687).

        Deletes destination rows whose ``upsert_key`` tuple is not in the
        set of keys observed across all batches, via
        :meth:`_build_mirror_delete`. Mirror mode is intended for
        small/medium reference tables — the ``toString()`` comparison skips
        any index on the upsert_key column; the temp-table strategy (#340
        follow-up) targets the high-cardinality case.

        ``mirror.strategy: tracked`` (#692) dispatches to
        :meth:`_finalize_mirror_tracked` instead — state-based diff rather
        than the whole-table diff below. Shares the empty-source guard, so
        a transient empty source also keeps a tracked baseline intact.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` —
        treats "no batch with records was ever observed" as a signal to
        skip the DELETE entirely, so a transient empty source doesn't
        wipe the destination.
        """
        assert isinstance(config, ClickHouseDestinationConfig)
        if not self._mirror_keys:
            return None

        if sync_options.mirror is not None and sync_options.mirror.strategy == "tracked":
            return self._finalize_mirror_tracked(config, sync_options)

        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        table_q = self._quote_ident(config.table)

        # mirror.scope (#687/#692) — restrict the diff to rows under parents
        # this run actually observed. list(), not sorted() — scope values
        # may include None (unorderable).
        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        scopes = list(self._mirror_scopes or set()) if scope_cols else None

        client = self._connect(config)
        try:
            sql, params = self._build_mirror_delete(
                table_q, upsert_cols, keys, scope_cols, scopes, negate=True
            )
            client.command(
                tag_query(sql, sync_options), parameters=params, settings={"mutations_sync": 1}
            )
        finally:
            client.close()

        # SyncResult has no dedicated `deleted` field; future work tracks
        # this separately. Returning a bare SyncResult signals "finalize
        # ran successfully" to the engine without inflating success/failed.
        return SyncResult()

    def _finalize_mirror_tracked(self, config: Any, sync_options: SyncOptions) -> SyncResult | None:
        """``mirror.strategy: tracked`` (#692) — delete only rows drt synced.

        Same Census-style algorithm as
        ``BaseSqlDestination._finalize_mirror_tracked`` (Postgres/MySQL,
        #686/#694) and the Snowflake destination's version: reads the
        previously-synced key set for this sync from a drt-managed
        ``_drt_synced_keys`` table, deletes ``previous - current`` from the
        target, and rewrites the state to the current key set. First run
        (or lost state) baselines: record keys, delete nothing, WARN.

        Two real ClickHouse-specific differences from the other three
        dialects:

        - **No table qualification needed** — like the target table
          (``config.table``, unqualified — see ``ClickHouseDestinationConfig``),
          the state table is created and addressed unqualified, resolving
          against the connection's own default database (set at connect
          time via ``database``/``database_env``).
        - **No cross-statement transaction.** Postgres/MySQL/Snowflake commit
          the target DELETE and the state rewrite together; ClickHouse has no
          such thing here — each statement is its own mutation. Ordering is
          chosen so a failure between them degrades safely: the target
          DELETE runs *first*, so if the state rewrite fails afterward, the
          state table is left holding entries for now-deleted target rows —
          harmless (a stale key deleted a second time is a no-op) — or, if
          the state table's own DELETE half succeeds but the INSERT half
          doesn't, ``previous`` reads back empty next run and the algorithm's
          existing "no prior state" baseline path takes over (WARN,
          re-baseline, no deletes) rather than deleting anything wrongly.
          Reversing the order — state first, target DELETE second — would
          fail the other way: a target-DELETE failure would leave already-
          gone-from-state keys still present on the target with nothing to
          ever clean them up.

        ``mirror.scope`` + ``strategy: tracked`` (#694 part 1) prunes both
        the state read and the state rewrite to the observed scope — see
        ``BaseSqlDestination._finalize_mirror_tracked`` for the full
        rationale; the algorithm here is identical, just against
        ClickHouse's own mutation/insert primitives.

        SQL-side diff (#694 part 2): unlike the other three dialects, no
        scratch/temp table is used here — ClickHouse's ``Array(String)``
        named parameters already hold this run's entire key-hash set as a
        *single* bound value (no per-element placeholder, so no marker-count
        limit the way Databricks' native paramstyle has), so both directions
        of the diff are plain ``NOT IN`` / ``arrayJoin`` queries against that
        one parameter instead of a joined-against table. ``previous -
        current`` (the diff, typically small **for unscoped tracked
        mirror**) and ``current - previous`` (genuinely-new keys) both run
        entirely in ClickHouse; only their results reach Python. Scope-
        filtering the diff in Python afterward is mathematically equivalent
        to filtering the full previous set by scope first (same proof as
        the base implementation: scope membership and current-membership
        are independent conditions) — but the diff query itself has no
        scope predicate, so a scoped run touching one of many historically-
        tracked scopes doesn't get the same memory win (#890). The old
        "read every untouched row so it can be reinserted unchanged" step
        for scope-preserved rows is gone — untouched rows are simply never
        selected by either query.

        The failure-mode ordering above still holds with the split state
        rewrite (delete-diffed-hashes, then insert-new-hashes, rather than
        one blanket delete-all-then-insert-current): if the state DELETE
        half succeeds but the INSERT half fails, only the *new* keys this
        run observed go untracked — strictly narrower exposure than the
        original blanket rewrite, where the same failure lost tracking for
        every key in the current run.
        """
        import logging

        from drt.destinations._mirror_state import (
            STATE_TABLE,
            decode_key,
            key_hash,
            key_json,
            scope_key_json,
            scope_spec_json,
        )

        assert isinstance(config, ClickHouseDestinationConfig)
        sync_name = sync_options._sync_name or config.table
        current = list({tuple(k) for k in self._mirror_keys or []})
        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()
        table_q = self._quote_ident(config.table)
        state_q = self._quote_ident(STATE_TABLE)
        current_by_hash = {key_hash(k): k for k in current}
        current_hashes = list(current_by_hash.keys())

        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        scope_positions = [upsert_cols.index(c) for c in scope_cols] if scope_cols else None
        observed_scopes = set(self._mirror_scopes or set()) if scope_positions else None

        client = self._connect(config)
        try:
            # Pre-provisioning (mirrors #695): only CREATE when the state
            # table is genuinely absent, so a locked-down destination user
            # can run against one an admin created ahead of time.
            exists = client.query(tag_query(f"EXISTS TABLE {state_q}", sync_options))
            fresh_table = not exists.result_rows[0][0]
            if fresh_table:
                client.command(
                    tag_query(
                        f"CREATE TABLE IF NOT EXISTS {state_q} ("
                        "sync_name String, key_hash String, key_json String, "
                        "scope_spec Nullable(String), scope_key Nullable(String)"
                        ") ENGINE = MergeTree ORDER BY (sync_name, key_hash)",
                        sync_options,
                    )
                )

            # #890: scope columns let the diff be narrowed server-side. Probed
            # only for a scoped run — an unscoped sync gains nothing and must
            # not pay a probe, let alone DDL, for it.
            #
            # ``Nullable(String)`` rather than plain ``String``: the predicate
            # below has to tell "written before these columns existed" apart
            # from "written with an empty scope value", and ClickHouse's default
            # String has no NULL to carry that difference.
            #
            # Asked via ``system.columns`` rather than ``information_schema`` —
            # the latter exists on modern ClickHouse but is a compatibility
            # shim, which is also why ``drt/destinations/schema.py`` does not
            # cover this dialect.
            scope_spec = scope_spec_json(list(scope_cols)) if scope_cols else None
            scope_sql = False
            if scope_positions is not None:
                if fresh_table:
                    scope_sql = True  # created with the columns
                else:
                    probe = client.query(
                        tag_query(
                            "SELECT count() FROM system.columns "
                            "WHERE database = currentDatabase() AND table = {tbl:String} "
                            "AND name IN ('scope_spec', 'scope_key')",
                            sync_options,
                        ),
                        parameters={"tbl": STATE_TABLE},
                    )
                    if probe.result_rows and probe.result_rows[0][0] == 2:
                        scope_sql = True
                    else:
                        # No SAVEPOINT, as on the Snowflake leg and unlike
                        # Postgres/MySQL: there is no open transaction here to
                        # poison, so a refused ALTER fails on its own. A refusal
                        # keeps the run on the Python-only filter, permanently
                        # and without an error (#695 family).
                        try:
                            client.command(
                                tag_query(
                                    f"ALTER TABLE {state_q} "
                                    "ADD COLUMN scope_spec Nullable(String), "
                                    "ADD COLUMN scope_key Nullable(String)",
                                    sync_options,
                                )
                            )
                        except Exception:  # noqa: BLE001 — a supported state
                            pass
                        else:
                            scope_sql = True

            # Baseline check: a cheap existence probe, never a full read.
            baseline_probe = client.query(
                tag_query(
                    f"SELECT 1 FROM {state_q} WHERE sync_name = {{sync_name:String}} LIMIT 1",
                    sync_options,
                ),
                parameters={"sync_name": sync_name},
            )
            previous_exists = bool(baseline_probe.result_rows)

            # #890, mirroring the other legs. The first two branches are what
            # keep this a purely *coarse* filter — every row they let through is
            # re-checked exactly by the Python filter below, so it can only ever
            # return too many rows, never too few:
            #   scope_key IS NULL → written before the columns existed
            #   scope_spec != ... → written under a different mirror.scope, so
            #                       its frozen scope_key means nothing here
            #
            # Unlike Postgres/MySQL/Snowflake, the first bucket is never healed
            # here. Those dialects backfill scope_key lazily from the diff —
            # cheap, since the rows are already fetched and decoded, and the
            # write is a plain UPDATE. On ClickHouse the equivalent is ALTER
            # TABLE ... UPDATE, a mutation that rewrites parts rather than a
            # cheap per-row write, so "already in hand" doesn't make it cheap
            # here too. Deliberate (#906): keys tracked before this upgrade
            # keep falling through to the Python filter, permanently, unless
            # an operator rebaselines. Keys added after the upgrade get the
            # full SQL-side narrowing.
            diff_sql = (
                f"SELECT key_hash, key_json FROM {state_q} "
                "WHERE sync_name = {sync_name:String} "
                "AND key_hash NOT IN {current_hashes:Array(String)}"
            )
            diff_params: dict[str, Any] = {
                "sync_name": sync_name,
                "current_hashes": current_hashes,
            }
            if scope_sql and observed_scopes:
                diff_sql += (
                    " AND (scope_key IS NULL OR scope_spec != {scope_spec:String} "
                    "OR scope_key IN {scope_keys:Array(String)})"
                )
                diff_params["scope_spec"] = scope_spec
                diff_params["scope_keys"] = sorted(key_json(sc) for sc in observed_scopes)
            diff_result = client.query(tag_query(diff_sql, sync_options), parameters=diff_params)
            raw_diff = diff_result.result_rows

            if scope_positions is not None and observed_scopes is not None:
                to_delete = [
                    decode_key(kj)
                    for _h, kj in raw_diff
                    if tuple(decode_key(kj)[p] for p in scope_positions) in observed_scopes
                ]
            else:
                to_delete = [decode_key(kj) for _h, kj in raw_diff]

            if to_delete:
                sql, params = self._build_mirror_delete(
                    table_q, upsert_cols, to_delete, None, None, negate=False
                )
                client.command(
                    tag_query(sql, sync_options),
                    parameters=params,
                    settings={"mutations_sync": 1},
                )
            elif not previous_exists:
                logging.getLogger(__name__).warning(
                    "tracked mirror: no prior state for sync %r in %s — "
                    "baselining this run's %d key(s); no deletes this run.",
                    sync_name,
                    STATE_TABLE,
                    len(current),
                )

            if to_delete:
                client.command(
                    tag_query(
                        f"ALTER TABLE {state_q} DELETE WHERE sync_name = {{sync_name:String}} "
                        "AND key_hash IN {hashes:Array(String)}",
                        sync_options,
                    ),
                    parameters={
                        "sync_name": sync_name,
                        "hashes": [key_hash(k) for k in to_delete],
                    },
                    settings={"mutations_sync": 1},
                )

            new_hashes_result = client.query(
                tag_query(
                    "SELECT arrayJoin({current_hashes:Array(String)}) AS key_hash "
                    f"WHERE key_hash NOT IN (SELECT key_hash FROM {state_q} "
                    "WHERE sync_name = {sync_name:String})",
                    sync_options,
                ),
                parameters={"sync_name": sync_name, "current_hashes": current_hashes},
            )
            new_hashes = [row[0] for row in new_hashes_result.result_rows]
            if new_hashes:
                if scope_sql and scope_positions is not None:
                    state_rows = [
                        [
                            sync_name,
                            h,
                            key_json(current_by_hash[h]),
                            scope_spec,
                            scope_key_json(current_by_hash[h], scope_positions),
                        ]
                        for h in new_hashes
                    ]
                    columns = ["sync_name", "key_hash", "key_json", "scope_spec", "scope_key"]
                else:
                    # Unscoped, or the columns are unavailable — they stay NULL,
                    # which the predicate above always lets through.
                    state_rows = [[sync_name, h, key_json(current_by_hash[h])] for h in new_hashes]
                    columns = ["sync_name", "key_hash", "key_json"]
                client.insert(state_q, state_rows, column_names=columns)
        finally:
            client.close()

        return SyncResult()

    @staticmethod
    def _quote_ident(table: str) -> str:
        """Backtick-quote a (possibly database-qualified) identifier.

        ``mydb.scores`` -> ``\\`mydb\\`.\\`scores\\``` ; ``scores`` -> ``\\`scores\\```.
        """
        from drt.destinations.sql_utils import backtick_quote_ident

        return backtick_quote_ident(table)

    def get_row_count(self, config: DestinationConfig) -> int:
        """Get the current row count from the destination table.

        Args:
            config: Destination configuration (must be ClickHouseDestinationConfig).

        Returns:
            Row count as integer.

        Raises:
            Exception: If connection or query fails.
        """
        assert isinstance(config, ClickHouseDestinationConfig)
        client = self._connect(config)
        try:
            result = client.query(f"SELECT COUNT(*) FROM {self._quote_ident(config.table)}")
            # clickhouse_connect returns a QueryResult object
            # result.result_rows is a list of tuples
            if result.result_rows:
                return int(result.result_rows[0][0])
            return 0
        finally:
            client.close()

    def get_table_name(self, config: DestinationConfig) -> str:
        """Implements ``QueryableDestination`` (#469)."""
        assert isinstance(config, ClickHouseDestinationConfig)
        return config.table

    def execute_test_query(self, config: DestinationConfig, query: str) -> int:
        """Implements ``QueryableDestination`` (#469).

        Raises:
            Exception: If connection or query fails.
        """
        assert isinstance(config, ClickHouseDestinationConfig)
        client = self._connect(config)
        try:
            result = client.query(query)
            val: Any = result.result_rows[0][0]
            return int(val)
        finally:
            client.close()

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, ClickHouseDestinationConfig)
        client = self._connect(config)
        try:
            client.command("SELECT 1")
        finally:
            client.close()

    @classmethod
    def _connect(cls, config: ClickHouseDestinationConfig) -> Any:
        try:
            import clickhouse_connect  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "ClickHouse destination requires: pip install drt-core[clickhouse]"
            ) from e

        # Connection string takes precedence
        conn_str = (
            resolve_env(None, config.connection_string_env)
            if config.connection_string_env
            else None
        )
        if conn_str:
            return clickhouse_connect.get_client(dsn=conn_str)

        # Fall back to individual parameters
        host = resolve_env(config.host, config.host_env)
        database = resolve_env(config.database, config.database_env)
        user = resolve_env(config.user, config.user_env)
        password = resolve_env(config.password, config.password_env) or ""

        if not host:
            raise ValueError("ClickHouse destination: host could not be resolved.")
        if not database:
            raise ValueError("ClickHouse destination: database could not be resolved.")

        return clickhouse_connect.get_client(
            host=host,
            port=config.port,
            database=database,
            username=user or "default",
            password=password,
            secure=config.secure,
        )
