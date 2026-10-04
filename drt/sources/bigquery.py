"""BigQuery source implementation.

Requires: pip install drt-core[bigquery]

Authentication methods:
  application_default — uses gcloud ADC (recommended for local dev)
  keyfile             — explicit service account JSON file
"""

from __future__ import annotations

import hashlib
import os
import re
import uuid
from collections.abc import Iterator
from typing import Any, Literal

from drt.config.credentials import BigQueryProfile, ProfileConfigLike
from drt.config.models import RetryConfig
from drt.config.query_tags import normalize_bigquery_label
from drt.destinations.retry import with_retry
from drt.sources.base import SnapshotDiffResult

_SNAPSHOT_TOKEN_LABEL = "drt_snapshot_run"
_PROJECT_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_DATASET_OR_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _raise_diff_concurrency_race(sync_name: str) -> None:
    """Fail loudly when another run replaced this run's scratch snapshot."""
    raise RuntimeError(
        f"sync.incremental_strategy: diff — another run of sync "
        f"{sync_name!r} appears to be building the same snapshot table "
        f"concurrently. diff-strategy syncs must not run concurrently "
        f"for the same sync — see drt serve's request coalescing (#854) "
        f"or your scheduler's own overlap protection."
    )


def _validate_identifier(kind: str, value: str, pattern: re.Pattern[str]) -> str:
    if not value or pattern.fullmatch(value) is None:
        raise ValueError(f"Invalid BigQuery {kind} identifier: {value!r}")
    return value


def _managed_table_id(config: BigQueryProfile, table_name: str) -> str:
    project = _validate_identifier("project", config.project, _PROJECT_RE)
    dataset = _validate_identifier("dataset", config.managed_schema, _DATASET_OR_TABLE_RE)
    table = _validate_identifier("table", table_name, _DATASET_OR_TABLE_RE)
    return f"{project}.{dataset}.{table}"


def _managed_table_identifier(config: BigQueryProfile, table_name: str) -> str:
    return f"`{_managed_table_id(config, table_name)}`"


def _quote_column(identifier: str) -> str:
    """Quote one BigQuery output column after rejecting unsafe SQL text."""
    if not identifier or "`" in identifier or any(ord(char) < 32 for char in identifier):
        raise ValueError(f"Invalid BigQuery column identifier: {identifier!r}")
    return f"`{identifier}`"


def _column_ref(alias: str, column: str) -> str:
    return f"{alias}.{_quote_column(column)}"


def _key_join_condition(key_columns: list[str], left: str, right: str) -> str:
    return " AND ".join(
        f"{_column_ref(left, column)} = {_column_ref(right, column)}" for column in key_columns
    )


def _diff_hash_expr(columns: list[str], alias: str) -> str:
    """Build a typed, deterministic BigQuery hash with explicit NULL tags.

    ``CONCAT`` is intentionally not used: one NULL argument makes the whole
    expression NULL in BigQuery, collapsing NULL/empty transitions. A STRUCT
    preserves each value's BigQuery type and field order; the adjacent
    nullness boolean makes the NULL distinction explicit before its stable
    JSON representation is passed to ``FARM_FINGERPRINT``.
    """
    fields = [
        expression
        for index, column in enumerate(columns)
        for expression in (
            f"{_column_ref(alias, column)} IS NULL AS `_drt_null_{index}`",
            f"{_column_ref(alias, column)} AS `_drt_value_{index}`",
        )
    ]
    return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({', '.join(fields)})))"


class BigQuerySource:
    """Extract records from Google BigQuery."""

    def __init__(self) -> None:
        # One source instance performs extraction and commit. The token ties
        # that invocation to the fixed-name scratch table it materialized.
        self._snapshot_diff_tokens: dict[tuple[str, str, str], str] = {}

    @staticmethod
    def _is_transient(exc: Exception) -> bool:
        """Use google-api-core's standard transient transport predicate."""
        from google.api_core.retry import if_transient_error

        return bool(if_transient_error(exc))

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run a SQL query and yield rows as dicts.

        ``query_tags`` (#768) becomes BigQuery job labels — the native
        mechanism ``INFORMATION_SCHEMA.JOBS`` cost-attribution queries key
        on, and the reason this needs its own path rather than relying on
        the engine's SQL-comment fallback alone: labels are queryable
        structured metadata, a comment is not.
        """
        assert isinstance(config, BigQueryProfile)
        client = self._build_client(config)
        job_config = self._job_config(query_tags)
        rows = client.query(query, job_config=job_config).result()
        for row in rows:
            yield dict(row)

    def _job_config(self, query_tags: dict[str, str] | None) -> Any:
        """``QueryJobConfig(labels=...)``, or ``None`` when tagging is off.

        BigQuery labels are queryable key-value pairs, not free text — both
        key and value are lowercase ``[a-z0-9_-]``, <=63 chars
        (:func:`normalize_bigquery_label`). ``client.query(query,
        job_config=None)`` behaves identically to omitting ``job_config``,
        so ``None`` here is not a special case downstream.
        """
        if not query_tags:
            return None
        from google.cloud import bigquery

        labels = {
            normalize_bigquery_label(k): normalize_bigquery_label(v) for k, v in query_tags.items()
        }
        return bigquery.QueryJobConfig(labels=labels)

    def test_connection(self, config: ProfileConfigLike) -> bool:
        """Return True if BigQuery is reachable with the given profile."""
        assert isinstance(config, BigQueryProfile)
        try:
            client = self._build_client(config)
            client.query("SELECT 1").result()
            return True
        except Exception:
            return False

    def _build_client(self, config: BigQueryProfile) -> Any:
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError("BigQuery support requires: pip install drt-core[bigquery]") from e

        if config.method == "keyfile" and config.keyfile:
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
                os.path.expanduser(config.keyfile)
            )
            return bigquery.Client(
                project=config.project,
                credentials=creds,
                location=config.location,
            )

        # Application Default Credentials (gcloud auth application-default login)
        return bigquery.Client(project=config.project, location=config.location)

    # --- ManagedTableCapable (#960/#1107, ADR 0005 step 3) ------------------
    #
    # Client-API based (get_dataset/create_dataset/get_table/delete_table),
    # not SQL DDL — mirrors destinations/bigquery.py's test_connection(),
    # which deliberately uses get_table() instead of a query job specifically
    # to avoid requiring the project-level bigquery.jobs.create permission
    # just to check something exists. Every probe/create/drop here stays on
    # that same client-API surface rather than mixing in DDL execution
    # (CREATE SCHEMA/TABLE, DROP TABLE), which would need bigquery.jobs.create
    # for the create/drop half even if the probes didn't.
    #
    # Unlike Postgres/Snowflake/Databricks, a single client.get_table() call
    # here already distinguishes "table absent" from "dataset absent" with
    # the same exception (NotFound) — there is no Databricks-style
    # SCHEMA_NOT_FOUND special case to guard against, so managed_table_exists
    # needs no separate dataset probe first.

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        """Create ``config.managed_schema`` as a dataset in ``config.project``
        if it does not already exist.

        Same two-part discipline as the other dialects:

        1. **The escape hatch**: probe first (``get_dataset``) — a
           locked-down principal with no ``bigquery.datasets.create``
           permission, but an admin-pre-provisioned dataset, must never have
           ``create_dataset`` called at all.
        2. **Concurrent first use**: on any exception from ``create_dataset``,
           re-probe rather than assuming failure — if the dataset exists now
           (another session won a first-use race, surfaced by BigQuery as
           ``Conflict``), that's success; anything else re-raises.
        """
        assert isinstance(config, BigQueryProfile)
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        client = self._build_client(config)
        dataset_ref = f"{config.project}.{config.managed_schema}"
        try:
            client.get_dataset(dataset_ref)
            return
        except NotFound:
            pass
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = config.location
            client.create_dataset(dataset)
        except Exception:
            # Re-probe rather than assuming failure: another session may
            # have won a first-use race (surfaced as Conflict, but caught
            # broadly since the exact type isn't load-bearing here). The
            # inner except's `pass` exits that frame before the bare
            # `raise` below, so `raise` re-raises the original create
            # failure being handled by this outer `except`, not NotFound.
            try:
                client.get_dataset(dataset_ref)
                return
            except NotFound:
                pass
            raise

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        """Only a plain base ``TABLE`` counts — a view/materialized view/
        snapshot/external table sharing this name is a different resource
        this capability doesn't own (caught in Codex review: treating any of
        those as "the managed table exists" would make a consumer skip its
        own ``CREATE TABLE`` and then fail on writes against, say, a view)."""
        assert isinstance(config, BigQueryProfile)
        from google.api_core.exceptions import NotFound

        client = self._build_client(config)
        table_id = f"{config.project}.{config.managed_schema}.{table_name}"
        try:
            table = client.get_table(table_id)
        except NotFound:
            return False
        return bool(table.table_type == "TABLE")

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        """No-op if absent *or* if ``table_name`` resolves to a non-table
        resource (view/materialized view/snapshot/external table) — this
        capability only ever creates plain base tables, so it must never
        delete something it doesn't own just because the name matches
        (caught in the same Codex review as the probe fix above)."""
        assert isinstance(config, BigQueryProfile)
        from google.api_core.exceptions import NotFound

        client = self._build_client(config)
        table_id = f"{config.project}.{config.managed_schema}.{table_name}"
        try:
            table = client.get_table(table_id)
        except NotFound:
            return
        if table.table_type != "TABLE":
            return
        # Preserve the Protocol's no-op-if-absent contract if another caller
        # removes the table between the ownership probe and this delete.
        client.delete_table(table_id, not_found_ok=True)

    # --- SnapshotDiffSource (#755/#1113, ADR 0005 step 5) -------------------

    def _snapshot_table_names(self, sync_name: str) -> tuple[str, str]:
        # BigQuery table names are case-sensitive by default. The digest still
        # keeps names distinct after sanitising/truncating unsupported text.
        digest = hashlib.sha1(sync_name.encode()).hexdigest()[:8]
        readable = re.sub(r"[^A-Za-z0-9_]", "_", sync_name)[:100]
        base = f"_drt_snapshot_{readable}_{digest}"
        return base, f"{base}_scratch"

    @staticmethod
    def _snapshot_token_key(config: BigQueryProfile, sync_name: str) -> tuple[str, str, str]:
        return config.project, config.managed_schema, sync_name

    @staticmethod
    def _managed_table_columns(client: Any, config: BigQueryProfile, table_name: str) -> list[str]:
        table = client.get_table(_managed_table_id(config, table_name))
        return [str(field.name) for field in table.schema]

    @staticmethod
    def _managed_table_exists_with_client(
        client: Any, config: BigQueryProfile, table_name: str
    ) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            table = client.get_table(_managed_table_id(config, table_name))
        except NotFound:
            return False
        return bool(table.table_type == "TABLE")

    @staticmethod
    def _managed_table_token(
        client: Any, config: BigQueryProfile, table_name: str
    ) -> tuple[bool, str | None]:
        from google.api_core.exceptions import NotFound

        try:
            table = client.get_table(_managed_table_id(config, table_name))
        except NotFound:
            return False, None
        if table.table_type != "TABLE":
            return False, None
        return True, (table.labels or {}).get(_SNAPSHOT_TOKEN_LABEL)

    @staticmethod
    def _set_snapshot_token(
        client: Any,
        config: BigQueryProfile,
        table_name: str,
        token: str,
    ) -> None:
        table = client.get_table(_managed_table_id(config, table_name))
        labels = dict(table.labels or {})
        labels[_SNAPSHOT_TOKEN_LABEL] = token
        table.labels = labels
        client.update_table(table, ["labels"])

    def _assert_snapshot_token(
        self,
        client: Any,
        config: BigQueryProfile,
        table_name: str,
        expected_token: str,
        sync_name: str,
    ) -> None:
        exists, actual_token = self._managed_table_token(client, config, table_name)
        if not exists or actual_token != expected_token:
            _raise_diff_concurrency_race(sync_name)

    def _snapshot_query_job_config(
        self,
        query_tags: dict[str, str] | None,
        destination: str,
    ) -> Any:
        from google.cloud import bigquery

        labels = None
        if query_tags:
            labels = {
                normalize_bigquery_label(key): normalize_bigquery_label(value)
                for key, value in query_tags.items()
            }
        return bigquery.QueryJobConfig(
            labels=labels,
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    @staticmethod
    def _copy_job_config() -> Any:
        from google.cloud import bigquery

        return bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    def extract_snapshot_diff(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        sync_name: str,
        key_columns: list[str],
        hash_columns: Literal["all"] | list[str],
        query_tags: dict[str, str] | None = None,
    ) -> SnapshotDiffResult:
        """See ``SnapshotDiffSource.extract_snapshot_diff``."""
        assert isinstance(config, BigQueryProfile)

        current_table, scratch_table = self._snapshot_table_names(sync_name)
        current_ident = _managed_table_identifier(config, current_table)
        scratch_ident = _managed_table_identifier(config, scratch_table)
        scratch_id = _managed_table_id(config, scratch_table)
        run_token = str(uuid.uuid4())

        def _setup() -> tuple[bool, bool, list[str]]:
            self.ensure_managed_schema(config)
            client = self._build_client(config)
            # A destination query job with WRITE_TRUNCATE materializes the
            # model atomically on successful job completion and heals scratch
            # left by an interrupted run. The table label is the best-effort
            # ownership token checked by all later classification jobs.
            client.query(
                query,
                job_config=self._snapshot_query_job_config(query_tags, scratch_id),
            ).result()
            self._set_snapshot_token(client, config, scratch_table, run_token)
            self._snapshot_diff_tokens[self._snapshot_token_key(config, sync_name)] = run_token
            all_columns = self._managed_table_columns(client, config, scratch_table)

            missing_keys = [column for column in key_columns if column not in all_columns]
            if missing_keys:
                raise ValueError(
                    "sync.incremental_strategy: diff — destination "
                    f"upsert_key column(s) {missing_keys} not found in the "
                    f"model's output columns {all_columns}."
                )
            if hash_columns == "all":
                diff_columns = [column for column in all_columns if column not in key_columns]
            else:
                missing_hash = [column for column in hash_columns if column not in all_columns]
                if missing_hash:
                    raise ValueError(
                        f"sync.diff.hash_columns: column(s) {missing_hash} "
                        "not found in the model's output columns "
                        f"{all_columns} — check for a typo."
                    )
                diff_columns = list(hash_columns)

            is_first_run = not self._managed_table_exists_with_client(client, config, current_table)
            reclassify_all_existing = False
            if not is_first_run:
                baseline_columns = self._managed_table_columns(client, config, current_table)
                if any(column not in baseline_columns for column in key_columns):
                    is_first_run = True
                else:
                    reclassify_all_existing = any(
                        column not in baseline_columns for column in diff_columns
                    )
                    diff_columns = [column for column in diff_columns if column in baseline_columns]
            return is_first_run, reclassify_all_existing, diff_columns

        is_first_run, reclassify_all_existing, diff_columns = with_retry(
            _setup, RetryConfig(), retry_on=self._is_transient
        )

        if is_first_run:
            added = self._stream_query(
                config,
                f"SELECT * FROM {scratch_ident}",
                scratch_table=scratch_table,
                expected_token=run_token,
                sync_name=sync_name,
                query_tags=query_tags,
            )
            changed: Iterator[dict[str, Any]] = iter(())
            removed_keys: Iterator[dict[str, Any]] = iter(())
        else:
            join_condition = _key_join_condition(key_columns, "s", "c")
            added = self._stream_query(
                config,
                f"SELECT s.* FROM {scratch_ident} AS s "
                f"LEFT JOIN {current_ident} AS c ON {join_condition} "
                f"WHERE {_column_ref('c', key_columns[0])} IS NULL",
                scratch_table=scratch_table,
                expected_token=run_token,
                sync_name=sync_name,
                query_tags=query_tags,
            )
            removed_keys = self._stream_query(
                config,
                "SELECT "
                + ", ".join(_column_ref("c", column) for column in key_columns)
                + f" FROM {current_ident} AS c LEFT JOIN {scratch_ident} AS s ON "
                + _key_join_condition(key_columns, "c", "s")
                + f" WHERE {_column_ref('s', key_columns[0])} IS NULL",
                scratch_table=scratch_table,
                expected_token=run_token,
                sync_name=sync_name,
                query_tags=query_tags,
            )
            if diff_columns or reclassify_all_existing:
                changed_filter = (
                    ""
                    if reclassify_all_existing
                    else f" WHERE {_diff_hash_expr(diff_columns, 's')} "
                    f"<> {_diff_hash_expr(diff_columns, 'c')}"
                )
                changed = self._stream_query(
                    config,
                    f"SELECT s.* FROM {scratch_ident} AS s "
                    f"JOIN {current_ident} AS c ON {join_condition}" + changed_filter,
                    scratch_table=scratch_table,
                    expected_token=run_token,
                    sync_name=sync_name,
                    query_tags=query_tags,
                )
            else:
                changed = iter(())

        return SnapshotDiffResult(
            added=added,
            changed=changed,
            removed_keys=removed_keys,
            is_first_run=is_first_run,
        )

    def _stream_query(
        self,
        config: BigQueryProfile,
        query: str,
        *,
        scratch_table: str,
        expected_token: str,
        sync_name: str,
        query_tags: dict[str, str] | None,
    ) -> Iterator[dict[str, Any]]:
        """Stream one classification query with best-effort overlap checks."""

        def _connect_and_execute() -> tuple[Any, Any]:
            client = self._build_client(config)
            self._assert_snapshot_token(client, config, scratch_table, expected_token, sync_name)
            rows = client.query(query, job_config=self._job_config(query_tags)).result()
            return client, rows

        client, rows = with_retry(_connect_and_execute, RetryConfig(), retry_on=self._is_transient)
        for row in rows:
            yield dict(row)
        self._assert_snapshot_token(client, config, scratch_table, expected_token, sync_name)

    def commit_snapshot_diff(self, config: ProfileConfigLike, sync_name: str) -> None:
        """Atomically copy this run's scratch table over the baseline.

        BigQuery documents ``WRITE_TRUNCATE`` copy jobs as one atomic update
        that occurs only after the job completes successfully. A backup copy
        lets a detected post-promotion overlap restore the prior baseline;
        there is still no transaction spanning token checks, destination
        delivery, and these copy jobs, so same-sync concurrency is unsupported.
        """
        assert isinstance(config, BigQueryProfile)
        token_key = self._snapshot_token_key(config, sync_name)
        expected_token = self._snapshot_diff_tokens.get(token_key)
        if expected_token is None:
            return

        current_table, scratch_table = self._snapshot_table_names(sync_name)
        backup_table = f"{current_table}_old"
        current_id = _managed_table_id(config, current_table)
        scratch_id = _managed_table_id(config, scratch_table)
        backup_id = _managed_table_id(config, backup_table)
        client = self._build_client(config)

        scratch_exists, scratch_token = self._managed_table_token(client, config, scratch_table)
        if not scratch_exists:
            promoted, current_token = self._managed_table_token(client, config, current_table)
            if promoted and current_token == expected_token:
                self._snapshot_diff_tokens.pop(token_key, None)
                return
            _raise_diff_concurrency_race(sync_name)
        if scratch_token != expected_token:
            _raise_diff_concurrency_race(sync_name)

        current_exists = self._managed_table_exists_with_client(client, config, current_table)
        copy_config = self._copy_job_config()
        if current_exists:
            client.copy_table(current_id, backup_id, job_config=copy_config).result()

        client.copy_table(scratch_id, current_id, job_config=copy_config).result()
        try:
            self._assert_snapshot_token(client, config, scratch_table, expected_token, sync_name)
            # Copy-job metadata propagation is not the ownership contract:
            # write and then verify the baseline's token explicitly.
            self._set_snapshot_token(client, config, current_table, expected_token)
            self._assert_snapshot_token(client, config, scratch_table, expected_token, sync_name)
            self._assert_snapshot_token(client, config, current_table, expected_token, sync_name)
        except Exception:
            try:
                if current_exists:
                    client.copy_table(backup_id, current_id, job_config=copy_config).result()
                else:
                    client.delete_table(current_id, not_found_ok=True)
            except Exception:
                pass
            raise

        # Scratch and backup are deliberately retained. The next atomic
        # WRITE_TRUNCATE heals both; deleting fixed names here could remove a
        # concurrent run's newly-created recovery state after our final check.
        self._snapshot_diff_tokens.pop(token_key, None)
