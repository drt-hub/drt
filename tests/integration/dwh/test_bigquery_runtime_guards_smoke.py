"""Live BigQuery proof for run-time delete guards (#1218 part 2).

BigQuery DML cannot be rolled back after the job, so the guard counts the exact
predicate first and then re-checks the job's ``num_dml_affected_rows``. A pass
here also proves that count is populated (otherwise ``GuardOutcomeCommitted``
would be raised). Every table is dropped in ``finally``.
"""

from __future__ import annotations

from typing import Any

import pytest

from drt.config.models import BigQueryDestinationConfig, SyncOptions
from drt.destinations.bigquery import BigQueryDestination
from drt.engine.guards import GuardTripped

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")


def _credentials() -> dict[str, str]:
    return require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )


class _Env:
    def __init__(self, creds: dict[str, str]) -> None:
        self.creds = creds
        self.client = bigquery.Client.from_service_account_json(
            creds["DRT_SMOKE_BIGQUERY_KEYFILE"], project=creds["DRT_SMOKE_BIGQUERY_PROJECT"]
        )

    def config(self, table: str, **extra: Any) -> BigQueryDestinationConfig:
        return BigQueryDestinationConfig(
            type="bigquery",
            project=self.creds["DRT_SMOKE_BIGQUERY_PROJECT"],
            dataset=self.creds["DRT_SMOKE_BIGQUERY_DATASET"],
            table=table,
            mode="insert",
            upsert_key=extra.pop("upsert_key", ["id"]),
            method="keyfile",
            keyfile=self.creds["DRT_SMOKE_BIGQUERY_KEYFILE"],
            **extra,
        )

    def fq(self, table: str) -> str:
        return (
            f"{self.creds['DRT_SMOKE_BIGQUERY_PROJECT']}."
            f"{self.creds['DRT_SMOKE_BIGQUERY_DATASET']}.{table}"
        )

    def run(self, sql: str) -> None:
        self.client.query(sql).result()

    def seed(self, table: str, ids: range) -> None:
        self.run(f"CREATE OR REPLACE TABLE `{self.fq(table)}` (id INT64, v STRING)")
        values = ", ".join(f"({i}, 'v{i}')" for i in ids)
        self.run(f"INSERT INTO `{self.fq(table)}` (id, v) VALUES {values}")

    def ids(self, table: str) -> list[int]:
        rows = self.client.query(f"SELECT id FROM `{self.fq(table)}` ORDER BY id").result()
        return [r[0] for r in rows]

    def cleanup(self, table: str) -> None:
        dataset_id, name = self.fq(table).rsplit(".", 1)
        self.client.delete_table(self.fq(table), not_found_ok=True)
        prefixes = (f"{name}_drt_tmp_", f"{name}__drt_swap_", f"{name}__drt_mirror_keys")
        for scratch in self.client.list_tables(dataset_id):
            if scratch.table_id.startswith(prefixes):
                self.client.delete_table(f"{dataset_id}.{scratch.table_id}", not_found_ok=True)


def _mirror_attempt(env: _Env, table: str, observed: list[int], options: SyncOptions) -> None:
    config = env.config(table)
    destination = BigQueryDestination()
    destination.load([{"id": i, "v": f"n{i}"} for i in observed], config, options)
    destination.finalize_sync(config, options)


def _mirror(**limits: Any) -> SyncOptions:
    return SyncOptions(mode="mirror", guards=limits)  # type: ignore[arg-type]


def test_destination_mirror_counts_first_then_deletes_and_verifies_the_job() -> None:
    env = _Env(_credentials())
    table = unique_table("drt_guard_dest")
    try:
        env.seed(table, range(1, 6))

        with pytest.raises(GuardTripped) as raised:
            _mirror_attempt(env, table, [1, 2], _mirror(max_deletes=2))
        assert raised.value.trips[0].observed == 3
        assert env.ids(table) == [1, 2, 3, 4, 5], "a refusal must delete nothing"

        with pytest.raises(GuardTripped):
            _mirror_attempt(env, table, [1, 2], _mirror(max_delete_pct=59))
        assert env.ids(table) == [1, 2, 3, 4, 5]

        _mirror_attempt(env, table, [1, 2], _mirror(max_delete_pct=60))
        assert env.ids(table) == [1, 2]

        env.seed(table, range(1, 6))
        forced = _mirror(max_deletes=0)
        forced._force_guards = True
        _mirror_attempt(env, table, [1, 2], forced)
        assert env.ids(table) == [1, 2]
        assert [(t.guard, t.observed) for t in forced._forced_guard_trips] == [("max_deletes", 3)]
    finally:
        env.cleanup(table)


def test_diff_mirror_refuses_before_delete_and_percentage_fails_closed() -> None:
    env = _Env(_credentials())
    table = unique_table("drt_guard_diff")
    try:
        env.seed(table, range(1, 6))
        config = env.config(table)

        def options(**limits: Any) -> SyncOptions:
            opts = SyncOptions(
                mode="mirror",
                incremental_strategy="diff",
                mirror={"strategy": "diff"},  # type: ignore[arg-type]
                guards=limits,  # type: ignore[arg-type]
            )
            opts._diff_removed_keys = [{"id": 1}, {"id": 2}]
            return opts

        for limits in ({"max_deletes": 1}, {"max_delete_pct": 90}):
            with pytest.raises(GuardTripped):
                BigQueryDestination().finalize_sync(config, options(**limits))
            assert env.ids(table) == [1, 2, 3, 4, 5]

        BigQueryDestination().finalize_sync(config, options(max_deletes=2))
        assert env.ids(table) == [3, 4, 5]
    finally:
        env.cleanup(table)


def test_replace_swap_refusal_keeps_live_table_and_force_overwrites() -> None:
    env = _Env(_credentials())
    table = unique_table("drt_guard_swap")
    try:
        env.seed(table, range(1, 6))
        config = env.config(table)

        def attempt(options: SyncOptions) -> None:
            destination = BigQueryDestination()
            destination.load([{"id": 10, "v": "n10"}], config, options)
            destination.finalize_sync(config, options)

        options = SyncOptions(
            mode="replace",
            replace_strategy="swap",
            guards={"max_deletes": 2},  # type: ignore[arg-type]
        )
        with pytest.raises(GuardTripped):
            attempt(options)
        assert env.ids(table) == [1, 2, 3, 4, 5], "a refused swap must not overwrite the table"

        options._force_guards = True
        attempt(options)
        assert env.ids(table) == [10]
        assert options._forced_guard_trips[0].guard == "max_deletes"
    finally:
        env.cleanup(table)
