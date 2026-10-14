"""Live PostgreSQL proof for immutable canary replay and terminal claims (#1255)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.credentials import PostgresProfile

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")

runner = CliRunner()


@pytest.fixture(scope="module")
def pg() -> Iterator[PostgresProfile]:
    require_docker()
    with testcontainers_postgres.PostgresContainer(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        yield PostgresProfile(
            type="postgres",
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            dbname="testdb",
            user="admin",
            password="adminpass",
        )


def _sql(pg: PostgresProfile, *statements: str) -> list[tuple[Any, ...]]:
    connection = psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
    )
    try:
        rows: list[tuple[Any, ...]] = []
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
                if cursor.description:
                    rows = cursor.fetchall()
        connection.commit()
        return rows
    finally:
        connection.close()


def test_three_wave_upsert_halts_then_new_plan_applies_remaining_rows(
    pg: PostgresProfile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.cli.commands import run as run_command
    from drt.config import credentials as credentials

    source_table = "src_canary_apply"
    destination_table = "dst_canary_apply"
    _sql(
        pg,
        f"DROP TABLE IF EXISTS {source_table}; DROP TABLE IF EXISTS {destination_table}",
        f"CREATE TABLE {source_table} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        f"CREATE TABLE {destination_table} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        f"INSERT INTO {source_table} "
        "SELECT value, 'row-' || value::text FROM generate_series(1, 400) AS value",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DRT_PLAN_KEY", "canary-smoke-key")
    monkeypatch.setattr(credentials, "load_profile", lambda *_args, **_kwargs: pg)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "canary-smoke", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    sync_path = tmp_path / "syncs" / "canary.yml"
    sync_document = {
        "name": "canary",
        "model": f"SELECT id, name FROM {source_table}",
        "destination": {
            "type": "postgres",
            "host": pg.host,
            "port": pg.port,
            "dbname": pg.dbname,
            "user": pg.user,
            "password": pg.password,
            "table": destination_table,
            "upsert_key": ["id"],
        },
        "sync": {"mode": "upsert"},
    }
    sync_path.write_text(yaml.dump(sync_document))

    planned = runner.invoke(app, ["plan", "canary", "--out", "plan.json"])
    assert planned.exit_code == 0, planned.output

    real_run_one = run_command._run_one
    wave_calls = 0

    def fail_second_health_check(*args: Any, **kwargs: Any) -> tuple[str, dict[str, object], bool]:
        nonlocal wave_calls
        name, entry, had_error = real_run_one(*args, **kwargs)
        if args[1].dry_run:
            return name, entry, had_error
        wave_calls += 1
        if wave_calls == 1:
            # The spool already exists. Later waves must not see this live
            # mutation or the newly inserted row.
            _sql(
                pg,
                f"UPDATE {source_table} SET name = 'live-source-changed' WHERE id > 1",
                f"INSERT INTO {source_table} VALUES (401, 'late-row')",
            )
        if wave_calls == 2:
            entry = dict(entry)
            entry["_row_error_count"] = int(entry.get("rows_extracted", 0)) or 1
        return name, entry, had_error

    monkeypatch.setattr(run_command, "_run_one", fail_second_health_check)
    halted = runner.invoke(
        app,
        ["apply", "plan.json", "--auto-approve", "--canary", "1,10,100"],
    )
    assert halted.exit_code == 1, halted.output
    assert "wave 2/3" in halted.output and "HALTED" in halted.output

    applied_before_replan = int(_sql(pg, f"SELECT COUNT(*) FROM {destination_table}")[0][0])
    assert 0 < applied_before_replan < 400
    old_plan_id = json.loads((tmp_path / "plan.json").read_text())["plan_id"]
    old_claim = json.loads(
        (tmp_path / ".drt" / "applied_plans" / f"{old_plan_id}.json").read_text()
    )
    assert old_claim["status"] == "halted"
    written_snapshot = _sql(pg, f"SELECT id, name FROM {destination_table} ORDER BY id")
    assert written_snapshot
    assert all(name == f"row-{row_id}" for row_id, name in written_snapshot)
    assert all(row_id != 401 for row_id, _name in written_snapshot)
    assert not list((tmp_path / ".drt" / "applied_plans").glob("*.spool.jsonl"))

    _sql(
        pg,
        f"DELETE FROM {source_table} WHERE id = 401",
        f"UPDATE {source_table} SET name = 'row-' || id::text",
    )

    monkeypatch.setattr(run_command, "_run_one", real_run_one)
    replanned = runner.invoke(app, ["plan", "canary", "--out", "remaining.json"])
    assert replanned.exit_code == 0, replanned.output
    remaining = json.loads((tmp_path / "remaining.json").read_text())
    assert len(remaining["entries"]) == 400 - applied_before_replan

    completed = runner.invoke(app, ["apply", "remaining.json", "--auto-approve"])
    assert completed.exit_code == 0, completed.output
    actual = _sql(pg, f"SELECT id, name FROM {destination_table} ORDER BY id")
    expected = [(index, f"row-{index}") for index in range(1, 401)]
    assert actual == expected
    assert len({row[0] for row in actual}) == 400

    # A kill-style interruption is attributable before the wave starts,
    # terminal, and leaves no value-bearing spool behind.
    from drt.cli._apply_flow import apply_plan

    _sql(pg, f"INSERT INTO {source_table} VALUES (401, 'row-401')")
    interrupted_plan = runner.invoke(app, ["plan", "canary", "--out", "interrupt.json"])
    assert interrupted_plan.exit_code == 0, interrupted_plan.output

    def interrupt_wave(*args: Any, **kwargs: Any) -> Any:
        if not args[1].dry_run:
            raise SystemExit(143)
        return real_run_one(*args, **kwargs)

    monkeypatch.setattr(run_command, "_run_one", interrupt_wave)
    with pytest.raises(SystemExit):
        apply_plan(
            (tmp_path / "interrupt.json").read_text(),
            project_dir=tmp_path,
            approved=True,
            canary_waves=[100],
        )
    interrupted_id = json.loads((tmp_path / "interrupt.json").read_text())["plan_id"]
    interrupted_claim = json.loads(
        (tmp_path / ".drt" / "applied_plans" / f"{interrupted_id}.json").read_text()
    )
    assert interrupted_claim["state"] == "halted"
    assert interrupted_claim["reason"] == "interrupted"
    assert interrupted_claim["current_run_id"] == interrupted_claim["run_ids"][0]
    assert not list((tmp_path / ".drt" / "applied_plans").glob("*.spool.jsonl"))
    assert int(_sql(pg, f"SELECT COUNT(*) FROM {destination_table}")[0][0]) == 400
    monkeypatch.setattr(run_command, "_run_one", real_run_one)
    _sql(pg, f"DELETE FROM {source_table} WHERE id = 401")

    # The live destination declares the required capability; ClickHouse's
    # informational key is rejected by rollout validation.
    from pydantic import ValidationError

    from drt.config.models import SyncConfig
    from drt.destinations.postgres import PostgresDestination

    assert PostgresDestination().supports_keyed_upsert(
        SyncConfig.model_validate(sync_document).destination
    )
    clickhouse = {
        **sync_document,
        "destination": {
            "type": "clickhouse",
            "host": "localhost",
            "database": "d",
            "table": "t",
            "upsert_key": ["id"],
        },
        "sync": {"mode": "upsert", "rollout": {"waves": [100]}},
    }
    with pytest.raises(ValidationError, match="keyed-upsert destination"):
        SyncConfig.model_validate(clickhouse)

    # CLI overrides are checked at apply time even when the freshly replanned
    # destination has no remaining changes.
    for mode, extra, message in (
        ("incremental", {"cursor_field": "id"}, "non-incremental"),
        ("mirror", {}, "non-incremental"),
    ):
        sync_document["sync"] = {"mode": mode, **extra}
        sync_path.write_text(yaml.dump(sync_document))
        plan_file = f"{mode}.json"
        planned_unsupported = runner.invoke(app, ["plan", "canary", "--out", plan_file])
        assert planned_unsupported.exit_code == 0, planned_unsupported.output
        refused = runner.invoke(app, ["apply", plan_file, "--auto-approve", "--canary", "1,10,100"])
        assert refused.exit_code == 1
        assert message in refused.output
