"""Live PostgreSQL proof for canary halt/resume and deferred mirror delete (#1255).

The middle-wave health failure is injected after the real database write. This
keeps the test focused on rollout orchestration while every extraction, upsert,
resume reconstruction and final mirror DELETE still runs against PostgreSQL.
"""

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


def test_three_wave_mirror_halts_then_resumes_without_early_delete(
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
        f"INSERT INTO {destination_table} VALUES (999, 'delete-only-after-final-wave')",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DRT_PLAN_KEY", "canary-smoke-key")
    monkeypatch.setattr(credentials, "load_profile", lambda *_args, **_kwargs: pg)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "canary-smoke", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "canary.yml").write_text(
        yaml.dump(
            {
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
                "sync": {"mode": "mirror", "mirror": {"strategy": "destination"}},
            }
        )
    )

    planned = runner.invoke(app, ["plan", "canary", "--out", "plan.json"])
    assert planned.exit_code == 0, planned.output

    real_run_one = run_command._run_one
    wave_calls = 0

    def fail_second_check(*args: Any, **kwargs: Any) -> tuple[str, dict[str, object], bool]:
        nonlocal wave_calls
        name, entry, had_error = real_run_one(*args, **kwargs)
        if not args[1].mirror_state_only:
            wave_calls += 1
        if wave_calls == 2:
            entry = dict(entry)
            synced = int(entry.get("rows_synced", 0))
            entry["rows_failed"] = max(synced, 1)
            had_error = True
        return name, entry, had_error

    monkeypatch.setattr(run_command, "_run_one", fail_second_check)
    halted = runner.invoke(
        app,
        ["apply", "plan.json", "--auto-approve", "--canary", "1,10,100"],
    )
    assert halted.exit_code == 1, halted.output
    assert "wave 2/3" in halted.output and "HALTED" in halted.output
    # Mirror's destructive pass has not run: the stale destination-only row remains.
    assert _sql(pg, f"SELECT COUNT(*) FROM {destination_table} WHERE id = 999") == [(1,)]

    monkeypatch.setattr(run_command, "_run_one", real_run_one)
    resumed = runner.invoke(app, ["apply", "plan.json", "--resume"])
    assert resumed.exit_code == 0, resumed.output

    actual = _sql(pg, f"SELECT id, name FROM {destination_table} ORDER BY id")
    expected = [(index, f"row-{index}") for index in range(1, 401)]
    assert actual == expected
    assert len({row[0] for row in actual}) == 400
    plan_id = json.loads((tmp_path / "plan.json").read_text())["plan_id"]
    claim = json.loads((tmp_path / ".drt" / "applied_plans" / f"{plan_id}.json").read_text())
    assert claim["status"] == "success"
    assert claim["completed_wave_indexes"] == [0, 1, 2]
