"""Live PostgreSQL proof for immutable canary replay and terminal claims (#1255)."""

from __future__ import annotations

import json
import socket
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.credentials import PostgresProfile
from drt.engine.canary import canary_bucket

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")

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
    rejected_id = next(
        value for value in range(1, 401) if 100 <= canary_bucket({"id": value}, ["id"]) < 1000
    )
    _sql(
        pg,
        f"DROP TABLE IF EXISTS {source_table}; DROP TABLE IF EXISTS {destination_table}",
        f"CREATE TABLE {source_table} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        f"CREATE TABLE {destination_table} ("
        "id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        f"CONSTRAINT reject_middle_wave CHECK (id <> {rejected_id}))",
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

    def mutate_after_first_wave(*args: Any, **kwargs: Any) -> tuple[str, dict[str, object], bool]:
        nonlocal wave_calls
        name, entry, had_error = real_run_one(*args, **kwargs)
        wave_calls += 1
        if wave_calls == 1:
            # The spool already exists. Later waves must not see this live
            # mutation or the newly inserted row.
            _sql(
                pg,
                f"UPDATE {source_table} SET name = 'live-source-changed' WHERE id > 1",
                f"INSERT INTO {source_table} VALUES (401, 'late-row')",
            )
        return name, entry, had_error

    monkeypatch.setattr(run_command, "_run_one", mutate_after_first_wave)
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
        f"ALTER TABLE {destination_table} DROP CONSTRAINT reject_middle_wave",
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

    # Simulate a SIGKILL residue: a pending same-host lease with a dead PID.
    # The next canary apply must halt the orphaned claim and remove its values.
    _sql(pg, f"INSERT INTO {source_table} VALUES (401, 'row-401')")
    monkeypatch.setattr(run_command, "_run_one", real_run_one)
    orphan_dir = tmp_path / ".drt" / "applied_plans"
    orphan_spool = orphan_dir / "orphan.spool.jsonl"
    orphan_spool.write_text("value-bearing residue")
    orphan_claim = orphan_dir / "orphan.json"
    orphan_claim.write_text(
        json.dumps(
            {
                "plan_id": "orphan",
                "state": "pending",
                "hostname": socket.gethostname(),
                "pid": 999_999_999,
                "started_at": "2026-01-01T00:00:00+00:00",
                "spool_path": str(orphan_spool),
            }
        )
    )
    swept_plan = runner.invoke(app, ["plan", "canary", "--out", "swept.json"])
    assert swept_plan.exit_code == 0, swept_plan.output
    swept = runner.invoke(app, ["apply", "swept.json", "--auto-approve", "--canary", "100"])
    assert swept.exit_code == 0, swept.output
    assert not orphan_spool.exists()
    assert json.loads(orphan_claim.read_text())["reason"] == "orphaned"
    assert int(_sql(pg, f"SELECT COUNT(*) FROM {destination_table}")[0][0]) == 401
    _sql(pg, f"DELETE FROM {source_table} WHERE id = 401")

    # The live destination declares the required capability; ClickHouse's
    # informational key is rejected by rollout validation.
    from pydantic import ValidationError

    from drt.config.models import SyncConfig
    from drt.destinations.postgres import PostgresDestination

    assert PostgresDestination().supports_keyed_upsert(
        SyncConfig.model_validate(sync_document).destination
    )
    assert (
        PostgresDestination().verify_keyed_upsert(
            SyncConfig.model_validate(sync_document).destination
        )
        is None
    )
    no_unique_table = "dst_canary_no_unique"
    _sql(
        pg,
        f"DROP TABLE IF EXISTS {no_unique_table}",
        f"CREATE TABLE {no_unique_table} (id INTEGER NOT NULL, name TEXT NOT NULL)",
    )
    original_destination = dict(sync_document["destination"])
    sync_document["destination"] = {**original_destination, "table": no_unique_table}
    sync_document["sync"] = {"mode": "upsert"}
    sync_path.write_text(yaml.dump(sync_document))
    no_unique_plan = runner.invoke(app, ["plan", "canary", "--out", "no-unique.json"])
    assert no_unique_plan.exit_code == 0, no_unique_plan.output
    no_unique_apply = runner.invoke(
        app, ["apply", "no-unique.json", "--auto-approve", "--canary", "100"]
    )
    assert no_unique_apply.exit_code == 1
    assert "UNIQUE or PRIMARY KEY" in no_unique_apply.output
    assert int(_sql(pg, f"SELECT COUNT(*) FROM {no_unique_table}")[0][0]) == 0
    sync_document["destination"] = original_destination
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


def test_mysql_canary_key_constraint_is_proved_live() -> None:
    from drt.config.models import MySQLDestinationConfig
    from drt.destinations.mysql import MySQLDestination

    require_docker()
    with testcontainers_mysql.MySqlContainer("mysql:8.0") as mysql:
        connection = pymysql.connect(
            host=mysql.get_container_host_ip(),
            port=int(mysql.get_exposed_port(3306)),
            user=mysql.username,
            password=mysql.password,
            database=mysql.dbname,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "CREATE TABLE canary_unique (tenant_id INTEGER, id INTEGER, "
                    "name TEXT, UNIQUE KEY canary_key (tenant_id, id))"
                )
                cursor.execute(
                    "CREATE TABLE canary_no_unique (tenant_id INTEGER, id INTEGER, name TEXT)"
                )
            connection.commit()
        finally:
            connection.close()

        common = {
            "type": "mysql",
            "host": mysql.get_container_host_ip(),
            "port": int(mysql.get_exposed_port(3306)),
            "dbname": mysql.dbname,
            "user": mysql.username,
            "password": mysql.password,
            "upsert_key": ["tenant_id", "id"],
        }
        destination = MySQLDestination()
        unique = MySQLDestinationConfig(**common, table="canary_unique")
        no_unique = MySQLDestinationConfig(**common, table="canary_no_unique")

        assert destination.supports_keyed_upsert(unique)
        assert destination.verify_keyed_upsert(unique) is None
        assert "UNIQUE or PRIMARY KEY" in str(destination.verify_keyed_upsert(no_unique))
