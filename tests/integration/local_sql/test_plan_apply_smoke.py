"""Live Postgres proof for ``drt plan`` / ``drt apply`` (#1216, #1217, #1218).

The unit tests fake the engine. This drives the real CLI against a real
Postgres source and destination, so the diff, the value hashes, the drift
check and the guard all run on actual rows. Skips without Docker.
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
        "postgres:16-alpine", username="admin", password="adminpass", dbname="testdb", driver=None
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
    conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.dbname, user=pg.user, password=pg.password
    )
    try:
        rows: list[tuple[Any, ...]] = []
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
                if cur.description:
                    rows = cur.fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


@pytest.fixture
def project(
    pg: PostgresProfile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Path:
    """A project whose source and destination tables are private to this test."""
    from drt.config import credentials as creds

    suffix = request.node.name.replace("test_", "")[:30].replace("[", "_").replace("]", "")
    src, dst = f"src_{suffix}", f"dst_{suffix}"
    _sql(
        pg,
        f"DROP TABLE IF EXISTS {src}; DROP TABLE IF EXISTS {dst}",
        f"CREATE TABLE {src} (id INTEGER, name TEXT)",
        f"INSERT INTO {src} VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie')",
        f"CREATE TABLE {dst} (id INTEGER PRIMARY KEY, name TEXT)",
        f"INSERT INTO {dst} VALUES (1, 'alpha')",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DRT_PLAN_KEY", "smoke-test-key")
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "smoke", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    monkeypatch.setattr(creds, "load_profile", lambda *_a, **_k: pg, raising=False)
    project_files = {"src": src, "dst": dst}
    (tmp_path / "tables.json").write_text(json.dumps(project_files))
    return tmp_path


def _write_sync(project: Path, pg: PostgresProfile, mode: str = "upsert", **sync: Any) -> None:
    tables = json.loads((project / "tables.json").read_text())
    (project / "syncs" / "orders.yml").write_text(
        yaml.dump(
            {
                "name": "orders",
                "model": f"SELECT id, name FROM {tables['src']}",
                "destination": {
                    "type": "postgres",
                    "host": pg.host,
                    "port": pg.port,
                    "dbname": pg.dbname,
                    "user": pg.user,
                    "password": pg.password,
                    "table": tables["dst"],
                    "upsert_key": ["id"],
                },
                "sync": {"mode": mode, **sync},
            }
        )
    )


def _dest(project: Path, pg: PostgresProfile) -> list[tuple[Any, ...]]:
    table = json.loads((project / "tables.json").read_text())["dst"]
    return _sql(pg, f"SELECT id, name FROM {table} ORDER BY id")


def test_plan_then_apply_writes_exactly_what_was_planned(
    project: Path, pg: PostgresProfile
) -> None:
    _write_sync(project, pg)

    planned = runner.invoke(app, ["plan", "orders", "--out", "plan.json"])
    assert planned.exit_code == 0, planned.output
    doc = json.loads((project / "plan.json").read_text())
    assert doc["summary"]["create"] == 2 and doc["summary"]["update"] == 0
    assert "charlie" not in json.dumps(doc)  # keys and hashes only, never row values
    assert _dest(project, pg) == [(1, "alpha")]  # planning wrote nothing

    applied = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--approved-by", "smoke"])
    assert applied.exit_code == 0, applied.output
    assert _dest(project, pg) == [(1, "alpha"), (2, "bravo"), (3, "charlie")]

    again = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])
    assert again.exit_code == 1 and "already applied" in again.output
    assert len(_dest(project, pg)) == 3


def test_apply_aborts_without_writing_when_the_destination_drifts(
    project: Path, pg: PostgresProfile
) -> None:
    _write_sync(project, pg)
    assert runner.invoke(app, ["plan", "orders", "--out", "plan.json"]).exit_code == 0
    table = json.loads((project / "tables.json").read_text())["dst"]
    _sql(pg, f"INSERT INTO {table} VALUES (2, 'someone else wrote this')")  # row 2 now exists

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1 and "no longer matches" in result.output
    assert _dest(project, pg) == [(1, "alpha"), (2, "someone else wrote this")]  # untouched
    assert not (project / ".drt" / "applied_plans").exists()


def test_apply_aborts_when_a_value_changes_but_key_and_action_do_not(
    project: Path, pg: PostgresProfile
) -> None:
    _write_sync(project, pg)
    assert runner.invoke(app, ["plan", "orders", "--out", "plan.json"]).exit_code == 0
    src = json.loads((project / "tables.json").read_text())["src"]
    _sql(pg, f"UPDATE {src} SET name = 'CHANGED AFTER REVIEW' WHERE id = 2")

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1 and "no longer matches" in result.output
    assert "CHANGED AFTER REVIEW" not in result.output
    assert _dest(project, pg) == [(1, "alpha")]


def test_a_tripped_guard_blocks_apply_until_it_is_forced(
    project: Path, pg: PostgresProfile
) -> None:
    src = json.loads((project / "tables.json").read_text())["src"]
    _sql(pg, f"DELETE FROM {src} WHERE id <> 1")  # the source now has only row 1
    table = json.loads((project / "tables.json").read_text())["dst"]
    _sql(pg, f"INSERT INTO {table} VALUES (7, 'g'), (8, 'h'), (9, 'i')")
    _write_sync(
        project,
        pg,
        mode="mirror",
        mirror={"strategy": "destination"},
        guards={"max_deletes": 1, "max_delete_pct": 90},
    )

    planned = runner.invoke(app, ["plan", "orders", "--out", "plan.json"])
    assert planned.exit_code == 0, planned.output
    doc = json.loads((project / "plan.json").read_text())
    assert doc["summary"]["delete"] == 3
    assert [t["guard"] for t in doc["guards"]["tripped"]] == ["max_deletes"]

    refused = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])
    assert refused.exit_code == 1 and "Change guards tripped" in refused.output
    assert len(_dest(project, pg)) == 4  # nothing was deleted

    # The refusal did not burn the plan; forcing it is an explicit, recorded act.
    forced = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--force-guards"])
    assert forced.exit_code == 0, forced.output
    assert _dest(project, pg) == [(1, "alpha")]
    results = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert results["results"][0]["guards_forced"][0]["guard"] == "max_deletes"


def test_the_ci_path_plan_all_then_apply_the_directory(project: Path, pg: PostgresProfile) -> None:
    """What the generated workflows run: `drt plan --all`, then `drt apply <dir>`."""
    _write_sync(project, pg)

    planned = runner.invoke(app, ["plan", "--all", "--out-dir", "plans", "--output", "markdown"])
    assert planned.exit_code == 0, planned.output
    assert "<!-- drt-plan -->" in planned.stdout and "charlie" not in planned.stdout
    manifest = json.loads((project / "plans" / "manifest.json").read_text())
    assert [e["status"] for e in manifest["syncs"]] == ["planned"]

    applied = runner.invoke(
        app, ["apply", "plans", "--auto-approve", "--approved-by", "merge of PR #1 by @smoke"]
    )
    assert applied.exit_code == 0, applied.output
    assert _dest(project, pg) == [(1, "alpha"), (2, "bravo"), (3, "charlie")]

    again = runner.invoke(app, ["apply", "plans", "--auto-approve"])
    assert again.exit_code == 1 and "already applied" in again.output
