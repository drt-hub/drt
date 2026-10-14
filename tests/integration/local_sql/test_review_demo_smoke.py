"""Live Postgres proof for the review-before-you-write example."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest
from typer.testing import CliRunner

from drt.cli import main as _main  # noqa: F401 -- register commands on the shared app
from drt.cli._app import app
from drt.config.credentials import PostgresProfile

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")

EXAMPLE = Path(__file__).resolve().parents[3] / "examples" / "review-demo"
runner = CliRunner()


def _load_seed() -> ModuleType:
    spec = importlib.util.spec_from_file_location("review_demo_smoke_seed", EXAMPLE / "seed.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pg() -> Iterator[PostgresProfile]:
    require_docker()
    with testcontainers_postgres.PostgresContainer(
        "postgres:16-alpine", username="drt", password="drt", dbname="crm", driver=None
    ) as postgres:
        yield PostgresProfile(
            type="postgres",
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            dbname="crm",
            user="drt",
            password="drt",
        )


def _dsn(pg: PostgresProfile) -> str:
    return f"postgresql://{pg.user}:{pg.password}@{pg.host}:{pg.port}/{pg.dbname}"


def _rows(pg: PostgresProfile) -> list[tuple[Any, ...]]:
    connection = psycopg2.connect(_dsn(pg))
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT company_id, industry FROM companies ORDER BY company_id")
            return cast(list[tuple[Any, ...]], cursor.fetchall())
    finally:
        connection.close()


def test_review_demo_plan_apply_and_guard(
    pg: PostgresProfile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "review-demo"
    shutil.copytree(EXAMPLE, project, ignore=shutil.ignore_patterns(".env", "*.duckdb"))
    home = tmp_path / "home"
    (home / ".drt").mkdir(parents=True)
    shutil.copy(project / "profiles.yml.example", home / ".drt" / "profiles.yml")

    monkeypatch.chdir(project)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("DRT_PLAN_KEY", "review-demo-smoke-key")
    monkeypatch.setenv("REVIEW_DEMO_PG_DSN", _dsn(pg))

    seed = _load_seed()
    data = seed.build_dataset()
    counts = seed.seed_duckdb(project / "warehouse.duckdb", data)
    assert seed.seed_postgres(_dsn(pg), data) == counts
    print(seed.format_counts(counts))
    seeded_output = capsys.readouterr().out
    assert "Rows to fill: 450" in seeded_output
    assert "Different rep-entered values kept: 300" in seeded_output
    assert "CRM-only rows not in warehouse: 12" in seeded_output

    before = dict(_rows(pg))
    planned = runner.invoke(app, ["plan", "fill_industry", "--out", "plan.json"])
    assert planned.exit_code == 0, planned.output
    plan = json.loads((project / "plan.json").read_text())
    assert plan["summary"] == {
        "create": 0,
        "delete": 0,
        "insert": 0,
        "kept_values": 300,
        "replace": 0,
        "total_destination_rows": 1_500,
        "total_source_rows": 1_500,
        "update": 450,
    }
    assert plan["guards"]["tripped"] == []
    assert dict(_rows(pg)) == before, "planning must be read-only"

    from drt.engine import sync as sync_module

    apply_results: list[Any] = []
    real_run_sync = sync_module.run_sync

    def capture_apply_result(*args: Any, **kwargs: Any) -> Any:
        # apply's JSON entry has aggregate counts but not SyncResult.row_errors;
        # retain the real result so a CI assertion shows the connector cause.
        result = real_run_sync(*args, **kwargs)
        apply_results.append(result)
        return result

    monkeypatch.setattr(sync_module, "run_sync", capture_apply_result)
    applied = runner.invoke(
        app,
        [
            "apply",
            "plan.json",
            "--auto-approve",
            "--approved-by",
            "local smoke",
            "--output",
            "json",
        ],
    )
    all_row_errors = [
        error.error_message
        for result in apply_results
        for error in getattr(result, "row_errors", [])
    ]
    apply_diagnostics = {
        "output": applied.output,
        "row_error_count": len(all_row_errors),
        "row_errors": list(dict.fromkeys(all_row_errors))[:20],
        "exception": repr(applied.exception) if applied.exception else None,
    }
    assert applied.exit_code == 0, json.dumps(apply_diagnostics, indent=2)
    assert json.loads(applied.stdout)["result"]["rows_failed"] == 0
    assert seed.verify_postgres_after_apply(_dsn(pg), data) == counts

    after_fill = dict(_rows(pg))
    warehouse = {row["company_id"]: row for row in data.warehouse}
    original = {row["company_id"]: row for row in data.crm}
    for company_id, industry in after_fill.items():
        old_value = original[company_id]["industry"]
        if company_id not in warehouse:
            assert industry == old_value
        elif old_value is None or old_value.strip(" ") == "":
            assert industry == warehouse[company_id]["inferred_industry"]
        else:
            assert industry == old_value

    dangerous = runner.invoke(
        app,
        [
            "plan",
            "dangerous_mirror",
            "--out",
            "dangerous-plan.json",
            "--detailed-exitcode",
        ],
    )
    assert dangerous.exit_code == 2, dangerous.output
    danger_plan = json.loads((project / "dangerous-plan.json").read_text())
    assert danger_plan["summary"]["delete"] == 1_487
    assert danger_plan["summary"]["create"] == 0
    assert danger_plan["guards"]["tripped"] == [
        {
            "guard": "max_delete_pct",
            "limit": 5.0,
            "message": "max_delete_pct: the plan deletes 98.35% of 1512 rows (limit 5.0%)",
            "observed": 98.35,
        }
    ]

    refused = runner.invoke(
        app,
        [
            "apply",
            "dangerous-plan.json",
            "--auto-approve",
            "--approved-by",
            "local smoke",
        ],
    )
    assert refused.exit_code == 1, refused.output
    assert "Change guards tripped" in refused.output
    assert dict(_rows(pg)) == after_fill, "a refused guarded apply must delete nothing"
