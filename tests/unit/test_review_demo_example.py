"""Guard the zero-credential review demo against config and fixture drift."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from drt.config.parser import load_syncs

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "review-demo"


def _load_seed() -> ModuleType:
    spec = importlib.util.spec_from_file_location("review_demo_seed", EXAMPLE / "seed.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip(" ") == "")


def test_review_demo_syncs_load_through_the_real_parser() -> None:
    syncs = {sync.name: sync for sync in load_syncs(EXAMPLE)}

    assert set(syncs) == {"dangerous_mirror", "fill_industry"}

    fill = syncs["fill_industry"]
    assert fill.sync.mode == "upsert"
    # Planner support for update_only is the documented gap; max_creates=0 is
    # the executable, fail-closed substitute until that gap is closed.
    assert fill.sync.match_policy == "upsert"
    assert fill.sync.write_policy == "fill_empty"
    assert fill.sync.guards is not None
    assert fill.sync.guards.max_creates == 0
    assert fill.sync.guards.max_deletes == 0

    dangerous = syncs["dangerous_mirror"]
    assert dangerous.sync.mode == "mirror"
    assert dangerous.sync.mirror is not None
    assert dangerous.sync.mirror.strategy == "destination"
    assert dangerous.sync.guards is not None
    assert dangerous.sync.guards.max_creates == 0
    assert dangerous.sync.guards.max_delete_pct == 5


def test_review_demo_seed_counts_are_derived_and_duckdb_is_real(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    seed = _load_seed()
    data = seed.build_dataset()
    counts = seed.expected_counts(data)

    warehouse = {row["company_id"]: row for row in data.warehouse}
    overlap = [row for row in data.crm if row["company_id"] in warehouse]
    independently_counted = {
        "warehouse_rows": len(warehouse),
        "crm_rows": len(data.crm),
        "rows_to_fill": sum(_empty(row["industry"]) for row in overlap),
        "values_kept": sum(
            not _empty(row["industry"])
            and row["industry"] != warehouse[row["company_id"]]["inferred_industry"]
            for row in overlap
        ),
        "rows_not_in_warehouse": sum(row["company_id"] not in warehouse for row in data.crm),
        "dangerous_deletes": len(data.crm) - seed.DANGEROUS_SUBSET_COUNT,
    }

    assert (
        counts
        == independently_counted
        == {
            "warehouse_rows": 1_500,
            "crm_rows": 1_512,
            "rows_to_fill": 450,
            "values_kept": 300,
            "rows_not_in_warehouse": 12,
            "dangerous_deletes": 1_487,
        }
    )

    database = tmp_path / "warehouse.duckdb"
    assert seed.seed_duckdb(database, data) == counts
    connection = duckdb.connect(str(database), read_only=True)
    try:
        assert connection.execute("SELECT count(*) FROM enriched_companies").fetchone() == (1_500,)
        assert connection.execute(
            "SELECT count(DISTINCT company_id) FROM enriched_companies"
        ).fetchone() == (1_500,)
    finally:
        connection.close()


def test_review_demo_partial_upsert_columns_are_nullable() -> None:
    seed_source = (EXAMPLE / "seed.py").read_text()

    assert (
        "CREATE TABLE companies (\n"
        "                    company_id BIGINT PRIMARY KEY,\n"
        "                    domain TEXT,\n"
        "                    name TEXT,\n"
        "                    industry TEXT\n" in seed_source
    )
    assert "every unselected column must be nullable or have a" in seed_source


def test_review_demo_shell_contracts(tmp_path: Path) -> None:
    demo = tmp_path / "review-demo"
    demo.mkdir()
    script = demo / "run-demo.sh"
    script.write_text((EXAMPLE / "run-demo.sh").read_text())
    script.chmod(0o755)
    (demo / ".env").write_text(
        "POSTGRES_USER=drt\nPOSTGRES_PASSWORD=drt\nPOSTGRES_DB=crm\nPOSTGRES_HOST_PORT=56543\n"
    )

    mcp_env = subprocess.run(
        ["bash", str(script), "mcp-env"],
        cwd=demo,
        text=True,
        capture_output=True,
        check=False,
    )
    assert mcp_env.returncode == 0, mcp_env.stderr
    assert "postgresql://drt:drt@localhost:56543/crm" in mcp_env.stdout
    assert f'"HOME": "{demo}/.demo-home"' in mcp_env.stdout
    assert '"DRT_PLAN_KEY": "review-demo-fixed-local-key"' in mcp_env.stdout
    assert "drt mcp run" in mcp_env.stdout

    invalid_reset = subprocess.run(
        ["bash", str(script), "--reset", "plan"],
        cwd=demo,
        text=True,
        capture_output=True,
        check=False,
    )
    assert invalid_reset.returncode == 2
    assert "--reset is only valid with the all or setup stage" in invalid_reset.stderr


def test_review_demo_compose_and_docs_contracts() -> None:
    assert (EXAMPLE / "docker-compose.yml").read_text().startswith("name: drt-review-demo\n")
    assert "REVIEW_DEMO_PG_DSN=" not in (EXAMPLE / ".env.example").read_text()

    readme = (EXAMPLE / "README.md").read_text()
    assert "Partial-upsert schema gotcha" in readme
    assert "explicitly named `drt-review-demo`" in readme

    prompts = (EXAMPLE / "AGENT_PROMPTS.md").read_text()
    assert "./run-demo.sh mcp-env" in prompts
    assert "returns the generic sync-configuration JSON" in prompts
