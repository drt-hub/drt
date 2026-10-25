"""Plan → apply → compare on a live warehouse (#1223, 3b): shared driver.

The unit and container tests prove plan/apply on Postgres. This asks the same
question of the warehouses the nightly smoke already reaches: *does the change set
``drt plan`` reported equal what ``drt apply`` actually did?* A mismatch is a bug,
not noise. The source is a throwaway DuckDB file (no extra cost); only the
destination touches the warehouse, and every table is dropped afterwards.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.credentials import DuckDBProfile

from .conftest import unique_table

runner = CliRunner()

# source rows -> destination before -> expected plan: 2 create (3, 4), 1 update (2), 1 unchanged (1)
SOURCE_ROWS = [(1, "alpha"), (2, "bravo-new"), (3, "charlie"), (4, "delta")]
DEST_BEFORE = [(1, "alpha"), (2, "bravo")]
DEST_AFTER = [(1, "alpha"), (2, "bravo-new"), (3, "charlie"), (4, "delta")]


class Warehouse:
    """What a warehouse must provide to be driven by the fidelity check."""

    def __init__(
        self,
        name: str,
        create: Callable[[str, list[tuple[int, str]]], None],
        read: Callable[[str], list[tuple[int, str]]],
        drop: Callable[[str], None],
        destination: Callable[[str], dict[str, Any]],
    ) -> None:
        self.name = name
        self.create = create
        self.read = read
        self.drop = drop
        self.destination = destination


@pytest.fixture
def source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A project whose source is a DuckDB file holding SOURCE_ROWS."""
    duckdb = pytest.importorskip("duckdb")
    from drt.config import credentials as creds

    db = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(db))
    try:
        conn.execute("CREATE TABLE src (id INTEGER, name VARCHAR)")
        conn.executemany("INSERT INTO src VALUES (?, ?)", SOURCE_ROWS)
    finally:
        conn.close()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DRT_PLAN_KEY", "fidelity-smoke-key")
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "fidelity", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    profile = DuckDBProfile(type="duckdb", database=str(db))
    monkeypatch.setattr(creds, "load_profile", lambda *_a, **_k: profile, raising=False)
    return tmp_path


def check_fidelity(source: Path, wh: Warehouse) -> None:
    table = unique_table("DRT_FIDELITY")
    try:
        wh.create(table, DEST_BEFORE)
        (source / "syncs" / "orders.yml").write_text(
            yaml.dump(
                {
                    "name": "orders",
                    "model": "SELECT id, name FROM src",
                    "destination": wh.destination(table),
                    "sync": {"mode": "upsert"},
                }
            )
        )

        planned = runner.invoke(app, ["plan", "orders", "--out", "plan.json"])
        assert planned.exit_code == 0, planned.output
        summary = json.loads((source / "plan.json").read_text())["summary"]
        assert wh.read(table) == DEST_BEFORE, "planning must not write"
        assert (summary["create"], summary["update"], summary["delete"]) == (2, 1, 0)

        applied = runner.invoke(
            app, ["apply", "plan.json", "--auto-approve", "--approved-by", "fidelity-smoke"]
        )
        assert applied.exit_code == 0, applied.output

        after = wh.read(table)
        assert after == DEST_AFTER
        changed = {row for row in after if row not in DEST_BEFORE}
        assert len(changed) == summary["create"] + summary["update"], (
            f"{wh.name}: planned {summary} but apply changed {sorted(changed)}"
        )
    finally:
        wh.drop(table)
