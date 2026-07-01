"""Tests for `drt docs generate --format json` + manifest.json schema v1 (P2 of #499)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.docs import (
    SCHEMA_VERSION,
    Destination,
    Edge,
    Manifest,
    Project,
    Source,
    Sync,
    SyncStateSnapshot,
    build_manifest,
)

runner = CliRunner()


def _write_project(project_dir: Path, profile: str = "bq_prod", name: str = "demo") -> None:
    (project_dir / "drt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": name,
                "version": "0.1",
                "profile": profile,
                "source": {"type": "bigquery"},
            }
        )
    )


def _write_sync(project_dir: Path, filename: str, sync: dict[str, object]) -> None:
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    (syncs_dir / filename).write_text(yaml.safe_dump(sync))


def _pg_sync(name: str, table: str) -> dict[str, object]:
    return {
        "name": name,
        "model": f"SELECT * FROM {name}",
        "destination": {
            "type": "postgres",
            "host": "localhost",
            "dbname": "warehouse",
            "table": table,
            "upsert_key": ["id"],
        },
        "sync": {"mode": "upsert"},
    }


def _write_state(project_dir: Path, payload: dict[str, dict[str, object]]) -> None:
    state_dir = project_dir / ".drt"
    state_dir.mkdir(exist_ok=True)
    (state_dir / "state.json").write_text(json.dumps(payload))


class TestManifestRoundTrip:
    def test_round_trip_preserves_all_fields(self) -> None:
        original = Manifest(
            schema_version=SCHEMA_VERSION,
            drt_version="0.8.0",
            generated_at="2026-05-14T01:00:00Z",
            project=Project(name="demo", profile="bq_prod"),
            syncs=[
                Sync(
                    name="users_to_hubspot",
                    source="bq_prod",
                    destination="dest_hubspot_x",
                    mode="upsert",
                    description="Sync users",
                    tags=("production", "crm"),
                    state=SyncStateSnapshot(
                        last_sync_at="2026-05-14T00:00:00Z",
                        last_cursor_value="2026-05-13T23:55:00Z",
                        rows_synced=1248,
                        last_status="success",
                        last_error=None,
                    ),
                ),
            ],
            sources=[Source(name="bq_prod", type="bigquery")],
            destinations=[Destination(name="dest_hubspot_x", type="hubspot", label="hubspot (x)")],
            edges=[
                Edge(kind="source_to_sync", from_="bq_prod", to="users_to_hubspot"),
                Edge(kind="sync_to_destination", from_="users_to_hubspot", to="dest_hubspot_x"),
            ],
        )

        roundtrip = Manifest.from_dict(original.to_dict())
        assert roundtrip == original

    def test_edge_from_is_serialized_as_from_keyword(self) -> None:
        m = Manifest(
            schema_version=SCHEMA_VERSION,
            drt_version="0.8.0",
            edges=[Edge(kind="lookup", from_="a", to="b")],
        )
        data = m.to_dict()
        assert data["edges"][0] == {"kind": "lookup", "from": "a", "to": "b"}

    def test_state_omitted_when_none(self) -> None:
        m = Manifest(
            schema_version=SCHEMA_VERSION,
            drt_version="0.8.0",
            syncs=[Sync(name="s", source="src", destination="dst", mode="full")],
        )
        sync_dict = m.to_dict()["syncs"][0]
        assert "state" not in sync_dict


class TestBuildManifestWithState:
    def test_include_state_attaches_renamed_fields(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        _write_sync(tmp_path, "users.yml", _pg_sync("users", "public.users"))
        _write_state(
            tmp_path,
            {
                "users": {
                    "sync_name": "users",
                    "last_run_at": "2026-05-14T00:00:00Z",
                    "records_synced": 1248,
                    "status": "success",
                    "error": None,
                    "last_cursor_value": "2026-05-13T23:55:00Z",
                }
            },
        )

        manifest = build_manifest(tmp_path, include_state=True)
        sync = next(s for s in manifest.syncs if s.name == "users")

        assert sync.state is not None
        # Public schema names — renamed from internal SyncState
        assert sync.state.last_sync_at == "2026-05-14T00:00:00Z"
        assert sync.state.rows_synced == 1248
        assert sync.state.last_status == "success"
        assert sync.state.last_error is None
        assert sync.state.last_cursor_value == "2026-05-13T23:55:00Z"

    def test_include_state_false_skips_state(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        _write_sync(tmp_path, "users.yml", _pg_sync("users", "public.users"))
        _write_state(
            tmp_path,
            {
                "users": {
                    "sync_name": "users",
                    "last_run_at": "2026-05-14T00:00:00Z",
                    "records_synced": 1248,
                    "status": "success",
                    "error": None,
                    "last_cursor_value": None,
                }
            },
        )

        manifest = build_manifest(tmp_path, include_state=False)
        sync = next(s for s in manifest.syncs if s.name == "users")
        assert sync.state is None

    def test_sync_without_state_row(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        _write_sync(tmp_path, "users.yml", _pg_sync("users", "public.users"))
        # No state.json on disk
        manifest = build_manifest(tmp_path, include_state=True)
        sync = next(s for s in manifest.syncs if s.name == "users")
        assert sync.state is None

    def test_generated_at_is_iso8601_z(self, tmp_path: Path) -> None:
        _write_project(tmp_path)
        manifest = build_manifest(tmp_path, include_state=False)
        # ISO-8601 with Z suffix (UTC)
        assert manifest.generated_at.endswith("Z")
        assert "T" in manifest.generated_at

    def test_project_block_populated(self, tmp_path: Path) -> None:
        _write_project(tmp_path, profile="my_profile", name="my_project")
        manifest = build_manifest(tmp_path)
        assert manifest.project is not None
        assert manifest.project.name == "my_project"
        assert manifest.project.profile == "my_profile"


class TestDocsGenerateJsonCLI:
    def test_writes_manifest_json_to_output_dir(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_project(tmp_path)
        _write_sync(tmp_path, "users.yml", _pg_sync("users", "public.users"))

        result = runner.invoke(
            app, ["docs", "generate", "--format", "json", "--output", "out"]
        )

        assert result.exit_code == 0
        manifest_path = tmp_path / "out" / "manifest.json"
        assert manifest_path.exists()

        data = json.loads(manifest_path.read_text())
        assert data["schema_version"] == 1
        assert "drt_version" in data
        assert data["project"]["name"] == "demo"
        assert len(data["syncs"]) == 1
        assert data["syncs"][0]["name"] == "users"

    def test_default_output_is_target_docs(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_project(tmp_path)
        result = runner.invoke(app, ["docs", "generate", "--format", "json"])
        assert result.exit_code == 0
        assert (tmp_path / "target" / "docs" / "manifest.json").exists()

    def test_no_state_flag_omits_state_block(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        _write_project(tmp_path)
        _write_sync(tmp_path, "users.yml", _pg_sync("users", "public.users"))
        _write_state(
            tmp_path,
            {
                "users": {
                    "sync_name": "users",
                    "last_run_at": "2026-05-14T00:00:00Z",
                    "records_synced": 1,
                    "status": "success",
                    "error": None,
                    "last_cursor_value": None,
                }
            },
        )

        result = runner.invoke(
            app,
            ["docs", "generate", "--format", "json", "--output", "out", "--no-state"],
        )
        assert result.exit_code == 0
        data = json.loads((tmp_path / "out" / "manifest.json").read_text())
        assert "state" not in data["syncs"][0]

    def test_html_writes_static_site(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_project(tmp_path)
        result = runner.invoke(app, ["docs", "generate", "--format", "html"])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "target" / "docs" / "index.html").exists()
