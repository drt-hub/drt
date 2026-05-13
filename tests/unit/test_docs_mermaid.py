"""Tests for Mermaid project documentation generation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.docs import build_manifest, render_mermaid

runner = CliRunner()


def _write_project(project_dir: Path, profile: str = "bigquery_prod") -> None:
    (project_dir / "drt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "demo",
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


def _rest_sync(name: str, url: str = "https://example.com/api") -> dict[str, object]:
    return {
        "name": name,
        "model": f"SELECT * FROM {name}",
        "destination": {
            "type": "rest_api",
            "url": url,
            "method": "POST",
        },
        "sync": {"mode": "upsert"},
    }


def _postgres_sync(
    name: str,
    table: str,
    lookups: dict[str, object] | None = None,
) -> dict[str, object]:
    destination: dict[str, object] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "warehouse",
        "table": table,
        "upsert_key": ["id"],
    }
    if lookups:
        destination["lookups"] = lookups
    return {
        "name": name,
        "model": f"SELECT * FROM {name}",
        "destination": destination,
        "sync": {"mode": "upsert"},
    }


def test_mermaid_renders_basic_project_graph(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "users.yml", _rest_sync("users"))

    mermaid = render_mermaid(build_manifest(tmp_path))

    assert mermaid.startswith("graph LR\n")
    assert "subgraph Sources" in mermaid
    assert "src_bigquery_prod[bigquery_prod]" in mermaid
    assert "sync_users{{users<br/>upsert}}" in mermaid
    assert "sync_users -->|load| dst_rest_api_example_com_api" in mermaid


def test_mermaid_deduplicates_shared_source(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "users.yml", _rest_sync("users", "https://example.com/users"))
    _write_sync(tmp_path, "accounts.yml", _rest_sync("accounts", "https://example.com/accounts"))

    mermaid = render_mermaid(build_manifest(tmp_path))

    assert mermaid.count("src_bigquery_prod[bigquery_prod]") == 1
    assert "src_bigquery_prod -->|extract| sync_users" in mermaid
    assert "src_bigquery_prod -->|extract| sync_accounts" in mermaid


def test_mermaid_renders_lookup_edges_between_syncs(tmp_path: Path) -> None:
    _write_project(tmp_path, profile="postgres_replica")
    _write_sync(tmp_path, "users.yml", _postgres_sync("users", "public.users"))
    _write_sync(
        tmp_path,
        "accounts.yml",
        _postgres_sync(
            "accounts",
            "public.accounts",
            lookups={
                "user_id": {
                    "table": "users",
                    "match": {"id": "user_id"},
                    "select": "id",
                }
            },
        ),
    )

    mermaid = render_mermaid(build_manifest(tmp_path))

    assert "sync_accounts -.lookup.-> sync_users" in mermaid


def test_docs_generate_mermaid_prints_to_stdout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path, "users.yml", _rest_sync("users"))
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["docs", "generate", "--format", "mermaid"])

    assert result.exit_code == 0
    assert result.output.startswith("graph LR\n")
    assert "sync_users{{users<br/>upsert}}" in result.output


def test_docs_generate_empty_project_exits_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["docs", "generate", "--format", "mermaid"])

    assert result.exit_code == 0
    assert "graph LR" not in result.output
    assert "No sync definitions found" in result.stderr


def test_docs_help_shows_generate_and_serve() -> None:
    result = runner.invoke(app, ["docs", "--help"])

    assert result.exit_code == 0
    assert "generate" in result.output
    assert "serve" in result.output


def test_docs_serve_is_clear_placeholder() -> None:
    result = runner.invoke(app, ["docs", "serve"])

    assert result.exit_code == 1
    assert "not implemented yet" in result.output
    assert "#501" in result.output
