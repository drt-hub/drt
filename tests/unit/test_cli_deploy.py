"""Tests for `drt deploy github-actions` (#785).

The scaffolder scans the project raw (no ${VAR} expansion), infers drt-core
extras from connector types, and enumerates every required secret into the
generated workflow's env block.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from drt.cli.commands.deploy import _TYPE_TO_EXTRA
from drt.cli.main import app

runner = CliRunner()


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Minimal project: snowflake profile, hubspot sync with ${VAR} + *_env."""
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    syncs = tmp_path / "syncs"
    syncs.mkdir()
    (syncs / "users_to_hubspot.yml").write_text(
        """
name: users_to_hubspot
model: ref('users')
destination:
  type: hubspot
  object_type: contacts
  token_env: HUBSPOT_TOKEN
sync:
  mode: full
""".lstrip()
    )
    (syncs / "events_to_rest.yml").write_text(
        """
name: events_to_rest
model: ref('events')
destination:
  type: rest_api
  url: "https://${API_HOST}/v1/events"
  auth:
    type: bearer
    token_env: EVENTS_API_TOKEN
sync:
  mode: full
""".lstrip()
    )
    (tmp_path / "profiles.yml").write_text(
        """
default:
  type: snowflake
  account: acme-xy12345
  user: DRT_SERVICE
  password_env: SNOWFLAKE_PASSWORD
  database: ANALYTICS
  warehouse: WH
""".lstrip()
    )
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_deploy_writes_workflow_with_secrets_and_extras(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--schedule", "40 3 * * *"])

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()

    assert 'cron: "40 3 * * *"' in workflow
    assert "uses: drt-hub/drt-action@v1" in workflow
    assert 'extras: "snowflake"' in workflow  # inferred from profiles.yml
    # Every *_env value and ${VAR} placeholder becomes a wired secret:
    for name in ("HUBSPOT_TOKEN", "EVENTS_API_TOKEN", "SNOWFLAKE_PASSWORD", "API_HOST"):
        assert f"{name}: ${{{{ secrets.{name} }}}}" in workflow
    # Checklist names the secrets for copy-paste:
    assert "gh secret set HUBSPOT_TOKEN" in result.output


def test_deploy_without_schedule_is_dispatch_only(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()
    assert "workflow_dispatch:" in workflow
    assert '- cron: "40 3 * * *"' not in workflow.replace('#   - cron: "40 3 * * *"', "")


def test_deploy_rejects_malformed_cron(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--schedule", "hourly"])
    assert result.exit_code == 1
    assert "5-field cron" in result.output


def test_deploy_dry_run_prints_without_writing(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "drt-hub/drt-action@v1" in result.output
    assert not (project / ".github/workflows/drt-sync.yml").exists()


def test_deploy_refuses_overwrite_without_force(project: Path) -> None:
    first = runner.invoke(app, ["deploy", "github-actions"])
    assert first.exit_code == 0

    second = runner.invoke(app, ["deploy", "github-actions"])
    assert second.exit_code == 1
    assert "--force" in second.output

    forced = runner.invoke(app, ["deploy", "github-actions", "--force"])
    assert forced.exit_code == 0


def test_deploy_extras_override_and_options(project: Path) -> None:
    result = runner.invoke(
        app,
        [
            "deploy",
            "github-actions",
            "--extras",
            "postgres,bigquery",
            "--select",
            "tag:nightly",
            "--profile",
            "prd",
        ],
    )

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()
    assert 'extras: "postgres,bigquery"' in workflow
    assert 'select: "tag:nightly"' in workflow
    assert 'profile: "prd"' in workflow


def test_deploy_warns_when_repo_profiles_missing(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (project / "profiles.yml").unlink()

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output
    assert "profiles.yml" in result.output  # checklist tells the user to commit one


def test_deploy_outside_project_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["deploy", "github-actions"])
    assert result.exit_code == 1
    assert "drt_project.yml" in result.output


def test_extras_mapping_matches_registered_connectors() -> None:
    """Drift guard: every mapped type must be a registered connector type."""
    from drt.connectors import registry

    registered = set(registry._source_registry) | set(registry._destination_registry)
    unknown = set(_TYPE_TO_EXTRA) - registered
    assert not unknown, f"_TYPE_TO_EXTRA references unregistered connector types: {unknown}"


def test_extras_mapping_matches_pyproject_extras() -> None:
    """Drift guard: every mapped extra must exist in pyproject optional-dependencies."""
    import tomllib

    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    declared = set(data["project"]["optional-dependencies"])
    unknown = set(_TYPE_TO_EXTRA.values()) - declared
    assert not unknown, f"_TYPE_TO_EXTRA references undeclared extras: {unknown}"
