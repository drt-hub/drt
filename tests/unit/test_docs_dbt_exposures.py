"""Tests for ``drt docs generate --format dbt-exposures`` (#781)."""

from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def _write_project(project_dir: Path) -> None:
    (project_dir / "drt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "demo",
                "version": "0.1",
                "profile": "bq_prod",
                "source": {"type": "bigquery"},
            }
        )
    )


def _write_sync(
    project_dir: Path,
    filename: str,
    *,
    name: str,
    model: str,
    destination_type: str = "hubspot",
    mode: str = "upsert",
) -> None:
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    destination: dict[str, object]
    if destination_type == "hubspot":
        destination = {"type": "hubspot", "object_type": "contacts"}
    else:
        destination = {
            "type": "postgres",
            "host": "localhost",
            "dbname": "warehouse",
            "table": f"public.{name}",
            "upsert_key": ["id"],
        }
    (syncs_dir / filename).write_text(
        yaml.safe_dump(
            {
                "name": name,
                "model": model,
                "destination": destination,
                "sync": {"mode": mode},
            },
            sort_keys=False,
        )
    )


def test_cli_emits_valid_ref_only_dbt_exposures_and_skip_note(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync(
        tmp_path,
        "z_users.yml",
        name="users_to_hubspot",
        model="ref('mart_users')",
    )
    _write_sync(
        tmp_path,
        "a_raw.yml",
        name="raw_accounts",
        model="SELECT * FROM analytics.accounts",
        destination_type="postgres",
    )
    _write_sync(
        tmp_path,
        "m_override.yml",
        name="overridden_users",
        model="ref('overridden_model')",
    )
    models_dir = tmp_path / "syncs" / "models"
    models_dir.mkdir()
    (models_dir / "overridden_model.sql").write_text("SELECT * FROM unrelated_local_table")

    result = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert result.exit_code == 0
    parsed = yaml.safe_load(result.output)
    assert parsed == {
        "exposures": [
            {
                "name": "drt_users_to_hubspot",
                "type": "application",
                "maturity": "high",
                "owner": {"name": "drt"},
                "depends_on": ["ref('mart_users')"],
                "description": ("drt sync users_to_hubspot -> hubspot (upsert). Managed by drt."),
                "url": "docs/sync/users-to-hubspot.html",
                "meta": {
                    "drt": {
                        "sync": "users_to_hubspot",
                        "destination": "hubspot",
                        "mode": "upsert",
                    }
                },
            }
        ]
    }
    assert (
        '# Skipped sync "raw_accounts": model is not ref(...); '
        "raw-SQL lineage parsing is out of scope."
    ) in result.output
    assert (
        '# Skipped sync "overridden_users": local SQL override takes precedence over ref(...); '
        "dbt lineage would be inaccurate."
    ) in result.output
    assert "analytics.accounts" not in result.output
    assert "overridden_model" not in result.output
    assert "unrelated_local_table" not in result.output


def test_output_is_identical_before_and_after_html_docs_generation(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync(
        tmp_path,
        "users.yml",
        name="users_to_hubspot",
        model="ref('mart_users')",
    )

    before = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])
    html = runner.invoke(app, ["docs", "generate", "--format", "html", "--no-state"])
    after = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert before.exit_code == html.exit_code == after.exit_code == 0
    assert (tmp_path / "target" / "docs" / "sync" / "users-to-hubspot.html").is_file()
    assert before.output == after.output
    assert yaml.safe_load(before.output)["exposures"][0]["url"] == (
        "docs/sync/users-to-hubspot.html"
    )


def test_output_is_sorted_by_sync_name_and_byte_identical(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync(
        tmp_path,
        "a_file.yml",
        name="zebra_sync",
        model='ref("zebra_model")',
        destination_type="postgres",
    )
    _write_sync(
        tmp_path,
        "z_file.yml",
        name="alpha_sync",
        model="ref( 'alpha_model' )",
    )

    first = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])
    second = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert first.exit_code == second.exit_code == 0
    assert first.output == second.output
    parsed = yaml.safe_load(first.output)
    assert [entry["name"] for entry in parsed["exposures"]] == [
        "drt_alpha_sync",
        "drt_zebra_sync",
    ]


def test_empty_ref_set_is_valid_yaml_with_sorted_skip_notes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync(
        tmp_path,
        "one.yml",
        name="raw_only",
        model="SELECT 1",
        destination_type="postgres",
    )

    result = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert result.exit_code == 0
    assert yaml.safe_load(result.output) == {"exposures": []}
    assert '# Skipped sync "raw_only"' in result.output


def test_exposure_name_is_a_valid_dbt_identifier_and_meta_keeps_original(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    original_name = "123 Users.to HubSpot!"
    _write_sync(
        tmp_path,
        "punctuation.yml",
        name=original_name,
        model="ref('mart_users')",
    )

    result = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert result.exit_code == 0
    exposure = yaml.safe_load(result.output)["exposures"][0]
    assert exposure["name"] == "drt_123_users_to_hubspot"
    assert exposure["meta"]["drt"]["sync"] == original_name


def test_page_slug_collisions_omit_only_affected_urls(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync(tmp_path, "dash.yml", name="a-b", model="ref('dash_model')")
    _write_sync(tmp_path, "underscore.yml", name="a_b", model="ref('underscore_model')")
    _write_sync(tmp_path, "unique.yml", name="unique", model="ref('unique_model')")

    result = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert result.exit_code == 0
    exposures = {
        exposure["meta"]["drt"]["sync"]: exposure
        for exposure in yaml.safe_load(result.output)["exposures"]
    }
    assert "url" not in exposures["a-b"]
    assert "url" not in exposures["a_b"]
    assert exposures["unique"]["url"] == "docs/sync/unique.html"
    assert {exposures["a-b"]["name"], exposures["a_b"]["name"]} == {
        "drt_a_b",
        "drt_a_b_2",
    }


def _write_sync_with(project_dir: Path, name: str, extra: dict[str, object]) -> None:
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    (syncs_dir / f"{name}.yml").write_text(
        yaml.safe_dump(
            {
                "name": name,
                "model": "ref('mart_users')",
                "destination": {"type": "hubspot", "object_type": "contacts"},
                "sync": {"mode": "upsert"},
                **extra,
            },
            sort_keys=False,
        )
    )


def test_owner_maturity_and_declared_freshness_flow_into_the_exposure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync_with(
        tmp_path,
        "users_to_hubspot",
        {
            "meta": {
                "owner": "Growth team",
                "owner_email": "growth@example.com",
                "maturity": "low",
            },
            "alerts": {"on_stale": {"max_age": "6h", "never_run": "error"}},
        },
    )

    result = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"])

    assert result.exit_code == 0
    [exposure] = yaml.safe_load(result.output)["exposures"]
    assert exposure["owner"] == {"name": "Growth team"}  # no email in a dbt-bound artifact
    assert "growth@example.com" not in result.output
    assert exposure["maturity"] == "low"
    assert exposure["meta"]["drt"]["freshness"] == {"max_age_seconds": 21600, "never_run": "error"}


def test_a_sync_without_meta_keeps_the_previous_defaults(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync_with(tmp_path, "users_to_hubspot", {})

    [exposure] = yaml.safe_load(
        runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"]).output
    )["exposures"]

    assert exposure["owner"] == {"name": "drt"} and exposure["maturity"] == "high"
    assert "freshness" not in exposure["meta"]["drt"]


def test_regeneration_with_meta_and_freshness_is_byte_identical(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync_with(
        tmp_path,
        "users_to_hubspot",
        {"meta": {"owner": "Growth"}, "alerts": {"on_stale": {"max_age": "1d"}}},
    )

    first = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"]).output
    second = runner.invoke(app, ["docs", "generate", "--format", "dbt-exposures"]).output

    assert first == second


def test_unknown_meta_keys_are_rejected(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync_with(tmp_path, "users_to_hubspot", {"meta": {"ownr": "typo"}})

    result = runner.invoke(app, ["validate"])

    assert result.exit_code != 0


def test_the_public_renderer_never_emits_an_email_even_for_a_full_labels_manifest(
    tmp_path: Path, monkeypatch
) -> None:
    from drt.docs.builder import build_manifest
    from drt.docs.dbt_exposures import render_dbt_exposures

    monkeypatch.chdir(tmp_path)
    _write_project(tmp_path)
    _write_sync_with(
        tmp_path, "users_to_hubspot", {"meta": {"owner": "G", "owner_email": "g@example.com"}}
    )
    manifest = build_manifest(Path("."), full_labels=True)
    assert manifest.syncs[0].meta is not None and manifest.syncs[0].meta.owner_email

    out = render_dbt_exposures(manifest, {"users_to_hubspot": "mart_users"})

    assert "g@example.com" not in out and "email" not in out
