"""dbt-derived column contracts (#896, ADR 0017 section 3)."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli._plan_runner import sync_fingerprint
from drt.cli.main import app
from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import DestinationConfig, SyncOptions
from drt.config.parser import load_syncs, load_syncs_safe
from drt.config.schema import generate_sync_schema, validate_yaml_against_schema
from drt.destinations.base import SyncResult
from drt.docs.builder import build_manifest
from drt.engine.plan import config_hash
from drt.engine.sync import run_sync
from drt.integrations.dbt import portable_type_from_dbt

runner = CliRunner()


def _node(
    *,
    name: str = "customers",
    package: str = "analytics",
    relation_name: str = '"warehouse"."marts"."customers"',
    columns: dict[str, dict[str, Any]] | None = None,
    enforced: bool = True,
) -> dict[str, Any]:
    return {
        "resource_type": "model",
        "name": name,
        "package_name": package,
        "relation_name": relation_name,
        "config": {"contract": {"enforced": enforced}},
        "columns": columns
        if columns is not None
        else {
            "id": {"name": "id", "data_type": "bigint", "description": "Customer key"},
            "email": {"name": "email", "data_type": "varchar(255)"},
        },
    }


def _write_manifest(
    project: Path,
    nodes: dict[str, dict[str, Any]],
    relative_path: str = "target/manifest.json",
) -> Path:
    path = project / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"metadata": {"dbt_schema_version": "v12"}, "nodes": nodes}))
    return path


def _sync_data(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "name": "customers_to_api",
        "model": "ref('customers')",
        "destination": {"type": "rest_api", "url": "https://example.invalid"},
        "contract": {"enforced": True, "source": "dbt"},
        "sync": {"batch_size": 2},
    }
    value.update(overrides)
    return value


def _write_sync(project: Path, **overrides: Any) -> Path:
    syncs = project / "syncs"
    syncs.mkdir(parents=True, exist_ok=True)
    path = syncs / "customers.yml"
    path.write_text(yaml.safe_dump(_sync_data(**overrides), sort_keys=False))
    return path


def _write_project(project: Path) -> None:
    (project / "drt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "dbt-contract-test",
                "version": "1",
                "profile": "warehouse",
                "source": {"type": "bigquery"},
            }
        )
    )


@pytest.mark.parametrize(
    ("dbt_type", "portable"),
    [
        ("varchar(255)", "string"),
        ("TEXT", "string"),
        ("string", "string"),
        ("int", "integer"),
        ("BIGINT", "integer"),
        ("smallint", "integer"),
        ("numeric(10, 2)", "number"),
        ("decimal(8,1)", "number"),
        ("FLOAT", "number"),
        ("double", "number"),
        ("real", "number"),
        ("bool", "boolean"),
        ("BOOLEAN", "boolean"),
        ("timestamp_ntz(9)", "timestamp"),
        ("TIMESTAMP WITH TIME ZONE", "timestamp"),
        ("datetime", "timestamp"),
        ("date", "date"),
        ("json", "json"),
        ("JSONB", "json"),
        ("variant", "json"),
        ("object", "json"),
        ("array", "json"),
    ],
)
def test_dbt_type_mapping(dbt_type: str, portable: str) -> None:
    assert portable_type_from_dbt(dbt_type) == portable


def test_loader_derives_columns_from_real_manifest(tmp_path: Path) -> None:
    columns = {
        "name": {"name": "name", "data_type": "VARCHAR(255)", "description": "Display"},
        "visits": {"name": "visits", "data_type": "INT"},
        "score": {"name": "score", "data_type": "numeric(10, 2)"},
        "active": {"name": "active", "data_type": "BOOLEAN"},
        "created_at": {"name": "created_at", "data_type": "timestamp_ntz(9)"},
        "birthday": {"name": "birthday", "data_type": "DATE"},
        "traits": {"name": "traits", "data_type": "VARIANT"},
    }
    _write_sync(tmp_path)
    _write_manifest(tmp_path, {"model.analytics.customers": _node(columns=columns)})

    sync = load_syncs(tmp_path)[0]

    assert [(column.name, column.type, column.description) for column in sync.columns or []] == [
        ("name", "string", "Display"),
        ("visits", "integer", None),
        ("score", "number", None),
        ("active", "boolean", None),
        ("created_at", "timestamp", None),
        ("birthday", "date", None),
        ("traits", "json", None),
    ]
    assert sync._dbt_contract_resolved
    assert sync._dbt_relation_name == '"warehouse"."marts"."customers"'


def test_explicit_manifest_path_is_project_relative(tmp_path: Path) -> None:
    _write_sync(tmp_path)
    custom = _write_manifest(
        tmp_path, {"model.analytics.customers": _node()}, "build/dbt-manifest.json"
    )

    result = load_syncs(tmp_path, manifest_path=Path("build/dbt-manifest.json"))

    assert result[0].columns is not None
    assert custom.exists()


def test_hand_written_columns_win_and_validate_warns(tmp_path: Path) -> None:
    _write_sync(
        tmp_path,
        columns=[
            {"name": "id", "type": "string", "description": "Local override"},
            {"name": "local_only", "type": "boolean"},
        ],
    )
    _write_manifest(tmp_path, {"model.analytics.customers": _node()})

    result = load_syncs_safe(tmp_path)

    assert not result.errors
    assert [(column.name, column.type) for column in result.syncs[0].columns or []] == [
        ("id", "string"),
        ("local_only", "boolean"),
    ]
    warning = result.warnings["customers_to_api"][0]
    assert "hand-written columns override dbt model 'customers'" in warning
    assert "id type is string (hand-written) vs integer (dbt)" in warning
    assert "only hand-written: local_only" in warning
    assert "only in dbt: email" in warning


def test_matching_hand_written_columns_do_not_warn(tmp_path: Path) -> None:
    _write_sync(
        tmp_path,
        columns=[
            {"name": "id", "type": "integer", "description": "Customer key"},
            {"name": "email", "type": "string"},
        ],
    )
    _write_manifest(tmp_path, {"model.analytics.customers": _node()})

    result = load_syncs_safe(tmp_path)

    assert not result.errors
    assert not result.warnings


def test_validate_prints_disagreement_and_strict_promotes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_sync(tmp_path, columns=[{"name": "id", "type": "string"}])
    _write_manifest(tmp_path, {"model.analytics.customers": _node()})
    monkeypatch.chdir(tmp_path)

    warning = runner.invoke(app, ["validate"], catch_exceptions=False)
    strict = runner.invoke(app, ["validate", "--strict"], catch_exceptions=False)

    assert warning.exit_code == 0
    assert "WARNING" in warning.output
    assert "hand-written columns override dbt model" in warning.output
    assert strict.exit_code == 1
    assert "hand-written columns override dbt model" in strict.output


@pytest.mark.parametrize(
    ("model", "nodes", "message"),
    [
        (
            "ref('missing')",
            {"model.analytics.customers": _node()},
            "dbt model 'missing'.*was not found",
        ),
        (
            "ref('customers')",
            {"model.analytics.customers": _node(columns={})},
            "dbt model 'customers' has no declared columns",
        ),
        (
            "ref('customers')",
            {
                "model.analytics.customers": _node(
                    columns={"id": {"name": "id", "description": "No type"}}
                )
            },
            "model 'customers' column 'id' has no declared data_type",
        ),
        (
            "ref('customers')",
            {
                "model.analytics.customers": _node(
                    columns={"shape": {"name": "shape", "data_type": "GEOGRAPHY"}}
                )
            },
            "model 'customers' column 'shape'.*data_type 'GEOGRAPHY'",
        ),
        (
            "ref('customers')",
            {
                "model.pkg_a.customers": _node(package="pkg_a"),
                "model.pkg_b.customers": _node(package="pkg_b"),
            },
            r"ref\('customers'\) is ambiguous.*pkg_a, pkg_b",
        ),
    ],
)
def test_manifest_contract_errors_name_the_relevant_model_column_and_type(
    tmp_path: Path,
    model: str,
    nodes: dict[str, dict[str, Any]],
    message: str,
) -> None:
    _write_sync(tmp_path, model=model)
    _write_manifest(tmp_path, nodes)

    result = load_syncs_safe(tmp_path)

    assert not result.syncs
    assert "customers" in result.errors
    assert any(re.search(message, error) for error in result.errors["customers"])


def test_missing_manifest_is_validate_error(tmp_path: Path) -> None:
    _write_sync(tmp_path)

    result = load_syncs_safe(tmp_path)

    assert not result.syncs
    assert result.errors["customers"] == [
        f"dbt manifest not found: {tmp_path / 'target' / 'manifest.json'}"
    ]


def test_hand_written_override_does_not_make_manifest_optional(tmp_path: Path) -> None:
    _write_sync(tmp_path, columns=[{"name": "id", "type": "integer"}])

    result = load_syncs_safe(tmp_path)

    assert not result.syncs
    assert "dbt manifest not found" in result.errors["customers"][0]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("not-json", "Could not read dbt manifest"),
        ("[]", "must contain a JSON object"),
        ('{"nodes": []}', "invalid 'nodes' object"),
        ('{"nodes": {"broken": []}}', "model 'customers'.*was not found"),
    ],
)
def test_malformed_manifest_is_validate_error(tmp_path: Path, payload: str, message: str) -> None:
    _write_sync(tmp_path)
    path = tmp_path / "target" / "manifest.json"
    path.parent.mkdir(parents=True)
    path.write_text(payload)

    result = load_syncs_safe(tmp_path)

    assert any(re.search(message, error) for error in result.errors["customers"])


def test_invalid_column_object_is_validate_error(tmp_path: Path) -> None:
    _write_sync(tmp_path)
    node = _node()
    node["columns"] = {"id": "bigint"}
    _write_manifest(tmp_path, {"model.analytics.customers": node})

    result = load_syncs_safe(tmp_path)

    assert "model 'customers' column 'id' is invalid" in result.errors["customers"][0]


def test_duplicate_manifest_column_names_are_validate_error(tmp_path: Path) -> None:
    _write_sync(tmp_path)
    _write_manifest(
        tmp_path,
        {
            "model.analytics.customers": _node(
                columns={
                    "first": {"name": "id", "data_type": "bigint"},
                    "second": {"name": "id", "data_type": "bigint"},
                }
            )
        },
    )

    result = load_syncs_safe(tmp_path)

    assert "duplicate column name(s): 'id'" in result.errors["customers"][0]


def test_source_dbt_requires_exact_ref_model(tmp_path: Path) -> None:
    _write_sync(tmp_path, model="SELECT * FROM customers")
    _write_manifest(tmp_path, {"model.analytics.customers": _node()})

    result = load_syncs_safe(tmp_path)

    assert "requires model: ref('model_name')" in result.errors["customers"][0]


def test_schema_allows_dbt_source_without_authored_columns(tmp_path: Path) -> None:
    path = _write_sync(tmp_path)

    assert validate_yaml_against_schema(path, generate_sync_schema()) == []


def test_resolved_columns_change_config_and_plan_fingerprints(tmp_path: Path) -> None:
    _write_sync(tmp_path)
    manifest_path = _write_manifest(tmp_path, {"model.analytics.customers": _node()})
    first = load_syncs(tmp_path)[0]

    changed_node = _node(
        columns={
            "id": {"name": "id", "data_type": "varchar"},
            "email": {"name": "email", "data_type": "varchar"},
        }
    )
    manifest_path.write_text(json.dumps({"nodes": {"model.analytics.customers": changed_node}}))
    second = load_syncs(tmp_path)[0]

    assert config_hash(first) != config_hash(second)
    assert sync_fingerprint(first, tmp_path) != sync_fingerprint(second, tmp_path)


class _CapturingSource:
    def __init__(self) -> None:
        self.query: str | None = None

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.query = query
        yield {"id": 1, "email": "one@example.com"}


class _Destination:
    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        return SyncResult(success=len(records))


@pytest.mark.parametrize("dry_run", [False, True], ids=["run", "plan-engine-path"])
def test_run_and_plan_path_use_one_manifest_snapshot_even_if_file_changes(
    tmp_path: Path, dry_run: bool
) -> None:
    _write_sync(tmp_path)
    manifest_path = _write_manifest(tmp_path, {"model.analytics.customers": _node()})
    sync = load_syncs(tmp_path)[0]
    manifest_path.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.analytics.customers": _node(
                        relation_name='"mutated"."wrong"."customers"',
                        columns={"id": {"name": "id", "data_type": "varchar"}},
                    )
                }
            }
        )
    )
    source = _CapturingSource()

    result = run_sync(
        sync,
        source,
        _Destination(),
        BigQueryProfile(type="bigquery", project="p", dataset="d"),
        tmp_path,
        dry_run=dry_run,
        compute_diff=dry_run,
    )

    assert result.failed == 0
    assert result.success == 1
    assert source.query is not None
    assert '"warehouse"."marts"."customers"' in source.query
    assert "mutated" not in source.query


def test_docs_manifest_reuses_v6_columns_and_resolved_fingerprint(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path)
    _write_manifest(tmp_path, {"model.analytics.customers": _node()})

    manifest = build_manifest(tmp_path, include_state=False)
    serialized = manifest.to_dict()["syncs"][0]

    assert manifest.schema_version == 6
    assert serialized["columns"] == [
        {"name": "id", "type": "integer", "description": "Customer key"},
        {"name": "email", "type": "string"},
    ]
    assert serialized["config_hash"] == sync_fingerprint(load_syncs(tmp_path)[0], tmp_path)[7:]


def test_validate_accepts_explicit_manifest_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_sync(tmp_path)
    _write_manifest(tmp_path, {"model.analytics.customers": _node()}, "artifacts/dbt-manifest.json")
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        ["validate", "--manifest-path", "artifacts/dbt-manifest.json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "customers_to_api" in result.output


def test_adapter_native_dbt_types_map_to_the_portable_vocabulary() -> None:
    cases = {
        "INT64": "integer",
        "FLOAT64": "number",
        "BIGNUMERIC": "number",
        "NUMBER(38,0)": "number",
        "ARRAY<STRING>": "json",
        "STRUCT<a INT64, b STRING>": "json",
        "character varying(255)": "string",
        "TIMESTAMP_TZ": "timestamp",
    }
    for raw, expected in cases.items():
        assert portable_type_from_dbt(raw) == expected, raw


def test_manifest_text_is_made_safe_before_it_reaches_messages() -> None:
    from drt.integrations.dbt import _safe

    cleaned = _safe("x\x1b[31m\n[bold]" + "y" * 200)
    assert "\x1b" not in cleaned and "\n" not in cleaned
    assert "[" not in cleaned and "]" not in cleaned
    assert len(cleaned) <= 80


def test_a_model_without_an_enforced_dbt_contract_is_rejected(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path)
    _write_manifest(tmp_path, {"model.analytics.customers": _node(enforced=False)})

    with pytest.raises(ValueError, match="no enforced dbt contract"):
        load_syncs(tmp_path)


def test_a_local_sql_override_cannot_use_a_dbt_derived_contract(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path)
    _write_manifest(tmp_path, {"model.analytics.customers": _node()})
    (tmp_path / "syncs" / "models").mkdir()
    (tmp_path / "syncs" / "models" / "customers.sql").write_text("SELECT 1 AS other")

    with pytest.raises(ValueError, match="local SQL override"):
        load_syncs(tmp_path)


def test_a_versioned_model_is_reported_as_versioned_not_just_ambiguous(tmp_path: Path) -> None:
    _write_project(tmp_path)
    _write_sync(tmp_path)
    v1 = {**_node(), "version": 1}
    v2 = {**_node(), "version": 2}
    _write_manifest(
        tmp_path, {"model.analytics.customers.v1": v1, "model.analytics.customers.v2": v2}
    )

    with pytest.raises(ValueError, match="several versions"):
        load_syncs(tmp_path)


def test_the_manifest_is_parsed_once_per_file_version(tmp_path: Path, monkeypatch) -> None:
    import drt.integrations.dbt as dbt_module

    path = _write_manifest(tmp_path, {"model.analytics.customers": _node()})
    calls: list[int] = []
    real_loads = dbt_module.json.loads
    monkeypatch.setattr(dbt_module.json, "loads", lambda text: calls.append(1) or real_loads(text))

    first = dbt_module._read_manifest(path)
    second = dbt_module._read_manifest(path)

    assert first is second and len(calls) == 1
    path.write_text(json.dumps({"nodes": {}, "metadata": {"changed": True}}))
    assert dbt_module._read_manifest(path) is not first and len(calls) == 2
