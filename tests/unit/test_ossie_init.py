"""Apache Ossie reader and ``drt init --from-ossie`` spike tests (#1254)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

import drt.cli.main  # noqa: F401 — importing registers commands on the shared app
from drt.cli._app import app
from drt.config.parser import load_syncs_safe
from drt.integrations.ossie import (
    OssieModelError,
    build_ossie_scaffold,
    load_ossie_model,
)

runner = CliRunner()
FIXTURES = Path(__file__).parents[1] / "fixtures" / "ossie"


def _model_data() -> dict[str, Any]:
    return {
        "version": "0.2.0.dev0",
        "name": "activation_model",
        "description": "Activation-ready example",
        "datasets": [
            {
                "name": "customers",
                "source": "analytics.public.customers",
                "primary_key": ["customer_id"],
                "unique_keys": [["email"]],
                "description": "Customer records",
                "fields": [
                    {
                        "name": "customer_id",
                        "expression": {
                            "dialects": [
                                {"dialect": "ANSI_SQL", "expression": "customer_id"},
                                {"dialect": "SNOWFLAKE", "expression": "customer_id::NUMBER"},
                            ]
                        },
                        "datatype": "Integer",
                    },
                    {
                        "name": "email",
                        "expression": {
                            "dialects": [{"dialect": "ANSI_SQL", "expression": "LOWER(email)"}]
                        },
                        "datatype": "String",
                    },
                ],
            },
            {
                "name": "order_items",
                "source": "analytics.public.order_items",
                "primary_key": ["order_id", "line_number"],
                "fields": [
                    {
                        "name": "order_id",
                        "expression": {
                            "dialects": [{"dialect": "ANSI_SQL", "expression": "order_id"}]
                        },
                    },
                    {
                        "name": "line_number",
                        "expression": {
                            "dialects": [{"dialect": "ANSI_SQL", "expression": "line_number"}]
                        },
                    },
                ],
            },
            {
                "name": "events",
                "source": "analytics.public.events",
                "fields": [
                    {
                        "name": "event_id",
                        "expression": {
                            "dialects": [{"dialect": "ANSI_SQL", "expression": "event_id"}]
                        },
                    }
                ],
            },
        ],
        "relationships": [],
        "metrics": [
            {
                "name": "customer_count",
                "expression": {
                    "dialects": [
                        {"dialect": "ANSI_SQL", "expression": "COUNT(customers.customer_id)"}
                    ]
                },
            }
        ],
        "custom_extensions": [],
    }


def _write_model(
    tmp_path: Path, data: dict[str, Any] | None = None, *, as_json: bool = False
) -> Path:
    path = tmp_path / ("model.json" if as_json else "model.yaml")
    payload = data if data is not None else _model_data()
    path.write_text(json.dumps(payload) if as_json else yaml.safe_dump(payload, sort_keys=False))
    return path


def test_reader_loads_vendored_tpcds_example() -> None:
    model = load_ossie_model(FIXTURES / "tpcds_semantic_model.yaml")

    assert model.name == "tpcds_retail_model"
    assert model.version == "0.2.0.dev0"
    assert len(model.datasets) == 5
    assert model.datasets[0].primary_key == ("ss_item_sk", "ss_ticket_number")
    assert model.datasets[2].fields[-1].name == "c_email_address"
    assert model.metrics[0].name == "total_sales"


def test_reader_rejects_vendored_flights_ontology_shape() -> None:
    with pytest.raises(OssieModelError, match="datasets must be an array"):
        load_ossie_model(FIXTURES / "flights.yaml")


def test_reader_is_lenient_on_unknown_keys_and_version(tmp_path: Path) -> None:
    data = _model_data()
    data["version"] = "99.0-experimental"
    data["future_root_key"] = {"anything": True}
    data["datasets"][0]["future_dataset_key"] = "ignored"

    model = load_ossie_model(_write_model(tmp_path, data))

    assert model.datasets[0].name == "customers"
    assert len(model.warnings) == 1
    assert "99.0-experimental" in model.warnings[0]


def test_reader_missing_version_warns_and_json_is_supported(tmp_path: Path) -> None:
    data = _model_data()
    del data["version"]

    model = load_ossie_model(_write_model(tmp_path, data, as_json=True))

    assert model.version is None
    assert model.warnings == (
        "Ossie model has no version; continuing leniently because the fields used by "
        "this scaffold are present.",
    )


@pytest.mark.parametrize(
    ("data", "message"),
    [
        ([], "Ossie document must be an object"),
        ({"version": "0.2.0.dev0", "datasets": []}, "name must be a non-empty string"),
        ({"version": "0.2.0.dev0", "name": "empty", "datasets": []}, "at least one"),
    ],
)
def test_reader_rejects_structurally_unusable_input(
    tmp_path: Path, data: object, message: str
) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(data))

    with pytest.raises(OssieModelError, match=message):
        load_ossie_model(path)


def test_dialect_selection_prefers_requested_then_falls_back_to_ansi(tmp_path: Path) -> None:
    model = load_ossie_model(_write_model(tmp_path))
    scaffold = build_ossie_scaffold(
        model,
        model.datasets[0],
        destination="postgres",
        dialect="SNOWFLAKE",
    )

    assert "customer_id::NUMBER AS customer_id" in scaffold.yaml_text
    assert "LOWER(email) AS email" in scaffold.yaml_text
    assert scaffold.ansi_fallback_fields == ("email",)


def test_missing_requested_and_ansi_dialect_names_field(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][1]["expression"]["dialects"] = [
        {"dialect": "BIGQUERY", "expression": "LOWER(email)"}
    ]
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(
        OssieModelError,
        match=(
            "Dataset 'customers' field 'email' has no SNOWFLAKE expression and no ANSI_SQL fallback"
        ),
    ):
        build_ossie_scaffold(model, model.datasets[0], dialect="SNOWFLAKE")


def test_postgres_scaffold_keeps_composite_primary_key(tmp_path: Path) -> None:
    model = load_ossie_model(_write_model(tmp_path))
    scaffold = build_ossie_scaffold(
        model, model.datasets[1], destination="postgres", dialect="ANSI_SQL"
    )
    parsed = yaml.safe_load(scaffold.yaml_text)

    assert parsed["name"] == "sync_order_items"
    assert parsed["destination"]["upsert_key"] == ["order_id", "line_number"]
    assert parsed["sync"]["mode"] == "upsert"
    assert "Generated from Apache Ossie model activation_model" in parsed["description"]


def test_primary_key_missing_from_fields_is_added_directly_for_canonical_example() -> None:
    model = load_ossie_model(FIXTURES / "tpcds_semantic_model.yaml")

    scaffold = build_ossie_scaffold(model, model.datasets[0], destination="postgres")
    parsed = yaml.safe_load(scaffold.yaml_text)

    assert scaffold.implicit_primary_key_fields == ("ss_ticket_number",)
    assert "ss_ticket_number AS ss_ticket_number" in parsed["model"]
    assert parsed["destination"]["upsert_key"] == ["ss_item_sk", "ss_ticket_number"]
    assert "Primary-key columns added directly" in scaffold.yaml_text


def test_hubspot_requires_a_single_primary_key(tmp_path: Path) -> None:
    model = load_ossie_model(_write_model(tmp_path))

    with pytest.raises(OssieModelError, match="HubSpot requires exactly one id_property"):
        build_ossie_scaffold(model, model.datasets[1], destination="hubspot")


def test_query_source_is_wrapped_and_unsafe_table_source_is_rejected(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = "SELECT customer_id, email FROM raw.customers;"
    model = load_ossie_model(_write_model(tmp_path, data))

    scaffold = build_ossie_scaffold(model, model.datasets[0])
    sql = yaml.safe_load(scaffold.yaml_text)["model"]
    assert "FROM (\nSELECT customer_id, email FROM raw.customers\n) AS _ossie_source" in sql

    data["datasets"][0]["source"] = "raw.customers; DROP TABLE customers"
    model = load_ossie_model(_write_model(tmp_path, data))
    with pytest.raises(OssieModelError, match="safe unquoted table identifier"):
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize("destination", ["postgres", "hubspot", "rest_api"])
def test_generated_yaml_round_trips_through_real_sync_parser(
    tmp_path: Path, destination: str
) -> None:
    model = load_ossie_model(_write_model(tmp_path))
    scaffold = build_ossie_scaffold(model, model.datasets[0], destination=destination)
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / scaffold.filename).write_text(scaffold.yaml_text)

    result = load_syncs_safe(tmp_path)

    assert result.errors == {}
    assert [sync.name for sync in result.syncs] == ["sync_customers"]


def test_cli_selects_datasets_skips_missing_key_and_uses_output_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model_path = _write_model(tmp_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "init",
            "--from-ossie",
            str(model_path),
            "--dataset",
            "customers",
            "--dataset",
            "events",
            "--destination",
            "postgres",
            "--output-dir",
            "generated",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "generated" / "sync_customers.yml").exists()
    assert not (tmp_path / "generated" / "sync_events.yml").exists()
    assert "skip events (no primary_key)" in result.output
    assert "metrics available but not generated (entity grain unclear)" in result.output


def test_cli_defaults_to_all_keyed_datasets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model_path = _write_model(tmp_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "--from-ossie", str(model_path)])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "syncs" / "sync_customers.yml").exists()
    assert (tmp_path / "syncs" / "sync_order_items.yml").exists()
    assert not (tmp_path / "syncs" / "sync_events.yml").exists()
    assert "skip events (no primary_key)" in result.output


def test_cli_invocation_with_vendored_example_and_dialect_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "init",
            "--from-ossie",
            str(FIXTURES / "tpcds_semantic_model.yaml"),
            "--dataset",
            "customer",
            "--dialect",
            "SNOWFLAKE",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "syncs" / "sync_customer.yml").exists()
    assert "used ANSI_SQL fallback" in result.output
    assert "total_sales" in result.output


def test_cli_unknown_dataset_is_a_clear_error(tmp_path: Path) -> None:
    model_path = _write_model(tmp_path)

    result = runner.invoke(
        app,
        ["init", "--from-ossie", str(model_path), "--dataset", "does_not_exist"],
    )

    assert result.exit_code == 1
    assert "Unknown Ossie dataset(s): does_not_exist" in result.output
