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
    MAX_OSSIE_MODEL_BYTES,
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


def test_reader_rejects_input_above_five_megabytes(tmp_path: Path) -> None:
    path = tmp_path / "too-large.yaml"
    path.write_bytes(b" " * (MAX_OSSIE_MODEL_BYTES + 1))

    with pytest.raises(OssieModelError, match="exceeds the 5 MB input limit"):
        load_ossie_model(path)


def test_reader_wraps_deep_yaml_recursion_as_model_error(tmp_path: Path) -> None:
    path = tmp_path / "deep.yaml"
    path.write_text("nested: " + "[" * 2_000 + "0" + "]" * 2_000)

    with pytest.raises(OssieModelError, match="Could not read Ossie model"):
        load_ossie_model(path)


@pytest.mark.parametrize("token", [";", "--", "/*", "*/", "\x00"])
def test_reader_rejects_unsafe_field_expression_tokens(tmp_path: Path, token: str) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][1]["expression"]["dialects"][0]["expression"] = (
        f"email {token} injected"
    )

    with pytest.raises(
        OssieModelError,
        match=r"Dataset 'customers' field 'email' expression contains forbidden",
    ):
        load_ossie_model(_write_model(tmp_path, data))


@pytest.mark.parametrize(
    "expression",
    [
        "$q$'$q$; DROP TABLE victims; /*",
        "E'x\\''; DROP TABLE victims; /*",
        "'unterminated; DROP TABLE victims",
        '"unterminated; DROP TABLE victims',
        "`unterminated; DROP TABLE victims",
        "'mysql\\'; DROP TABLE victims; /*'",
        "U&'unicode'",
        "B'0101'",
        "R'raw'",
        "value # comment",
        "value\rDROP TABLE victims",
        "0 + {{ var('unused', '; DROP TABLE victims; --') }}",
        "{% set x = 1 %}value",
        "value {# hidden #}",
        "'{{ env_var(\"SECRET\") }}'",
    ],
)
def test_reader_refuses_ambiguous_or_dialect_specific_field_expressions(
    tmp_path: Path, expression: str
) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][1]["expression"]["dialects"][0]["expression"] = expression

    with pytest.raises(
        OssieModelError,
        match="before scaffolding|quoted span|remove it|ordinary quoted literal|template",
    ):
        load_ossie_model(_write_model(tmp_path, data))


def test_reader_rejects_unsafe_metric_expression(tmp_path: Path) -> None:
    data = _model_data()
    data["metrics"][0]["expression"]["dialects"][0]["expression"] = "COUNT(*) -- injected"

    with pytest.raises(
        OssieModelError,
        match=r"Metric 'customer_count' expression contains forbidden SQL.*'--'",
    ):
        load_ossie_model(_write_model(tmp_path, data))


def test_expression_scanner_allows_tokens_inside_all_supported_quote_styles(
    tmp_path: Path,
) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][1]["expression"]["dialects"][0]["expression"] = (
        "COALESCE('it''s; --', \"double\"\"; /*\", `backtick``; */`)"
    )

    model = load_ossie_model(_write_model(tmp_path, data))
    scaffold = build_ossie_scaffold(model, model.datasets[0])

    assert "COALESCE('it''s; --'" in yaml.safe_load(scaffold.yaml_text)["model"]


def test_all_vendored_tpcds_datasets_still_scaffold_and_validate(tmp_path: Path) -> None:
    model = load_ossie_model(FIXTURES / "tpcds_semantic_model.yaml")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()

    for dataset in model.datasets:
        scaffold = build_ossie_scaffold(model, dataset)
        (syncs_dir / scaffold.filename).write_text(scaffold.yaml_text)

    result = load_syncs_safe(tmp_path)

    assert result.errors == {}
    assert {sync.name for sync in result.syncs} == {
        "sync_customer",
        "sync_date_dim",
        "sync_item",
        "sync_store",
        "sync_store_sales",
    }


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


def test_rest_api_scaffold_is_rejected_instead_of_claiming_upsert(tmp_path: Path) -> None:
    model = load_ossie_model(_write_model(tmp_path))

    with pytest.raises(OssieModelError, match="Unsupported Ossie scaffold destination 'rest_api'"):
        build_ossie_scaffold(model, model.datasets[0], destination="rest_api")


def test_query_source_is_wrapped_and_trailing_separator_is_stripped(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = "SELECT customer_id, email FROM raw.customers;"
    model = load_ossie_model(_write_model(tmp_path, data))

    scaffold = build_ossie_scaffold(model, model.datasets[0])
    sql = yaml.safe_load(scaffold.yaml_text)["model"]
    assert "FROM (\nSELECT customer_id, email FROM raw.customers\n) AS _ossie_source" in sql


def test_source_query_allows_separator_and_comments_inside_quoted_literals(
    tmp_path: Path,
) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = (
        'SELECT customer_id, \'; -- /* */\' AS note, "a""; --" AS quoted FROM raw.customers;'
    )
    model = load_ossie_model(_write_model(tmp_path, data))

    scaffold = build_ossie_scaffold(model, model.datasets[0])

    assert "'; -- /* */' AS note" in yaml.safe_load(scaffold.yaml_text)["model"]


@pytest.mark.parametrize(
    "source",
    [
        "SELECT $q$'$q$; DROP TABLE victims; /*",
        "SELECT E'x\\''; DROP TABLE victims; /*",
        "SELECT 'unterminated; DROP TABLE victims",
        "SELECT 'mysql\\'; DROP TABLE victims; /*'",
    ],
)
def test_source_query_refuses_ambiguous_dialect_quoting(tmp_path: Path, source: str) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = source
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(
        OssieModelError, match="before scaffolding|quoted span|ordinary quoted literal"
    ):
        build_ossie_scaffold(model, model.datasets[0])


def test_source_query_rejects_more_than_one_statement(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = "SELECT * FROM raw.customers; SELECT 1;"
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="forbidden SQL separator/comment token ';'"):
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize(
    "source",
    ["db..table", "db.", "db.123table", "one.two.three.four", "analytics.select"],
)
def test_source_identifier_segments_are_strict(tmp_path: Path, source: str) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = source
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="source identifier"):
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize("field_name", ["CustomerID", "select", "bad-name", "123id"])
def test_output_alias_must_be_safe_lowercase_and_not_reserved(
    tmp_path: Path, field_name: str
) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][0]["name"] = field_name
    data["datasets"][0]["primary_key"] = [field_name]
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="rename the field in the Ossie model"):
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize("word", ["case", "default", "distinct", "values", "window", "qualify"])
def test_common_cross_dialect_reserved_words_are_rejected_as_aliases(
    tmp_path: Path, word: str
) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][0]["name"] = word
    data["datasets"][0]["primary_key"] = [word]
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="reserved SQL word"):
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize("word", ["case", "default", "distinct", "values", "window", "qualify"])
def test_common_cross_dialect_reserved_words_are_rejected_in_source_segments(
    tmp_path: Path, word: str
) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = f"analytics.{word}"
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="reserved SQL word"):
        build_ossie_scaffold(model, model.datasets[0])


def test_duplicate_primary_key_columns_are_rejected(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["primary_key"] = ["external_id", "external_id"]

    with pytest.raises(OssieModelError, match="primary_key contains duplicate column"):
        load_ossie_model(_write_model(tmp_path, data))


def test_duplicate_field_output_aliases_are_rejected(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["fields"].append(data["datasets"][0]["fields"][1])

    with pytest.raises(OssieModelError, match="duplicate output alias 'email'"):
        load_ossie_model(_write_model(tmp_path, data))


@pytest.mark.parametrize("destination", ["postgres", "hubspot"])
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


def test_generated_yaml_safely_serializes_adversarial_metadata(tmp_path: Path) -> None:
    data = _model_data()
    data["name"] = "activation\r---\n...\n# model\n- model"
    data["datasets"][0]["description"] = (
        "Customers\r---\n...\ntags: [injected]\n# dataset\n- dataset"
    )
    data["metrics"][0]["name"] = "m\ntags: [injected]\r---\n...\n# metric\n- metric"
    data["metrics"][0]["description"] = (
        "Metric description\r---\n...\ntags: [injected]\n# description\n- description"
    )
    model = load_ossie_model(_write_model(tmp_path, data))
    scaffold = build_ossie_scaffold(model, model.datasets[0])

    parsed = yaml.safe_load(scaffold.yaml_text)
    assert set(parsed) == {"name", "description", "model", "destination", "sync"}
    assert "tags" not in parsed
    assert parsed["name"] == "sync_customers"
    assert parsed["description"].startswith(model.datasets[0].description)

    yaml_start = scaffold.yaml_text.index("name:")
    comment_block = scaffold.yaml_text[:yaml_start]
    assert all(not line or line.startswith("#") for line in comment_block.splitlines())

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


def test_cli_rejects_case_insensitive_output_path_collision_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = _model_data()
    second = json.loads(json.dumps(data["datasets"][0]))
    data["datasets"] = [data["datasets"][0], second]
    data["datasets"][0]["name"] = "Customer"
    data["datasets"][1]["name"] = "customer"
    model_path = _write_model(tmp_path, data)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "--from-ossie", str(model_path)])

    assert result.exit_code == 1
    assert "case-insensitively colliding output" in result.output
    assert "sync_Customer.yml" in result.output
    assert "sync_customer.yml" in result.output
    assert not (tmp_path / "syncs").exists()


def test_reader_refuses_jinja_in_a_source_query(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["source"] = "SELECT {{ var('x', '; DROP TABLE victims') }} AS id"

    with pytest.raises(OssieModelError, match="Jinja template marker"):
        model = load_ossie_model(_write_model(tmp_path, data))
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize(
    "alias",
    ["asc", "desc", "if", "nulls", "preceding", "enum", "assert_rows_modified", "graph_table"],
)
def test_reader_rejects_bigquery_reserved_aliases(tmp_path: Path, alias: str) -> None:
    data = _model_data()
    data["datasets"][0]["fields"][1]["name"] = alias
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="reserved SQL word"):
        build_ossie_scaffold(model, model.datasets[0])


def test_reader_rejects_overlong_dataset_names(tmp_path: Path) -> None:
    data = _model_data()
    data["datasets"][0]["name"] = "a" * 300

    with pytest.raises(OssieModelError, match="limit is 64"):
        model = load_ossie_model(_write_model(tmp_path, data))
        build_ossie_scaffold(model, model.datasets[0])


@pytest.mark.parametrize(
    "marker",
    [
        "${AWS_SECRET_ACCESS_KEY}",
        "{{ cycler.__init__.__globals__.os.environ.AWS_SECRET_ACCESS_KEY }}{{ var('noop', '') }}",
        "{% if True %}x{% endif %}",
        "{# note #}",
    ],
)
@pytest.mark.parametrize("where", ["dataset_description", "model_name", "model_version"])
def test_template_or_env_syntax_in_generated_description_text_is_refused(
    tmp_path: Path, marker: str, where: str
) -> None:
    data = _model_data()
    if where == "dataset_description":
        data["datasets"][0]["description"] = f"Customers {marker}"
    elif where == "model_name":
        data["name"] = f"activation {marker}"
    else:
        data["version"] = f"0.2.0 {marker}"
    model = load_ossie_model(_write_model(tmp_path, data))

    with pytest.raises(OssieModelError, match="would expand when loading"):
        build_ossie_scaffold(model, model.datasets[0])
