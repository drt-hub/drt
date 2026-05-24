"""Tests for drt sources and drt destinations commands."""

from __future__ import annotations

from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.connectors import DESTINATIONS, SOURCES

runner = CliRunner()


# ---------------------------------------------------------------------------
# drt sources
# ---------------------------------------------------------------------------


def test_sources_command_succeeds() -> None:
    """drt sources should exit with code 0 and contain header."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    assert "Available sources:" in result.output


def test_sources_command_contains_all_connectors() -> None:
    """drt sources should list all available source connectors."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    for source_type, description in SOURCES:
        assert source_type in result.output
        assert description in result.output


# ---------------------------------------------------------------------------
# drt destinations
# ---------------------------------------------------------------------------


def test_destinations_command_succeeds() -> None:
    """drt destinations should exit with code 0 and contain header."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0
    assert "Available destinations:" in result.output


def test_destinations_command_contains_all_connectors() -> None:
    """drt destinations should list all available destination connectors."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0
    for dest_type, description in DESTINATIONS:
        assert dest_type in result.output
        assert description in result.output


# ---------------------------------------------------------------------------
# --detailed flag (#543)
# ---------------------------------------------------------------------------


def test_sources_detailed_includes_sample_yaml_for_postgres() -> None:
    """``drt sources --detailed`` surfaces a copy-pasteable YAML stanza."""
    result = runner.invoke(app, ["sources", "--detailed"])
    assert result.exit_code == 0
    # Sample YAML must include the type discriminator
    assert "type: postgres" in result.output
    # And some host/db connection hint — Postgres profile uses host/dbname
    assert "host" in result.output


def test_destinations_detailed_includes_postgres_required_fields() -> None:
    """``drt destinations --detailed`` lists required fields per connector."""
    result = runner.invoke(app, ["destinations", "--detailed"])
    assert result.exit_code == 0
    assert "type: postgres" in result.output
    # Postgres destination has table + upsert_key as required
    assert "table" in result.output
    assert "upsert_key" in result.output


# ---------------------------------------------------------------------------
# --format json
# ---------------------------------------------------------------------------


def test_sources_json_format_is_machine_readable() -> None:
    """``drt sources --format json`` produces a parseable JSON document."""
    import json as _json

    result = runner.invoke(app, ["sources", "--format", "json"])
    assert result.exit_code == 0
    payload = _json.loads(result.output)
    assert "connectors" in payload
    types = {c["type"] for c in payload["connectors"]}
    assert "postgres" in types and "duckdb" in types
    # Without --detailed, payload is the short shape (type + display_name + kind)
    assert set(payload["connectors"][0].keys()) == {"type", "display_name", "kind"}


def test_destinations_detailed_json_includes_field_metadata() -> None:
    """``--detailed --format json`` carries the structured detail dict."""
    import json as _json

    result = runner.invoke(app, ["destinations", "--detailed", "--format", "json"])
    assert result.exit_code == 0
    payload = _json.loads(result.output)
    pg = next(c for c in payload["connectors"] if c["type"] == "postgres")
    expected_keys = {
        "type",
        "display_name",
        "kind",
        "config_class",
        "required_env_vars",
        "optional_env_vars",
        "required_fields",
        "sample_yaml",
    }
    assert set(pg.keys()) == expected_keys
    assert "table" in pg["required_fields"]
    assert "type: postgres" in pg["sample_yaml"]
    # Postgres config class lives in drt.config.models
    assert pg["config_class"].startswith("drt.config.models.PostgresDestinationConfig")


# ---------------------------------------------------------------------------
# Registry parity — guard against new connectors landing without metadata
# ---------------------------------------------------------------------------


def test_every_source_in_SOURCES_has_a_registered_config_class() -> None:
    """Adding a Source to SOURCES without registering its Profile class
    would silently produce ``(unregistered)`` in ``--detailed`` output.
    """
    from drt.cli._connector_detail import SOURCE_CONFIG_CLASSES

    missing = [t for t, _ in SOURCES if t not in SOURCE_CONFIG_CLASSES]
    assert missing == [], (
        f"Sources missing from SOURCE_CONFIG_CLASSES: {missing}. "
        f"Register the Profile class in drt/cli/_connector_detail.py."
    )


def test_every_destination_in_DESTINATIONS_has_a_registered_config_class() -> None:
    """Adding a Destination to DESTINATIONS without registering its config class
    would silently produce ``(unregistered)`` in ``--detailed`` output.
    """
    from drt.cli._connector_detail import DESTINATION_CONFIG_CLASSES

    missing = [t for t, _ in DESTINATIONS if t not in DESTINATION_CONFIG_CLASSES]
    assert missing == [], (
        f"Destinations missing from DESTINATION_CONFIG_CLASSES: {missing}. "
        f"Register the Pydantic config class in drt/cli/_connector_detail.py."
    )
