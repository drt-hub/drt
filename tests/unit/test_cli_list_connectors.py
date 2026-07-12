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
    # Postgres config class lives in drt.config.destinations_sql after the #721
    # split (re-exported from drt.config.models — both import paths work).
    assert pg["config_class"].startswith("drt.config.destinations_sql.PostgresDestinationConfig")


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


# ---------------------------------------------------------------------------
# Registry-parity — the OTHER direction (registry ⊆ connectors.py lists).
# The two tests above guard DESTINATIONS/SOURCES ⊆ _connector_detail, but
# nothing checked that every *registered* connector is actually listed here —
# so bigquery/databricks/s3/gcs/azure_blob/elasticsearch/snowflake/mixpanel/
# salesforce_bulk were registered yet invisible to `drt destinations` all the
# way through v0.7.9. These lock the registry as the source of truth.
# ---------------------------------------------------------------------------


def test_DESTINATIONS_matches_registry() -> None:
    """``connectors.py`` DESTINATIONS must list exactly the registered destinations.

    A destination registered in ``drt/connectors/registry.py`` but absent from
    ``DESTINATIONS`` silently vanishes from ``drt destinations`` /
    ``--detailed`` / ``--format json``. This guards against that drift at PR
    time (the post-merge ``check_drift.sh`` audit never covered this list).
    """
    import drt.connectors.registry as registry

    registered = set(registry._destination_registry)
    listed = {t for t, _ in DESTINATIONS}
    assert listed == registered, (
        "connectors.py DESTINATIONS is out of sync with the connector registry. "
        f"Registered but not listed: {sorted(registered - listed)}; "
        f"listed but not registered: {sorted(listed - registered)}. "
        "Update DESTINATIONS in drt/config/connectors.py (and the config-class "
        "map in drt/cli/_connector_detail.py)."
    )


def test_SOURCES_matches_registry() -> None:
    """``connectors.py`` SOURCES must list exactly the registered sources."""
    import drt.connectors.registry as registry

    registered = set(registry._source_registry)
    listed = {t for t, _ in SOURCES}
    assert listed == registered, (
        "connectors.py SOURCES is out of sync with the connector registry. "
        f"Registered but not listed: {sorted(registered - listed)}; "
        f"listed but not registered: {sorted(listed - registered)}."
    )


# ---------------------------------------------------------------------------
# Defensive paths — exercised directly so codecov sees them
# ---------------------------------------------------------------------------


def test_unknown_detail_returns_unregistered_sentinel() -> None:
    """The fallback used when a connector is missing from the registry.

    Unreachable in production (registry-parity tests above prevent it),
    but worth exercising so the shape is locked: callers can rely on
    the same ConnectorDetail fields being present.
    """
    from drt.cli._connector_detail import _unknown_detail

    detail = _unknown_detail("mystery", "Mystery Connector", "destination")
    assert detail.type == "mystery"
    assert detail.display_name == "Mystery Connector"
    assert detail.kind == "destination"
    assert detail.config_class == "(unregistered)"
    assert detail.sample_yaml == "type: mystery"


def test_sample_yaml_caps_at_eight_lines_when_required_fields_overflow() -> None:
    """When a connector declares 8+ required fields, the renderer stops
    at the cap rather than producing an unbounded stanza.
    """
    from drt.cli._connector_detail import _FieldInfo, _render_sample_yaml

    fields = [
        _FieldInfo(name=f"f{i}", is_env_var=False, is_required=True, default_repr="")
        for i in range(12)
    ]
    yaml = _render_sample_yaml("synthetic", "destination", fields)
    lines = yaml.split("\n")
    # 1 line for "type:" + 7 required fields = 8 lines max
    assert len(lines) == 8
    assert lines[0].endswith("type: synthetic")
    assert lines[-1].endswith("f6: <f6>")  # last required field that fit


# ---------------------------------------------------------------------------
# install_target / connector_inventory (SSoT for the MCP inventory)
# ---------------------------------------------------------------------------


def test_install_target_extras_and_core() -> None:
    from drt.config.connectors import install_target

    assert install_target("postgres") == "drt-core[postgres]"
    assert install_target("azure_blob") == "drt-core[azure]"  # extra name != type
    assert install_target("google_sheets") == "drt-core[sheets]"  # extra name != type
    assert install_target("slack") == "(core)"
    assert install_target("duckdb") == "(core)"  # bundled in core despite a [duckdb] extra


def test_connector_inventory_covers_every_registered_type() -> None:
    """The derived inventory lists exactly the SSoT types (which
    test_DESTINATIONS_matches_registry keeps aligned with the registry) — the
    structural guard against the drift that once dropped salesforce_bulk."""
    from drt.config.connectors import connector_inventory

    inv = connector_inventory()
    assert {c["type"] for c in inv["sources"]} == {t for t, _ in SOURCES}
    assert {c["type"] for c in inv["destinations"]} == {t for t, _ in DESTINATIONS}
    # every entry carries name + type + install
    for entry in inv["sources"] + inv["destinations"]:
        assert entry.keys() == {"name", "type", "install"}
    # the connector that fell out of the old hand-maintained list is present
    assert any(c["type"] == "salesforce_bulk" for c in inv["destinations"])
