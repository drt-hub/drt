"""Per-connector detail builder for ``drt sources --detailed`` /
``drt destinations --detailed`` (#543).

Derives the user-facing detail (required env vars, optional env vars,
sample YAML) by introspecting the same Pydantic / dataclass config
classes the rest of drt uses — no hand-maintained tables to drift.

Sources are dataclasses defined in ``drt.config.credentials``;
destinations are Pydantic BaseModels in ``drt.config.models``. Both
expose ``__dataclass_fields__`` / ``model_fields`` that we walk to
classify each field as required-string, optional-env-var, default-
literal, etc.
"""

from __future__ import annotations

import dataclasses
import typing
from dataclasses import is_dataclass
from typing import Any

from pydantic import BaseModel

from drt.config import credentials as _creds
from drt.config import models as _models

# ---------------------------------------------------------------------------
# Type → config-class registry. Single source of truth alongside
# drt.config.connectors.SOURCES / DESTINATIONS.
# ---------------------------------------------------------------------------


SOURCE_CONFIG_CLASSES: dict[str, type] = {
    "bigquery": _creds.BigQueryProfile,
    "clickhouse": _creds.ClickHouseProfile,
    "databricks": _creds.DatabricksProfile,
    "duckdb": _creds.DuckDBProfile,
    "mysql": _creds.MySQLProfile,
    "postgres": _creds.PostgresProfile,
    "redshift": _creds.RedshiftProfile,
    "rest_api": _creds.RestApiProfile,
    "snowflake": _creds.SnowflakeProfile,
    "sqlite": _creds.SQLiteProfile,
    "sqlserver": _creds.SQLServerProfile,
}


DESTINATION_CONFIG_CLASSES: dict[str, type[BaseModel]] = {
    "amplitude": _models.AmplitudeDestinationConfig,
    "clickhouse": _models.ClickHouseDestinationConfig,
    "discord": _models.DiscordDestinationConfig,
    "email_smtp": _models.EmailSmtpDestinationConfig,
    "file": _models.FileDestinationConfig,
    "github_actions": _models.GitHubActionsDestinationConfig,
    "google_ads": _models.GoogleAdsDestinationConfig,
    "google_sheets": _models.GoogleSheetsDestinationConfig,
    "hubspot": _models.HubSpotDestinationConfig,
    "intercom": _models.IntercomDestinationConfig,
    "jira": _models.JiraDestinationConfig,
    "linear": _models.LinearDestinationConfig,
    "mysql": _models.MySQLDestinationConfig,
    "notion": _models.NotionDestinationConfig,
    "parquet": _models.ParquetDestinationConfig,
    "postgres": _models.PostgresDestinationConfig,
    "rest_api": _models.RestApiDestinationConfig,
    "sendgrid": _models.SendGridDestinationConfig,
    "slack": _models.SlackDestinationConfig,
    "staged_upload": _models.StagedUploadDestinationConfig,
    "teams": _models.TeamsDestinationConfig,
    "twilio": _models.TwilioDestinationConfig,
    "zendesk": _models.ZendeskDestinationConfig,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ConnectorDetail:
    """Structured detail for one connector. Renders to terminal or JSON."""

    type: str
    display_name: str
    kind: str  # "source" | "destination"
    config_class: str  # fully qualified class name for advanced users
    required_env_vars: list[str]
    optional_env_vars: list[str]
    required_fields: list[str]
    sample_yaml: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def build_source_detail(type_str: str, display_name: str) -> ConnectorDetail:
    cls = SOURCE_CONFIG_CLASSES.get(type_str)
    if cls is None:  # pragma: no cover — registry kept in sync via tests
        return _unknown_detail(type_str, display_name, "source")
    fields = _walk_dataclass_fields(cls)
    return _assemble(type_str, display_name, "source", cls, fields)


def build_destination_detail(type_str: str, display_name: str) -> ConnectorDetail:
    cls = DESTINATION_CONFIG_CLASSES.get(type_str)
    if cls is None:  # pragma: no cover — registry kept in sync via tests
        return _unknown_detail(type_str, display_name, "destination")
    fields = _walk_pydantic_fields(cls)
    return _assemble(type_str, display_name, "destination", cls, fields)


# ---------------------------------------------------------------------------
# Field walkers — normalise dataclass and Pydantic into a common shape
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _FieldInfo:
    name: str
    is_env_var: bool  # name ends in ``_env``
    is_required: bool
    default_repr: str  # for sample YAML; empty when no useful default


def _walk_dataclass_fields(cls: type) -> list[_FieldInfo]:
    if not is_dataclass(cls):  # pragma: no cover — sources are all dataclasses
        return []
    out: list[_FieldInfo] = []
    for f in dataclasses.fields(cls):
        if f.name == "type":
            continue  # discriminator, surfaced separately
        is_required = (
            f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
        )
        default_repr = _repr_dataclass_default(f)
        out.append(
            _FieldInfo(
                name=f.name,
                is_env_var=f.name.endswith("_env"),
                is_required=is_required,
                default_repr=default_repr,
            )
        )
    return out


def _walk_pydantic_fields(cls: type[BaseModel]) -> list[_FieldInfo]:
    out: list[_FieldInfo] = []
    for name, field in cls.model_fields.items():
        if name == "type":
            continue
        is_required = field.is_required()
        default_repr = ""
        if not is_required and field.default is not None:
            default_repr = repr(field.default)
        out.append(
            _FieldInfo(
                name=name,
                is_env_var=name.endswith("_env"),
                is_required=is_required,
                default_repr=default_repr,
            )
        )
    return out


def _repr_dataclass_default(f: dataclasses.Field[Any]) -> str:
    if f.default is not dataclasses.MISSING and f.default is not None:
        return repr(f.default)
    return ""


# ---------------------------------------------------------------------------
# Detail assembly
# ---------------------------------------------------------------------------


def _assemble(
    type_str: str,
    display_name: str,
    kind: str,
    cls: type,
    fields: list[_FieldInfo],
) -> ConnectorDetail:
    required_env_vars = sorted(f.name for f in fields if f.is_env_var and f.is_required)
    optional_env_vars = sorted(f.name for f in fields if f.is_env_var and not f.is_required)
    required_fields = sorted(f.name for f in fields if f.is_required and not f.is_env_var)
    return ConnectorDetail(
        type=type_str,
        display_name=display_name,
        kind=kind,
        config_class=f"{cls.__module__}.{cls.__qualname__}",
        required_env_vars=required_env_vars,
        optional_env_vars=optional_env_vars,
        required_fields=required_fields,
        sample_yaml=_render_sample_yaml(type_str, kind, fields),
    )


def _render_sample_yaml(type_str: str, kind: str, fields: list[_FieldInfo]) -> str:
    """Produce a copy-paste-ready 3–8 line YAML stanza.

    Strategy: required fields first (with ``<placeholder>``), then top-up
    with the most useful optional fields — connection-shaped env vars
    and common config keys — until we hit ~7 lines. Cap keeps terminal
    output digestible; full field surface lives on the config class.
    """
    indent = "      " if kind == "destination" else "  "
    lines: list[str] = [f"{indent}type: {type_str}"]
    emitted_names: set[str] = {"type"}

    # 1. Required fields take priority — user must supply these to validate.
    for f in fields:
        if not f.is_required or f.name in emitted_names:
            continue
        placeholder = f.default_repr if f.default_repr else f"<{f.name}>"
        lines.append(f"{indent}{f.name}: {placeholder}")
        emitted_names.add(f.name)
        if len(lines) >= 8:
            return "\n".join(lines)

    # 2. Preferred connection fields — names commonly needed even when
    # they have defaults (host, port, dbname, etc.). Source profiles use
    # bare field names; destination configs use ``_env`` suffixed forms.
    preferred = (
        "host",
        "host_env",
        "port",
        "port_env",
        "dbname",
        "dbname_env",
        "database",
        "database_env",
        "user",
        "user_env",
        "password_env",
        "account_env",
        "token_env",
        "api_key_env",
    )
    field_by_name = {f.name: f for f in fields}
    for name in preferred:
        if name not in field_by_name or name in emitted_names:
            continue
        f = field_by_name[name]
        if f.default_repr:
            placeholder = f.default_repr
        elif name.endswith("_env"):
            placeholder = f"<{name.upper()}>"
        else:
            placeholder = f"<{name}>"
        lines.append(f"{indent}{name}: {placeholder}")
        emitted_names.add(name)
        if len(lines) >= 8:
            break

    return "\n".join(lines)


def _unknown_detail(type_str: str, display_name: str, kind: str) -> ConnectorDetail:
    """Fallback when a connector is listed in SOURCES/DESTINATIONS but no class is
    registered. Should never happen in production — guarded by a registry-
    parity test.
    """
    return ConnectorDetail(
        type=type_str,
        display_name=display_name,
        kind=kind,
        config_class="(unregistered)",
        required_env_vars=[],
        optional_env_vars=[],
        required_fields=[],
        sample_yaml=f"type: {type_str}",
    )


# Re-export the typing module so ``mypy --strict`` is happy about the
# imports above; the field walkers do not currently need runtime
# typing.get_type_hints, but future field-type rendering will.
_ = typing  # noqa: F841 — placeholder for upcoming typed rendering
