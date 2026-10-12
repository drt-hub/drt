"""Lenient Apache Ossie reader and drt sync scaffold renderer (#1254).

Apache Ossie's official Python package is not yet published on PyPI and
requires Python 3.11+, while drt supports Python 3.10.  This deliberately
small adapter reads only the metadata needed to scaffold activation syncs.
Keeping the boundary in one module makes replacing it with the official
package straightforward once its distribution and Python floor fit drt.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from drt.engine.resolver import validate_sql_identifier

SUPPORTED_OSSIE_VERSION = "0.2.0.dev0"
SUPPORTED_DESTINATIONS = frozenset({"postgres", "hubspot", "rest_api"})
_QUERY_START = re.compile(r"^\s*(?:SELECT|WITH)\b", re.IGNORECASE)


class OssieModelError(ValueError):
    """An Ossie document cannot be used to scaffold a drt sync."""


@dataclass(frozen=True)
class OssieDialectExpression:
    dialect: str
    expression: str


@dataclass(frozen=True)
class OssieField:
    name: str
    expressions: tuple[OssieDialectExpression, ...]
    description: str = ""
    datatype: str | None = None
    is_time: bool | None = None
    ai_context: str | dict[str, Any] | None = None


@dataclass(frozen=True)
class OssieDataset:
    name: str
    source: str
    primary_key: tuple[str, ...]
    unique_keys: tuple[tuple[str, ...], ...]
    fields: tuple[OssieField, ...]
    description: str = ""
    ai_context: str | dict[str, Any] | None = None


@dataclass(frozen=True)
class OssieMetric:
    name: str
    expressions: tuple[OssieDialectExpression, ...]
    description: str = ""


@dataclass(frozen=True)
class OssieModel:
    name: str
    version: str | None
    datasets: tuple[OssieDataset, ...]
    description: str = ""
    ai_context: str | dict[str, Any] | None = None
    relationships: tuple[dict[str, Any], ...] = ()
    metrics: tuple[OssieMetric, ...] = ()
    custom_extensions: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class OssieScaffold:
    """One rendered sync plus dialect-fallback information for CLI output."""

    dataset_name: str
    sync_name: str
    filename: str
    yaml_text: str
    ansi_fallback_fields: tuple[str, ...]
    implicit_primary_key_fields: tuple[str, ...]


class _LiteralString(str):
    """Marker used by the YAML dumper for readable SQL and provenance blocks."""


class _ScaffoldDumper(yaml.SafeDumper):
    pass


def _represent_literal(dumper: yaml.SafeDumper, data: _LiteralString) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


_ScaffoldDumper.add_representer(_LiteralString, _represent_literal)


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise OssieModelError(f"{path} must be an object.")
    return value


def _sequence(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise OssieModelError(f"{path} must be an array.")
    return value


def _required_string(data: dict[str, Any], key: str, path: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise OssieModelError(f"{path}.{key} must be a non-empty string.")
    return value.strip()


def _optional_string(data: dict[str, Any], key: str, path: str) -> str:
    value = data.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise OssieModelError(f"{path}.{key} must be a string when present.")
    return value.strip()


def _ai_context(data: dict[str, Any], path: str) -> str | dict[str, Any] | None:
    value = data.get("ai_context")
    if value is None:
        return None
    if not isinstance(value, (str, dict)):
        raise OssieModelError(f"{path}.ai_context must be a string or object when present.")
    return value


def _string_list(value: Any, path: str) -> tuple[str, ...]:
    values = _sequence(value, path)
    parsed: list[str] = []
    for index, item in enumerate(values):
        if not isinstance(item, str) or not item.strip():
            raise OssieModelError(f"{path}[{index}] must be a non-empty string.")
        parsed.append(item.strip())
    return tuple(parsed)


def _expressions(value: Any, path: str) -> tuple[OssieDialectExpression, ...]:
    expression = _mapping(value, path)
    dialects = _sequence(expression.get("dialects"), f"{path}.dialects")
    if not dialects:
        raise OssieModelError(f"{path}.dialects must contain at least one expression.")

    parsed: list[OssieDialectExpression] = []
    seen: set[str] = set()
    for index, raw in enumerate(dialects):
        dialect_path = f"{path}.dialects[{index}]"
        item = _mapping(raw, dialect_path)
        dialect = _required_string(item, "dialect", dialect_path).upper()
        sql = _required_string(item, "expression", dialect_path)
        if dialect in seen:
            raise OssieModelError(f"{path} declares dialect {dialect!r} more than once.")
        seen.add(dialect)
        parsed.append(OssieDialectExpression(dialect=dialect, expression=sql))
    return tuple(parsed)


def _fields(value: Any, dataset_path: str) -> tuple[OssieField, ...]:
    if value is None:
        return ()
    fields = _sequence(value, f"{dataset_path}.fields")
    parsed: list[OssieField] = []
    seen: set[str] = set()
    for index, raw in enumerate(fields):
        path = f"{dataset_path}.fields[{index}]"
        item = _mapping(raw, path)
        name = _required_string(item, "name", path)
        if name in seen:
            raise OssieModelError(f"{dataset_path} declares field {name!r} more than once.")
        seen.add(name)
        datatype = item.get("datatype")
        if datatype is not None and not isinstance(datatype, str):
            raise OssieModelError(f"{path}.datatype must be a string when present.")
        dimension = item.get("dimension")
        is_time: bool | None = None
        if dimension is not None:
            dimension_data = _mapping(dimension, f"{path}.dimension")
            raw_is_time = dimension_data.get("is_time")
            if raw_is_time is not None and not isinstance(raw_is_time, bool):
                raise OssieModelError(f"{path}.dimension.is_time must be a boolean.")
            is_time = raw_is_time
        parsed.append(
            OssieField(
                name=name,
                expressions=_expressions(item.get("expression"), f"{path}.expression"),
                description=_optional_string(item, "description", path),
                datatype=datatype,
                is_time=is_time,
                ai_context=_ai_context(item, path),
            )
        )
    return tuple(parsed)


def _datasets(value: Any) -> tuple[OssieDataset, ...]:
    datasets = _sequence(value, "datasets")
    if not datasets:
        raise OssieModelError("datasets must contain at least one dataset.")

    parsed: list[OssieDataset] = []
    seen: set[str] = set()
    for index, raw in enumerate(datasets):
        path = f"datasets[{index}]"
        item = _mapping(raw, path)
        name = _required_string(item, "name", path)
        if name in seen:
            raise OssieModelError(f"datasets declares {name!r} more than once.")
        seen.add(name)

        primary_key = _string_list(item.get("primary_key", []), f"{path}.primary_key")
        raw_unique_keys = _sequence(item.get("unique_keys", []), f"{path}.unique_keys")
        unique_keys = tuple(
            _string_list(key, f"{path}.unique_keys[{key_index}]")
            for key_index, key in enumerate(raw_unique_keys)
        )
        parsed.append(
            OssieDataset(
                name=name,
                source=_required_string(item, "source", path),
                primary_key=primary_key,
                unique_keys=unique_keys,
                fields=_fields(item.get("fields"), path),
                description=_optional_string(item, "description", path),
                ai_context=_ai_context(item, path),
            )
        )
    return tuple(parsed)


def _object_list(value: Any, path: str) -> tuple[dict[str, Any], ...]:
    if value is None:
        return ()
    return tuple(
        _mapping(item, f"{path}[{index}]") for index, item in enumerate(_sequence(value, path))
    )


def _metrics(value: Any) -> tuple[OssieMetric, ...]:
    if value is None:
        return ()
    parsed: list[OssieMetric] = []
    for index, raw in enumerate(_sequence(value, "metrics")):
        path = f"metrics[{index}]"
        item = _mapping(raw, path)
        parsed.append(
            OssieMetric(
                name=_required_string(item, "name", path),
                expressions=_expressions(item.get("expression"), f"{path}.expression"),
                description=_optional_string(item, "description", path),
            )
        )
    return tuple(parsed)


def load_ossie_model(path: Path) -> OssieModel:
    """Read one flat Ossie YAML/JSON model, ignoring fields drt does not use."""
    if not path.exists():
        raise FileNotFoundError(f"Ossie model not found: {path}")

    try:
        loaded = yaml.safe_load(path.read_text())
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise OssieModelError(f"Could not read Ossie model {path}: {exc}") from exc

    root = _mapping(loaded, "Ossie document")
    name = _required_string(root, "name", "Ossie document")
    raw_version = root.get("version")
    warnings: list[str] = []
    version: str | None
    if raw_version is None:
        version = None
        warnings.append(
            "Ossie model has no version; continuing leniently because the fields used by "
            "this scaffold are present."
        )
    elif not isinstance(raw_version, str) or not raw_version.strip():
        raise OssieModelError("Ossie document.version must be a non-empty string when present.")
    else:
        version = raw_version.strip()
        if version != SUPPORTED_OSSIE_VERSION:
            warnings.append(
                f"Ossie model version {version!r} is not the version this spike was tested "
                f"against ({SUPPORTED_OSSIE_VERSION}); continuing with the supported subset."
            )

    return OssieModel(
        name=name,
        version=version,
        datasets=_datasets(root.get("datasets")),
        description=_optional_string(root, "description", "Ossie document"),
        ai_context=_ai_context(root, "Ossie document"),
        relationships=_object_list(root.get("relationships"), "relationships"),
        metrics=_metrics(root.get("metrics")),
        custom_extensions=_object_list(root.get("custom_extensions"), "custom_extensions"),
        warnings=tuple(warnings),
    )


def _select_expression(dataset: OssieDataset, field: OssieField, dialect: str) -> tuple[str, bool]:
    requested = dialect.upper()
    by_dialect = {item.dialect: item.expression for item in field.expressions}
    if requested in by_dialect:
        return by_dialect[requested], False
    if requested != "ANSI_SQL" and "ANSI_SQL" in by_dialect:
        return by_dialect["ANSI_SQL"], True

    available = ", ".join(sorted(by_dialect)) or "none"
    fallback = " and no ANSI_SQL fallback" if requested != "ANSI_SQL" else ""
    raise OssieModelError(
        f"Dataset {dataset.name!r} field {field.name!r} has no {requested} expression"
        f"{fallback} (available dialects: {available})."
    )


def _render_source(dataset: OssieDataset) -> str:
    source = dataset.source.strip()
    if _QUERY_START.match(source):
        query = source[:-1].rstrip() if source.endswith(";") else source
        if ";" in query or "\x00" in query:
            raise OssieModelError(
                f"Dataset {dataset.name!r} source query contains multiple statements or "
                "a NUL byte; only one SELECT/WITH query can be scaffolded."
            )
        return f"(\n{query}\n) AS _ossie_source"

    try:
        return validate_sql_identifier(source, label=f"dataset {dataset.name!r} source")
    except ValueError as exc:
        raise OssieModelError(
            f"Dataset {dataset.name!r} source must be a SELECT/WITH query or a safe "
            f"unquoted table identifier: {exc}"
        ) from exc


def _destination_config(destination: str, dataset: OssieDataset) -> dict[str, Any]:
    if destination == "postgres":
        return {
            "type": "postgres",
            "connection_string_env": "DRT_POSTGRES_URL",
            "table": "TODO_destination_table",
            "upsert_key": list(dataset.primary_key),
        }
    if destination == "hubspot":
        if len(dataset.primary_key) != 1:
            raise OssieModelError(
                f"Dataset {dataset.name!r} has composite primary_key "
                f"{list(dataset.primary_key)!r}; HubSpot requires exactly one id_property. "
                "Choose a single stable key or scaffold a SQL/REST destination instead."
            )
        return {
            "type": "hubspot",
            "object_type": "contacts",
            "id_property": dataset.primary_key[0],
            "auth": {"type": "bearer", "token_env": "HUBSPOT_TOKEN"},
        }
    if destination == "rest_api":
        return {
            "type": "rest_api",
            "url": "https://example.com/api",
            "method": "POST",
        }
    supported = ", ".join(sorted(SUPPORTED_DESTINATIONS))
    raise OssieModelError(
        f"Unsupported Ossie scaffold destination {destination!r}; choose one of: {supported}."
    )


def _todo_comments(
    model: OssieModel,
    dataset: OssieDataset,
    destination: str,
    implicit_primary_key_fields: list[str],
) -> list[str]:
    comments = [
        f"# Generated from Apache Ossie model {model.name!r}.",
        "# TODO before running:",
        "#   - Configure drt_project.yml and the source profile/credentials.",
        "#   - Replace destination credential and endpoint placeholders.",
        "#   - Review column/property names; add sync.field_mappings where they differ.",
    ]
    if destination == "postgres":
        comments.append("#   - Replace destination.table with the real target table name.")
    elif destination == "hubspot":
        comments.append("#   - Review object_type for the real HubSpot object.")
    else:
        comments.append("#   - Map fields to the destination API's real property names/body shape.")
    if implicit_primary_key_fields:
        names = ", ".join(implicit_primary_key_fields)
        comments.append(
            "# Primary-key columns added directly because Ossie fields did not declare them: "
            + names
        )
    if model.metrics:
        names = ", ".join(metric.name for metric in model.metrics)
        comments.append(
            "# Ossie metrics available but not generated (entity grain unclear): " + names
        )
    comments.append("")
    return comments


def build_ossie_scaffold(
    model: OssieModel,
    dataset: OssieDataset,
    *,
    destination: str = "rest_api",
    dialect: str = "ANSI_SQL",
) -> OssieScaffold:
    """Render one selected Ossie dataset as a validating drt sync YAML."""
    destination = destination.lower()
    dialect = dialect.upper()
    if not dataset.primary_key:
        raise OssieModelError(f"Dataset {dataset.name!r} has no primary_key.")
    if not dataset.fields:
        raise OssieModelError(f"Dataset {dataset.name!r} has no fields to generate.")

    try:
        validate_sql_identifier(dataset.name, label="dataset name")
        if "." in dataset.name:
            raise ValueError(f"dataset name {dataset.name!r} must not be qualified.")
        for field in dataset.fields:
            validate_sql_identifier(field.name, label=f"dataset {dataset.name!r} field name")
            if "." in field.name:
                raise ValueError(
                    f"dataset {dataset.name!r} field name {field.name!r} must not be qualified."
                )
        for key in dataset.primary_key:
            validate_sql_identifier(key, label=f"dataset {dataset.name!r} primary key")
            if "." in key:
                raise ValueError(
                    f"dataset {dataset.name!r} primary key {key!r} must not be qualified."
                )
    except ValueError as exc:
        raise OssieModelError(str(exc)) from exc

    field_names = {field.name for field in dataset.fields}
    implicit_primary_key_fields = [key for key in dataset.primary_key if key not in field_names]

    select_items: list[str] = []
    fallback_fields: list[str] = []
    for field in dataset.fields:
        expression, used_fallback = _select_expression(dataset, field, dialect)
        select_items.append(f"  {expression} AS {field.name}")
        if used_fallback:
            fallback_fields.append(field.name)
    select_items.extend(f"  {key} AS {key}" for key in implicit_primary_key_fields)
    sql = "SELECT\n" + ",\n".join(select_items) + f"\nFROM {_render_source(dataset)}"

    version = model.version or "unspecified"
    description_lines = [dataset.description or f"Sync {dataset.name} to destination"]
    description_lines.append(f"Generated from Apache Ossie model {model.name} (version {version}).")
    sync_name = f"sync_{dataset.name}"
    sync_data = {
        "name": sync_name,
        "description": _LiteralString("\n".join(description_lines)),
        "model": _LiteralString(sql),
        "destination": _destination_config(destination, dataset),
        "sync": {"mode": "upsert"},
    }
    rendered = yaml.dump(
        sync_data,
        Dumper=_ScaffoldDumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    comments = "\n".join(
        _todo_comments(
            model,
            dataset,
            destination,
            implicit_primary_key_fields,
        )
    )
    return OssieScaffold(
        dataset_name=dataset.name,
        sync_name=sync_name,
        filename=f"{sync_name}.yml",
        yaml_text=comments + rendered,
        ansi_fallback_fields=tuple(fallback_fields),
        implicit_primary_key_fields=tuple(implicit_primary_key_fields),
    )
