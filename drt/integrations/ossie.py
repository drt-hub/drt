"""Lenient Apache Ossie reader and drt sync scaffold renderer (#1254).

Apache Ossie's official Python package is not yet published on PyPI and
requires Python 3.11+, while drt supports Python 3.10.  This deliberately
small adapter reads only the metadata needed to scaffold activation syncs.
Keeping the boundary in one module makes replacing it with the official
package straightforward once its distribution and Python floor fit drt.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_OSSIE_VERSION = "0.2.0.dev0"
SUPPORTED_DESTINATIONS = frozenset({"postgres", "hubspot"})
MAX_OSSIE_MODEL_BYTES = 5 * 1024 * 1024
_QUERY_START = re.compile(r"^\s*(?:SELECT|WITH)\b", re.IGNORECASE)
_IDENTIFIER_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PREFIXED_STRING = re.compile(r"(?:u&|[ebr])(?=['\"])", re.IGNORECASE)
_COMMENT_TEXT_LIMIT = 500

# Deliberately conservative: SQL:2016 reserved words plus common reserved
# extensions from PostgreSQL, MySQL, BigQuery, Snowflake, and Databricks. A
# false positive asks the user to choose a portable alias; a false negative can
# generate SQL whose meaning changes by destination dialect.
_RESERVED_WORDS = frozenset(
    """
    abs accessible acos add all allocate alter analyse analyze and anti any are array
    array_agg array_max_cardinality as asensitive asin asymmetric at atan atomic
    authorization avg begin begin_frame begin_partition between bigint binary blob
    boolean both by cache call called cardinality cascaded case cast ceil ceiling change
    char character character_length char_length check classifier clear clone close cluster
    coalesce collate collect column commit condition connect constraint contains convert corr
    corresponding cos cosh count covar_pop covar_samp create cross cube cume_dist current
    current_catalog current_date current_default_transform_group current_path current_role
    current_row current_schema current_time current_timestamp current_transform_group_for_type
    current_user cursor cycle database databases date day deallocate dec decimal declare default
    define delayed delete dense_rank deref describe deterministic disconnect distinct distribute
    div do double drop dynamic each element else empty enclosed end end_frame end_partition equals
    escape escaped every except exclude exec execute exists exit exp explain external extract false
    fetch fill filter first_value float floor following for force foreign format frame_row free
    freeze from full fulltext function fusion get global grant group grouping groups gscluster hash
    having high_priority hold hour identity ignore ilike in increment index indicator infile initial
    inner inout insensitive insert int integer intersect intersection interval into io_after_gtids
    io_before_gtids is isnull issue join json_array json_arrayagg json_exists json_object
    json_objectagg json_query json_table json_table_primitive json_value key keys kill lag language
    large last_value lateral lead leading left like like_regex limit linear lines listagg ln load
    local localtime localtimestamp lock long longblob longtext lookup lower low_priority master_bind
    master_ssl_verify_server_cert match matches match_number match_recognize max mediumblob
    mediumint mediumtext member merge method min minus minute mod modifies module month msck
    multiset national
    natural nchar nclob new no none normalize not notnull nth_value ntile null nullif numeric
    octet_length occurrences_regex of offset old omit on one only open optimize option optionally
    or order organization out outer outfile over overlaps overlay overwrite parameter partition
    pattern per percent percentile_cont percentile_disc percent_rank period pivot placing portion
    position position_regex power precedes precision prepare primary procedure proto purge qualify
    range rank read reads real recursive reduce ref references referencing refresh regexp regr_avgx
    regr_avgy regr_count regr_intercept regr_r2 regr_slope regr_sxx regr_sxy regr_syy release
    rename repair repeat replace require respect result return returning returns revoke right rlike
    rollback rollup row rows row_number running sample savepoint schema schemas scope scroll search
    second seek select semi sensitive session_user set show similar sin sinh skip smallint some sort
    spatial specific specifictype sql sqlexception sqlstate sqlwarning sqrt ssl start starting
    static stddev_pop stddev_samp straight_join struct submultiset subset substring substring_regex
    succeeds sum suspend symmetric system system_time system_user table tablesample tan tanh
    terminated then time timestamp timezone_hour timezone_minute tinyblob tinyint tinytext to
    trailing translate translate_regex translation treat trigger trim trim_array true truncate
    uescape
    unbounded uncache union unique unknown unlock unnest unpivot unsigned update upper use
    user using utc_date utc_time utc_timestamp value values varbinary varchar variadic varying
    var_pop var_samp verbose versioning when whenever where width_bucket window with within without
    write xor year zerofill
    """.split()
)


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


# GoogleSQL (BigQuery) reserved keywords, merged in because BigQuery is the strictest
# supported dialect. The check is deliberately best-effort: a reserved word missed here
# produces SQL the warehouse rejects when the sync is validated or run (a loud failure,
# not an unsafe one), and the user renames the field in the semantic model.
_RESERVED_WORDS = _RESERVED_WORDS | frozenset(
    """
    all and any array as asc assert_rows_modified at between by case cast collate contains
    create cross cube current default define desc distinct else end enum escape except
    exclude exists extract false fetch following for from full graph_table group grouping
    groups hash having if ignore in inner intersect interval into is join lateral left like
    limit lookup merge natural new no not null nulls of on or order outer over partition
    preceding proto qualify range recursive respect right rollup rows select set some struct
    tablesample then to treat true unbounded union unnest using when where window with within
    """.split()
)


def _reject_template_syntax(text: str, *, label: str) -> None:
    """Refuse text that drt would expand when it loads a generated sync.

    drt substitutes ``${VAR}`` from the environment and renders Jinja (``{{``, ``{%``,
    ``{#``) in config strings, so untrusted semantic-model text must not carry either.
    """
    for marker in ("${", "{{", "{%", "{#"):
        if marker in text:
            raise OssieModelError(
                f"{label} contains {marker!r}, which drt would expand when loading the "
                "generated sync; remove the template or environment syntax from the Ossie model."
            )


def _sql_error(label: str, problem: str, change: str) -> OssieModelError:
    return OssieModelError(f"{label} {problem}; {change}.")


def _validate_sql_expression(
    sql: str, *, label: str, allow_trailing_separator: bool = False
) -> None:
    """Accept only a small, unambiguous SQL quoting subset.

    This is intentionally not a dialect parser. Anything whose tokenization
    differs among supported warehouses is refused before it can be placed in a
    generated query.
    """
    # drt renders a sync's `model` through Jinja, so a template marker anywhere
    # (even inside a quoted literal) would execute at render time.
    for marker in ("{{", "{%", "{#"):
        if marker in sql:
            raise _sql_error(
                label,
                f"contains the Jinja template marker {marker!r}, which drt would render",
                "remove template syntax from the semantic model before scaffolding",
            )

    quote: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]

        if char == "\\":
            raise _sql_error(
                label,
                "contains a backslash, whose escaping rules vary by SQL dialect",
                "rewrite it without backslash escapes before scaffolding",
            )
        if char == "$":
            raise _sql_error(
                label,
                "contains a dollar sign, which may start dialect-specific quoting or substitution",
                "replace dollar quoting/placeholders with portable SQL before scaffolding",
            )
        if unicodedata.category(char) == "Cc" and char not in {"\t", "\n"}:
            display = "NUL" if char == "\x00" else f"control character U+{ord(char):04X}"
            raise _sql_error(
                label,
                f"contains forbidden {display}",
                "remove control characters before scaffolding",
            )

        if quote is not None:
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue

        prefixed = _PREFIXED_STRING.match(sql, index)
        if prefixed is not None and (
            index == 0 or not (sql[index - 1].isalnum() or sql[index - 1] == "_")
        ):
            raise _sql_error(
                label,
                f"contains dialect-specific prefixed string literal {prefixed.group(0)!r}",
                "use an ordinary quoted literal with doubled quotes instead",
            )

        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue

        token: str | None = None
        if char == ";":
            if allow_trailing_separator and not sql[index + 1 :].strip():
                index += 1
                continue
            token = ";"
        elif sql.startswith("--", index):
            token = "--"
        elif sql.startswith("/*", index):
            token = "/*"
        elif sql.startswith("*/", index):
            token = "*/"
        elif char == "#":
            token = "#"
        if token is not None:
            raise _sql_error(
                label,
                f"contains forbidden SQL separator/comment token {token!r} outside a quoted span",
                "remove it and provide one expression or one uncommented SELECT/WITH query",
            )
        index += 1

    if quote is not None:
        quote_name = {"'": "single", '"': "double", "`": "backtick"}[quote]
        raise _sql_error(
            label,
            f"contains an unterminated {quote_name}-quoted span",
            f"close the {quote_name}-quoted span and escape embedded quotes by doubling them",
        )


def _safe_comment_lines(text: str) -> list[str]:
    """Render bounded comment text with every physical line safely prefixed."""
    raw_lines = text.splitlines() or [""]
    clean_text = "\n".join(
        "".join(char for char in line if not unicodedata.category(char).startswith("C"))
        for line in raw_lines
    )
    if len(clean_text) > _COMMENT_TEXT_LIMIT:
        clean_text = clean_text[: _COMMENT_TEXT_LIMIT - 3] + "..."
    return [f"# {line}" for line in clean_text.split("\n")]


def _validate_alias(identifier: str, *, label: str) -> str:
    if not _IDENTIFIER_SEGMENT.fullmatch(identifier) or identifier != identifier.lower():
        raise OssieModelError(
            f"{label} {identifier!r} must be a safe lowercase SQL identifier; "
            "rename the field in the Ossie model."
        )
    if identifier in _RESERVED_WORDS:
        raise OssieModelError(
            f"{label} {identifier!r} is a reserved SQL word; rename the field in the Ossie model."
        )
    return identifier


MAX_DATASET_NAME_LENGTH = 64


def _validate_dataset_name(identifier: str) -> str:
    if len(identifier) > MAX_DATASET_NAME_LENGTH:
        raise OssieModelError(
            f"Dataset name {identifier[:20]!r}... is {len(identifier)} characters; the limit is "
            f"{MAX_DATASET_NAME_LENGTH} because it becomes a file name. Shorten it in the "
            "Ossie model."
        )
    if not _IDENTIFIER_SEGMENT.fullmatch(identifier):
        raise OssieModelError(
            f"Dataset name {identifier!r} must contain only letters, digits, and underscores "
            "and must not start with a digit."
        )
    return identifier


def _validate_source_identifier(identifier: str, *, dataset_name: str) -> str:
    segments = identifier.split(".")
    if len(segments) > 3:
        raise OssieModelError(
            f"Dataset {dataset_name!r} source identifier {identifier!r} has more than "
            "three segments."
        )
    for segment in segments:
        if not _IDENTIFIER_SEGMENT.fullmatch(segment):
            raise OssieModelError(
                f"Dataset {dataset_name!r} source identifier {identifier!r} has invalid "
                f"segment {segment!r}; each dot-separated segment must start with a letter "
                "or underscore and contain only letters, digits, and underscores."
            )
        if segment.lower() in _RESERVED_WORDS:
            raise OssieModelError(
                f"Dataset {dataset_name!r} source identifier {identifier!r} uses reserved "
                f"SQL word {segment!r} as a segment."
            )
    return identifier


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


def _expressions(
    value: Any, path: str, *, expression_label: str
) -> tuple[OssieDialectExpression, ...]:
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
        _validate_sql_expression(sql, label=expression_label)
        if dialect in seen:
            raise OssieModelError(f"{path} declares dialect {dialect!r} more than once.")
        seen.add(dialect)
        parsed.append(OssieDialectExpression(dialect=dialect, expression=sql))
    return tuple(parsed)


def _fields(value: Any, dataset_path: str, dataset_name: str) -> tuple[OssieField, ...]:
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
            raise OssieModelError(f"Dataset {dataset_name!r} has duplicate output alias {name!r}.")
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
                expressions=_expressions(
                    item.get("expression"),
                    f"{path}.expression",
                    expression_label=f"Dataset {dataset_name!r} field {name!r} expression",
                ),
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
        seen_keys: set[str] = set()
        duplicate_keys: set[str] = set()
        for key in primary_key:
            if key in seen_keys:
                duplicate_keys.add(key)
            seen_keys.add(key)
        if duplicate_keys:
            raise OssieModelError(
                f"Dataset {name!r} primary_key contains duplicate column(s): "
                f"{', '.join(sorted(duplicate_keys))}."
            )
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
                fields=_fields(item.get("fields"), path, name),
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
        name = _required_string(item, "name", path)
        parsed.append(
            OssieMetric(
                name=name,
                expressions=_expressions(
                    item.get("expression"),
                    f"{path}.expression",
                    expression_label=f"Metric {name!r} expression",
                ),
                description=_optional_string(item, "description", path),
            )
        )
    return tuple(parsed)


def load_ossie_model(path: Path) -> OssieModel:
    """Read one flat Ossie YAML/JSON model, ignoring fields drt does not use."""
    if not path.exists():
        raise FileNotFoundError(f"Ossie model not found: {path}")

    try:
        with path.open("rb") as model_file:
            payload = model_file.read(MAX_OSSIE_MODEL_BYTES + 1)
        if len(payload) > MAX_OSSIE_MODEL_BYTES:
            raise OssieModelError(
                f"Ossie model {path} exceeds the {MAX_OSSIE_MODEL_BYTES // (1024 * 1024)} MB "
                "input limit."
            )
        loaded = yaml.safe_load(payload.decode("utf-8"))
    except OssieModelError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError, RecursionError, MemoryError) as exc:
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
        _validate_sql_expression(
            source,
            label=f"Dataset {dataset.name!r} source query",
            allow_trailing_separator=True,
        )
        if source.endswith(";"):
            source = source[:-1].rstrip()
        return f"(\n{source}\n) AS _ossie_source"

    return _validate_source_identifier(source, dataset_name=dataset.name)


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
                "Choose a single stable key or scaffold a PostgreSQL destination instead."
            )
        return {
            "type": "hubspot",
            "object_type": "contacts",
            "id_property": dataset.primary_key[0],
            "auth": {"type": "bearer", "token_env": "HUBSPOT_TOKEN"},
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
        "# TODO before running:",
        "#   - Configure drt_project.yml and the source profile/credentials.",
        "#   - Replace destination credential and target placeholders.",
        "#   - Review column/property names; add sync.field_mappings where they differ.",
    ]
    comments[0:0] = _safe_comment_lines(f"Generated from Apache Ossie model {model.name!r}.")
    if destination == "postgres":
        comments.append("#   - Replace destination.table with the real target table name.")
    elif destination == "hubspot":
        comments.append("#   - Review object_type for the real HubSpot object.")
    if implicit_primary_key_fields:
        names = ", ".join(implicit_primary_key_fields)
        comments.extend(
            _safe_comment_lines(
                "Primary-key columns added directly because Ossie fields did not declare them: "
                + names
            )
        )
    if model.metrics:
        names = ", ".join(metric.name for metric in model.metrics)
        comments.extend(
            _safe_comment_lines(
                "Ossie metrics available but not generated (entity grain unclear): " + names
            )
        )
    comments.append("")
    return comments


def build_ossie_scaffold(
    model: OssieModel,
    dataset: OssieDataset,
    *,
    destination: str = "postgres",
    dialect: str = "ANSI_SQL",
) -> OssieScaffold:
    """Render one selected Ossie dataset as a validating drt sync YAML."""
    destination = destination.lower()
    dialect = dialect.upper()
    if not dataset.primary_key:
        raise OssieModelError(f"Dataset {dataset.name!r} has no primary_key.")
    if not dataset.fields:
        raise OssieModelError(f"Dataset {dataset.name!r} has no fields to generate.")

    _validate_dataset_name(dataset.name)
    for field in dataset.fields:
        _validate_alias(field.name, label=f"Dataset {dataset.name!r} field name")
    for key in dataset.primary_key:
        _validate_alias(key, label=f"Dataset {dataset.name!r} primary-key column")

    field_names = {field.name for field in dataset.fields}
    implicit_primary_key_fields = [key for key in dataset.primary_key if key not in field_names]

    select_items: list[str] = []
    output_aliases: set[str] = set()
    fallback_fields: list[str] = []
    for field in dataset.fields:
        if field.name in output_aliases:
            raise OssieModelError(
                f"Dataset {dataset.name!r} has duplicate output alias {field.name!r}."
            )
        output_aliases.add(field.name)
        expression, used_fallback = _select_expression(dataset, field, dialect)
        select_items.append(f"  {expression} AS {field.name}")
        if used_fallback:
            fallback_fields.append(field.name)
    for key in implicit_primary_key_fields:
        if key in output_aliases:
            raise OssieModelError(f"Dataset {dataset.name!r} has duplicate output alias {key!r}.")
        output_aliases.add(key)
        select_items.append(f"  {key} AS {key}")
    sql = "SELECT\n" + ",\n".join(select_items) + f"\nFROM {_render_source(dataset)}"

    version = model.version or "unspecified"
    # These strings become a YAML *value*, which drt expands on load (`${VAR}`
    # environment substitution and Jinja `{{ ... }}`), so template syntax in them could
    # read environment values into the config, docs and manifest. Refuse it.
    _reject_template_syntax(dataset.description, label=f"Dataset {dataset.name!r} description")
    _reject_template_syntax(model.name, label="Ossie model name")
    _reject_template_syntax(version, label="Ossie model version")
    description_lines = [dataset.description or f"Sync {dataset.name} to destination"]
    description_lines.append(f"Generated from Apache Ossie model {model.name} (version {version}).")
    sync_name = f"sync_{dataset.name}"
    sync_data = {
        "name": sync_name,
        "description": "\n".join(description_lines),
        "model": sql,
        "destination": _destination_config(destination, dataset),
        "sync": {"mode": "upsert"},
    }
    rendered = yaml.safe_dump(
        sync_data,
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
