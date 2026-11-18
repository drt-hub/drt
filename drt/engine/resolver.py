"""Model reference resolver — translates `ref()` to runnable SQL.

Mirrors dbt's ref() concept but without requiring a dbt manifest.
Resolution order:
  1. syncs/models/{name}.sql  — raw SQL file wins
  2. dbt target/manifest.json — fully-qualified relation_name from dbt
  3. ref('name')              — SELECT * FROM {dataset}.{name}
  4. anything else            — used as SQL directly
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from jinja2 import TemplateSyntaxError, meta

from drt.config.credentials import (
    BigQueryProfile,
    DatabricksProfile,
    DuckDBProfile,
    MySQLProfile,
    PostgresProfile,
    SnowflakeProfile,
    SQLServerProfile,
)
from drt.config.profiles import ProfileConfigLike
from drt.config.vars import has_var_template, var_environment
from drt.engine.backfill import BackfillBound, BackfillWindow, canonicalize_bound
from drt.sources.base import WINDOWED_BACKFILL_QUERY_MARKER

# Matches: ref('table') or ref("table")
_REF_PATTERN = re.compile(r"""^ref\(\s*['"]([^'"]+)['"]\s*\)$""", re.IGNORECASE)


def parse_ref(model_str: str) -> str | None:
    """Extract table name from ref() syntax.

    Returns:
        Table name string if model_str matches ref() pattern, else None.

    Examples:
        >>> parse_ref("ref('new_users')")
        'new_users'
        >>> parse_ref("ref(\\"orders\\")")
        'orders'
        >>> parse_ref("SELECT * FROM orders")
        None
    """
    m = _REF_PATTERN.match(model_str.strip())
    return m.group(1) if m else None


def resolve_model_ref(
    model_str: str,
    project_dir: Path,
    profile: ProfileConfigLike,
    cursor_field: str | None = None,
    last_cursor_value: str | None = None,
    vars: dict[str, Any] | None = None,
    dbt_relation_name: str | None = None,
    dbt_manifest_resolved: bool = False,
    window_bounds: BackfillWindow | None = None,
    sync_name: str | None = None,
    windowed_backfill: bool = False,
) -> str:
    """Resolve a model reference to a runnable SQL query.

    Args:
        model_str: Value of the ``model:`` field in sync YAML.
            Can be ref('table_name'), a raw SQL string, or a table name.
        project_dir: Root of the drt project (contains syncs/).
        profile: Resolved profile (supplies dataset for ref() expansion).
        cursor_field: Column name used for incremental filtering (e.g. updated_at).
        last_cursor_value: Previous watermark; rows with cursor > this are fetched.
        vars: Resolved project vars for ``{{ var('name') }}`` (#783).
        dbt_relation_name: Relation snapshotted while resolving a dbt-backed
            column contract. Avoids a second manifest read during execution.
        dbt_manifest_resolved: Whether the manifest was already resolved even
            if that model has no ``relation_name``.
        window_bounds: Optional half-open ``(low, high)`` range for a windowed
            backfill. When present, the resolver always applies both bounds
            outside the resolved model subquery.
        sync_name: Name used to make windowed-template errors actionable.
        windowed_backfill: Validate the resolved model for backfill use even
            before a numeric automatic ceiling supplies concrete bounds.

    Returns:
        A SQL query string ready to send to the source.
    """
    table_name = parse_ref(model_str)

    if table_name is not None:
        # 1. Check for a hand-written SQL file first
        sql_file = project_dir / "syncs" / "models" / f"{table_name}.sql"
        if sql_file.exists():
            base_sql = sql_file.read_text()
            if window_bounds is None and not windowed_backfill:
                base_sql = base_sql.strip()
        # 2. Check dbt manifest.json
        elif dbt_manifest_resolved and dbt_relation_name is not None:
            base_sql = f"SELECT * FROM {dbt_relation_name}"
        # A dbt-backed contract snapshots manifest resolution during config
        # loading. If its node has no relation_name, continue to the normal
        # profile fallback without opening the manifest again.
        elif (
            not dbt_manifest_resolved
            and (dbt_table := _resolve_from_dbt(table_name, project_dir)) is not None
        ):
            base_sql = f"SELECT * FROM {dbt_table}"
        elif isinstance(profile, BigQueryProfile):
            base_sql = f"SELECT * FROM `{profile.dataset}`.`{table_name}`"
        elif isinstance(profile, DuckDBProfile):
            base_sql = f"SELECT * FROM {table_name}"
        elif isinstance(profile, PostgresProfile):
            base_sql = f'SELECT * FROM "{table_name}"'
        elif isinstance(profile, MySQLProfile):
            base_sql = f"SELECT * FROM `{table_name}`"
        elif isinstance(profile, SnowflakeProfile):
            if profile.database:
                base_sql = f'SELECT * FROM "{profile.database}"."{profile.schema}"."{table_name}"'
            else:
                base_sql = f'SELECT * FROM "{table_name}"'
        elif isinstance(profile, DatabricksProfile):
            # Unity Catalog: catalog.schema.table, else schema.table
            if profile.catalog:
                base_sql = f"SELECT * FROM {profile.catalog}.{profile.schema}.{table_name}"
            else:
                base_sql = f"SELECT * FROM {profile.schema}.{table_name}"
        elif isinstance(profile, SQLServerProfile):
            base_sql = f"SELECT * FROM [{profile.schema}].[{table_name}]"
        else:
            base_sql = f"SELECT * FROM {table_name}"
    else:
        # Not a ref() — treat as raw SQL or bare table name
        base_sql = model_str

    # Expand environment variables: ${VAR} syntax
    base_sql = _expand_env_vars(base_sql)

    # Render the template surface — {{ cursor_value }} / {{ watermark }} and
    # {{ var('name') }} (#783) — in one pass, so SQL can use both.
    cursor_templated = has_cursor_template(base_sql)
    var_templated = has_var_template(base_sql)
    if cursor_templated and (window_bounds is not None or windowed_backfill):
        template = _cursor_template_display(base_sql)
        subject = f" for sync {sync_name!r}" if sync_name is not None else ""
        raise ValueError(
            f"sync.backfill{subject} cannot use the legacy lower-only cursor template "
            f"{template}. Remove the template and let drt apply both half-open window bounds."
        )
    if cursor_templated or var_templated:
        if cursor_templated and last_cursor_value is None:
            raise ValueError(
                "Cannot render cursor template: no cursor value provided. "
                "Set watermark.default_value in your sync config or use --cursor-value."
            )
        if var_templated and vars is None:
            # ``vars=None`` means "resolve from the project" — the same contract
            # ``load_syncs`` has. Callers that don't thread vars (``drt build``,
            # library callers) still get the ``vars:`` block + ``DRT_VAR_*``;
            # only ``--vars`` needs explicit threading. Read lazily and only
            # when the SQL actually uses ``var()``, so var-less projects pay
            # nothing.
            from drt.config.parser import project_vars

            vars = project_vars(project_dir)
        preserve_final_newline = (
            window_bounds is not None or windowed_backfill
        ) and base_sql.endswith(("\n", "\r"))
        base_sql = _render_template(base_sql, last_cursor_value, vars)
        if preserve_final_newline and not base_sql.endswith(("\n", "\r")):
            base_sql += "\n"
        if cursor_templated:
            # A cursor template means the user placed the predicate themselves;
            # never also inject a WHERE clause (pre-#783 behaviour).
            return base_sql

    if window_bounds is not None:
        if cursor_field is None:
            raise ValueError("sync.backfill requires sync.cursor_field for bounded extraction.")
        low, high = window_bounds
        return wrap_windowed_query(base_sql, profile, cursor_field, low, high)

    # Inject incremental WHERE clause when cursor info is available
    if cursor_field and last_cursor_value:
        safe_field = _validate_cursor_field(cursor_field)
        safe_value = last_cursor_value.replace("'", "''")  # standard SQL escaping
        return f"SELECT * FROM ({base_sql}) AS _drt_base WHERE {safe_field} > '{safe_value}'"

    return base_sql


# ---------------------------------------------------------------------------
# Cursor template helpers
# ---------------------------------------------------------------------------

_CURSOR_TEMPLATE_PATTERN = re.compile(r"\{\{\s*(cursor_value|watermark)\s*\}\}")
_JINJA_BLOCK_PATTERN = re.compile(r"\{\{(.*?)\}\}|\{%(.*?)%\}", re.DOTALL)
_CURSOR_NAME_PATTERN = re.compile(r"\b(?:cursor_value|watermark)\b")


def has_cursor_template(sql: str) -> bool:
    """Detect legacy cursor variables from the Jinja syntax tree.

    Filters and control-flow references count. Plain SQL mentions do not.
    Invalid Jinja scans expression and statement block bodies for either
    legacy cursor name so malformed templates fail closed.
    """
    try:
        parsed = var_environment().parse(sql)
    except TemplateSyntaxError:
        return any(
            _CURSOR_NAME_PATTERN.search(body) is not None
            for match in _JINJA_BLOCK_PATTERN.finditer(sql)
            if (body := (match.group(1) if match.group(1) is not None else match.group(2)))
            is not None
        )
    return bool({"cursor_value", "watermark"} & meta.find_undeclared_variables(parsed))


def _cursor_template_display(sql: str) -> str:
    """Return the first legacy cursor template in canonical display form."""
    match = _CURSOR_TEMPLATE_PATTERN.search(sql)
    if match is not None:
        return "{{ " + match.group(1) + " }}"
    try:
        names = meta.find_undeclared_variables(var_environment().parse(sql))
    except TemplateSyntaxError:
        names = set()
    name = "cursor_value" if "cursor_value" in names else "watermark"
    return "{{ " + name + " }}"


def _render_template(sql: str, cursor_value: str | None, vars: dict[str, Any] | None = None) -> str:
    """Render the SQL template surface: ``cursor_value`` / ``watermark`` plus
    ``var()`` (#783). Deliberately tiny — no control flow or macros."""
    env = var_environment(vars)
    tmpl = env.from_string(sql)
    return tmpl.render(cursor_value=cursor_value, watermark=cursor_value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_from_dbt(table_name: str, project_dir: Path) -> str | None:
    """Try to resolve a table name from dbt manifest.json."""
    from drt.integrations.dbt import resolve_ref_from_manifest

    return resolve_ref_from_manifest(table_name, project_dir)


_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env_vars(sql: str) -> str:
    """Expand ``${VAR}`` placeholders with environment variable values.

    Raises ``ValueError`` if a referenced variable is not set.

    .. note::
        For generic (non-SQL) expansion across YAML config trees,
        see :func:`drt.config.parser.expand_env_vars`.
    """

    def _replace(match: re.Match[str]) -> str:
        var = match.group(1)
        val = os.environ.get(var)
        if val is None:
            raise ValueError(f"Environment variable ${{{var}}} is not set")
        return val

    return _ENV_VAR_PATTERN.sub(_replace, sql)


_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


def _validate_cursor_field(field: str) -> str:
    """Ensure cursor_field is a safe SQL identifier (letters/digits/underscore/dot).

    Raises ValueError for anything that could enable SQL injection.
    """
    if not _SAFE_IDENTIFIER.match(field):
        raise ValueError(
            f"cursor_field {field!r} contains invalid characters. "
            "Only letters, digits, underscores, and dots are allowed."
        )
    return field


# ---------------------------------------------------------------------------
# Windowed backfill SQL
# ---------------------------------------------------------------------------

# Every built-in SQL-shaped source whose ordinary incremental cursor is
# injected by this resolver today. Delta Lake and Iceberg execute their model
# SQL through DuckDB and therefore use its ANSI quoting family.
SQL_SOURCE_PROFILE_TYPES = frozenset(
    {
        "bigquery",
        "clickhouse",
        "databricks",
        "duckdb",
        "mysql",
        "postgres",
        "redshift",
        "snowflake",
        "sqlite",
        "sqlserver",
        "deltalake",
        "iceberg",
    }
)

_BACKTICK_IDENTIFIER_TYPES = frozenset({"clickhouse", "databricks", "mysql"})
_DOUBLE_QUOTE_IDENTIFIER_TYPES = frozenset(
    {"duckdb", "postgres", "redshift", "snowflake", "sqlite", "deltalake", "iceberg"}
)


def is_sql_source_profile(profile: ProfileConfigLike) -> bool:
    """Return whether a built-in profile executes resolved SQL model text."""
    return profile.type in SQL_SOURCE_PROFILE_TYPES


def quote_window_identifier(identifier: str, profile: ProfileConfigLike) -> str:
    """Quote one top-level cursor identifier using the source dialect's rules."""
    if "." in identifier:
        raise ValueError(
            "nested cursor paths are not supported for windowed backfill; "
            "expose the field as a top-level column in the model."
        )
    if not identifier or any(ord(char) < 32 for char in identifier):
        raise ValueError(
            f"sync.cursor_field must be a non-empty SQL identifier; got {identifier!r}."
        )

    source_type = profile.type
    if source_type in {"postgres", "redshift"}:
        identifier = identifier.lower()
    elif source_type == "snowflake":
        identifier = identifier.upper()

    if source_type == "bigquery":
        # Match drt.sources.bigquery._quote_column: BigQuery has its own
        # escape grammar, so reject embedded backticks instead of borrowing
        # MySQL's doubling rule.
        if "`" in identifier:
            raise ValueError(f"Invalid BigQuery cursor column identifier: {identifier!r}")
        return f"`{identifier}`"
    if source_type in _BACKTICK_IDENTIFIER_TYPES:
        return f"`{identifier.replace('`', '``')}`"
    if source_type in _DOUBLE_QUOTE_IDENTIFIER_TYPES:
        return f'"{identifier.replace(chr(34), chr(34) * 2)}"'
    if source_type == "sqlserver":
        return f"[{identifier.replace(']', ']]')}]"
    raise ValueError(
        f"Source profile type {source_type!r} is not a SQL-shaped source for sync.backfill; "
        "configure an explicit sync.backfill.end and use a WindowedIncrementalSource."
    )


def _render_window_literal(
    value: BackfillBound, profile: ProfileConfigLike, *, field_name: str
) -> str:
    canonical = canonicalize_bound(value, field_name=field_name)
    if isinstance(canonical, int):
        return str(canonical)
    escaped = canonical.replace("'", "''")
    source_type = profile.type
    if source_type in {"postgres", "redshift", "duckdb", "deltalake", "iceberg"}:
        return f"TIMESTAMPTZ '{escaped}+00'"
    if source_type == "snowflake":
        return f"TO_TIMESTAMP_TZ('{escaped}+00:00')"
    if source_type == "bigquery":
        return f"TIMESTAMP '{escaped}+00'"
    if source_type == "databricks":
        return f"TIMESTAMP '{escaped}+00:00'"
    if source_type == "sqlserver":
        return f"CAST('{escaped}' AS datetime2(7))"
    if source_type == "clickhouse":
        return f"toDateTime64('{escaped}', 6, 'UTC')"
    if source_type in {"mysql", "sqlite"}:
        # MySQLSource switches generated windowed queries to a UTC session;
        # SQLite has no timezone-aware value type and compares canonical UTC
        # text. In both cases a naive cursor column is assumed to contain UTC.
        return f"'{escaped}'"
    raise ValueError(f"Unsupported SQL source profile type for timestamp backfill: {source_type!r}")


def _prepare_derived_model(resolved_model: str, profile: ProfileConfigLike) -> str:
    """Validate and normalize one model before derived-table insertion."""
    model = resolved_model.rstrip()
    while re.search(r"--[^\r\n]*\Z", model):
        model = re.sub(r"--[^\r\n]*\Z", "", model).rstrip()
    if model.endswith(";"):
        model = model[:-1].rstrip()
    comment_free = re.sub(r"--[^\r\n]*(?:\r?\n|\Z)|/\*.*?\*/", "", model, flags=re.S)
    if not comment_free.strip():
        raise ValueError("sync.backfill model must not be empty or contain only comments.")
    leading = re.sub(r"\A(?:\s+|--[^\r\n]*(?:\r?\n|\Z)|/\*.*?\*/)*", "", model, flags=re.S)
    if profile.type == "sqlserver" and re.match(r";?\s*WITH\b", leading, flags=re.I):
        raise ValueError(
            "A model starting with WITH is not supported for windowed backfill on SQL Server: "
            "move the CTE into a view or subquery."
        )
    return model


def _window_cursor_expression(quoted_field: str, profile: ProfileConfigLike) -> str:
    """Normalize DuckDB timestamp families to an epoch with naive values as UTC."""
    if profile.type in {"duckdb", "deltalake", "iceberg"}:
        return f"epoch({quoted_field})"
    return quoted_field


def _window_bound_expression(literal: str, profile: ProfileConfigLike) -> str:
    if profile.type in {"duckdb", "deltalake", "iceberg"}:
        return f"epoch({literal})"
    return literal


def wrap_windowed_query(
    resolved_model: str,
    profile: ProfileConfigLike,
    cursor_field: str,
    low: BackfillBound,
    high: BackfillBound,
) -> str:
    """Wrap a resolved SQL model with an exact half-open cursor predicate."""
    quoted_field = quote_window_identifier(cursor_field, profile)
    low_sql = _render_window_literal(low, profile, field_name="sync.backfill window low")
    high_sql = _render_window_literal(high, profile, field_name="sync.backfill window high")
    if (
        isinstance(low, bool)
        or isinstance(high, bool)
        or isinstance(low, int) != isinstance(high, int)
    ):
        raise ValueError("sync.backfill window bounds must use the same timestamp or integer type.")
    model = _prepare_derived_model(resolved_model, profile)
    cursor_sql = quoted_field
    if isinstance(low, str):
        cursor_sql = _window_cursor_expression(quoted_field, profile)
        low_sql = _window_bound_expression(low_sql, profile)
        high_sql = _window_bound_expression(high_sql, profile)
    return (
        f"SELECT {WINDOWED_BACKFILL_QUERY_MARKER} * FROM (\n"
        f"{model}\n"
        ") AS _drt_base\n"
        f"WHERE {cursor_sql} >= {low_sql} AND {cursor_sql} < {high_sql}"
    )


def build_numeric_ceiling_query(
    resolved_model: str,
    profile: ProfileConfigLike,
    cursor_field: str,
) -> str:
    """Build the one-row ``MAX(cursor_field)`` snapshot query for numeric backfills."""
    quoted_field = quote_window_identifier(cursor_field, profile)
    quoted_alias = quote_window_identifier("_drt_cursor_max", profile)
    model = _prepare_derived_model(resolved_model, profile)
    return f"SELECT MAX({quoted_field}) AS {quoted_alias} FROM (\n{model}\n) AS _drt_base"


def resolve_windowed_model_ref(
    model_str: str,
    project_dir: Path,
    profile: ProfileConfigLike,
    cursor_field: str,
    low: BackfillBound,
    high: BackfillBound,
    *,
    sync_name: str | None = None,
    vars: dict[str, Any] | None = None,
    dbt_relation_name: str | None = None,
    dbt_manifest_resolved: bool = False,
) -> str:
    """Resolve a model and apply one mandatory half-open backfill window."""
    return resolve_model_ref(
        model_str,
        project_dir,
        profile,
        cursor_field=cursor_field,
        vars=vars,
        dbt_relation_name=dbt_relation_name,
        dbt_manifest_resolved=dbt_manifest_resolved,
        window_bounds=(low, high),
        sync_name=sync_name,
        windowed_backfill=True,
    )
