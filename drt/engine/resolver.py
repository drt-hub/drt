"""Model reference resolver — translates `ref()` to runnable SQL.

Mirrors dbt's ref() concept but without requiring a dbt manifest.
Resolution order:
  1. syncs/models/{name}.sql  — raw SQL file wins
  2. dbt target/manifest.json — fully-qualified relation_name from dbt
  3. ref('name')              — SELECT * FROM {dataset}.{name}
  4. anything else            — used as SQL directly
"""

from __future__ import annotations

import re
from pathlib import Path

from drt.config.credentials import (
    BigQueryProfile,
    DuckDBProfile,
    PostgresProfile,
    ProfileConfig,
    SnowflakeProfile,
)

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
    profile: ProfileConfig,
    cursor_field: str | None = None,
    last_cursor_value: str | None = None,
) -> str:
    """Resolve a model reference to a runnable SQL query.

    Args:
        model_str: Value of the ``model:`` field in sync YAML.
            Can be ref('table_name'), a raw SQL string, or a table name.
        project_dir: Root of the drt project (contains syncs/).
        profile: Resolved profile (supplies dataset for ref() expansion).
        cursor_field: Column name used for incremental filtering (e.g. updated_at).
        last_cursor_value: Previous watermark; rows with cursor > this are fetched.

    Returns:
        A SQL query string ready to send to the source.
    """
    table_name = parse_ref(model_str)

    if table_name is not None:
        # 1. Check for a hand-written SQL file first
        sql_file = project_dir / "syncs" / "models" / f"{table_name}.sql"
        if sql_file.exists():
            base_sql = sql_file.read_text().strip()
        # 2. Check dbt manifest.json
        elif (dbt_table := _resolve_from_dbt(table_name, project_dir)) is not None:
            base_sql = f"SELECT * FROM {dbt_table}"
        elif isinstance(profile, BigQueryProfile):
            base_sql = f"SELECT * FROM `{profile.dataset}`.`{table_name}`"
        elif isinstance(profile, DuckDBProfile):
            base_sql = f"SELECT * FROM {table_name}"
        elif isinstance(profile, PostgresProfile):
            base_sql = f'SELECT * FROM "{table_name}"'
        elif isinstance(profile, SnowflakeProfile):
            if profile.database:
                base_sql = f'SELECT * FROM "{profile.database}"."{profile.schema}"."{table_name}"'
            else:
                base_sql = f'SELECT * FROM "{table_name}"'
        else:
            base_sql = f"SELECT * FROM {table_name}"
    else:
        # Not a ref() — treat as raw SQL or bare table name
        base_sql = model_str

    # Inject incremental WHERE clause when cursor info is available
    if cursor_field and last_cursor_value:
        safe_field = _validate_cursor_field(cursor_field)
        safe_value = last_cursor_value.replace("'", "''")  # standard SQL escaping
        return (
            f"SELECT * FROM ({base_sql}) AS _drt_base"
            f" WHERE {safe_field} > '{safe_value}'"
        )

    return base_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_from_dbt(table_name: str, project_dir: Path) -> str | None:
    """Try to resolve a table name from dbt manifest.json."""
    from drt.integrations.dbt import resolve_ref_from_manifest

    return resolve_ref_from_manifest(table_name, project_dir)


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
