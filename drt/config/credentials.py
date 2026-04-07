"""Credential and profile management — dbt profiles.yml pattern.

Credentials never live in drt_project.yml (which is Git-safe).
They live in ~/.drt/profiles.yml (outside version control).

Example ~/.drt/profiles.yml:

    dev:
      type: bigquery
      project: my-gcp-project
      dataset: analytics
      method: application_default

    local:
      type: duckdb
      database: ./data/warehouse.duckdb

    pg:
      type: postgres
      host: localhost
      port: 5432
      dbname: analytics
      user: analyst
      password_env: PG_PASSWORD
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

# ---------------------------------------------------------------------------
# Source profile types
# ---------------------------------------------------------------------------

@dataclass
class BigQueryProfile:
    type: Literal["bigquery"]
    project: str
    dataset: str
    method: Literal["application_default", "keyfile"] = "application_default"
    keyfile: str | None = None
    location: str = "US"  # e.g. "US", "EU", "asia-northeast1"
    
    def describe(self) -> str:
        return f"{self.type} ({self.project}.{self.dataset})"


@dataclass
class DuckDBProfile:
    type: Literal["duckdb"]
    database: str = ":memory:"  # path or :memory:

    def describe(self) -> str:
        return f"{self.type} ({self.database})"


@dataclass
class SQLiteProfile:
    type: Literal["sqlite"]
    database: str = ":memory:"  # path or :memory:

    def describe(self) -> str:
        return f"{self.type} ({self.database})"


@dataclass
class PostgresProfile:
    type: Literal["postgres"]
    host: str = "localhost"
    port: int = 5432
    dbname: str = ""
    user: str = ""
    password_env: str | None = None  # env var name
    password: str | None = None  # explicit (non-recommended)

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.dbname})"


@dataclass
class RedshiftProfile:
    """Amazon Redshift profile — PostgreSQL-compatible with schema support.

    Example ~/.drt/profiles.yml:
        redshift_prod:
          type: redshift
          host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
          port: 5439
          dbname: analytics
          user: analyst
          password_env: REDSHIFT_PASSWORD
          schema: public
    """

    type: Literal["redshift"]
    host: str = ""
    port: int = 5439  # Redshift default port
    dbname: str = ""
    user: str = ""
    password_env: str | None = None  # env var name
    password: str | None = None  # explicit (non-recommended)
    schema: str = "public"  # Redshift schema

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.dbname})"


@dataclass
class ClickHouseProfile:
    """ClickHouse profile via HTTP/s using clickhouse-connect."""

    type: Literal["clickhouse"]
    host: str = "localhost"
    port: int = 8123
    database: str = "default"
    user: str = "default"
    password_env: str | None = None  # env var name
    password: str | None = None  # explicit (non-recommended)

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.database})"


@dataclass
class SnowflakeProfile:
    """Snowflake profile using snowflake-connector-python."""

    type: Literal["snowflake"]
    account: str = ""
    user: str = ""
    password_env: str | None = None
    password: str | None = None
    database: str = ""
    schema: str = "PUBLIC"
    warehouse: str = ""
    role: str | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.account}/{self.database}.{self.schema})"

# Union type — used throughout the codebase
ProfileConfig = (
    BigQueryProfile
    | DuckDBProfile
    | SQLiteProfile
    | PostgresProfile
    | RedshiftProfile
    | ClickHouseProfile
    | SnowflakeProfile
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config_dir(override: Path | None = None) -> Path:
    return override if override is not None else Path.home() / ".drt"


def resolve_env(value: str | None, env_var: str | None) -> str | None:
    """Resolve a secret value: explicit value → env var → None."""
    if value is not None:
        return value
    if env_var is not None:
        return os.environ.get(env_var)
    return None


# ---------------------------------------------------------------------------
# Load / Save
# ---------------------------------------------------------------------------


def load_profile(profile_name: str, config_dir: Path | None = None) -> ProfileConfig:
    """Load a named profile from ~/.drt/profiles.yml.

    Args:
        profile_name: Key in profiles.yml (e.g. "dev", "local").
        config_dir: Override ~/.drt for testing.

    Raises:
        FileNotFoundError: profiles.yml does not exist.
        KeyError: profile_name not found.
        ValueError: Unknown source type or missing required fields.
    """
    profiles_path = _config_dir(config_dir) / "profiles.yml"
    if not profiles_path.exists():
        raise FileNotFoundError(
            f"profiles.yml not found at {profiles_path}. "
            "Run `drt init` to create it, or create it manually."
        )

    with profiles_path.open() as f:
        data = yaml.safe_load(f) or {}

    if profile_name not in data:
        available = ", ".join(data.keys()) or "(none)"
        raise KeyError(
            f"Profile '{profile_name}' not found in {profiles_path}. Available: {available}"
        )

    raw = data[profile_name]
    source_type = raw.get("type")

    if source_type == "bigquery":
        return BigQueryProfile(
            type="bigquery",
            project=raw["project"],
            dataset=raw["dataset"],
            method=raw.get("method", "application_default"),
            keyfile=raw.get("keyfile"),
            location=raw.get("location", "US"),
        )
    if source_type == "duckdb":
        return DuckDBProfile(
            type="duckdb",
            database=raw.get("database", ":memory:"),
        )

    if source_type == "sqlite":
        return SQLiteProfile(
            type="sqlite",
            database=raw.get("database", ":memory:"),
        )
    if source_type == "postgres":
        return PostgresProfile(
            type="postgres",
            host=raw.get("host", "localhost"),
            port=int(raw.get("port", 5432)),
            dbname=raw.get("dbname", ""),
            user=raw.get("user", ""),
            password_env=raw.get("password_env"),
            password=raw.get("password"),
        )

    if source_type == "redshift":
        return RedshiftProfile(
            type="redshift",
            host=raw.get("host", ""),
            port=int(raw.get("port", 5439)),
            dbname=raw.get("dbname", ""),
            user=raw.get("user", ""),
            password_env=raw.get("password_env"),
            password=raw.get("password"),
            schema=raw.get("schema", "public"),
        )

    if source_type == "clickhouse":
        return ClickHouseProfile(
            type="clickhouse",
            host=raw.get("host", "localhost"),
            port=int(raw.get("port", 8123)),
            database=raw.get("database", "default"),
            user=raw.get("user", "default"),
            password_env=raw.get("password_env"),
            password=raw.get("password"),
        )

    if source_type == "snowflake":
        _db = raw.get("database", "")
        if not _db:
            raise ValueError(
                "Snowflake profile requires 'database'. "
                "Add database: YOUR_DB to your profile in ~/.drt/profiles.yml"
            )
        return SnowflakeProfile(
            type="snowflake",
            account=raw.get("account", ""),
            user=raw.get("user", ""),
            password_env=raw.get("password_env"),
            password=raw.get("password"),
            database=_db,
            schema=raw.get("schema") or "PUBLIC",
            warehouse=raw.get("warehouse", ""),
            role=raw.get("role"),
        )

    raise ValueError(
        f"Unsupported source type '{source_type}'. "
        "Supported: bigquery, duckdb, sqlite, postgres, redshift, clickhouse, snowflake"
    )


def save_profile(
    profile_name: str,
    profile: ProfileConfig,
    config_dir: Path | None = None,
) -> Path:
    """Append or update a profile in ~/.drt/profiles.yml."""
    dir_path = _config_dir(config_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    profiles_path = dir_path / "profiles.yml"

    data: dict[str, Any] = {}
    if profiles_path.exists():
        with profiles_path.open() as f:
            data = yaml.safe_load(f) or {}

    if isinstance(profile, BigQueryProfile):
        entry: dict[str, Any] = {
            "type": "bigquery",
            "project": profile.project,
            "dataset": profile.dataset,
            "method": profile.method,
        }
        if profile.keyfile:
            entry["keyfile"] = profile.keyfile
    elif isinstance(profile, DuckDBProfile):
        entry = {"type": "duckdb", "database": profile.database}
    elif isinstance(profile, SQLiteProfile):
        entry = {"type": "sqlite", "database": profile.database}
    elif isinstance(profile, PostgresProfile):
        entry = {
            "type": "postgres",
            "host": profile.host,
            "port": profile.port,
            "dbname": profile.dbname,
            "user": profile.user,
        }
        if profile.password_env:
            entry["password_env"] = profile.password_env
    elif isinstance(profile, RedshiftProfile):
        entry = {
            "type": "redshift",
            "host": profile.host,
            "port": profile.port,
            "dbname": profile.dbname,
            "user": profile.user,
            "schema": profile.schema,
        }
        if profile.password_env:
            entry["password_env"] = profile.password_env
    elif isinstance(profile, ClickHouseProfile):
        entry = {
            "type": "clickhouse",
            "host": profile.host,
            "port": profile.port,
            "database": profile.database,
            "user": profile.user,
        }
        if profile.password_env:
            entry["password_env"] = profile.password_env
    elif isinstance(profile, SnowflakeProfile):
        entry = {
            "type": "snowflake",
            "account": profile.account,
            "user": profile.user,
            "database": profile.database,
            "schema": profile.schema,
            "warehouse": profile.warehouse,
        }
        if profile.password_env:
            entry["password_env"] = profile.password_env
        if profile.role:
            entry["role"] = profile.role
    else:
        raise ValueError(f"Unknown profile type: {type(profile)}")

    data[profile_name] = entry
    with profiles_path.open("w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

    return profiles_path
