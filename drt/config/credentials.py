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
class MySQLProfile:
    """MySQL profile for extracting data from MySQL databases.

    Example ~/.drt/profiles.yml:
        mysql:
          type: mysql
          host: localhost
          port: 3306
          dbname: analytics
          user: analyst
          password_env: MYSQL_PASSWORD
    """

    type: Literal["mysql"]
    host: str = "localhost"
    port: int = 3306
    dbname: str = ""
    user: str = ""
    password_env: str | None = None
    password: str | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.dbname})"


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


@dataclass
class SQLServerProfile:
    """SQL Server profile using pymssql."""

    type: Literal["sqlserver"]
    host: str = ""
    port: int = 1433
    database: str = ""
    user: str = ""
    password_env: str | None = None
    password: str | None = None
    schema: str = "dbo"

    def describe(self) -> str:
        return f"{self.type} ({self.host}/{self.database}.{self.schema})"


@dataclass
class DatabricksProfile:
    """Databricks SQL Warehouse profile using databricks-sql-connector."""

    type: Literal["databricks"]
    server_hostname: str = ""  # e.g. "dbc-abc.cloud.databricks.com"
    http_path: str = ""  # e.g. "/sql/1.0/warehouses/xxxxxx"
    access_token_env: str | None = None
    access_token: str | None = None
    catalog: str | None = None  # Unity Catalog (optional)
    schema: str = "default"

    def describe(self) -> str:
        path = f"{self.catalog}.{self.schema}" if self.catalog else self.schema
        return f"{self.type} ({self.server_hostname}/{path})"


# Union type — used throughout the codebase
ProfileConfig = (
    BigQueryProfile
    | DuckDBProfile
    | SQLiteProfile
    | PostgresProfile
    | RedshiftProfile
    | ClickHouseProfile
    | MySQLProfile
    | SnowflakeProfile
    | DatabricksProfile
    | SQLServerProfile
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config_dir(override: Path | None = None) -> Path:
    return override if override is not None else Path.home() / ".drt"


def _load_secrets(project_dir: Path | None = None) -> dict[str, Any]:
    """Load .drt/secrets.toml if it exists.

    Returns a nested dict matching the TOML structure.
    """
    secrets_path = (project_dir or Path(".")) / ".drt" / "secrets.toml"
    if not secrets_path.exists():
        return {}
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ModuleNotFoundError:
            return {}
    with secrets_path.open("rb") as f:
        data: dict[str, Any] = tomllib.load(f)
    return data


def _lookup_secrets_toml(env_var: str) -> str | None:
    """Look up an env-var-style key in secrets.toml.

    Walks all nested dicts searching for a matching key.
    """
    secrets = _load_secrets()

    def _search(d: dict[str, Any]) -> str | None:
        for k, v in d.items():
            if k == env_var and isinstance(v, str):
                return v
            if isinstance(v, dict):
                found = _search(v)
                if found is not None:
                    return found
        return None

    return _search(secrets)


def resolve_env(value: str | None, env_var: str | None) -> str | None:
    """Resolve a secret value: explicit value → env var → secrets.toml → None."""
    if value is not None:
        return value
    if env_var is not None:
        env_val = os.environ.get(env_var)
        if env_val is not None:
            return env_val
        return _lookup_secrets_toml(env_var)
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

    if source_type == "mysql":
        return MySQLProfile(
            type="mysql",
            host=raw.get("host", "localhost"),
            port=int(raw.get("port", 3306)),
            dbname=raw.get("dbname", ""),
            user=raw.get("user", ""),
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

    if source_type == "sqlserver":
        _db = raw.get("database", "")
        if not _db:
            raise ValueError("SQL Server profile requires 'database'.")
        return SQLServerProfile(
            type="sqlserver",
            host=raw.get("host", ""),
            port=int(raw.get("port", 1433)),
            database=_db,
            user=raw.get("user", ""),
            password_env=raw.get("password_env"),
            password=raw.get("password"),
            schema=raw.get("schema") or "dbo",
        )

    if source_type == "databricks":
        _host = raw.get("server_hostname", "")
        _path = raw.get("http_path", "")
        if not _host or not _path:
            raise ValueError("Databricks profile requires 'server_hostname' and 'http_path'.")
        return DatabricksProfile(
            type="databricks",
            server_hostname=_host,
            http_path=_path,
            access_token_env=raw.get("access_token_env"),
            access_token=raw.get("access_token"),
            catalog=raw.get("catalog"),
            schema=raw.get("schema") or "default",
        )

    raise ValueError(
        f"Unsupported source type '{source_type}'. "
        "Supported: bigquery, duckdb, sqlite, postgres, redshift, clickhouse, "
        "mysql, snowflake, databricks, sqlserver"
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
    elif isinstance(profile, MySQLProfile):
        entry = {
            "type": "mysql",
            "host": profile.host,
            "port": profile.port,
            "dbname": profile.dbname,
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
    elif isinstance(profile, SQLServerProfile):
        entry = {
            "type": "sqlserver",
            "host": profile.host,
            "port": profile.port,
            "database": profile.database,
            "user": profile.user,
            "schema": profile.schema,
        }
        if profile.password_env:
            entry["password_env"] = profile.password_env
    elif isinstance(profile, DatabricksProfile):
        entry = {
            "type": "databricks",
            "server_hostname": profile.server_hostname,
            "http_path": profile.http_path,
            "schema": profile.schema,
        }
        if profile.access_token_env:
            entry["access_token_env"] = profile.access_token_env
        if profile.catalog:
            entry["catalog"] = profile.catalog
    else:
        raise ValueError(f"Unknown profile type: {type(profile)}")

    data[profile_name] = entry
    with profiles_path.open("w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

    return profiles_path
