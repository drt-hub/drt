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
from collections.abc import Callable
from dataclasses import asdict, is_dataclass
from inspect import signature
from pathlib import Path
from typing import Any, Literal, TextIO

import yaml
from pydantic import BaseModel, Field

from drt.config.profiles import (
    BigQueryProfile,
    ClickHouseProfile,
    DatabricksProfile,
    DeltaLakeProfile,
    DuckDBProfile,
    IcebergProfile,
    MySQLProfile,
    PostgresProfile,
    ProfileConfig,
    ProfileConfigLike,
    RedshiftProfile,
    RestApiProfile,
    SnowflakeProfile,
    SQLiteProfile,
    SQLServerProfile,
)
from drt.config.secret_providers import resolve_provider_uri


class OtelConfig(BaseModel):
    endpoint: str | None = None
    service_name: str = "drt"
    headers: dict[str, str] = Field(default_factory=dict)
    span_processor: Literal["batch", "simple"] = "batch"


class ObservabilityConfig(BaseModel):
    otel: OtelConfig = Field(default_factory=OtelConfig)


# ---------------------------------------------------------------------------
# Source profile types
# ---------------------------------------------------------------------------


# Union type — used throughout the codebase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config_dir(override: Path | None = None) -> Path:
    return override if override is not None else Path.home() / ".drt"


def _load_secrets(project_dir: Path | None = None) -> dict[str, Any]:
    """Load .drt/secrets.toml or decrypt .drt/secrets.toml.enc in memory.

    The encrypted file takes precedence when both exist. This lets
    ``drt encrypt`` preserve the plaintext for safety while a user verifies
    that normal commands can read the encrypted copy before deleting it.
    Returns a nested dict matching the TOML structure.
    """
    secrets_dir = (project_dir or Path(".")) / ".drt"
    secrets_path = secrets_dir / "secrets.toml"
    encrypted_path = secrets_dir / "secrets.toml.enc"
    if not secrets_path.exists() and not encrypted_path.exists():
        return {}
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ModuleNotFoundError as exc:
            if encrypted_path.exists():
                raise ImportError(
                    "Encrypted secrets require the optional encryption extra. "
                    "Install it with: pip install 'drt-core[encryption]'"
                ) from exc
            return {}
    if encrypted_path.exists():
        from io import BytesIO

        from drt.config.encryption import decrypt_secrets

        plaintext = decrypt_secrets(encrypted_path.read_bytes())
        data: dict[str, Any] = tomllib.load(BytesIO(plaintext))
        return data

    with secrets_path.open("rb") as f:
        data = tomllib.load(f)
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
    """Resolve a secret value: explicit value → env var → secrets.toml →
    provider URI → None.

    The last step (#782) lets ``env_var`` itself be a ``scheme://...`` secret
    reference (e.g. ``aws-sm://prod/drt/snowflake#password``) instead of an
    env var name — it never matches a real env var or a secrets.toml key, so
    it falls through to here unchanged, where the scheme is detected and
    dispatched to the matching provider.
    """
    if value is not None:
        return value
    if env_var is not None:
        env_val = os.environ.get(env_var)
        if env_val is not None:
            return env_val
        from_secrets = _lookup_secrets_toml(env_var)
        if from_secrets is not None:
            return from_secrets

        return resolve_provider_uri(env_var)
    return None


def load_snowflake_private_key(pem: str, passphrase: str | None = None) -> bytes:
    """Decode a PEM private key to the DER bytes snowflake-connector expects.

    Shared by the Snowflake source and destination for key-pair auth (#737).
    ``cryptography`` is a snowflake-connector-python dependency, so it is
    present whenever the connector itself is installed — imported lazily to
    keep drt-core importable without the extra.
    """
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(
        pem.encode(), password=passphrase.encode() if passphrase else None
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def resolve_env_dict(options: dict[str, str]) -> dict[str, str]:
    """Resolve cloud storage / catalog options. A key ending in ``_ENV`` is read
    from the environment and re-keyed without the suffix, e.g.
    ``{"AWS_ACCESS_KEY_ID_ENV": "AWS_KEY"}`` -> ``{"AWS_ACCESS_KEY_ID": <$AWS_KEY>}``.
    Other keys pass through unchanged; a missing env var drops that key."""
    resolved: dict[str, str] = {}
    for key, value in options.items():
        if key.endswith("_ENV"):
            env_val = os.environ.get(value)
            if env_val is not None:
                resolved[key[:-4]] = env_val
        else:
            resolved[key] = value
    return resolved


def _load_profiles_yaml(config_dir: Path | None = None) -> dict[str, Any]:
    profiles_path = _config_dir(config_dir) / "profiles.yml"
    if not profiles_path.exists():
        raise FileNotFoundError(
            f"profiles.yml not found at {profiles_path}. "
            "Run `drt init` to create it, or create it manually."
        )

    with profiles_path.open() as f:
        return yaml.safe_load(f) or {}


def _profiles_mapping(data: dict[str, Any]) -> dict[str, Any]:
    profiles = data.get("profiles")
    if isinstance(profiles, dict):
        return profiles
    return {key: value for key, value in data.items() if key != "observability"}


def load_observability_config(config_dir: Path | None = None) -> ObservabilityConfig:
    """Load the top-level observability block from ~/.drt/profiles.yml."""
    data = _load_profiles_yaml(config_dir)
    observability_raw = data.get("observability")
    if observability_raw is None:
        return ObservabilityConfig()
    return ObservabilityConfig.model_validate(observability_raw)


# ---------------------------------------------------------------------------
# Load / Save
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Per-type load/save (#402): a spec table replaces what was a hand-written
# if/elif chain in each of load_profile()/save_profile(). Each built-in type
# keeps its own construction quirks (per-field defaults, int() coercion,
# required-field checks, terse-write conditionals) as a small loader/dumper
# function pair — this is a lookup-table refactor, not a shared schema, since
# the per-type differences are load-bearing (see individual functions).
# ---------------------------------------------------------------------------


def _load_bigquery(raw: dict[str, Any]) -> BigQueryProfile:
    return BigQueryProfile(
        type="bigquery",
        project=raw["project"],
        dataset=raw["dataset"],
        method=raw.get("method", "application_default"),
        keyfile=raw.get("keyfile"),
        location=raw.get("location", "US"),
    )


def _dump_bigquery(profile: BigQueryProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "bigquery",
        "project": profile.project,
        "dataset": profile.dataset,
        "method": profile.method,
    }
    if profile.keyfile:
        entry["keyfile"] = profile.keyfile
    return entry


def _load_duckdb(raw: dict[str, Any]) -> DuckDBProfile:
    return DuckDBProfile(type="duckdb", database=raw.get("database", ":memory:"))


def _dump_duckdb(profile: DuckDBProfile, profile_name: str) -> dict[str, Any]:
    return {"type": "duckdb", "database": profile.database}


def _load_sqlite(raw: dict[str, Any]) -> SQLiteProfile:
    return SQLiteProfile(type="sqlite", database=raw.get("database", ":memory:"))


def _dump_sqlite(profile: SQLiteProfile, profile_name: str) -> dict[str, Any]:
    return {"type": "sqlite", "database": profile.database}


def _load_postgres(raw: dict[str, Any]) -> PostgresProfile:
    return PostgresProfile(
        type="postgres",
        host=raw.get("host", "localhost"),
        port=int(raw.get("port", 5432)),
        dbname=raw.get("dbname", ""),
        user=raw.get("user", ""),
        password_env=raw.get("password_env"),
        password=raw.get("password"),
    )


def _dump_postgres(profile: PostgresProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "postgres",
        "host": profile.host,
        "port": profile.port,
        "dbname": profile.dbname,
        "user": profile.user,
    }
    if profile.password_env:
        entry["password_env"] = profile.password_env
    return entry


def _load_redshift(raw: dict[str, Any]) -> RedshiftProfile:
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


def _dump_redshift(profile: RedshiftProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "redshift",
        "host": profile.host,
        "port": profile.port,
        "dbname": profile.dbname,
        "user": profile.user,
        "schema": profile.schema,
    }
    if profile.password_env:
        entry["password_env"] = profile.password_env
    return entry


def _load_clickhouse(raw: dict[str, Any]) -> ClickHouseProfile:
    return ClickHouseProfile(
        type="clickhouse",
        host=raw.get("host", "localhost"),
        port=int(raw.get("port", 8123)),
        database=raw.get("database", "default"),
        user=raw.get("user", "default"),
        password_env=raw.get("password_env"),
        password=raw.get("password"),
    )


def _dump_clickhouse(profile: ClickHouseProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "clickhouse",
        "host": profile.host,
        "port": profile.port,
        "database": profile.database,
        "user": profile.user,
    }
    if profile.password_env:
        entry["password_env"] = profile.password_env
    return entry


def _load_mysql(raw: dict[str, Any]) -> MySQLProfile:
    return MySQLProfile(
        type="mysql",
        host=raw.get("host", "localhost"),
        port=int(raw.get("port", 3306)),
        dbname=raw.get("dbname", ""),
        user=raw.get("user", ""),
        password_env=raw.get("password_env"),
        password=raw.get("password"),
    )


def _dump_mysql(profile: MySQLProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "mysql",
        "host": profile.host,
        "port": profile.port,
        "dbname": profile.dbname,
        "user": profile.user,
    }
    if profile.password_env:
        entry["password_env"] = profile.password_env
    return entry


def _load_snowflake(raw: dict[str, Any]) -> SnowflakeProfile:
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
        private_key_env=raw.get("private_key_env"),
        private_key_passphrase_env=raw.get("private_key_passphrase_env"),
        database=_db,
        schema=raw.get("schema") or "PUBLIC",
        warehouse=raw.get("warehouse", ""),
        role=raw.get("role"),
    )


def _dump_snowflake(profile: SnowflakeProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "snowflake",
        "account": profile.account,
        "user": profile.user,
        "database": profile.database,
        "schema": profile.schema,
        "warehouse": profile.warehouse,
    }
    if profile.password_env:
        entry["password_env"] = profile.password_env
    if profile.private_key_env:
        entry["private_key_env"] = profile.private_key_env
    if profile.private_key_passphrase_env:
        entry["private_key_passphrase_env"] = profile.private_key_passphrase_env
    if profile.role:
        entry["role"] = profile.role
    return entry


def _load_sqlserver(raw: dict[str, Any]) -> SQLServerProfile:
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


def _dump_sqlserver(profile: SQLServerProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "sqlserver",
        "host": profile.host,
        "port": profile.port,
        "database": profile.database,
        "user": profile.user,
        "schema": profile.schema,
    }
    if profile.password_env:
        entry["password_env"] = profile.password_env
    return entry


def _load_databricks(raw: dict[str, Any]) -> DatabricksProfile:
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


def _dump_databricks(profile: DatabricksProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "databricks",
        "server_hostname": profile.server_hostname,
        "http_path": profile.http_path,
        "schema": profile.schema,
    }
    if profile.access_token_env:
        entry["access_token_env"] = profile.access_token_env
    if profile.catalog:
        entry["catalog"] = profile.catalog
    return entry


def _load_deltalake(raw: dict[str, Any]) -> DeltaLakeProfile:
    location = raw.get("location", "")
    if not location:
        raise ValueError("Delta Lake profile requires 'location'.")
    return DeltaLakeProfile(
        type="deltalake",
        location=location,
        table=raw.get("table"),
        storage_options=raw.get("storage_options") or {},
    )


def _dump_deltalake(profile: DeltaLakeProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {"type": "deltalake", "location": profile.location}
    if profile.table:
        entry["table"] = profile.table
    if profile.storage_options:
        entry["storage_options"] = profile.storage_options
    return entry


def _load_rest_api(raw: dict[str, Any]) -> RestApiProfile:
    url = raw.get("url", "")
    if not url:
        raise ValueError("REST API profile requires 'url'.")
    return RestApiProfile(
        type="rest_api",
        url=url,
        auth=raw.get("auth"),
        pagination=raw.get("pagination"),
        result_path=raw.get("result_path"),
        incremental=raw.get("incremental"),
    )


def _dump_rest_api(profile: RestApiProfile, profile_name: str) -> dict[str, Any]:
    # rest_api must stay on the *_env-only path every other built-in uses:
    # `auth` is a free-form dict that may legitimately carry a literal token,
    # so it is written only when it names env vars — anything else is dropped
    # with a pointer, rather than landing verbatim in profiles.yml (which is
    # what the generic dataclass fallback did once #997 gave it one).
    entry: dict[str, Any] = {"type": "rest_api", "url": profile.url}
    if profile.auth:
        literals = sorted(
            key
            for key, value in profile.auth.items()
            if key != "type" and isinstance(value, str) and not key.endswith("_env")
        )
        if literals:
            raise ValueError(
                f"Refusing to write profile '{profile_name}': its auth block sets "
                f"{', '.join(literals)} to a literal value. profiles.yml stores env var "
                "names, not secrets — use the matching *_env key instead."
            )
        entry["auth"] = profile.auth
    for field_name in ("pagination", "result_path", "incremental"):
        value = getattr(profile, field_name, None)
        if value:
            entry[field_name] = value
    return entry


def _load_iceberg(raw: dict[str, Any]) -> IcebergProfile:
    table = raw.get("table", "")
    if not table:
        raise ValueError("Iceberg profile requires 'table' (namespace.table).")
    return IcebergProfile(
        type="iceberg",
        table=table,
        catalog_uri=raw.get("catalog_uri"),
        warehouse=raw.get("warehouse"),
        catalog_name=raw.get("catalog_name") or "default",
        properties=raw.get("properties") or {},
    )


def _dump_iceberg(profile: IcebergProfile, profile_name: str) -> dict[str, Any]:
    entry: dict[str, Any] = {"type": "iceberg", "table": profile.table}
    if profile.catalog_uri:
        entry["catalog_uri"] = profile.catalog_uri
    if profile.warehouse:
        entry["warehouse"] = profile.warehouse
    if profile.catalog_name and profile.catalog_name != "default":
        entry["catalog_name"] = profile.catalog_name
    if profile.properties:
        entry["properties"] = profile.properties
    return entry


_ProfileLoader = Callable[[dict[str, Any]], ProfileConfigLike]
_ProfileDumper = Callable[[Any, str], dict[str, Any]]

# Keyed on the `type:` string from profiles.yml — matches _DISPATCHED_SOURCE_TYPES.
_PROFILE_LOADERS: dict[str, _ProfileLoader] = {
    "bigquery": _load_bigquery,
    "duckdb": _load_duckdb,
    "sqlite": _load_sqlite,
    "postgres": _load_postgres,
    "redshift": _load_redshift,
    "clickhouse": _load_clickhouse,
    "mysql": _load_mysql,
    "snowflake": _load_snowflake,
    "sqlserver": _load_sqlserver,
    "databricks": _load_databricks,
    "deltalake": _load_deltalake,
    "rest_api": _load_rest_api,
    "iceberg": _load_iceberg,
}

# Keyed on the concrete dataclass, mirroring the original isinstance() chain
# (dispatch by exact type, not by a `.type` string — a profile object's own
# `type` field is user/plugin-controlled data, not a safe dispatch key here).
_PROFILE_DUMPERS: dict[type, _ProfileDumper] = {
    BigQueryProfile: _dump_bigquery,
    DuckDBProfile: _dump_duckdb,
    SQLiteProfile: _dump_sqlite,
    PostgresProfile: _dump_postgres,
    RedshiftProfile: _dump_redshift,
    ClickHouseProfile: _dump_clickhouse,
    MySQLProfile: _dump_mysql,
    SnowflakeProfile: _dump_snowflake,
    SQLServerProfile: _dump_sqlserver,
    DatabricksProfile: _dump_databricks,
    DeltaLakeProfile: _dump_deltalake,
    RestApiProfile: _dump_rest_api,
    IcebergProfile: _dump_iceberg,
}

_DISPATCHED_SOURCE_TYPES: frozenset[str] = frozenset(_PROFILE_LOADERS)
"""Source types :func:`load_profile` constructs itself (#997).

Derived from ``_PROFILE_LOADERS`` so it cannot drift from the table by
construction. Data, not a slice of the error sentence: the "Also registered"
line is derived from this, so reflowing the message must not change which
types drt reports as supported. ``tests/unit/test_plugin_config_extensibility.py``
pins it against the connector registry.
"""


def load_profile(profile_name: str, config_dir: Path | None = None) -> ProfileConfigLike:
    """Load a named profile from ~/.drt/profiles.yml.

    Args:
        profile_name: Key in profiles.yml (e.g. "dev", "local").
        config_dir: Override ~/.drt for testing.

    Raises:
        FileNotFoundError: profiles.yml does not exist.
        KeyError: profile_name not found.
        ValueError: Unknown source type or missing required fields.
    """
    data = _load_profiles_yaml(config_dir)
    profiles = _profiles_mapping(data)

    if profile_name not in profiles:
        available = ", ".join(profiles.keys()) or "(none)"
        raise KeyError(
            f"Profile '{profile_name}' not found in {_config_dir(config_dir) / 'profiles.yml'}. "
            f"Available: {available}"
        )

    raw = profiles[profile_name]
    source_type = raw.get("type")

    loader = _PROFILE_LOADERS.get(source_type) if isinstance(source_type, str) else None
    if loader is not None:
        return loader(raw)

    # Imported inside the function: drt.connectors.registry imports the source
    # and destination implementations, which import this module back.
    from drt.connectors.registry import registered_source_types, source_profile_class

    # Past the built-ins: ask the connector registry before giving up (#997).
    #
    # The source-side half of ADR 0009's blocker. _PROFILE_LOADERS is a closed
    # table, so a third-party source could register itself and still never be
    # loadable from profiles.yml. Deliberately placed *after* the table lookup
    # rather than folded into it: every built-in keeps its exact construction,
    # including per-type defaults and the required-field checks in its own
    # loader function, and a plugin cannot shadow a built-in type.
    #
    # Profiles are plain dataclasses, not pydantic models, so there is no
    # validator to hook and no generic fallback model — the registered profile
    # class is constructed directly from the YAML mapping. Strict per-field
    # checking of a plugin's profile is the deferred second pass, not part of
    # #997; what *is* checked is the interface drt-core itself calls.
    profile_class = source_profile_class(source_type) if isinstance(source_type, str) else None
    if profile_class is not None:
        fields = {key: value for key, value in raw.items() if key != "type"}
        # Only a TypeError raised by the *call itself* is a profiles.yml/schema
        # mismatch. One raised inside __init__/__post_init__ is the plugin's own
        # bug and must not be rewritten into "your profile is wrong" — so the
        # signature is checked up front rather than by catching broadly.
        try:
            signature(profile_class).bind(type=source_type, **fields)
        except TypeError as exc:
            raise ValueError(
                f"Profile '{profile_name}' does not match the '{source_type}' profile "
                f"registered by {profile_class.__module__}.{profile_class.__qualname__}: {exc}"
            ) from exc
        profile = profile_class(type=source_type, **fields)

        # drt-core calls describe() on whatever load_profile() returns
        # (drt/cli/output.py, drt/cli/commands/run.py, drt/mcp/tools/run_sync.py),
        # and unlike the destination side those call sites have no hasattr guard.
        # Fail here, naming the class, rather than with an AttributeError several
        # layers into a run.
        if not callable(getattr(profile, "describe", None)):
            raise ValueError(
                f"Source type '{source_type}' is registered with "
                f"{profile_class.__module__}.{profile_class.__qualname__}, which does not "
                "implement describe(). A profile class must provide describe() -> str."
            )
        if not isinstance(profile, ProfileConfigLike):
            raise ValueError(
                f"Source type '{source_type}' is registered with "
                f"{profile_class.__module__}.{profile_class.__qualname__}, which does not "
                "provide the profile type required by ProfileConfigLike."
            )
        return profile

    # Types the registry knows but the chain above does not construct. Kept as a
    # set rather than re-split from the message string: the sentence is
    # presentation, and reflowing it must not change which types drt reports as
    # supported.
    also = sorted(set(registered_source_types()) - _DISPATCHED_SOURCE_TYPES)
    also_note = f" Also registered: {', '.join(also)}." if also else ""
    raise ValueError(
        f"Unsupported source type '{source_type}'. "
        f"Supported: {', '.join(sorted(_DISPATCHED_SOURCE_TYPES))}.{also_note}"
    )


def _ensure_private_dir(dir_path: Path) -> None:
    """Create ``dir_path`` (and parents), owner-only (0o700) on POSIX.

    The chmod is best-effort and POSIX-only — NTFS ACLs differ, so it's a
    no-op on Windows (guarded behind ``os.name``) rather than erroring.
    """
    dir_path.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        try:
            dir_path.chmod(0o700)
        except OSError:
            pass


def _open_private(path: Path) -> TextIO:
    """Open ``path`` for writing with owner-only (0o600) perms on POSIX.

    ``~/.drt/profiles.yml`` can hold inline credentials, so it should never be
    world-readable. ``os.open`` with ``O_CREAT`` + mode ``0o600`` means a newly
    created file is private from the moment it exists (no umask-default
    ``0o644`` window), and the explicit ``os.chmod`` on the descriptor also
    tightens a *pre-existing* file that an older drt may have written
    ``0o644``. No-op on Windows, where the call falls back to a plain text
    write.
    """
    if os.name != "posix":
        return path.open("w")
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        os.chmod(fd, 0o600)  # descriptor-based: also tightens a pre-existing file
    except OSError:
        pass
    return os.fdopen(fd, "w")


def save_profile(
    profile_name: str,
    profile: ProfileConfigLike,
    config_dir: Path | None = None,
) -> Path:
    """Append or update a profile in ~/.drt/profiles.yml.

    Accepts ``ProfileConfigLike`` rather than the closed ``ProfileConfig``
    union to stay symmetric with :func:`load_profile` (#1034) — the
    ``is_dataclass`` fallback branch below already handles a plugin profile
    at runtime (#997); only the declared type used to be narrower than what
    this function actually supports.
    """
    dir_path = _config_dir(config_dir)
    _ensure_private_dir(dir_path)
    profiles_path = dir_path / "profiles.yml"

    data: dict[str, Any] = {}
    if profiles_path.exists():
        with profiles_path.open() as f:
            data = yaml.safe_load(f) or {}

    observability = data.get("observability")
    profiles = data.get("profiles")
    if not isinstance(profiles, dict):
        profiles = {key: value for key, value in data.items() if key != "observability"}

    data = {}
    if observability is not None:
        data["observability"] = observability
    data["profiles"] = profiles

    dumper = _PROFILE_DUMPERS.get(type(profile))
    if dumper is not None:
        entry = dumper(profile, profile_name)
    elif is_dataclass(profile) and not isinstance(profile, type):
        # A profile from the connector registry (#997). load_profile() gained a
        # registry fallback; without the same here drt could read a plugin
        # profile it was unable to write back, which breaks `drt init`'s wizard
        # for any plugin source. asdict() round-trips the dataclass the fallback
        # constructed, dropping empty optionals so the file stays as terse as
        # the built-in dumpers above.
        entry = {
            key: value for key, value in asdict(profile).items() if value not in (None, {}, [])
        }
        entry["type"] = profile.type
    else:
        raise ValueError(f"Unknown profile type: {type(profile)}")

    profiles[profile_name] = entry
    with _open_private(profiles_path) as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

    return profiles_path


# ---------------------------------------------------------------------------
# Raw profile access (for `drt profile` CLI — list / show / add / remove)
#
# These operate on the raw YAML mapping (name → dict) without parsing into a
# typed ``ProfileConfig``. The CLI needs to display / edit profiles of any
# type, including ones with fields the typed loaders don't model, so it works
# at the dict level. ``load_profile`` / ``save_profile`` remain the typed path
# used by the engine.
# ---------------------------------------------------------------------------


def load_raw_profiles(config_dir: Path | None = None) -> dict[str, Any]:
    """Return ``{profile_name: raw_dict}`` from profiles.yml.

    Returns an empty dict when the file doesn't exist (so the CLI can show
    "no profiles" rather than raising). Unlike ``load_profile`` this does not
    parse or validate the entries — it's for listing / display / editing.
    """
    profiles_path = _config_dir(config_dir) / "profiles.yml"
    if not profiles_path.exists():
        return {}
    with profiles_path.open() as f:
        data = yaml.safe_load(f) or {}
    return _profiles_mapping(data)


def _rewrite_profiles(profiles: dict[str, Any], data: dict[str, Any], profiles_path: Path) -> None:
    """Write ``profiles`` back to disk, preserving the observability block."""
    observability = data.get("observability")
    out: dict[str, Any] = {}
    if observability is not None:
        out["observability"] = observability
    out["profiles"] = profiles
    _ensure_private_dir(profiles_path.parent)
    with _open_private(profiles_path) as f:
        yaml.dump(out, f, default_flow_style=False, allow_unicode=True)


def write_raw_profile(
    profile_name: str, entry: dict[str, Any], config_dir: Path | None = None
) -> Path:
    """Create or replace a raw profile entry in profiles.yml.

    Writes the given ``entry`` dict verbatim under ``profile_name`` (the
    ``drt profile add`` path builds the dict from prompted answers). Preserves
    any existing profiles and the top-level ``observability`` block.
    """
    profiles_path = _config_dir(config_dir) / "profiles.yml"
    data: dict[str, Any] = {}
    if profiles_path.exists():
        with profiles_path.open() as f:
            data = yaml.safe_load(f) or {}
    profiles = dict(_profiles_mapping(data))
    profiles[profile_name] = entry
    _rewrite_profiles(profiles, data, profiles_path)
    return profiles_path


def remove_profile(profile_name: str, config_dir: Path | None = None) -> Path:
    """Delete a profile entry from profiles.yml.

    Raises:
        FileNotFoundError: profiles.yml does not exist.
        KeyError: ``profile_name`` is not present.
    """
    profiles_path = _config_dir(config_dir) / "profiles.yml"
    if not profiles_path.exists():
        raise FileNotFoundError(f"profiles.yml not found at {profiles_path}.")
    with profiles_path.open() as f:
        data = yaml.safe_load(f) or {}
    profiles = dict(_profiles_mapping(data))
    if profile_name not in profiles:
        available = ", ".join(profiles.keys()) or "(none)"
        raise KeyError(f"Profile '{profile_name}' not found. Available: {available}")
    del profiles[profile_name]
    _rewrite_profiles(profiles, data, profiles_path)
    return profiles_path


__all__ = [
    "BigQueryProfile",
    "ClickHouseProfile",
    "DatabricksProfile",
    "DeltaLakeProfile",
    "DuckDBProfile",
    "IcebergProfile",
    "MySQLProfile",
    "ObservabilityConfig",
    "OtelConfig",
    "PostgresProfile",
    "ProfileConfig",
    "ProfileConfigLike",
    "RedshiftProfile",
    "RestApiProfile",
    "SQLServerProfile",
    "SQLiteProfile",
    "SnowflakeProfile",
    "load_observability_config",
    "load_profile",
    "load_raw_profiles",
    "load_snowflake_private_key",
    "remove_profile",
    "resolve_env",
    "resolve_env_dict",
    "save_profile",
    "write_raw_profile",
]
