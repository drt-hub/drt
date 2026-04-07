"""Tests for config models, parser, and credentials."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from drt.config.credentials import BigQueryProfile, load_profile, save_profile
from drt.config.models import (
    ApiKeyAuth,
    BasicAuth,
    BearerAuth,
    GoogleSheetsDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    ProjectConfig,
    RestApiDestinationConfig,
    SslConfig,
    SyncConfig,
    SyncOptions,
    SyncTest,
)
from drt.config.parser import load_project, load_syncs

# ---------------------------------------------------------------------------
# Auth model discrimination
# ---------------------------------------------------------------------------

def test_bearer_auth_discriminated() -> None:
    config = RestApiDestinationConfig.model_validate({
        "type": "rest_api",
        "url": "https://example.com",
        "auth": {"type": "bearer", "token_env": "MY_TOKEN"},
    })
    assert isinstance(config.auth, BearerAuth)
    assert config.auth.token_env == "MY_TOKEN"


def test_api_key_auth_discriminated() -> None:
    config = RestApiDestinationConfig.model_validate({
        "type": "rest_api",
        "url": "https://example.com",
        "auth": {"type": "api_key", "header": "X-Custom-Key", "value": "secret"},
    })
    assert isinstance(config.auth, ApiKeyAuth)
    assert config.auth.header == "X-Custom-Key"


def test_basic_auth_discriminated() -> None:
    config = RestApiDestinationConfig.model_validate({
        "type": "rest_api",
        "url": "https://example.com",
        "auth": {"type": "basic", "username_env": "USER", "password_env": "PASS"},
    })
    assert isinstance(config.auth, BasicAuth)


def test_no_auth() -> None:
    config = RestApiDestinationConfig.model_validate({
        "type": "rest_api",
        "url": "https://example.com",
    })
    assert config.auth is None


# ---------------------------------------------------------------------------
# ProjectConfig
# ---------------------------------------------------------------------------

def test_project_config_defaults() -> None:
    p = ProjectConfig(name="test")
    assert p.version == "0.1"
    assert p.profile == "default"
    assert p.source is None


def test_project_config_profile_field() -> None:
    p = ProjectConfig(name="test", profile="prod")
    assert p.profile == "prod"


# ---------------------------------------------------------------------------
# Parser — load_project
# ---------------------------------------------------------------------------

def test_load_project(tmp_path: Path) -> None:
    config_file = tmp_path / "drt_project.yml"
    config_file.write_text("name: my-project\nprofile: dev\n")

    project = load_project(tmp_path)
    assert project.name == "my-project"
    assert project.profile == "dev"


def test_load_project_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="drt_project.yml not found"):
        load_project(tmp_path)


# ---------------------------------------------------------------------------
# Parser — load_syncs
# ---------------------------------------------------------------------------

def _write_sync(syncs_dir: Path, name: str) -> None:
    syncs_dir.mkdir(exist_ok=True)
    (syncs_dir / f"{name}.yml").write_text(
        f"name: {name}\n"
        "model: ref('table')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com\n"
    )


def test_load_syncs_empty(tmp_path: Path) -> None:
    assert load_syncs(tmp_path) == []


def test_load_syncs(tmp_path: Path) -> None:
    syncs_dir = tmp_path / "syncs"
    _write_sync(syncs_dir, "alpha")
    _write_sync(syncs_dir, "beta")

    syncs = load_syncs(tmp_path)
    assert len(syncs) == 2
    assert [s.name for s in syncs] == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# Credentials — load_profile / save_profile
# ---------------------------------------------------------------------------

def test_save_and_load_profile(tmp_path: Path) -> None:
    profile = BigQueryProfile(
        type="bigquery",
        project="my-project",
        dataset="my_dataset",
        method="application_default",
    )
    save_profile("dev", profile, config_dir=tmp_path)
    loaded = load_profile("dev", config_dir=tmp_path)

    assert loaded.project == "my-project"
    assert loaded.dataset == "my_dataset"
    assert loaded.method == "application_default"


def test_load_profile_bigquery_location(tmp_path: Path) -> None:
    (tmp_path / "profiles.yml").write_text(
        "dev:\n  type: bigquery\n  project: p\n  dataset: d\n  location: asia-northeast1\n"
    )
    loaded = load_profile("dev", config_dir=tmp_path)
    assert loaded.location == "asia-northeast1"


def test_load_profile_bigquery_location_default(tmp_path: Path) -> None:
    (tmp_path / "profiles.yml").write_text(
        "dev:\n  type: bigquery\n  project: p\n  dataset: d\n"
    )
    loaded = load_profile("dev", config_dir=tmp_path)
    assert loaded.location == "US"


def test_load_profile_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="profiles.yml not found"):
        load_profile("dev", config_dir=tmp_path)


def test_load_profile_missing_key(tmp_path: Path) -> None:
    (tmp_path / "profiles.yml").write_text("prod:\n  type: bigquery\n  project: x\n  dataset: y\n")
    with pytest.raises(KeyError, match="Profile 'dev' not found"):
        load_profile("dev", config_dir=tmp_path)


def test_save_profile_appends(tmp_path: Path) -> None:
    existing = BigQueryProfile(type="bigquery", project="p1", dataset="d1")
    save_profile("dev", existing, config_dir=tmp_path)

    new_profile = BigQueryProfile(type="bigquery", project="p2", dataset="d2")
    save_profile("prod", new_profile, config_dir=tmp_path)

    profiles_path = tmp_path / "profiles.yml"
    data = yaml.safe_load(profiles_path.read_text())
    assert "dev" in data
    assert "prod" in data


# ---------------------------------------------------------------------------
# Google Sheets destination config
# ---------------------------------------------------------------------------

def test_google_sheets_destination_config_parses() -> None:
    raw = {
        "name": "export_to_sheets",
        "model": "ref('users')",
        "destination": {
            "type": "google_sheets",
            "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
            "sheet": "Sheet1",
            "mode": "overwrite",
        },
    }
    cfg = SyncConfig(**raw)
    assert cfg.destination.type == "google_sheets"
    assert cfg.destination.spreadsheet_id == "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
    assert cfg.destination.sheet == "Sheet1"
    assert cfg.destination.mode == "overwrite"


# ---------------------------------------------------------------------------
# SyncOptions — upsert mode
# ---------------------------------------------------------------------------

def test_sync_options_upsert_mode_accepted() -> None:
    """mode='upsert' is valid and behaves like 'full' (no cursor_field required)."""
    opts = SyncOptions(mode="upsert")
    assert opts.mode == "upsert"
    assert opts.cursor_field is None


def test_sync_options_full_mode_still_works() -> None:
    """Backward compat: mode='full' remains the default."""
    opts = SyncOptions()
    assert opts.mode == "full"


def test_sync_options_upsert_in_sync_config() -> None:
    """mode='upsert' works end-to-end inside a SyncConfig."""
    raw = {
        "name": "upsert_sync",
        "model": "ref('scores')",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
        },
        "sync": {"mode": "upsert"},
    }
    cfg = SyncConfig(**raw)
    assert cfg.sync.mode == "upsert"


def test_ssl_config_defaults() -> None:
    ssl = SslConfig()
    assert ssl.enabled is False
    assert ssl.ca_env is None
    assert ssl.cert_env is None
    assert ssl.key_env is None


def test_ssl_config_full() -> None:
    ssl = SslConfig(enabled=True, ca_env="SSL_CA", cert_env="SSL_CERT", key_env="SSL_KEY")
    assert ssl.enabled is True
    assert ssl.ca_env == "SSL_CA"


def test_postgres_destination_with_ssl() -> None:
    cfg = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="testdb",
        table="t",
        upsert_key=["id"],
        ssl=SslConfig(enabled=True, ca_env="PG_SSL_CA"),
    )
    assert cfg.ssl is not None
    assert cfg.ssl.enabled is True
    assert cfg.ssl.ca_env == "PG_SSL_CA"


def test_postgres_destination_without_ssl() -> None:
    cfg = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="testdb",
        table="t",
        upsert_key=["id"],
    )
    assert cfg.ssl is None


def test_mysql_destination_with_ssl() -> None:
    cfg = MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="testdb",
        table="t",
        upsert_key=["id"],
        ssl=SslConfig(enabled=True, ca_env="MYSQL_SSL_CA", cert_env="MYSQL_SSL_CERT"),
    )
    assert cfg.ssl is not None
    assert cfg.ssl.enabled is True
    assert cfg.ssl.ca_env == "MYSQL_SSL_CA"


def test_google_sheets_destination_defaults() -> None:
    cfg = GoogleSheetsDestinationConfig(
        type="google_sheets",
        spreadsheet_id="abc123",
    )
    assert cfg.sheet == "Sheet1"
    assert cfg.mode == "overwrite"
    assert cfg.credentials_path is None
    assert cfg.credentials_env is None


# ---------------------------------------------------------------------------
# PostgresDestinationConfig — connection_string_env
# ---------------------------------------------------------------------------

def test_postgres_config_connection_string_env() -> None:
    """connection_string_env should be accepted without host/dbname."""
    cfg = PostgresDestinationConfig(
        type="postgres",
        connection_string_env="DATABASE_URL",
        table="public.scores",
        upsert_key=["id"],
    )
    assert cfg.connection_string_env == "DATABASE_URL"
    assert cfg.host is None
    assert cfg.dbname is None


def test_postgres_config_individual_params() -> None:
    """Individual host/dbname params should still work (backward compat)."""
    cfg = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="analytics",
        table="public.scores",
        upsert_key=["id"],
    )
    assert cfg.host == "localhost"
    assert cfg.dbname == "analytics"
    assert cfg.connection_string_env is None


def test_postgres_config_no_connection_method_raises() -> None:
    """Validation should fail when no connection method is provided."""
    with pytest.raises(ValueError, match="connection_string_env"):
        PostgresDestinationConfig(
            type="postgres",
            table="public.scores",
            upsert_key=["id"],
        )


# ---------------------------------------------------------------------------
# MySQLDestinationConfig — connection_string_env
# ---------------------------------------------------------------------------

def test_mysql_config_connection_string_env() -> None:
    """connection_string_env should be accepted without host/dbname."""
    cfg = MySQLDestinationConfig(
        type="mysql",
        connection_string_env="MYSQL_URL",
        table="scores",
        upsert_key=["id"],
    )
    assert cfg.connection_string_env == "MYSQL_URL"
    assert cfg.host is None
    assert cfg.dbname is None


def test_mysql_config_individual_params() -> None:
    """Individual host/dbname params should still work (backward compat)."""
    cfg = MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="analytics",
        table="scores",
        upsert_key=["id"],
    )
    assert cfg.host == "localhost"
    assert cfg.dbname == "analytics"
    assert cfg.connection_string_env is None


def test_mysql_config_no_connection_method_raises() -> None:
    """Validation should fail when no connection method is provided."""
    with pytest.raises(ValueError, match="connection_string_env"):
        MySQLDestinationConfig(
            type="mysql",
            table="scores",
            upsert_key=["id"],
        )


# ---------------------------------------------------------------------------
# SyncConfig tests
# ---------------------------------------------------------------------------


def test_sync_config_with_tests() -> None:
    data = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "http://x",
            "method": "POST",
        },
        "tests": [
            {"row_count": {"min": 1}},
            {"not_null": {"columns": ["id", "name"]}},
        ],
    }
    sync = SyncConfig.model_validate(data)
    assert len(sync.tests) == 2
    assert sync.tests[0].row_count is not None
    assert sync.tests[1].not_null is not None


def test_sync_config_without_tests() -> None:
    data = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "http://x",
            "method": "POST",
        },
    }
    sync = SyncConfig.model_validate(data)
    assert sync.tests == []
