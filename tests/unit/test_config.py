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
    ProjectConfig,
    RestApiDestinationConfig,
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
