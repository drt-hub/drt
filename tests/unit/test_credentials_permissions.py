"""Tests for ~/.drt credential file/dir permission hardening (#650).

POSIX-only: the hardening is a no-op on Windows (NTFS ACLs differ), so the
mode assertions are skipped off POSIX. drt's CI runs on Linux, so these
execute there.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from drt.config.credentials import (
    BigQueryProfile,
    save_profile,
    write_raw_profile,
)

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX file modes only")


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def _bq() -> BigQueryProfile:
    return BigQueryProfile(
        type="bigquery",
        project="p",
        dataset="d",
        method="application_default",
    )


@posix_only
def test_save_profile_writes_0o600(tmp_path: Path) -> None:
    path = save_profile("dev", _bq(), config_dir=tmp_path)
    assert _mode(path) == 0o600


@posix_only
def test_save_profile_tightens_preexisting_world_readable_file(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles.yml"
    profiles.write_text("profiles: {}\n")
    profiles.chmod(0o644)
    save_profile("dev", _bq(), config_dir=tmp_path)
    assert _mode(profiles) == 0o600


@posix_only
def test_save_profile_creates_0o700_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "dotdrt"
    save_profile("dev", _bq(), config_dir=config_dir)
    assert _mode(config_dir) == 0o700


@posix_only
def test_write_raw_profile_writes_0o600(tmp_path: Path) -> None:
    path = write_raw_profile(
        "dev",
        {"type": "duckdb", "database": "./w.duckdb"},
        config_dir=tmp_path,
    )
    assert _mode(path) == 0o600


def test_save_profile_roundtrips_regardless_of_platform(tmp_path: Path) -> None:
    """The hardening must not change the written content (Windows or POSIX)."""
    from drt.config.credentials import load_profile

    save_profile("dev", _bq(), config_dir=tmp_path)
    loaded = load_profile("dev", config_dir=tmp_path)
    assert loaded.project == "p"
    assert loaded.dataset == "d"
