"""``resolve_profile_paths`` edge cases left by #1337: scheme detection and symlink loops."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from drt.config.credentials import DuckDBProfile, SQLiteProfile, resolve_profile_paths


@pytest.mark.parametrize(
    "database",
    [
        "md:analytics",
        "motherduck:analytics",
        "md:?motherduck_token=abc",
        "s3://bucket/warehouse.duckdb",
        "https://host/warehouse.duckdb",
        ":memory:",
    ],
)
def test_duckdb_connection_strings_are_left_untouched(tmp_path: Path, database: str) -> None:
    profile = DuckDBProfile(type="duckdb", database=database)

    assert resolve_profile_paths(profile, tmp_path) is profile


@pytest.mark.parametrize("database", ["data:warehouse.duckdb", "localhost:5432", "a:b/c.duckdb"])
def test_a_colon_in_a_relative_local_filename_is_still_resolved(
    tmp_path: Path, database: str
) -> None:
    resolved = resolve_profile_paths(DuckDBProfile(type="duckdb", database=database), tmp_path)

    assert Path(resolved.database) == (tmp_path.resolve() / database)


def test_a_local_file_named_like_a_scheme_can_be_written_with_a_dot_slash(tmp_path: Path) -> None:
    resolved = resolve_profile_paths(DuckDBProfile(type="duckdb", database="./md:x"), tmp_path)

    assert Path(resolved.database) == (tmp_path.resolve() / "md:x")


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs privileges on Windows")
def test_a_symlink_loop_is_a_value_error_not_an_internal_error(tmp_path: Path) -> None:
    os.symlink("loop_b", tmp_path / "loop_a")
    os.symlink("loop_a", tmp_path / "loop_b")

    with pytest.raises(ValueError, match="Cannot resolve relative database path"):
        resolve_profile_paths(SQLiteProfile(type="sqlite", database="loop_a/x.db"), tmp_path)
