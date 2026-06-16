"""Tests for the ``drt profile`` CLI sub-app (#423).

The profile commands read/write ``~/.drt/profiles.yml`` via the
``drt.config.credentials`` helpers, which resolve the config dir through
``_config_dir()``. Tests monkeypatch that to a tmp dir so nothing touches
the developer's real ``~/.drt``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


@pytest.fixture()
def drt_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point credentials' config dir at a tmp ~/.drt and return it."""
    home = tmp_path / ".drt"
    monkeypatch.setattr("drt.config.credentials._config_dir", lambda override=None: home)
    return home


def _write_profiles(home: Path, profiles: dict) -> None:
    home.mkdir(parents=True, exist_ok=True)
    with (home / "profiles.yml").open("w") as f:
        yaml.dump({"profiles": profiles}, f)


def _write_flat_profiles(home: Path, profiles: dict) -> None:
    """Write the legacy *flat* layout — profiles at the top level, no ``profiles:`` wrapper."""
    home.mkdir(parents=True, exist_ok=True)
    with (home / "profiles.yml").open("w") as f:
        yaml.dump(profiles, f)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_shows_profiles(drt_home: Path) -> None:
    _write_profiles(
        drt_home,
        {
            "dev": {"type": "duckdb", "database": ":memory:"},
            "prod": {"type": "bigquery", "project": "p", "dataset": "d"},
        },
    )
    result = runner.invoke(app, ["profile", "list"])
    assert result.exit_code == 0
    assert "dev" in result.output
    assert "duckdb" in result.output
    assert "prod" in result.output
    assert "bigquery" in result.output


def test_list_no_profiles(drt_home: Path) -> None:
    result = runner.invoke(app, ["profile", "list"])
    assert result.exit_code == 0
    assert "No profiles found" in result.output


# ---------------------------------------------------------------------------
# show (masking)
# ---------------------------------------------------------------------------


def test_show_masks_inline_secrets(drt_home: Path) -> None:
    _write_profiles(
        drt_home,
        {
            "pg": {
                "type": "postgres",
                "host": "db.internal",
                "user": "analyst",
                "password": "supersecretpw",  # inline secret — must be masked
                "password_env": "PG_PW",  # env var NAME — safe to show
            }
        },
    )
    result = runner.invoke(app, ["profile", "show", "pg"])
    assert result.exit_code == 0
    assert "db.internal" in result.output
    assert "analyst" in result.output
    # The raw secret value never appears, and not even a prefix leaks —
    # masking is full (`***`), not a `su***`-style head reveal.
    assert "supersecretpw" not in result.output
    assert "su***" not in result.output
    assert "PG_PW" in result.output


def test_show_masks_short_secret(drt_home: Path) -> None:
    """Any inline secret — short or long — is fully masked to ``***``."""
    _write_profiles(drt_home, {"x": {"type": "postgres", "password": "pw"}})
    result = runner.invoke(app, ["profile", "show", "x"])
    assert result.exit_code == 0
    assert "pw" not in result.output
    assert "***" in result.output


def test_show_unknown_profile_errors(drt_home: Path) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb"}})
    result = runner.invoke(app, ["profile", "show", "nope"])
    assert result.exit_code == 1
    assert "not found" in result.output


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


def test_remove_with_yes_flag(drt_home: Path) -> None:
    _write_profiles(
        drt_home, {"dev": {"type": "duckdb"}, "prod": {"type": "bigquery"}}
    )
    result = runner.invoke(app, ["profile", "remove", "dev", "--yes"])
    assert result.exit_code == 0
    assert "Removed 'dev'" in result.output

    remaining = yaml.safe_load((drt_home / "profiles.yml").read_text())["profiles"]
    assert "dev" not in remaining
    assert "prod" in remaining  # untouched


def test_remove_confirm_declined(drt_home: Path) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb"}})
    result = runner.invoke(app, ["profile", "remove", "dev"], input="n\n")
    assert result.exit_code == 0
    assert "Aborted" in result.output
    # Still present.
    assert "dev" in yaml.safe_load((drt_home / "profiles.yml").read_text())["profiles"]


def test_remove_unknown_profile_errors(drt_home: Path) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb"}})
    result = runner.invoke(app, ["profile", "remove", "ghost", "--yes"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_remove_no_profiles_file_errors(drt_home: Path) -> None:
    # No profiles.yml written at all.
    result = runner.invoke(app, ["profile", "remove", "anything", "--yes"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_add_preserves_observability_and_existing(drt_home: Path) -> None:
    """Adding a profile keeps the observability block and other profiles."""
    drt_home.mkdir(parents=True, exist_ok=True)
    with (drt_home / "profiles.yml").open("w") as f:
        yaml.dump(
            {
                "observability": {"otel": {"endpoint": "http://collector:4317"}},
                "profiles": {"existing": {"type": "duckdb", "database": ":memory:"}},
            },
            f,
        )

    result = runner.invoke(app, ["profile", "add", "new"], input="duckdb\n./n.duckdb\n")
    assert result.exit_code == 0, result.output

    data = yaml.safe_load((drt_home / "profiles.yml").read_text())
    assert data["observability"]["otel"]["endpoint"] == "http://collector:4317"
    assert "existing" in data["profiles"]
    assert data["profiles"]["new"]["database"] == "./n.duckdb"


# ---------------------------------------------------------------------------
# add (interactive)
# ---------------------------------------------------------------------------


def test_add_duckdb(drt_home: Path) -> None:
    # Prompts: type, then database (has a default → blank accepts it).
    result = runner.invoke(
        app, ["profile", "add", "local"], input="duckdb\n./my.duckdb\n"
    )
    assert result.exit_code == 0, result.output
    assert "Wrote profile 'local'" in result.output

    written = yaml.safe_load((drt_home / "profiles.yml").read_text())["profiles"]
    assert written["local"] == {"type": "duckdb", "database": "./my.duckdb"}


def test_add_postgres_coerces_port_to_int(drt_home: Path) -> None:
    # type, host, port, dbname, user, password_env, schema
    result = runner.invoke(
        app,
        ["profile", "add", "pg"],
        input="postgres\nlocalhost\n5432\nmydb\nme\nPG_PW\npublic\n",
    )
    assert result.exit_code == 0, result.output
    entry = yaml.safe_load((drt_home / "profiles.yml").read_text())["profiles"]["pg"]
    assert entry["type"] == "postgres"
    assert entry["port"] == 5432  # int, not "5432"
    assert isinstance(entry["port"], int)
    assert entry["dbname"] == "mydb"
    assert entry["password_env"] == "PG_PW"


def test_add_unsupported_type_errors(drt_home: Path) -> None:
    result = runner.invoke(app, ["profile", "add", "x"], input="oracle\n")
    assert result.exit_code == 1
    assert "Unsupported type" in result.output


def test_add_overwrite_declined(drt_home: Path) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb", "database": ":memory:"}})
    result = runner.invoke(app, ["profile", "add", "dev"], input="n\n")
    assert result.exit_code == 0
    assert "Aborted" in result.output
    # Original preserved.
    entry = yaml.safe_load((drt_home / "profiles.yml").read_text())["profiles"]["dev"]
    assert entry["database"] == ":memory:"


# ---------------------------------------------------------------------------
# legacy flat-layout migration (add / remove rewrite under `profiles:`)
# ---------------------------------------------------------------------------


def test_add_migrates_flat_layout_to_nested(drt_home: Path) -> None:
    """`add` on a legacy flat profiles.yml rewrites it under `profiles:`, keeping the old entry."""
    _write_flat_profiles(drt_home, {"old": {"type": "duckdb", "database": "./old.duckdb"}})

    result = runner.invoke(app, ["profile", "add", "new"], input="duckdb\n./new.duckdb\n")
    assert result.exit_code == 0, result.output

    data = yaml.safe_load((drt_home / "profiles.yml").read_text())
    # Now nested (top-level is just `profiles:`), and the pre-existing flat
    # profile survived the migration alongside the newly-added one.
    assert set(data) == {"profiles"}
    assert data["profiles"]["old"]["database"] == "./old.duckdb"
    assert data["profiles"]["new"]["database"] == "./new.duckdb"


def test_remove_migrates_flat_layout_to_nested(drt_home: Path) -> None:
    """`remove` on a legacy flat profiles.yml rewrites it under `profiles:`."""
    _write_flat_profiles(
        drt_home, {"dev": {"type": "duckdb"}, "prod": {"type": "bigquery"}}
    )

    result = runner.invoke(app, ["profile", "remove", "dev", "--yes"])
    assert result.exit_code == 0, result.output

    data = yaml.safe_load((drt_home / "profiles.yml").read_text())
    assert set(data) == {"profiles"}
    assert "dev" not in data["profiles"]
    assert "prod" in data["profiles"]  # the other flat profile survived


# ---------------------------------------------------------------------------
# test (connectivity)
# ---------------------------------------------------------------------------


def test_test_connection_ok(drt_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb", "database": ":memory:"}})

    class _FakeSource:
        def test_connection(self, profile: object) -> bool:
            return True

    monkeypatch.setattr(
        "drt.connectors.registry.get_source", lambda profile: _FakeSource()
    )
    result = runner.invoke(app, ["profile", "test", "dev"])
    assert result.exit_code == 0
    assert "connection OK" in result.output


def test_test_connection_failure(drt_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb", "database": ":memory:"}})

    class _FakeSource:
        def test_connection(self, profile: object) -> bool:
            raise RuntimeError("could not connect")

    monkeypatch.setattr(
        "drt.connectors.registry.get_source", lambda profile: _FakeSource()
    )
    result = runner.invoke(app, ["profile", "test", "dev"])
    assert result.exit_code == 1
    assert "could not connect" in result.output


def test_test_connection_returns_false(
    drt_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source whose test_connection returns False (no exception) → exit 1."""
    _write_profiles(drt_home, {"dev": {"type": "duckdb", "database": ":memory:"}})

    class _FakeSource:
        def test_connection(self, profile: object) -> bool:
            return False

    monkeypatch.setattr(
        "drt.connectors.registry.get_source", lambda profile: _FakeSource()
    )
    result = runner.invoke(app, ["profile", "test", "dev"])
    assert result.exit_code == 1
    assert "returned false" in result.output


def test_test_unknown_profile_errors(drt_home: Path) -> None:
    _write_profiles(drt_home, {"dev": {"type": "duckdb"}})
    result = runner.invoke(app, ["profile", "test", "missing"])
    assert result.exit_code == 1
    assert "not found" in result.output
