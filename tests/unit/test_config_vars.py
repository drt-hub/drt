"""Tests for project vars — `vars:` block, --vars, var() in SQL/YAML (#783)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from drt.config.credentials import DuckDBProfile
from drt.config.parser import load_syncs, load_syncs_safe, project_vars
from drt.config.vars import (
    ENV_VAR_PREFIX,
    VarError,
    env_vars,
    expand_vars,
    has_var_template,
    parse_cli_vars,
    render_vars,
    resolve_vars,
    suspicious_vars,
)
from drt.engine.resolver import resolve_model_ref

# ---------------------------------------------------------------------------
# --vars parsing
# ---------------------------------------------------------------------------


def test_parse_cli_vars_accepts_braceless_and_braced() -> None:
    """The issue's braceless form and a YAML flow mapping both work."""
    expected = {"lookback_days": 1, "hubspot_pipeline": "sandbox"}
    assert parse_cli_vars("lookback_days: 1, hubspot_pipeline: sandbox") == expected
    assert parse_cli_vars("{lookback_days: 1, hubspot_pipeline: sandbox}") == expected


def test_parse_cli_vars_empty_is_empty() -> None:
    assert parse_cli_vars("") == {}
    assert parse_cli_vars("   ") == {}


def test_parse_cli_vars_rejects_non_mapping() -> None:
    with pytest.raises(VarError, match="must be a mapping"):
        parse_cli_vars("just_a_scalar")


def test_parse_cli_vars_rejects_invalid_yaml() -> None:
    with pytest.raises(VarError, match="not valid YAML"):
        parse_cli_vars("a: [1, 2")


# ---------------------------------------------------------------------------
# precedence: project vars: < DRT_VAR_* < --vars
# ---------------------------------------------------------------------------


def test_env_vars_are_prefixed_and_lowercased(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{ENV_VAR_PREFIX}LOOKBACK_DAYS", "3")
    monkeypatch.setenv("UNRELATED", "x")
    assert env_vars()["lookback_days"] == "3"
    assert "unrelated" not in env_vars()


def test_precedence_env_beats_project(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{ENV_VAR_PREFIX}TAG", "from_env")
    assert resolve_vars({"tag": "from_project"})["tag"] == "from_env"


def test_precedence_cli_beats_env_and_project(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{ENV_VAR_PREFIX}TAG", "from_env")
    merged = resolve_vars({"tag": "from_project"}, {"tag": "from_cli"})
    assert merged["tag"] == "from_cli"


def test_project_vars_used_when_nothing_overrides() -> None:
    assert resolve_vars({"tag": "from_project"})["tag"] == "from_project"


# ---------------------------------------------------------------------------
# var() semantics
# ---------------------------------------------------------------------------


def test_var_renders_value_and_default() -> None:
    assert render_vars("{{ var('a') }}", {"a": "hit"}) == "hit"
    assert render_vars("{{ var('missing', 'fallback') }}", {}) == "fallback"


def test_undefined_var_without_default_raises() -> None:
    with pytest.raises(VarError, match="Undefined var 'nope'"):
        render_vars("{{ var('nope') }}", {})


def test_has_var_template_only_matches_var_calls() -> None:
    """A bare {{ token }} is not a var template — adding vars must not change
    how existing templates (e.g. {{ cursor_value }}) are treated."""
    assert has_var_template("{{ var('a') }}")
    assert has_var_template("{{var ('a')}}")
    assert not has_var_template("{{ cursor_value }}")
    assert not has_var_template("SELECT 1")


def test_expand_vars_walks_tree_and_leaves_non_var_strings() -> None:
    data = {"a": "{{ var('x') }}", "b": ["{{ var('x') }}", "plain"], "c": 5}
    assert expand_vars(data, {"x": "V"}) == {"a": "V", "b": ["V", "plain"], "c": 5}


# ---------------------------------------------------------------------------
# project + parser integration
# ---------------------------------------------------------------------------


def _project(tmp_path: Path, project_yaml: str, sync_yaml: str) -> Path:
    (tmp_path / "syncs").mkdir()
    (tmp_path / "drt_project.yml").write_text(project_yaml)
    (tmp_path / "syncs" / "s.yml").write_text(sync_yaml)
    return tmp_path


def test_project_vars_block_is_parsed(tmp_path: Path) -> None:
    p = _project(
        tmp_path,
        "name: d\nprofile: default\nvars:\n  lookback_days: 7\n  pipeline: default\n",
        "name: s\nmodel: 'SELECT 1'\ndestination: {type: rest_api, url: 'https://x'}\n",
    )
    assert project_vars(p) == {"lookback_days": 7, "pipeline": "default"}


def test_project_vars_without_project_file_still_resolves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A syncs-only directory must not start requiring drt_project.yml."""
    monkeypatch.setenv(f"{ENV_VAR_PREFIX}TAG", "env_only")
    assert project_vars(tmp_path) == {"tag": "env_only"}


def test_var_renders_in_yaml_string_fields(tmp_path: Path) -> None:
    p = _project(
        tmp_path,
        "name: d\nprofile: default\nvars: {pipeline: prod}\n",
        "name: s\nmodel: 'SELECT 1'\n"
        "destination: {type: rest_api, url: \"https://x/{{ var('pipeline') }}\"}\n",
    )
    assert load_syncs(p)[0].destination.url == "https://x/prod"


def test_cli_vars_override_yaml_fields(tmp_path: Path) -> None:
    p = _project(
        tmp_path,
        "name: d\nprofile: default\nvars: {pipeline: prod}\n",
        "name: s\nmodel: 'SELECT 1'\n"
        "destination: {type: rest_api, url: \"https://x/{{ var('pipeline') }}\"}\n",
    )
    syncs = load_syncs(p, vars=resolve_vars({"pipeline": "prod"}, {"pipeline": "sandbox"}))
    assert syncs[0].destination.url == "https://x/sandbox"


def test_model_sql_is_left_for_the_resolver(tmp_path: Path) -> None:
    """`model:` must survive load_syncs unrendered — it shares its template
    surface with {{ cursor_value }}, which only the resolver can supply.
    Rendering it at load time would blank the cursor and silently break an
    incremental predicate."""
    p = _project(
        tmp_path,
        "name: d\nprofile: default\nvars: {pipeline: P}\n",
        "name: s\n"
        "model: \"SELECT {{ var('pipeline') }} FROM t WHERE u > '{{ cursor_value }}'\"\n"
        "destination: {type: rest_api, url: 'https://x'}\n"
        "sync: {mode: incremental, cursor_field: u}\n",
    )
    model = load_syncs(p)[0].model
    assert "{{ cursor_value }}" in model
    assert "var('pipeline')" in model


def test_validate_surfaces_undefined_var_in_yaml_field(tmp_path: Path) -> None:
    p = _project(
        tmp_path,
        "name: d\nprofile: default\nvars: {}\n",
        "name: s\nmodel: 'SELECT 1'\n"
        "destination: {type: rest_api, url: \"https://x/{{ var('nope') }}\"}\n",
    )
    result = load_syncs_safe(p)
    assert result.syncs == []
    assert "Undefined var 'nope'" in result.errors["s"][0]


def test_validate_surfaces_undefined_var_in_model_sql(tmp_path: Path) -> None:
    """Undefined vars in model SQL are caught at validate time, not mid-run."""
    p = _project(
        tmp_path,
        "name: d\nprofile: default\nvars: {}\n",
        "name: s\nmodel: \"SELECT {{ var('nope') }}\"\n"
        "destination: {type: rest_api, url: 'https://x'}\n",
    )
    result = load_syncs_safe(p)
    assert "Undefined var 'nope'" in result.errors["s"][0]


# ---------------------------------------------------------------------------
# resolver (model SQL)
# ---------------------------------------------------------------------------

_DUCKDB = DuckDBProfile(type="duckdb")


def test_resolver_renders_var_in_sql(tmp_path: Path) -> None:
    sql = "SELECT * FROM t WHERE d > CURRENT_DATE - {{ var('lookback_days') }}"
    out = resolve_model_ref(sql, tmp_path, _DUCKDB, vars={"lookback_days": 7})
    assert out == "SELECT * FROM t WHERE d > CURRENT_DATE - 7"


def test_resolver_renders_var_and_cursor_together(tmp_path: Path) -> None:
    sql = "SELECT {{ var('p') }} FROM t WHERE u > '{{ cursor_value }}'"
    out = resolve_model_ref(sql, tmp_path, _DUCKDB, "u", "2026-01-01", vars={"p": "P"})
    assert out == "SELECT P FROM t WHERE u > '2026-01-01'"


def test_var_only_sql_still_gets_where_injection(tmp_path: Path) -> None:
    """A var() without a cursor template must not suppress the incremental
    WHERE clause the engine injects."""
    sql = "SELECT * FROM t WHERE p = '{{ var('p') }}'"
    out = resolve_model_ref(sql, tmp_path, _DUCKDB, "u", "2026-01-01", vars={"p": "P"})
    assert out == (
        "SELECT * FROM (SELECT * FROM t WHERE p = 'P') AS _drt_base WHERE u > '2026-01-01'"
    )


def test_sql_without_vars_is_unchanged(tmp_path: Path) -> None:
    """Adding vars must be a no-op for projects that don't use them."""
    assert resolve_model_ref("SELECT * FROM t", tmp_path, _DUCKDB) == "SELECT * FROM t"


def test_resolver_resolves_project_vars_when_caller_passes_none(tmp_path: Path) -> None:
    """``vars=None`` means "resolve from the project" — the same contract
    load_syncs has.

    Guards the `drt build` shape (#777): a command that reuses `_run_one` /
    `run_sync` without threading vars would otherwise raise "Undefined var" on
    model SQL that `drt run` renders fine. Only `--vars` needs threading.
    """
    (tmp_path / "drt_project.yml").write_text(
        "name: d\nprofile: default\nvars: {pipeline: from_project}\n"
    )
    out = resolve_model_ref("SELECT {{ var('pipeline') }}", tmp_path, _DUCKDB)
    assert out == "SELECT from_project"


def test_resolver_explicit_vars_still_win_over_project(tmp_path: Path) -> None:
    """An explicit vars= (what `drt run --vars` threads) is not overridden by
    the project block."""
    (tmp_path / "drt_project.yml").write_text(
        "name: d\nprofile: default\nvars: {pipeline: from_project}\n"
    )
    out = resolve_model_ref(
        "SELECT {{ var('pipeline') }}", tmp_path, _DUCKDB, vars={"pipeline": "from_cli"}
    )
    assert out == "SELECT from_cli"


def test_resolver_undefined_var_raises(tmp_path: Path) -> None:
    with pytest.raises(VarError, match="Undefined var 'nope'"):
        resolve_model_ref("SELECT {{ var('nope') }}", tmp_path, _DUCKDB, vars={})


def test_resolver_var_default_used(tmp_path: Path) -> None:
    out = resolve_model_ref("SELECT {{ var('nope', 42) }}", tmp_path, _DUCKDB, vars={})
    assert out == "SELECT 42"


def test_cursor_template_without_value_still_raises(tmp_path: Path) -> None:
    """Pre-#783 behaviour: a cursor template with no cursor value is an error."""
    with pytest.raises(ValueError, match="no cursor value provided"):
        resolve_model_ref("SELECT '{{ cursor_value }}'", tmp_path, _DUCKDB)


def test_suspicious_vars_flags_sql_metacharacters() -> None:
    """#783's injection posture: values carrying SQL metacharacters are
    surfaced (warning-only — they're project config, not user input)."""
    flagged = suspicious_vars(
        {
            "clean": "sandbox",
            "numeric": 7,
            "injected": "1; DROP TABLE users--",
            "quoted": "O'Brien",
            "commented": "x /* hidden */",
        }
    )
    assert flagged == ["commented", "injected", "quoted"]
    assert suspicious_vars({"a": "plain_value", "b": 1}) == []


# ---------------------------------------------------------------------------
# CLI — `drt run --vars` end-to-end
# ---------------------------------------------------------------------------


def _cli_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A project on disk with the profile lookup stubbed.

    ``drt run`` resolves the profile before it touches vars, so without this
    stub these tests would depend on the developer having a real
    ``~/.drt/profiles.yml`` — green locally, red on a clean runner.
    """
    from drt.config import credentials as creds

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_kw: DuckDBProfile(type="duckdb"), raising=False
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "drt_project.yml").write_text(
        "name: demo\nprofile: default\nvars:\n  pipeline: prod\n"
    )
    (tmp_path / "syncs" / "s.yml").write_text(
        "name: s\nmodel: 'SELECT 1'\n"
        "destination: {type: rest_api, url: \"https://x/{{ var('pipeline') }}\"}\n"
    )
    return tmp_path


def test_cli_run_vars_is_parsed_and_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The --vars value must reach load_syncs — guards the CLI plumbing that
    unit-testing the resolver alone would miss."""
    from drt.cli.main import app
    from drt.config import parser as parser_mod

    _cli_project(tmp_path, monkeypatch)
    seen: dict[str, Any] = {}

    def spy(project_dir: Path, vars: dict[str, Any] | None = None) -> list[Any]:
        seen["vars"] = vars
        return []

    monkeypatch.setattr(parser_mod, "load_syncs", spy)
    CliRunner().invoke(app, ["run", "--vars", "pipeline: sandbox"])
    assert seen["vars"]["pipeline"] == "sandbox", "--vars must override drt_project.yml vars:"


def test_cli_run_rejects_malformed_vars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A malformed --vars is a clean CLI error, not a traceback."""
    from drt.cli.main import app

    _cli_project(tmp_path, monkeypatch)
    result = CliRunner().invoke(app, ["run", "--vars", "bare_word"])
    assert result.exit_code == 1
    assert "must be a mapping" in result.output
    assert not isinstance(result.exception, VarError)


def test_var_in_sql_file_via_ref(tmp_path: Path) -> None:
    """SQL files reached via ref() never pass through the YAML loader, so the
    resolver is the only place their vars get rendered."""
    models = tmp_path / "syncs" / "models"
    models.mkdir(parents=True)
    (models / "m.sql").write_text("SELECT * FROM t WHERE p = '{{ var('p') }}'")
    out = resolve_model_ref("ref('m')", tmp_path, _DUCKDB, vars={"p": "P"})
    assert out == "SELECT * FROM t WHERE p = 'P'"
