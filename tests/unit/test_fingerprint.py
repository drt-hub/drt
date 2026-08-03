"""Tests for the per-sync file fingerprint backing ``state:modified`` (#772).

The properties matter far more than the digest algorithm. A fingerprint that
moves when the user changed nothing makes ``state:modified`` select everything
(useless); one that stays put when the user *did* change something makes it
select nothing (dangerous, and silent). Each test below pins one of those.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

from drt.config.fingerprint import sync_fingerprints

_SYNC_YAML = textwrap.dedent(
    """\
    name: users_to_api
    model: ref('users')
    destination:
      type: rest_api
      url: https://example.com/users
    sync:
      mode: full
    """
)


def _project(tmp_path: Path, syncs: dict[str, str], models: dict[str, str]) -> Path:
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "models").mkdir()
    for filename, body in syncs.items():
        (syncs_dir / filename).write_text(body)
    for filename, body in models.items():
        (syncs_dir / "models" / filename).write_text(body)
    return tmp_path


def _one(tmp_path: Path, yaml_body: str = _SYNC_YAML, sql: str = "SELECT 1") -> str:
    _project(tmp_path, {"a.yml": yaml_body}, {"users.sql": sql})
    return sync_fingerprints(tmp_path)["users_to_api"]


# --- stability ---------------------------------------------------------------


def test_fingerprint_is_stable_across_processes(tmp_path: Path) -> None:
    """Two interpreters with different hash seeds must agree.

    A second call in the *same* process would pass even if the implementation
    leaned on ``hash()`` or iterated a set, so this deliberately shells out
    twice with ``PYTHONHASHSEED`` pinned to different values.
    """
    _project(tmp_path, {"a.yml": _SYNC_YAML}, {"users.sql": "SELECT 1"})
    snippet = (
        "from drt.config.fingerprint import sync_fingerprints;"
        f"print(sync_fingerprints({str(tmp_path)!r})['users_to_api'])"
    )
    results = []
    for seed in ("0", "1"):
        proc = subprocess.run(
            [sys.executable, "-c", snippet],
            capture_output=True,
            text=True,
            check=True,
            env={"PATH": "/usr/bin:/bin", "PYTHONHASHSEED": seed, "HOME": str(tmp_path)},
        )
        results.append(proc.stdout.strip())
    assert results[0] == results[1]
    assert results[0]  # non-empty


def test_same_input_same_fingerprint(tmp_path: Path, tmp_path_factory) -> None:
    other = tmp_path_factory.mktemp("other")
    assert _one(tmp_path) == _one(other)


# --- what MUST change it -----------------------------------------------------


def test_model_sql_change_changes_fingerprint(tmp_path: Path) -> None:
    """The issue calls this out explicitly: ref()'d SQL files must be covered."""
    before = _one(tmp_path, sql="SELECT 1")
    (tmp_path / "syncs" / "models" / "users.sql").write_text("SELECT 2")
    assert sync_fingerprints(tmp_path)["users_to_api"] != before


def test_sync_yaml_change_changes_fingerprint(tmp_path: Path) -> None:
    before = _one(tmp_path)
    changed = _SYNC_YAML.replace("mode: full", "mode: upsert\n  upsert_key: [id]")
    (tmp_path / "syncs" / "a.yml").write_text(changed)
    assert sync_fingerprints(tmp_path)["users_to_api"] != before


def test_comment_only_change_still_marks_modified(tmp_path: Path) -> None:
    """Pinned deliberately, not discovered.

    Hashing raw bytes means a comment counts as a change. That is what dbt does
    with its file checksums, and being conservative here costs one extra dry-run
    while the opposite would be a silent miss. If this ever becomes intolerable
    it should be a deliberate change, so it fails loudly first.
    """
    before = _one(tmp_path)
    (tmp_path / "syncs" / "a.yml").write_text("# just a comment\n" + _SYNC_YAML)
    assert sync_fingerprints(tmp_path)["users_to_api"] != before


# --- what must NOT change it -------------------------------------------------


def test_run_state_does_not_change_fingerprint(tmp_path: Path) -> None:
    """Trap A regression: the watermark must never reach the hash input.

    ``resolve_model_ref`` substitutes the current cursor value into the SQL it
    returns, so fingerprinting *that* would move the hash on every run of an
    incremental sync and select everything, forever. The fingerprint must read
    files, not resolved queries.
    """
    before = _one(tmp_path)
    state_dir = tmp_path / ".drt"
    state_dir.mkdir(exist_ok=True)
    (state_dir / "state.json").write_text(
        '{"users_to_api": {"sync_name": "users_to_api", "last_run_at": "2026-08-03T00:00:00",'
        ' "records_synced": 10, "status": "success", "last_cursor_value": "2026-08-03"}}'
    )
    (state_dir / "watermarks.json").write_text('{"users_to_api": "2026-08-03T12:00:00"}')
    assert sync_fingerprints(tmp_path)["users_to_api"] == before


def test_secret_value_never_reaches_the_fingerprint(
    tmp_path: Path, tmp_path_factory, monkeypatch
) -> None:
    """Trap B regression: secrets are excluded *by construction*, not by a list.

    The file holds ``${API_TOKEN}``, never the value, so no field-classification
    exists to get wrong — the reason this hashes the file as written rather than
    the resolved config. Both the digest and the hashed bytes are checked: equal
    digests alone could also mean the token was dropped from the input for some
    other reason.
    """
    body = _SYNC_YAML.replace(
        "  url: https://example.com/users\n",
        "  url: https://example.com/users\n  auth:\n    type: bearer\n    token: ${API_TOKEN}\n",
    )
    monkeypatch.setenv("API_TOKEN", "secret-value-one")
    first = _one(tmp_path, yaml_body=body)

    other = tmp_path_factory.mktemp("other")
    monkeypatch.setenv("API_TOKEN", "totally-different-secret-two")
    second = _one(other, yaml_body=body)

    assert first == second
    assert "secret-value-one" not in (tmp_path / "syncs" / "a.yml").read_text()

    # The two assertions above would also hold if the implementation simply
    # ignored the whole auth block, so prove it does not: swapping which env
    # var is referenced is a real config change and must move the fingerprint.
    renamed = tmp_path_factory.mktemp("renamed")
    third = _one(renamed, yaml_body=body.replace("${API_TOKEN}", "${OTHER_TOKEN}"))
    assert third != first


def test_editing_one_sync_does_not_move_another(tmp_path: Path) -> None:
    second = _SYNC_YAML.replace("users_to_api", "orders_to_api").replace(
        "ref('users')", "ref('orders')"
    )
    _project(
        tmp_path,
        {"a.yml": _SYNC_YAML, "b.yml": second},
        {"users.sql": "SELECT 1", "orders.sql": "SELECT 2"},
    )
    before = sync_fingerprints(tmp_path)
    (tmp_path / "syncs" / "a.yml").write_text(_SYNC_YAML + "description: edited\n")
    after = sync_fingerprints(tmp_path)

    assert after["users_to_api"] != before["users_to_api"]
    assert after["orders_to_api"] == before["orders_to_api"]


# --- shape -------------------------------------------------------------------


def test_missing_model_file_is_tolerated(tmp_path: Path) -> None:
    """``ref()`` may point at a dbt model or a bare table, with no local .sql."""
    _project(tmp_path, {"a.yml": _SYNC_YAML}, {})
    prints = sync_fingerprints(tmp_path)
    assert prints["users_to_api"]


def test_no_syncs_dir_returns_empty(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    assert sync_fingerprints(tmp_path) == {}


def test_undefined_env_var_elsewhere_does_not_hide_the_sync(tmp_path: Path) -> None:
    """A sync referencing an unset env var must still appear in the map.

    Regression for a real bug: expanding the whole document to find ``name:``
    meant one undefined ``${VAR}`` anywhere in the file dropped the sync
    entirely, so ``state:modified`` silently would not select it. A PR adding a
    sync whose secret is not provisioned yet is precisely what CI should be
    looking at.
    """
    body = _SYNC_YAML.replace(
        "  url: https://example.com/users\n",
        "  url: https://example.com/users\n  auth:\n    type: bearer\n"
        "    token: ${DEFINITELY_NOT_SET_ANYWHERE}\n",
    )
    _project(tmp_path, {"a.yml": body}, {"users.sql": "SELECT 1"})
    assert "users_to_api" in sync_fingerprints(tmp_path)


# --- degenerate inputs -------------------------------------------------------
#
# Every branch below decides whether a sync is *visible* to state:modified.
# An invisible sync is a sync CI silently does not run, so these paths deserve
# tests more than the happy path does.


def test_inline_sql_model_is_supported(tmp_path: Path) -> None:
    """``model:`` may be raw SQL rather than ``ref()`` — a normal usage, not an edge.

    There is no .sql file to read in that case; the SQL is already inside the
    yaml bytes being hashed, so editing it still moves the fingerprint.
    """
    body = _SYNC_YAML.replace("model: ref('users')", "model: SELECT id FROM users")
    before = _one(tmp_path, yaml_body=body)

    other_body = body.replace("SELECT id FROM users", "SELECT id, email FROM users")
    (tmp_path / "syncs" / "a.yml").write_text(other_body)
    assert sync_fingerprints(tmp_path)["users_to_api"] != before


def test_malformed_yaml_does_not_poison_the_other_syncs(tmp_path: Path) -> None:
    """One broken file must not make the whole project invisible.

    ``drt validate`` is where a broken sync gets reported; a selector that blew
    up on an unrelated malformed file would be worse than one that skips it.
    """
    _project(
        tmp_path,
        {"a.yml": _SYNC_YAML, "broken.yml": "name: [unclosed\n  : :\n"},
        {"users.sql": "SELECT 1"},
    )
    prints = sync_fingerprints(tmp_path)
    assert "users_to_api" in prints


def test_non_mapping_yaml_is_skipped(tmp_path: Path) -> None:
    _project(tmp_path, {"a.yml": _SYNC_YAML, "list.yml": "- just\n- a list\n"}, {})
    assert list(sync_fingerprints(tmp_path)) == ["users_to_api"]


def test_sync_without_a_name_is_skipped(tmp_path: Path) -> None:
    _project(tmp_path, {"a.yml": _SYNC_YAML, "anon.yml": "model: ref('x')\n"}, {})
    assert list(sync_fingerprints(tmp_path)) == ["users_to_api"]


def test_missing_or_non_string_model_is_tolerated(tmp_path: Path) -> None:
    body = _SYNC_YAML.replace("model: ref('users')\n", "")
    _project(tmp_path, {"a.yml": body}, {})
    assert sync_fingerprints(tmp_path)["users_to_api"]

    listy = _SYNC_YAML.replace("model: ref('users')", "model:\n  - not\n  - a string")
    (tmp_path / "syncs" / "a.yml").write_text(listy)
    assert sync_fingerprints(tmp_path)["users_to_api"]


def test_unresolvable_name_falls_back_to_the_literal(tmp_path: Path) -> None:
    """The name's own expansion failing must not delete the sync either.

    Distinct from the undefined-var-elsewhere case above: there the name
    resolves fine and the failure is in another field. Here expansion of the
    name itself fails, and the literal is kept so the sync stays visible rather
    than vanishing from the map.
    """
    body = _SYNC_YAML.replace("name: users_to_api", "name: ${NOT_SET_NAME_VAR}")
    _project(tmp_path, {"a.yml": body}, {"users.sql": "SELECT 1"})
    prints = sync_fingerprints(tmp_path)
    assert len(prints) == 1
    assert next(iter(prints.values()))
