"""Unit tests for the drt MCP server tools.

Requires: pip install drt-core[mcp]
These tests are skipped automatically when fastmcp is not installed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("fastmcp", reason="requires drt-core[mcp]")

from typing import Any

from fastmcp import FastMCP  # noqa: E402

from drt.mcp.server import create_server  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def call(server: FastMCP, tool_name: str, **kwargs: Any) -> Any:
    """Call an MCP tool and return the structured result.

    FastMCP wraps non-dict returns in {"result": value};
    dict returns are passed through directly.
    """
    result = await server.call_tool(tool_name, kwargs)
    sc = result.structured_content
    # list / scalar returns are wrapped in {"result": ...}
    if isinstance(sc, dict) and list(sc.keys()) == ["result"]:
        return sc["result"]
    return sc


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    (tmp_path / "drt_project.yml").write_text("name: test-project\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
    )
    return tmp_path


@pytest.fixture()
def server(project_dir: Path) -> FastMCP:
    return create_server(project_dir)


# ---------------------------------------------------------------------------
# Server creation
# ---------------------------------------------------------------------------


def test_create_server_returns_fastmcp_instance() -> None:
    from fastmcp import FastMCP

    assert isinstance(create_server(), FastMCP)


@pytest.mark.asyncio
async def test_server_has_expected_tools() -> None:
    srv = create_server()
    tools = await srv._local_provider._list_tools()
    tool_names = {t.name for t in tools}
    expected = {
        "drt_list_syncs",
        "drt_run_sync",
        "drt_run_test",
        "drt_get_status",
        "drt_validate",
        "drt_get_schema",
    }
    assert expected <= tool_names


# ---------------------------------------------------------------------------
# drt_list_syncs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_syncs_returns_sync(server: FastMCP) -> None:
    result = await call(server, "drt_list_syncs")
    assert len(result) == 1
    assert result[0]["name"] == "notify"
    assert result[0]["destination_type"] == "rest_api"


@pytest.mark.asyncio
async def test_list_syncs_empty_project(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: empty\nprofile: default\n")
    (tmp_path / "syncs").mkdir()
    srv = create_server(tmp_path)
    result = await call(srv, "drt_list_syncs")
    assert result == []


# ---------------------------------------------------------------------------
# drt_validate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_returns_valid_syncs(server: FastMCP) -> None:
    result = await call(server, "drt_validate")
    assert "notify" in result["valid"]
    assert result["errors"] == {}


# ---------------------------------------------------------------------------
# drt_run_test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_test_no_syncs(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: empty\nprofile: default\n")
    (tmp_path / "syncs").mkdir()
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test")
    assert result == {"status": "no_syncs", "results": []}


@pytest.mark.asyncio
async def test_run_test_sync_not_found(server: FastMCP) -> None:
    result = await call(server, "drt_run_test", sync_name="nonexistent")
    assert "error" in result


@pytest.mark.asyncio
async def test_run_test_no_tests_defined(server: FastMCP) -> None:
    # The default fixture sync has no `tests:` block
    result = await call(server, "drt_run_test")
    assert result == {"status": "no_tests", "results": []}


@pytest.mark.asyncio
async def test_run_test_skips_non_queryable_destination(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    # rest_api is not queryable — tests should report skipped
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
        "tests:\n"
        "  - row_count: { min: 1 }\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", sync_name="notify")
    assert result["status"] == "passed"
    assert len(result["results"]) == 1
    assert result["results"][0]["skipped"] is True
    assert "rest_api" in result["results"][0]["reason"]


# ---------------------------------------------------------------------------
# drt_get_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_no_history(server: FastMCP) -> None:
    result = await call(server, "drt_get_status")
    assert result == {}


@pytest.mark.asyncio
async def test_get_status_specific_not_found(server: FastMCP) -> None:
    result = await call(server, "drt_get_status", sync_name="nonexistent")
    assert "error" in result


@pytest.mark.asyncio
async def test_get_status_after_state_saved(server, project_dir: Path) -> None:
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="notify",
            last_run_at="2026-03-30T12:00:00",
            records_synced=42,
            status="success",
        )
    )
    result = await call(server, "drt_get_status", sync_name="notify")
    assert result["notify"]["records_synced"] == 42
    assert result["notify"]["status"] == "success"


# ---------------------------------------------------------------------------
# drt_get_schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_schema_sync(server: FastMCP) -> None:
    schema = await call(server, "drt_get_schema", schema_type="sync")
    assert isinstance(schema, dict)
    assert "$defs" in schema or "properties" in schema


@pytest.mark.asyncio
async def test_get_schema_project(server: FastMCP) -> None:
    schema = await call(server, "drt_get_schema", schema_type="project")
    assert isinstance(schema, dict)
    assert "$defs" in schema or "properties" in schema


# ---------------------------------------------------------------------------
# drt_run_sync — compute_diff parameter (#413 parity)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_sync_returns_error_for_unknown_sync(project_dir: Path, monkeypatch: Any) -> None:
    """Unknown ``sync_name`` returns a structured error (no engine call).

    Bypasses ``load_profile`` (which would otherwise try to read the
    real ``~/.drt/profiles.yml`` on the developer's machine) — the
    sync-name match happens after profile loading in the flow.
    """
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="nonexistent")
    assert "error" in result
    assert "nonexistent" in result["error"]


@pytest.mark.asyncio
async def test_run_sync_compute_diff_requires_dry_run(server: FastMCP) -> None:
    """``compute_diff=True`` without ``dry_run=True`` is a contract
    violation — matches the CLI ``drt run --diff`` requiring
    ``--dry-run``. Returns a structured error rather than executing
    the sync against a live destination.
    """
    result = await call(
        server, "drt_run_sync", sync_name="notify", compute_diff=True, dry_run=False
    )
    assert "error" in result
    assert "dry_run" in result["error"]


@pytest.mark.asyncio
async def test_run_sync_compute_diff_threads_diff_into_response(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``compute_diff=True`` + ``dry_run=True`` → response carries a
    ``diff`` field built from ``diff_to_dict``. This is the success
    path that exercises the load_project / run_sync / response-with-diff
    branch — which the error-path tests can't reach.

    Patches the engine + source/destination factory functions at
    their source modules so the inside-function imports resolve to
    the test doubles, avoiding a real warehouse / HTTP destination.
    """
    from drt.engine.sync import SyncResult

    fake_diff = object()  # diff_to_dict tolerates None / unknown shapes

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        result = SyncResult()
        result.success = 1
        result.failed = 0
        result.diff = fake_diff  # type: ignore[attr-defined]
        return result

    def fake_diff_to_dict(_diff: object) -> dict[str, Any]:
        return {"added": [{"id": 1}], "updated": [], "deleted": [], "unchanged": []}

    # Patch the engine + factory layers at their source modules so the
    # inside-function imports inside `drt_run_sync` pick up the doubles.
    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    monkeypatch.setattr("drt.cli.output.diff_to_dict", fake_diff_to_dict)

    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="notify", dry_run=True, compute_diff=True)

    assert "diff" in result
    assert result["diff"] == {
        "added": [{"id": 1}],
        "updated": [],
        "deleted": [],
        "unchanged": [],
    }
    assert result["dry_run"] is True
    assert result["success"] == 1


@pytest.mark.asyncio
async def test_run_sync_dry_run_without_compute_diff_omits_diff_field(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``compute_diff=False`` → response has no ``diff`` field even
    when ``dry_run=True``. Exercises the response-building path
    without the diff serialisation branch."""
    from drt.engine.sync import SyncResult

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        result = SyncResult()
        result.success = 1
        return result

    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())

    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="notify", dry_run=True)

    assert "diff" not in result
    assert result["dry_run"] is True
    assert result["success"] == 1


# ---------------------------------------------------------------------------
# drt_doctor — environment diagnostics (mirrors `drt doctor` CLI)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_doctor_returns_structured_report(project_dir: Path, monkeypatch: Any) -> None:
    """``drt_doctor`` returns ``{passed, checks}`` with at minimum
    Python version + drt version + project file rows.
    """
    # `_check_*` helpers in drt/cli/doctor.py read from CWD; cd into
    # the temp project so project_file and profile checks see real
    # files instead of whatever the test runner's CWD is.
    monkeypatch.chdir(project_dir)
    srv = create_server(project_dir)

    result = await call(srv, "drt_doctor")
    assert "passed" in result
    assert "checks" in result
    assert isinstance(result["checks"], list)
    # At minimum: Python version, drt version, project file
    names = {c["name"] for c in result["checks"]}
    assert "Python version" in names
    assert "drt version" in names
    assert "Project file" in names
    # Each check has the documented shape
    for check in result["checks"]:
        assert set(check.keys()) >= {"category", "name", "ok", "message"}


@pytest.mark.asyncio
async def test_doctor_passes_on_well_formed_project(project_dir: Path, monkeypatch: Any) -> None:
    """On a well-formed project (project file + profile file + syncs/),
    ``passed`` is True. The fixture creates exactly this shape, so any
    regression that breaks the happy path surfaces here."""
    monkeypatch.chdir(project_dir)

    # Profile fixture: ~/.drt/profiles.yml gets read by _check_profile.
    # The fixture project references profile "default"; provide a
    # minimal profiles.yml under a fake HOME to keep the test
    # self-contained and avoid touching the developer's real
    # ~/.drt/profiles.yml.
    fake_home = project_dir / "fake_home"
    (fake_home / ".drt").mkdir(parents=True)
    (fake_home / ".drt" / "profiles.yml").write_text("default: { type: duckdb }\n")
    monkeypatch.setenv("HOME", str(fake_home))

    srv = create_server(project_dir)
    result = await call(srv, "drt_doctor")
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_doctor_fails_without_project_file(tmp_path: Path, monkeypatch: Any) -> None:
    """Outside a drt project, ``passed`` is False — the project-file
    check is required for a green report."""
    srv = create_server(tmp_path)
    monkeypatch.chdir(tmp_path)  # empty dir, no drt_project.yml
    result = await call(srv, "drt_doctor")
    assert result["passed"] is False
    project_file_row = next(c for c in result["checks"] if c["name"] == "Project file")
    assert project_file_row["ok"] is False


@pytest.mark.asyncio
async def test_server_lists_drt_doctor_tool() -> None:
    """The newly added `drt_doctor` is registered alongside the
    existing tools."""
    srv = create_server()
    tools = await srv._local_provider._list_tools()
    tool_names = {t.name for t in tools}
    assert "drt_doctor" in tool_names


# ---------------------------------------------------------------------------
# drt_list_connectors — inventory / registry parity (#718)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_connectors_matches_connector_ssot(server: FastMCP) -> None:
    """The MCP inventory must list *exactly* the registered connector types.

    This is the test-time guard against the drift that let `salesforce_bulk`
    silently fall out of the inventory: comparing against the `drt.config.
    connectors` SSoT (kept in lockstep with the registry by
    `test_cli_list_connectors`) catches both missing and extra entries.
    """
    from drt.config.connectors import DESTINATIONS, SOURCES

    result = await call(server, "drt_list_connectors")
    assert {c["type"] for c in result["destinations"]} == {t for t, _ in DESTINATIONS}
    assert {c["type"] for c in result["sources"]} == {t for t, _ in SOURCES}


# ---------------------------------------------------------------------------
# drt_dlq — Dead Letter Queue inspection (#718, v0.7.9 parity)
# ---------------------------------------------------------------------------


def _seed_dlq(project_dir: Path, ids: list[int]) -> None:
    from drt.state.dlq import DeadLetter, DlqStore

    DlqStore(project_dir).append(
        "notify",
        [DeadLetter(record={"id": i}, error_message="boom") for i in ids],
    )


@pytest.mark.asyncio
async def test_dlq_empty_project_reports_no_depths(server: FastMCP) -> None:
    result = await call(server, "drt_dlq")
    assert result == {"depths": {}}


@pytest.mark.asyncio
async def test_dlq_reports_depth_and_records(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3])
    result = await call(server, "drt_dlq", sync_name="notify")
    assert result["depth"] == 3
    assert [r["record"]["id"] for r in result["records"]] == [1, 2, 3]
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_dlq_truncates_records_to_limit(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3, 4, 5])
    result = await call(server, "drt_dlq", sync_name="notify", limit=2)
    assert result["depth"] == 5
    assert len(result["records"]) == 2
    assert result["truncated"] is True


@pytest.mark.asyncio
async def test_dlq_all_depths(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2])
    result = await call(server, "drt_dlq")
    assert result["depths"] == {"notify": 2}


# ---------------------------------------------------------------------------
# drt_retry — Dead Letter Queue replay (#718, mirrors `drt retry`)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_empty_queue(server: FastMCP) -> None:
    result = await call(server, "drt_retry", sync_name="notify")
    assert result["status"] == "empty"


@pytest.mark.asyncio
async def test_retry_unknown_sync(server: FastMCP) -> None:
    result = await call(server, "drt_retry", sync_name="nope")
    assert "error" in result


@pytest.mark.asyncio
async def test_retry_negative_limit(server: FastMCP) -> None:
    result = await call(server, "drt_retry", sync_name="notify", limit=-1)
    assert "error" in result


@pytest.mark.asyncio
async def test_retry_dry_run_sends_nothing(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3])
    result = await call(server, "drt_retry", sync_name="notify", dry_run=True)
    assert result["status"] == "dry_run"
    assert result["would_retry"] == 3
    from drt.state.dlq import DlqStore

    assert DlqStore(project_dir).depth("notify") == 3  # untouched


@pytest.mark.asyncio
async def test_retry_clear(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3])
    result = await call(server, "drt_retry", sync_name="notify", clear=True)
    assert result["status"] == "cleared"
    assert result["cleared"] == 3
    from drt.state.dlq import DlqStore

    assert DlqStore(project_dir).depth("notify") == 0


# ---------------------------------------------------------------------------
# drt_get_manifest — sync catalog + lineage (#718, `drt docs` JSON)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_manifest_returns_catalog(server: FastMCP) -> None:
    result = await call(server, "drt_get_manifest")
    assert isinstance(result, dict)
    assert "schema_version" in result
    assert [s["name"] for s in result["syncs"]] == ["notify"]


@pytest.mark.asyncio
async def test_get_manifest_labels_are_docs_safe_by_default(server: FastMCP) -> None:
    """The manifest is the same artifact `drt docs generate` ships (#696), so
    the MCP tool defaults to the same safe labels as the CLI."""
    import json

    result = await call(server, "drt_get_manifest")
    assert result["destinations"][0]["label"] == "rest_api"
    assert "example.com" not in json.dumps(result)


@pytest.mark.asyncio
async def test_get_manifest_full_labels_opts_in(server: FastMCP) -> None:
    """`full_labels=True` mirrors `drt docs generate --full-labels`."""
    result = await call(server, "drt_get_manifest", full_labels=True)
    assert result["destinations"][0]["label"] == "rest_api (https://example.com/hook)"


# ---------------------------------------------------------------------------
# drt_list_profiles / drt_test_profile — credential diagnostics (#718)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_profiles(server: FastMCP, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "drt.config.credentials.load_raw_profiles",
        lambda: {"default": {"type": "duckdb"}, "prod": {"type": "bigquery"}},
    )
    result = await call(server, "drt_list_profiles")
    assert result["profiles"] == [
        {"name": "default", "type": "duckdb"},
        {"name": "prod", "type": "bigquery"},
    ]


@pytest.mark.asyncio
async def test_test_profile_not_found(server: FastMCP, monkeypatch: Any) -> None:
    def _raise(_name: str) -> Any:
        raise KeyError("Profile 'nope' not found.")

    monkeypatch.setattr("drt.config.credentials.load_profile", _raise)
    result = await call(server, "drt_test_profile", name="nope")
    assert result["ok"] is False
    assert "error" in result


@pytest.mark.asyncio
async def test_test_profile_ok(server: FastMCP, monkeypatch: Any) -> None:
    from types import SimpleNamespace

    monkeypatch.setattr(
        "drt.config.credentials.load_profile",
        lambda _name: SimpleNamespace(type="duckdb"),
    )
    monkeypatch.setattr(
        "drt.connectors.registry.get_source",
        lambda _profile: SimpleNamespace(test_connection=lambda _p: True),
    )
    result = await call(server, "drt_test_profile", name="default")
    assert result == {"name": "default", "type": "duckdb", "ok": True}


@pytest.mark.asyncio
async def test_test_profile_connection_error(server: FastMCP, monkeypatch: Any) -> None:
    """A source whose test_connection raises → ok=False with the error message
    (the profile loaded fine, so `type` is still reported)."""
    from types import SimpleNamespace

    def _boom(_profile: object) -> bool:
        raise ConnectionError("connection refused")

    monkeypatch.setattr(
        "drt.config.credentials.load_profile",
        lambda _name: SimpleNamespace(type="postgres"),
    )
    monkeypatch.setattr(
        "drt.connectors.registry.get_source",
        lambda _profile: SimpleNamespace(test_connection=_boom),
    )
    result = await call(server, "drt_test_profile", name="prod")
    assert result["ok"] is False
    assert result["type"] == "postgres"
    assert "connection refused" in result["error"]


# ---------------------------------------------------------------------------
# drt_run_sync — cursor_value / profile_name overrides (#718)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_sync_threads_profile_override_and_full_mode_cursor_guard(
    project_dir: Path, monkeypatch: Any
) -> None:
    """`profile_name` is resolved via `resolve_profile_name(override, default)`,
    and `cursor_value` is suppressed for a non-incremental sync (the fixture
    `notify` is full-mode), mirroring the `drt run` guard."""
    from drt.engine.sync import SyncResult

    resolve_args: dict[str, Any] = {}
    captured: dict[str, Any] = {}

    def fake_resolve(override: str | None, default: str) -> str:
        resolve_args["value"] = (override, default)
        return "default"

    def fake_run_sync(*_args: Any, **kwargs: Any) -> SyncResult:
        captured.update(kwargs)
        return SyncResult()

    monkeypatch.setattr("drt.cli._helpers.resolve_profile_name", fake_resolve)
    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())

    srv = create_server(project_dir)
    await call(srv, "drt_run_sync", sync_name="notify", cursor_value="100", profile_name="prod")

    assert resolve_args["value"] == ("prod", "default")
    assert captured["cursor_value_override"] is None  # full-mode sync → guarded off


@pytest.mark.asyncio
async def test_server_lists_new_parity_tools() -> None:
    srv = create_server()
    tools = await srv._local_provider._list_tools()
    names = {t.name for t in tools}
    assert {
        "drt_dlq",
        "drt_retry",
        "drt_get_manifest",
        "drt_list_profiles",
        "drt_test_profile",
    } <= names
