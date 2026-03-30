"""Unit tests for the drt MCP server tools.

Requires: pip install drt-core[mcp]
These tests are skipped automatically when fastmcp is not installed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("fastmcp", reason="requires drt-core[mcp]")

from drt.mcp.server import create_server  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def call(server, tool_name: str, **kwargs):  # type: ignore[no-untyped-def]
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
def server(project_dir: Path):  # type: ignore[no-untyped-def]
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
        "drt_list_syncs", "drt_run_sync", "drt_get_status", "drt_validate", "drt_get_schema"
    }
    assert expected <= tool_names


# ---------------------------------------------------------------------------
# drt_list_syncs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_syncs_returns_sync(server) -> None:  # type: ignore[no-untyped-def]
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
async def test_validate_returns_valid_syncs(server) -> None:  # type: ignore[no-untyped-def]
    result = await call(server, "drt_validate")
    assert "notify" in result["valid"]
    assert result["errors"] == {}


# ---------------------------------------------------------------------------
# drt_get_status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_status_no_history(server) -> None:  # type: ignore[no-untyped-def]
    result = await call(server, "drt_get_status")
    assert result == {}


@pytest.mark.asyncio
async def test_get_status_specific_not_found(server) -> None:  # type: ignore[no-untyped-def]
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
async def test_get_schema_sync(server) -> None:  # type: ignore[no-untyped-def]
    schema = await call(server, "drt_get_schema", schema_type="sync")
    assert isinstance(schema, dict)
    assert "$defs" in schema or "properties" in schema


@pytest.mark.asyncio
async def test_get_schema_project(server) -> None:  # type: ignore[no-untyped-def]
    schema = await call(server, "drt_get_schema", schema_type="project")
    assert isinstance(schema, dict)
    assert "$defs" in schema or "properties" in schema
