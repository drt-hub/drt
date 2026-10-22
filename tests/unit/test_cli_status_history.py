"""CLI tests for ``drt status --history`` and the corresponding MCP tool (#276)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from drt.cli.main import app
from drt.state.history import HistoryEntry, HistoryManager

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_history(project_dir: Path, sync: str = "demo", count: int = 3) -> None:
    """Drop ``count`` history entries for ``sync`` into <project_dir>/.drt/history/."""
    mgr = HistoryManager(project_dir)
    for i in range(count):
        mgr.append(
            HistoryEntry(
                sync_name=sync,
                started_at=f"2026-05-0{i + 1}T10:00:00+00:00",
                completed_at=f"2026-05-0{i + 1}T10:00:30+00:00",
                duration_seconds=30.0,
                status="success" if i % 2 == 0 else "partial",
                records_synced=100 * (i + 1),
                records_failed=i,
                errors=([f"err {i}"] if i > 0 else []),
            )
        )


# ---------------------------------------------------------------------------
# drt status --history
# ---------------------------------------------------------------------------


class TestStatusHistoryLineage:
    """#1222: the Plan column appears only when some run applied a plan."""

    def _run(self, project_dir: Path, *args: str) -> Any:
        import os

        old = os.getcwd()
        os.chdir(project_dir)
        try:
            return runner.invoke(
                app,
                ["status", "--history", *args],
                catch_exceptions=False,
                env={"COLUMNS": "200"},
            )
        finally:
            os.chdir(old)

    def test_applied_plan_and_approver_are_listed(self, tmp_path: Path) -> None:
        HistoryManager(tmp_path).append(
            HistoryEntry(
                sync_name="alpha",
                started_at="2026-05-01T10:00:00+00:00",
                completed_at="2026-05-01T10:00:30+00:00",
                duration_seconds=30.0,
                status="success",
                records_synced=3,
                records_failed=0,
                plan_id="sha256:cafe1234",
                approved_by="alice",
            )
        )

        result = self._run(tmp_path)

        assert result.exit_code == 0
        assert "Plan" in result.stdout
        assert "alice" in result.stdout
        payload = json.loads(self._run(tmp_path, "--output", "json").stdout)
        assert payload["entries"][0]["plan_id"] == "sha256:cafe1234"
        assert payload["entries"][0]["approved_by"] == "alice"

    def test_an_approver_name_cannot_inject_rich_markup(self, tmp_path: Path) -> None:
        HistoryManager(tmp_path).append(
            HistoryEntry(
                sync_name="alpha",
                started_at="2026-05-01T10:00:00+00:00",
                completed_at="2026-05-01T10:00:30+00:00",
                duration_seconds=30.0,
                status="success",
                records_synced=3,
                records_failed=0,
                plan_id="sha256:abc",
                approved_by="\\\\[conceal]mallory",
            )
        )

        result = self._run(tmp_path)

        assert "mallory" in result.stdout and "conceal" in result.stdout

    def test_a_hand_edited_non_string_lineage_value_does_not_crash_status(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / ".drt" / "history"
        path.mkdir(parents=True)
        (path / "alpha.jsonl").write_text(
            json.dumps(
                {
                    "sync_name": "alpha",
                    "started_at": "2026-05-01T10:00:00+00:00",
                    "completed_at": "2026-05-01T10:00:30+00:00",
                    "duration_seconds": 1.0,
                    "status": "success",
                    "records_synced": 1,
                    "records_failed": 0,
                    "plan_id": 12345,
                    "approved_by": {"name": "x"},
                }
            )
            + "\n"
        )

        assert self._run(tmp_path).exit_code == 0

    def test_ordinary_history_keeps_its_existing_columns(self, tmp_path: Path) -> None:
        _seed_history(tmp_path, sync="alpha", count=1)

        assert "Plan" not in self._run(tmp_path).stdout


class TestStatusHistoryText:
    def test_history_text_output_contains_sync_name(self, tmp_path: Path) -> None:
        _seed_history(tmp_path, sync="alpha", count=2)
        result = runner.invoke(app, ["status", "--history"], catch_exceptions=False)
        # Run the CLI inside tmp_path by switching cwd
        # (typer.testing has no native cwd kwarg — we rely on Path("."))

        # We need to actually run from tmp_path. Re-run with monkeypatched cwd.
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["status", "--history"], catch_exceptions=False)
        finally:
            os.chdir(old)

        assert result.exit_code == 0
        assert "alpha" in result.stdout
        # Newest first → 2026-05-02 should come before 2026-05-01.
        idx_5_02 = result.stdout.find("2026-05-02")
        idx_5_01 = result.stdout.find("2026-05-01")
        assert idx_5_02 != -1 and idx_5_01 != -1
        assert idx_5_02 < idx_5_01

    def test_history_empty_message(self, tmp_path: Path) -> None:
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["status", "--history"], catch_exceptions=False)
        finally:
            os.chdir(old)
        assert result.exit_code == 0
        assert "No history" in result.stdout

    def test_history_filtered_by_sync(self, tmp_path: Path) -> None:
        _seed_history(tmp_path, sync="alpha", count=2)
        _seed_history(tmp_path, sync="beta", count=2)
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(
                app, ["status", "--history", "--sync", "alpha"], catch_exceptions=False
            )
        finally:
            os.chdir(old)
        assert result.exit_code == 0
        assert "alpha" in result.stdout
        assert "beta" not in result.stdout

    def test_history_limit_caps_results(self, tmp_path: Path) -> None:
        _seed_history(tmp_path, sync="s", count=5)
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(
                app,
                ["status", "--history", "--sync", "s", "--limit", "2"],
                catch_exceptions=False,
            )
        finally:
            os.chdir(old)
        assert result.exit_code == 0
        # Two entries shown — count distinct started_at lines (2026-05-XX prefix)
        assert result.stdout.count("2026-05-") == 2


class TestStatusHistoryJson:
    def test_history_json_output_is_parseable(self, tmp_path: Path) -> None:
        _seed_history(tmp_path, sync="alpha", count=3)
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(
                app,
                ["status", "--history", "--output", "json"],
                catch_exceptions=False,
            )
        finally:
            os.chdir(old)
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "entries" in data
        assert len(data["entries"]) == 3
        # JSON output preserves all fields
        first = data["entries"][0]
        assert first["sync_name"] == "alpha"
        assert "duration_seconds" in first
        assert "status" in first

    def test_history_json_empty(self, tmp_path: Path) -> None:
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(
                app,
                ["status", "--history", "--output", "json"],
                catch_exceptions=False,
            )
        finally:
            os.chdir(old)
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data == {"entries": []}


# ---------------------------------------------------------------------------
# drt_get_history MCP tool
# ---------------------------------------------------------------------------


class TestMcpGetHistoryTool:
    """Drive drt_get_history via the real FastMCP call path (matches test_mcp.py).

    Skipped when ``fastmcp`` (the optional ``[mcp]`` extra) is not installed.
    """

    @staticmethod
    async def _call(server: Any, tool_name: str, **kwargs: Any) -> Any:
        result = await server.call_tool(tool_name, kwargs)
        sc = result.structured_content
        if isinstance(sc, dict) and list(sc.keys()) == ["result"]:
            return sc["result"]
        return sc

    @pytest.mark.asyncio
    async def test_mcp_returns_entries_for_specific_sync(self, tmp_path: Path) -> None:
        pytest.importorskip("fastmcp", reason="requires drt-core[mcp]")
        _seed_history(tmp_path, sync="alpha", count=3)
        _seed_history(tmp_path, sync="beta", count=2)
        from drt.mcp.server import create_server

        srv = create_server(tmp_path)
        result = await self._call(srv, "drt_get_history", sync_name="alpha", limit=10)
        assert "entries" in result
        assert len(result["entries"]) == 3
        assert all(e["sync_name"] == "alpha" for e in result["entries"])

    @pytest.mark.asyncio
    async def test_mcp_returns_all_syncs_when_unspecified(self, tmp_path: Path) -> None:
        pytest.importorskip("fastmcp", reason="requires drt-core[mcp]")
        _seed_history(tmp_path, sync="alpha", count=2)
        _seed_history(tmp_path, sync="beta", count=2)
        from drt.mcp.server import create_server

        srv = create_server(tmp_path)
        result = await self._call(srv, "drt_get_history", limit=20)
        names = {e["sync_name"] for e in result["entries"]}
        assert names == {"alpha", "beta"}
        assert len(result["entries"]) == 4

    @pytest.mark.asyncio
    async def test_mcp_returns_empty_when_no_history(self, tmp_path: Path) -> None:
        pytest.importorskip("fastmcp", reason="requires drt-core[mcp]")
        from drt.mcp.server import create_server

        srv = create_server(tmp_path)
        result = await self._call(srv, "drt_get_history", limit=10)
        assert result == {"entries": []}
