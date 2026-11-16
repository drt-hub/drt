"""Implementation for the ``drt_test_profile`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``. Takes no context — the original tool never touched
``project_dir``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def test_profile(name: str, project_dir: Path | None = None) -> dict[str, Any]:
    from drt.config.credentials import load_profile, project_dir_context, resolve_profile_paths
    from drt.connectors.registry import get_source

    try:
        profile = resolve_profile_paths(load_profile(name), project_dir)
    except (FileNotFoundError, KeyError, ValueError) as e:
        return {"name": name, "ok": False, "error": str(e)}

    source = get_source(profile)
    try:
        with project_dir_context(project_dir):
            ok = source.test_connection(profile)
    except Exception as e:
        return {"name": name, "type": profile.type, "ok": False, "error": str(e)}

    return {"name": name, "type": profile.type, "ok": bool(ok)}
