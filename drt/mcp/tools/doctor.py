"""Implementation for the ``drt_doctor`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``. Takes no context — the original tool never touched
``project_dir`` (the underlying ``_check_*`` helpers read from CWD).
"""

from __future__ import annotations

from typing import Any


def doctor() -> dict[str, Any]:
    from drt import __version__ as drt_version
    from drt.cli.doctor import (
        _check_env_vars,
        _check_extras,
        _check_profile,
        _check_project_file,
        _check_python,
        _check_syncs,
    )

    checks: list[dict[str, Any]] = []
    required_ok = True

    py_ok, py_msg = _check_python()
    checks.append({"category": "runtime", "name": "Python version", "ok": py_ok, "message": py_msg})
    required_ok = required_ok and py_ok

    checks.append(
        {
            "category": "runtime",
            "name": "drt version",
            "ok": True,
            "message": drt_version,
        }
    )

    proj_ok, proj_msg, project_data = _check_project_file()
    checks.append(
        {"category": "project", "name": "Project file", "ok": proj_ok, "message": proj_msg}
    )
    required_ok = required_ok and proj_ok

    if project_data:
        prof_ok, prof_msg = _check_profile(project_data)
        checks.append(
            {"category": "project", "name": "Profile", "ok": prof_ok, "message": prof_msg}
        )
        required_ok = required_ok and prof_ok

        _, syncs_ok, syncs_msg = _check_syncs(project_data)
        checks.append(
            {"category": "project", "name": "Syncs", "ok": syncs_ok, "message": syncs_msg}
        )

    for label, ok, msg in _check_extras():
        # Extras are optional — they affect ``ok`` of the row but not
        # overall ``passed``. A user can run a duckdb-only project with
        # no other extras installed and that's fine.
        checks.append({"category": "extras", "name": label, "ok": ok, "message": msg})

    for var, ok, msg in _check_env_vars(project_data):
        checks.append({"category": "env", "name": var, "ok": ok, "message": msg})

    return {"passed": required_ok, "checks": checks}
