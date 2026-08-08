"""Shared context-loading helper for MCP tools (#723 part 1).

Extracted from ``drt/mcp/server.py``, where the same project-config /
sync-config setup was re-loaded inline by nearly every ``@mcp.tool()``
closure. ``McpContext`` centralizes that loading; each tool implementation
in ``drt/mcp/tools/`` takes a context instance and pulls only what it
already used to load — nothing is loaded eagerly, so a tool that never
touched sync config before this refactor still won't.

Lives at the same level as ``server.py`` and the ``tools/`` package —
mirrors ``drt/cli/_helpers.py`` sitting beside ``drt/cli/commands/``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from drt.config.models import ProjectConfig, SyncConfig
    from drt.config.parser import SyncLoadResult


@dataclass
class McpContext:
    """Bundles a resolved ``project_dir`` with on-demand project/sync
    loaders. Each method mirrors an inline call an MCP tool used to make
    directly — same function, same arguments, just centralized.
    """

    project_dir: Path

    def load_project(self) -> ProjectConfig:
        from drt.config.parser import load_project

        return load_project(self.project_dir)

    def load_project_for_state(self) -> ProjectConfig:
        """Load backend config without making state reads require a project.

        State/history/DLQ tools historically worked against a standalone
        ``.drt`` directory. A missing project file therefore selects today's
        local default; a present file still goes through normal validation.
        """
        from drt.config.models import ProjectConfig

        if (self.project_dir / "drt_project.yml").exists():
            return self.load_project()
        return ProjectConfig(name="drt")

    def load_syncs(self) -> list[SyncConfig]:
        from drt.config.parser import load_syncs

        return load_syncs(self.project_dir)

    def load_syncs_safe(self) -> SyncLoadResult:
        from drt.config.parser import load_syncs_safe

        return load_syncs_safe(self.project_dir)

    def find_sync(self, sync_name: str) -> SyncConfig | None:
        """First sync named *sync_name* among ``load_syncs()``, or None."""
        return next((s for s in self.load_syncs() if s.name == sync_name), None)


def _load_ctx(project_dir: Path) -> McpContext:
    """Build the shared context for one ``create_server()`` instance."""
    return McpContext(project_dir)
