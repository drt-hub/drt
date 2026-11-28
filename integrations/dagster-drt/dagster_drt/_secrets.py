"""Scope drt's project-local ``.drt/secrets.toml`` lookup to one operation.

``drt.config.credentials.secrets_project_dir`` (drt-core, #1357) makes
``resolve_env()`` read ``<project_dir>/.drt/secrets.toml`` instead of the
process working directory. A Dagster code location's cwd is usually not the
drt project, so every boundary that loads a profile or resolves credentials
enters this scope.

Older drt-core releases do not have the symbol; the scope then degrades to the
previous behaviour (cwd lookup) rather than failing, so the declared
``drt-core>=1.0.0`` minimum is unchanged.

Do not hold the scope across a ``yield``: it is context-local state, and a
generator that suspends inside it would leak the scope to its consumer.
"""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from pathlib import Path


def project_secrets_scope(project_dir: str | Path) -> AbstractContextManager[None]:
    """Return a context manager resolving secrets against *project_dir*."""
    try:
        from drt.config.credentials import secrets_project_dir
    except ImportError:  # drt-core older than #1357
        return nullcontext()
    return secrets_project_dir(Path(project_dir))
