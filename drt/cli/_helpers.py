"""Shared CLI helpers — factories + profile resolution.

Extracted from ``drt/cli/main.py`` in Phase 2 of #546 so that per-command
modules under ``drt/cli/commands/`` can share these utilities without
pulling the whole main module (and its many import dependencies) into
their import graph.

Lives at the same level as ``_app.py`` and ``_connector_detail.py`` —
"underscore-prefixed CLI internals." Not part of the public Python API;
the only stable surface is the ``drt`` CLI itself.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

if TYPE_CHECKING:
    from drt.config.models import SyncConfig
    from drt.config.profiles import ProfileConfigLike
    from drt.destinations.base import Destination
    from drt.sources.base import Source


def confirm_destructive(prompt: str, yes: bool) -> bool:
    """Confirm a destructive operation, or explain how to skip the prompt.

    Shared so every destructive command behaves the same way (#776). The case
    that matters is **non-interactive**: calling ``typer.confirm`` directly
    there aborts with a bare ``Aborted.``, which fails correctly but names
    neither the cause nor the fix — and the ``[y/N]`` it prints actively
    misleads, since it suggests piping ``y`` would work when the real answer
    is ``--yes``.

    Deliberately *not* gated on ``sys.stdin.isatty()``. A pipe carrying a real
    answer is not a TTY either, so an isatty check refuses input that was
    genuinely supplied — including every ``CliRunner(input=...)`` test, which
    is how this was caught. Instead the prompt is attempted and only the EOF
    case (``click.Abort``) is translated: that is precisely "nothing to read",
    which is the CI situation and nothing else.

    ``--yes`` always skips the prompt — it means "don't ask me", not "ask me
    anyway".
    """
    import click

    if yes:
        return True

    try:
        return bool(typer.confirm(prompt))
    except (click.Abort, EOFError):
        typer.secho(
            "\nError: this is a destructive operation and needs confirmation.\n"
            "       Re-run with --yes in a non-interactive context.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1) from None


def exit_code_for_signal(signum: int) -> int:
    """POSIX convention: 128 + signal number (SIGINT=2 → 130, SIGTERM=15 → 143)."""
    return 128 + signum


def resolve_profile_name(cli_flag: str | None, project_profile: str) -> str:
    """Resolve which profile to use.

    Precedence: ``--profile`` flag > ``DRT_PROFILE`` env var > drt_project.yml
    """
    if cli_flag:
        return cli_flag
    env = os.environ.get("DRT_PROFILE")
    if env:
        return env
    return project_profile


def get_source(profile: ProfileConfigLike) -> Source:
    """Return a Source instance for the given profile configuration.

    Uses the connector registry for automatic connector discovery and
    instantiation.
    """
    from drt.connectors import get_source as _registry_get_source

    return _registry_get_source(profile)


def get_destination(sync: SyncConfig) -> Destination:
    """Return a Destination instance for the given sync configuration.

    Uses the connector registry for automatic connector discovery and
    instantiation.
    """
    from drt.connectors import get_destination as _registry_get_destination

    return _registry_get_destination(sync.destination)


def get_watermark_storage(sync: SyncConfig, project_dir: Path) -> Any:
    """Build watermark storage from sync config, or ``None`` if not configured."""
    from drt.state.watermark import (
        BigQueryWatermarkStorage,
        GCSWatermarkStorage,
        LocalWatermarkStorage,
    )

    wm = sync.sync.watermark
    if wm is None:
        return None

    if wm.storage == "local":
        return LocalWatermarkStorage(project_dir)
    elif wm.storage == "gcs":
        assert wm.bucket is not None
        assert wm.key is not None
        return GCSWatermarkStorage(bucket=wm.bucket, key=wm.key)
    elif wm.storage == "bigquery":
        assert wm.project is not None
        assert wm.dataset is not None
        return BigQueryWatermarkStorage(
            project=wm.project,
            dataset=wm.dataset,
        )
    return None
