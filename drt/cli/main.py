"""drt CLI entry point."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig
    from drt.config.models import SyncConfig
    from drt.destinations.base import Destination
    from drt.sources.base import Source


from drt import __version__
from drt.cli import commands as _commands  # noqa: F401 — register commands

# The shared Typer instance lives in drt.cli._app so that per-command
# modules under drt/cli/commands/ can import it without circular imports
# (this main module then imports the commands package to trigger their
# @app.command decorator side effects).
from drt.cli._app import app
from drt.cli.output import (
    console,
)


def _resolve_profile_name(cli_flag: str | None, project_profile: str) -> str:
    """Resolve which profile to use.

    Precedence: --profile flag > DRT_PROFILE env var > drt_project.yml
    """
    if cli_flag:
        return cli_flag
    env = os.environ.get("DRT_PROFILE")
    if env:
        return env
    return project_profile


def version_callback(value: bool) -> None:
    if value:
        import platform
        import sys

        import drt as drt_pkg

        # First line stays `drt version X.Y.Z` so scripts grepping for that
        # pattern keep working. The follow-up lines are diagnostic context
        # that saves a round-trip on bug reports.
        py = sys.version_info
        impl = platform.python_implementation()
        install_path = Path(drt_pkg.__file__).resolve().parent
        plat = f"{platform.system()} {platform.release()} ({platform.machine()})"

        console.print(f"drt version {__version__}")
        console.print(f"Python {py.major}.{py.minor}.{py.micro} ({impl})")
        console.print(f"Install: {install_path}")
        console.print(f"Platform: {plat}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-v", callback=version_callback, is_eager=True, help="Show version."
    ),
) -> None:
    pass


# `drt init` lives in drt/cli/commands/init.py (#546 Phase 2)
# `drt sources` / `drt destinations` live in drt/cli/commands/connectors.py
# `drt clean` lives in drt/cli/commands/clean.py
# `drt run` lives in drt/cli/commands/run.py (#573 Phase 2b PR (a) —
# `LogFormat`, `_JsonFormatter`, `_configure_json_logging`, `_RunContext`,
# `_exit_code_for_signal`, `_run_one`, `_print_watermark_summary` are
# re-exported below as back-compat shims for callers that still import
# them from `drt.cli.main`)


# `drt list` lives in drt/cli/commands/list_syncs.py (#546)
# `drt validate` lives in drt/cli/commands/validate.py (#573 Phase 2b PR (b) —
# `_group_secret_findings` and `_run_connection_test` are re-exported below
# as back-compat shims)
# `drt doctor` lives in drt/cli/commands/doctor.py (#546)
# `drt status` lives in drt/cli/commands/status.py (#573 Phase 2b PR (b) —
# `_print_history` is re-exported below)
# `drt test` lives in drt/cli/commands/test.py (#573 Phase 2b PR (b) —
# `_SyncTestResult` and `_test_display_name` are re-exported below)
# `drt serve` lives in drt/cli/commands/serve.py (#546 Phase 2)


# Sub-Typer namespaces — each one lives in its own module under
# drt/cli/commands/ (#546). Imported via drt.cli.commands package which
# fires the registration decorators.
#
#   `drt config ...`  → drt/cli/commands/config.py
#   `drt cloud ...`   → drt/cli/commands/cloud.py
#   `drt docs ...`    → drt/cli/commands/docs.py
#   `drt mcp ...`     → drt/cli/commands/mcp.py


# ---------------------------------------------------------------------------
# Source / Destination factories — backward-compat shims
# ---------------------------------------------------------------------------
#
# The real implementations now live in ``drt/cli/_helpers.py``. These thin
# wrappers preserve the legacy ``from drt.cli.main import _get_source`` /
# ``_get_destination`` import path that several tests rely on (see #565
# back-compat note). New callers should import directly from _helpers.


def _get_source(profile: ProfileConfig) -> Source:
    """Back-compat shim — see ``drt.cli._helpers.get_source``."""
    from drt.cli._helpers import get_source

    return get_source(profile)


def _get_watermark_storage(sync: SyncConfig, project_dir: Path) -> Any:
    """Back-compat shim — see ``drt.cli._helpers.get_watermark_storage``."""
    from drt.cli._helpers import get_watermark_storage

    return get_watermark_storage(sync, project_dir)


def _get_destination(sync: SyncConfig) -> Destination:
    """Back-compat shim — see ``drt.cli._helpers.get_destination``."""
    from drt.cli._helpers import get_destination

    return get_destination(sync)


# ---------------------------------------------------------------------------
# Back-compat re-exports from drt.cli.commands.run (#573 Phase 2b PR (a))
# ---------------------------------------------------------------------------
# Tests and library callers import these names from drt.cli.main. The
# implementations now live in drt/cli/commands/run.py; re-export so the
# legacy import path keeps working. New callers should import from
# drt.cli.commands.run directly.
from drt.cli.commands.run import (  # noqa: E402, F401
    LogFormat,
    _configure_json_logging,
    _exit_code_for_signal,
    _JsonFormatter,
    _print_watermark_summary,
    _run_one,
    _RunContext,
)

# ---------------------------------------------------------------------------
# Back-compat re-exports from validate / status / test (#573 Phase 2b PR (b))
# ---------------------------------------------------------------------------
from drt.cli.commands.status import _print_history  # noqa: E402, F401
from drt.cli.commands.test import (  # noqa: E402, F401
    _SyncTestResult,
    _test_display_name,
)
from drt.cli.commands.validate import (  # noqa: E402, F401
    _group_secret_findings,
    _run_connection_test,
)
