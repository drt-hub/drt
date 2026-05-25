"""Per-command CLI handler modules.

Importing this package triggers each command module's import, which in
turn runs the ``@app.command()`` decorators that register the command on
the shared ``typer.Typer`` instance in ``drt.cli._app``.

New commands extracted from ``drt/cli/main.py`` should land here in their
own module, importing ``from drt.cli._app import app``. See
``drt/cli/commands/doctor.py`` as the minimal template.
"""

from __future__ import annotations

# Side-effect imports — each module's @app.command decorators register
# the command on drt.cli._app.app. Namespace sub-Typers (config, cloud,
# docs, mcp) sit alongside top-level commands.
from drt.cli.commands import (
    cloud,  # noqa: F401
    config,  # noqa: F401
    docs,  # noqa: F401
    doctor,  # noqa: F401
    list_syncs,  # noqa: F401
    mcp,  # noqa: F401
)
