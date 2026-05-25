"""`drt mcp run` — start the drt MCP server."""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import print_error

mcp_app = typer.Typer(name="mcp", help="MCP server commands.", no_args_is_help=True)
app.add_typer(mcp_app)


@mcp_app.command(name="run")
def mcp_run() -> None:
    """Start the drt MCP server (stdio transport).

    Requires: pip install drt-core[mcp]

    Add to Claude Desktop or Cursor:
        {
          "mcpServers": {
            "drt": {
              "command": "uvx",
              "args": ["drt-core[mcp]", "mcp", "run"]
            }
          }
        }
    """
    try:
        from drt.mcp.server import run as mcp_server_run
    except ImportError:
        print_error("MCP server requires: pip install drt-core[mcp]")
        raise typer.Exit(1)

    mcp_server_run()
