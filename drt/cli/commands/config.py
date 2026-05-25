"""`drt config` — user-level settings (currently telemetry only)."""

from __future__ import annotations

import json

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error

config_app = typer.Typer(
    name="config",
    help="Manage user-level drt settings (~/.drt/).",
    no_args_is_help=True,
)
app.add_typer(config_app)


@config_app.command(name="set")
def config_set(key: str, value: str) -> None:
    """Set a user-level setting. Currently supports: telemetry.enabled."""
    from drt import telemetry

    if key == "telemetry.enabled":
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            telemetry.set_enabled(True)
            console.print("[green]Telemetry enabled.[/green] Thanks for helping improve drt.")
        elif normalized in {"false", "0", "no", "off"}:
            telemetry.set_enabled(False)
            console.print("Telemetry disabled.")
        else:
            print_error(f"Invalid boolean value: {value!r}")
            raise typer.Exit(code=2)
        return
    print_error(f"Unknown config key: {key!r}. Known keys: telemetry.enabled")
    raise typer.Exit(code=2)


@config_app.command(name="unset")
def config_unset(key: str) -> None:
    """Remove a user-level setting (returns to default)."""
    from drt import telemetry

    if key == "telemetry.enabled":
        telemetry.unset_enabled()
        console.print("Telemetry preference cleared (default: off).")
        return
    print_error(f"Unknown config key: {key!r}.")
    raise typer.Exit(code=2)


@config_app.command(name="show-telemetry")
def config_show_telemetry() -> None:
    """Print the exact payload that would be sent for the next sync.

    Helps users verify what data leaves their machine before opting in.
    """
    from drt import telemetry

    enabled = telemetry.is_enabled()
    sample = telemetry.build_sync_completed_payload(
        distinct_id="<anonymous-id>",
        sync_mode="<sync.sync.mode>",
        source_type="<profile.type>",
        destination_type="<destination.type>",
        rows_synced=0,
        duration_seconds=0.0,
        status="<success|partial|failed>",
    )
    sample.pop("api_key", None)
    console.print(f"Telemetry enabled: [{'green' if enabled else 'yellow'}]{enabled}[/]")
    console.print("Payload schema (api_key elided):")
    console.print_json(json.dumps(sample))
