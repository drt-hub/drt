"""`drt docs generate` / `drt docs serve` — sync catalog & lineage UI (epic #499)."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error

docs_app = typer.Typer(
    name="docs",
    help="Generate or serve the project's sync catalog.",
    no_args_is_help=True,
)
app.add_typer(docs_app)


@docs_app.command(name="generate")
def docs_generate(
    output: Path = typer.Option(
        Path("target/docs"), "--output", "-o", help="Output directory."
    ),
    format: str = typer.Option(
        "html", "--format", "-f", help="Output format: html | mermaid | json."
    ),
    no_state: bool = typer.Option(
        False, "--no-state", help="Exclude per-sync run state from the manifest."
    ),
) -> None:
    """Generate the project's sync catalog (P1 mermaid + P2 json)."""
    from drt.docs.builder import build_manifest
    from drt.docs.mermaid import render_mermaid

    fmt = format.lower()
    include_state = not no_state

    if fmt == "mermaid":
        manifest = build_manifest(Path("."), include_state=include_state)
        print(render_mermaid(manifest))
        return

    if fmt == "json":
        manifest = build_manifest(Path("."), include_state=include_state)
        output.mkdir(parents=True, exist_ok=True)
        manifest_path = output / "manifest.json"
        with manifest_path.open("w") as f:
            json.dump(manifest.to_dict(), f, indent=2)
        console.print(
            f"Wrote [bold]{manifest_path}[/bold] "
            f"({len(manifest.syncs)} sync(s), schema_version={manifest.schema_version})"
        )
        return

    if fmt == "html":
        try:
            from drt.docs.html import render_html
        except ImportError:
            print_error("HTML docs generation requires: pip install drt-core[docs]")
            raise typer.Exit(1)

        manifest = build_manifest(Path("."), include_state=include_state)
        try:
            written = render_html(manifest, output)
        except ValueError as e:
            print_error(str(e))
            raise typer.Exit(1) from e
        console.print(
            f"Wrote [bold]{len(written)}[/bold] file(s) to [bold]{output}[/bold] "
            f"({len(manifest.syncs)} sync(s)). Open [bold]{output / 'index.html'}[/bold]."
        )
        return

    raise typer.BadParameter(
        f"Unknown --format value: {format!r}. Expected: html | mermaid | json."
    )


@docs_app.command(name="serve")
def docs_serve() -> None:
    """Live Web UI for the sync catalog (scheduled for v0.8.x — epic #499)."""
    raise NotImplementedError(
        "`drt docs serve` is scheduled for v0.8.x (Phase 4 of epic #499). "
        "Use `drt docs generate --format mermaid` in the meantime."
    )
