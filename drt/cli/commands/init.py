"""`drt init` — initialize a new drt project.

Three entry modes:

- ``drt init`` — interactive wizard (asks for source type, profile, etc.)
- ``drt init --template <name>`` — scaffold from a curated static template
- ``drt init --from-dbt <manifest.json>`` — generate sync YAMLs from
  a dbt project's manifest

Each mode is implemented as a private helper called from the single
``init`` Typer command.
"""

from __future__ import annotations

from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error, print_init_success


@app.command()
def init(
    from_dbt: str = typer.Option(
        None,
        "--from-dbt",
        help="Path to dbt manifest.json — generate sync YAMLs from dbt models.",
    ),
    template: str = typer.Option(
        None,
        "--template",
        help="Scaffold from a curated template. Use 'list' to see available templates.",
    ),
) -> None:
    """Initialize a new drt project in the current directory."""
    if from_dbt:
        _init_from_dbt(Path(from_dbt))
        return

    if template == "list":
        _list_templates()
        return
    if template:
        _init_from_template(template, Path("."))
        return

    from drt.cli.init_wizard import run_wizard, scaffold_project

    try:
        answers = run_wizard()
        created = scaffold_project(answers, Path("."))
        print_init_success(created)
    except (KeyboardInterrupt, typer.Abort):
        console.print("\n[dim]Aborted.[/dim]")
        raise typer.Exit(1)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(1)


def _list_templates() -> None:
    """Print available ``drt init --template`` choices."""
    from drt.cli._init_templates import TEMPLATES

    console.print("\n[bold]Available templates:[/bold]\n")
    for name, info in TEMPLATES.items():
        console.print(f"  [cyan]{name}[/cyan] — {info.description}")
    console.print("\nUse: [bold]drt init --template <name>[/bold]\n")


def _init_from_template(name: str, project_dir: Path) -> None:
    """Scaffold a project from a curated template and print next steps."""
    from drt.cli._init_templates import TEMPLATES, write_template

    if name not in TEMPLATES:
        print_error(f"Unknown template: {name!r}")
        console.print("Run [bold]drt init --template list[/bold] to see available templates.")
        raise typer.Exit(1)

    # Ensure minimal project shell exists so `drt validate` / `drt run` work.
    created: list[str] = []
    project_file = project_dir / "drt_project.yml"
    if not project_file.exists():
        project_file.write_text(
            "name: my_drt_project\nprofile: default\n"
        )
        created.append(str(project_file))

    drt_dir = project_dir / ".drt"
    drt_dir.mkdir(exist_ok=True)
    drt_gitignore = drt_dir / ".gitignore"
    if not drt_gitignore.exists():
        drt_gitignore.write_text("*\n")
        created.append(str(drt_gitignore))

    sync_path = write_template(name, project_dir)
    created.append(str(sync_path))

    print_init_success(created)

    info = TEMPLATES[name]
    console.print(f"\n[bold]Next steps for '{name}':[/bold]")
    for i, step in enumerate(info.next_steps, 1):
        console.print(f"  {i}. {step}")
    console.print()


def _init_from_dbt(manifest_path: Path) -> None:
    """Generate sync YAML scaffolds from dbt manifest.json."""
    import yaml

    from drt.integrations.dbt import list_models_from_manifest

    try:
        models = list_models_from_manifest(manifest_path)
    except FileNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(1)

    if not models:
        console.print("[dim]No models found in manifest.[/dim]")
        return

    console.print(f"\n[bold]Found {len(models)} dbt models:[/bold]\n")
    for i, m in enumerate(models):
        desc = f" — {m.description}" if m.description else ""
        console.print(f"  {i + 1}. {m.name}{desc}")

    console.print("")
    raw = typer.prompt(
        "Select models (comma-separated numbers, or 'all')",
        default="all",
    )

    if raw.strip().lower() == "all":
        selected = models
    else:
        indices = [int(x.strip()) - 1 for x in raw.split(",") if x.strip().isdigit()]
        selected = [models[i] for i in indices if 0 <= i < len(models)]

    if not selected:
        console.print("[dim]No models selected.[/dim]")
        return

    syncs_dir = Path(".") / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    created: list[str] = []

    for model in selected:
        sync_data = {
            "name": f"sync_{model.name}",
            "description": model.description or f"Sync {model.name} to destination",
            "model": f"ref('{model.name}')",
            "destination": {
                "type": "rest_api",
                "url": "https://example.com/api",
                "method": "POST",
            },
        }
        path = syncs_dir / f"sync_{model.name}.yml"
        if path.exists():
            console.print(f"  [dim]skip[/dim] {path} (already exists)")
            continue
        with path.open("w") as f:
            yaml.dump(sync_data, f, default_flow_style=False, sort_keys=False)
        created.append(str(path))

    if created:
        console.print(f"\n[green]Created {len(created)} sync file(s):[/green]")
        for c in created:
            console.print(f"  {c}")
        console.print(
            "\n[dim]Edit the destination config in each file,"
            " then run: drt validate && drt run --dry-run[/dim]"
        )
    else:
        console.print("[dim]No new sync files created.[/dim]")
