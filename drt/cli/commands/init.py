"""`drt init` — initialize a new drt project.

Four entry modes:

- ``drt init`` — interactive wizard (asks for source type, profile, etc.)
- ``drt init --template <name>`` — scaffold from a curated static template
- ``drt init --from-dbt <manifest.json>`` — generate sync YAMLs from
  a dbt project's manifest
- ``drt init --from-ossie <model.yaml>`` — generate sync YAMLs from
  an Apache Ossie semantic model

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
    from_ossie: str = typer.Option(
        None,
        "--from-ossie",
        help="Path to an Apache Ossie YAML/JSON model — generate sync YAMLs from datasets.",
    ),
    dataset: list[str] = typer.Option(
        None,
        "--dataset",
        help="Ossie dataset name to scaffold. Repeat to select multiple; default: all with keys.",
    ),
    destination: str = typer.Option(
        "postgres",
        "--destination",
        help="Destination scaffold for --from-ossie: postgres | hubspot.",
    ),
    dialect: str = typer.Option(
        "ANSI_SQL",
        "--dialect",
        help="Preferred Ossie field-expression dialect; falls back to ANSI_SQL.",
    ),
    output_dir: Path = typer.Option(
        Path("syncs"),
        "--output-dir",
        help="Directory for --from-ossie generated sync YAMLs.",
    ),
    template: str = typer.Option(
        None,
        "--template",
        help="Scaffold from a curated template. Use 'list' to see available templates.",
    ),
) -> None:
    """Initialize a new drt project in the current directory."""
    if from_dbt and from_ossie:
        print_error("Choose only one of --from-dbt or --from-ossie.")
        raise typer.Exit(1)

    if from_ossie:
        _init_from_ossie(
            Path(from_ossie),
            dataset_names=dataset,
            destination=destination,
            dialect=dialect,
            output_dir=output_dir,
        )
        return

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
        project_file.write_text("name: my_drt_project\nprofile: default\n")
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


def _init_from_ossie(
    model_path: Path,
    *,
    dataset_names: list[str],
    destination: str,
    dialect: str,
    output_dir: Path,
) -> None:
    """Generate sync YAML scaffolds from an Apache Ossie model document."""
    from drt.integrations.ossie import OssieModelError, build_ossie_scaffold, load_ossie_model

    try:
        model = load_ossie_model(model_path)
        by_name = {item.name: item for item in model.datasets}
        if dataset_names:
            unknown = sorted(set(dataset_names) - by_name.keys())
            if unknown:
                available = ", ".join(by_name)
                raise OssieModelError(
                    f"Unknown Ossie dataset(s): {', '.join(unknown)}. Available: {available}."
                )
            selected = [by_name[name] for name in dict.fromkeys(dataset_names)]
        else:
            selected = list(model.datasets)

        scaffolds = []
        for item in selected:
            if not item.primary_key:
                continue
            scaffolds.append(
                build_ossie_scaffold(
                    model,
                    item,
                    destination=destination,
                    dialect=dialect,
                )
            )

        filenames: dict[str, str] = {}
        for scaffold in scaffolds:
            folded = scaffold.filename.casefold()
            previous = filenames.get(folded)
            if previous is not None:
                raise OssieModelError(
                    "Selected Ossie datasets produce case-insensitively colliding output "
                    f"paths: {previous!r} and {scaffold.filename!r}. Rename one dataset "
                    "before generating files."
                )
            filenames[folded] = scaffold.filename
    except (FileNotFoundError, OssieModelError) as exc:
        print_error(str(exc))
        raise typer.Exit(1)

    version = model.version or "unspecified"
    console.print(
        f"\n[bold]Found {len(model.datasets)} datasets in Apache Ossie model "
        f"{model.name!r} (version {version}):[/bold]\n"
    )
    for warning in model.warnings:
        console.print(f"[yellow]Warning:[/yellow] {warning}")
    for item in selected:
        if item.primary_key:
            console.print(f"  [cyan]select[/cyan] {item.name}")
        else:
            console.print(f"  [dim]skip[/dim] {item.name} (no primary_key)")

    output_dir.mkdir(parents=True, exist_ok=True)
    created: list[str] = []
    for scaffold in scaffolds:
        path = output_dir / scaffold.filename
        if path.exists():
            console.print(f"  [dim]skip[/dim] {path} (already exists)")
            continue
        path.write_text(scaffold.yaml_text)
        created.append(str(path))
        if scaffold.ansi_fallback_fields:
            fields = ", ".join(scaffold.ansi_fallback_fields)
            console.print(
                f"  [dim]{scaffold.dataset_name}: used ANSI_SQL fallback for {fields}[/dim]"
            )
        if scaffold.implicit_primary_key_fields:
            fields = ", ".join(scaffold.implicit_primary_key_fields)
            console.print(
                f"  [dim]{scaffold.dataset_name}: added undeclared primary-key "
                f"column(s) directly: {fields}[/dim]"
            )

    if model.metrics:
        metrics = ", ".join(metric.name for metric in model.metrics)
        console.print(
            "\n[yellow]Ossie metrics available but not generated "
            f"(entity grain unclear):[/yellow] {metrics}"
        )

    if created:
        console.print(f"\n[green]Created {len(created)} sync file(s):[/green]")
        for created_path in created:
            console.print(f"  {created_path}")
        console.print(
            "\n[dim]Resolve the TODOs in each file, then run: "
            "drt validate && drt run --dry-run[/dim]"
        )
    else:
        console.print("[dim]No new sync files created.[/dim]")
