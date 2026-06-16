"""``drt profile`` — manage source credential profiles in ``~/.drt/profiles.yml``.

Most DB tools (psql, bq, snowsql) ship profile-management commands; until now
drt profiles were hand-edited YAML. This sub-app adds:

    drt profile list            # all profiles (name + type)
    drt profile show <name>     # one profile, secrets masked
    drt profile test <name>     # connectivity check (source.test_connection)
    drt profile add <name>      # interactive prompt → write to profiles.yml
    drt profile remove <name>   # delete a profile entry

Secrets are never printed in plain text: ``show`` masks any inline secret
value (drt's design keeps real secrets in env vars / secrets.toml, so the
profile usually only references env-var *names*, which are safe to display).
"""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import console

profile_app = typer.Typer(
    name="profile",
    help="Manage source credential profiles in ~/.drt/profiles.yml.",
    no_args_is_help=True,
)
app.add_typer(profile_app)


# Per-type field prompts for `drt profile add`. Each entry is
# (key, prompt, default). A default of None makes the field required.
# Covers the common source types; others can be hand-added to profiles.yml.
_ADD_FIELD_SPECS: dict[str, list[tuple[str, str, str | None]]] = {
    "bigquery": [
        ("project", "GCP project id", None),
        ("dataset", "BigQuery dataset", None),
        ("location", "Location", "US"),
        ("method", "Auth method (application_default / keyfile)", "application_default"),
    ],
    "duckdb": [("database", "DuckDB file path (or :memory:)", "./warehouse.duckdb")],
    "sqlite": [("database", "SQLite file path (or :memory:)", "./warehouse.db")],
    "postgres": [
        ("host", "Host", "localhost"),
        ("port", "Port", "5432"),
        ("dbname", "Database name", None),
        ("user", "User", None),
        ("password_env", "Env var holding the password", "PGPASSWORD"),
        ("schema", "Schema", "public"),
    ],
    "redshift": [
        ("host", "Host", None),
        ("port", "Port", "5439"),
        ("dbname", "Database name", None),
        ("user", "User", None),
        ("password_env", "Env var holding the password", "REDSHIFT_PASSWORD"),
        ("schema", "Schema", "public"),
    ],
    "mysql": [
        ("host", "Host", "localhost"),
        ("port", "Port", "3306"),
        ("dbname", "Database name", None),
        ("user", "User", None),
        ("password_env", "Env var holding the password", "MYSQL_PASSWORD"),
    ],
    "clickhouse": [
        ("host", "Host", "localhost"),
        ("port", "Port", "8123"),
        ("database", "Database", "default"),
        ("user", "User", "default"),
        ("password_env", "Env var holding the password", "CLICKHOUSE_PASSWORD"),
    ],
    "snowflake": [
        ("account", "Account identifier", None),
        ("user", "User", None),
        ("database", "Database", None),
        ("schema", "Schema", "PUBLIC"),
        ("warehouse", "Warehouse", None),
        ("password_env", "Env var holding the password", "SNOWFLAKE_PASSWORD"),
    ],
}

# Keys whose VALUE should be masked in `show`. Env-var-name keys (``*_env``)
# and file paths (``keyfile``) are safe to display — they're not the secret.
_SECRET_KEY_HINTS = ("password", "token", "secret", "api_key", "account_sid")


def _is_secret_key(key: str) -> bool:
    if key.endswith("_env") or key == "keyfile":
        return False
    return any(hint in key.lower() for hint in _SECRET_KEY_HINTS)


def _mask(value: object) -> str:
    # Full mask — never reveal a prefix. Inline secret values are rare (drt
    # keeps real secrets in env vars), but if one is present we don't want to
    # leak even a recognisable prefix of it.
    return "***"


@profile_app.command("list")
def profile_list() -> None:
    """List all profiles in ~/.drt/profiles.yml (name + source type)."""
    from rich.table import Table

    from drt.config.credentials import load_raw_profiles

    profiles = load_raw_profiles()
    if not profiles:
        console.print(
            "[dim]No profiles found. Run `drt init` or `drt profile add <name>`.[/dim]"
        )
        return

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Profile")
    table.add_column("Type")
    for name, raw in profiles.items():
        ptype = raw.get("type", "[red]?[/red]") if isinstance(raw, dict) else "[red]?[/red]"
        table.add_row(name, str(ptype))
    console.print(table)


@profile_app.command("show")
def profile_show(name: str = typer.Argument(..., help="Profile name to display.")) -> None:
    """Print a profile with any inline secret values masked."""
    from drt.config.credentials import load_raw_profiles

    profiles = load_raw_profiles()
    raw = profiles.get(name)
    if not isinstance(raw, dict):
        console.print(f"[red]Profile '{name}' not found.[/red]")
        available = ", ".join(profiles.keys()) or "(none)"
        console.print(f"[dim]Available: {available}[/dim]")
        raise typer.Exit(1)

    console.print(f"\n[bold]{name}[/bold]")
    for key, value in raw.items():
        shown = _mask(value) if _is_secret_key(key) else value
        console.print(f"  {key}: {shown}")
    console.print()


@profile_app.command("test")
def profile_test(name: str = typer.Argument(..., help="Profile name to test.")) -> None:
    """Verify connectivity for a profile (runs the source's connection check)."""
    from drt.config.credentials import load_profile
    from drt.connectors.registry import get_source

    try:
        profile = load_profile(name)
    except (FileNotFoundError, KeyError, ValueError) as e:
        console.print(f"[red]✗ {e}[/red]")
        raise typer.Exit(1) from e

    source = get_source(profile)
    console.print(f"Testing profile [bold]{name}[/bold] ({profile.type})…")
    try:
        ok = source.test_connection(profile)
    except Exception as e:
        console.print(f"[red]✗ connection failed: {e}[/red]")
        raise typer.Exit(1) from e

    if ok:
        console.print("[green]✓ connection OK[/green]")
    else:
        console.print("[red]✗ connection check returned false[/red]")
        raise typer.Exit(1)


@profile_app.command("remove")
def profile_remove(
    name: str = typer.Argument(..., help="Profile name to remove."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Delete a profile entry from ~/.drt/profiles.yml."""
    from drt.config.credentials import remove_profile

    if not yes and not typer.confirm(f"Remove profile '{name}'?"):
        console.print("[dim]Aborted.[/dim]")
        raise typer.Exit(0)

    try:
        path = remove_profile(name)
    except (FileNotFoundError, KeyError) as e:
        console.print(f"[red]✗ {e}[/red]")
        raise typer.Exit(1) from e

    console.print(f"[green]✓ Removed '{name}' from {path}[/green]")


@profile_app.command("add")
def profile_add(name: str = typer.Argument(..., help="Profile name to create.")) -> None:
    """Interactively add a profile to ~/.drt/profiles.yml.

    Prompts for the source type and the fields that type needs. Supported
    types: bigquery / duckdb / sqlite / postgres / redshift / mysql /
    clickhouse / snowflake. Other types can be hand-added to profiles.yml.
    """
    from drt.config.credentials import load_raw_profiles, write_raw_profile

    existing = load_raw_profiles()
    if name in existing and not typer.confirm(f"Profile '{name}' exists. Overwrite?"):
        console.print("[dim]Aborted.[/dim]")
        raise typer.Exit(0)

    types = ", ".join(_ADD_FIELD_SPECS.keys())
    ptype = typer.prompt(f"Source type ({types})").strip().lower()
    spec = _ADD_FIELD_SPECS.get(ptype)
    if spec is None:
        console.print(
            f"[red]Unsupported type '{ptype}'.[/red] "
            f"[dim]Hand-add it to ~/.drt/profiles.yml; supported here: {types}.[/dim]"
        )
        raise typer.Exit(1)

    entry: dict[str, object] = {"type": ptype}
    for key, prompt_text, default in spec:
        if default is None:
            value = typer.prompt(prompt_text)
        else:
            value = typer.prompt(prompt_text, default=default)
        # Coerce obvious integers (ports) so the YAML stores them as ints.
        if key == "port":
            entry[key] = int(value)
        else:
            entry[key] = value

    path = write_raw_profile(name, entry, None)
    console.print(f"[green]✓ Wrote profile '{name}' to {path}[/green]")
    console.print(f"[dim]Verify it with: drt profile test {name}[/dim]")
