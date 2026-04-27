"""drt doctor — environment diagnostics."""

from __future__ import annotations

import importlib
import os
import sys
import warnings
from pathlib import Path

import yaml

from drt import __version__


def _check_python() -> tuple[bool, str]:
    """Check Python version >= 3.10."""
    major, minor = sys.version_info[:2]
    ok = (major, minor) >= (3, 10)
    ver = f"{major}.{minor}.{sys.version_info.micro}"
    return ok, f"{ver} {'✅' if ok else '❌ (need >= 3.10)'}"


def _check_project_file() -> tuple[bool, str, dict[str, object] | None]:
    """Check drt_project.yml exists and loads."""
    path = Path("drt_project.yml")
    if not path.exists():
        return False, "❌ drt_project.yml not found (run `drt init` first)", None
    try:
        data = yaml.safe_load(path.read_text()) or {}
        return True, "✅", data
    except yaml.YAMLError as e:
        return False, f"❌ Invalid YAML: {e}", None


def _check_profile(project_data: dict[str, object] | None) -> tuple[bool, str]:
    """Check profile configuration in ~/.drt/profiles.yml."""
    if not project_data:
        return False, "❌ No project data"

    profile_name = project_data.get("profile", "default")
    profiles_path = Path.home() / ".drt" / "profiles.yml"

    if not profiles_path.exists():
        return False, f"❌ {profiles_path} not found"

    try:
        profiles = yaml.safe_load(profiles_path.read_text()) or {}
        if profile_name in profiles:
            return True, f"✅ {profile_name}"
        if "default" in profiles:
            return True, f"✅ {profile_name} (fallback to default)"
        return False, f"❌ Profile '{profile_name}' not found in {profiles_path}"
    except yaml.YAMLError as e:
        return False, f"❌ Invalid profiles.yml: {e}"


def _check_syncs(project_data: dict[str, object] | None) -> tuple[int, bool, str]:
    """Count and validate sync files."""
    syncs_dir = Path("syncs")
    if not syncs_dir.exists():
        return 0, False, "❌ syncs/ directory not found"

    yaml_files = list(syncs_dir.glob("*.yml")) + list(syncs_dir.glob("*.yaml"))
    count = len(yaml_files)

    if count == 0:
        return 0, True, "⚠️  No sync files found (create one to get started)"

    return count, True, f"✅ {count} sync file{'s' if count != 1 else ''}"


# Optional extras to check (mirrors [project.optional-dependencies] in pyproject.toml).
# Tuple shape: (extra_name, install_hint_package, import_name).
_EXTRAS = [
    ("bigquery",   "google-cloud-bigquery",      "google.cloud.bigquery"),
    ("duckdb",     "duckdb",                     "duckdb"),
    ("postgres",   "psycopg2-binary",            "psycopg2"),
    ("redshift",   "psycopg2-binary",            "psycopg2"),
    ("clickhouse", "clickhouse-connect",         "clickhouse_connect"),
    ("snowflake",  "snowflake-connector-python", "snowflake.connector"),
    ("databricks", "databricks-sql-connector",   "databricks.sql"),
    ("sqlserver",  "pymssql",                    "pymssql"),
    ("sheets",     "google-api-python-client",   "googleapiclient"),
    ("mysql",      "pymysql",                    "pymysql"),
    ("parquet",    "pyarrow",                    "pyarrow"),
    ("mcp",        "fastmcp",                    "fastmcp"),
]


def _check_extras() -> list[tuple[str, bool, str]]:
    """Check which optional extras are installed."""
    # Suppress deprecation warnings during import checks
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        results = []
        for label, _package, import_name in _EXTRAS:
            try:
                mod = importlib.import_module(import_name)
                ver = getattr(mod, "__version__", "installed")
                results.append((label, True, f"✅ {ver}"))
            except (ImportError, ModuleNotFoundError):
                results.append((label, False, f"❌ not installed (pip install drt-core[{label}])"))

        return results


def _check_env_vars(project_data: dict[str, object] | None) -> list[tuple[str, bool, str]]:
    """Check relevant environment variables."""
    common_vars = [
        ("GOOGLE_APPLICATION_CREDENTIALS", False),
        ("DRT_PROFILE", False),
        ("OPENAI_API_KEY", False),
        ("ANTHROPIC_API_KEY", False),
    ]

    results = []
    for var, required in common_vars:
        val = os.environ.get(var)
        if val:
            # Mask sensitive values
            display = val[:4] + "***" if len(val) > 8 else "***"
            results.append((var, True, f"✅ set ({display})"))
        elif required:
            results.append((var, False, "❌ required but not set"))
        else:
            results.append((var, True, "not set (optional)"))

    return results


def run_doctor() -> None:
    """Run all diagnostics and print results."""
    from drt.cli.output import console

    console.print("\n[bold]🩺 drt doctor[/bold] — environment diagnostics\n")

    # Python version
    ok, msg = _check_python()
    console.print(f"[bold]Python version:[/bold] {msg}")

    # drt version
    console.print(f"[bold]drt version:[/bold] {__version__} ✅")

    # Project file
    ok, msg, project_data = _check_project_file()
    console.print(f"[bold]Project file:[/bold] {msg}")

    # Profile
    if project_data:
        ok, msg = _check_profile(project_data)
        console.print(f"[bold]Profile:[/bold] {msg}")
    else:
        console.print("[bold]Profile:[/bold] ⏭️  skipped (no project file)")

    # Syncs
    if project_data:
        count, ok, msg = _check_syncs(project_data)
        console.print(f"[bold]Syncs:[/bold] {msg}")

    # Extras
    console.print("\n[bold]Extras installed:[/bold]")
    extras = _check_extras()
    for label, ok, msg in extras:
        if ok:
            console.print(f"  {label:>15s}: {msg}")

    # Environment variables
    console.print("\n[bold]Environment variables:[/bold]")
    env_vars = _check_env_vars(project_data)
    for var, ok, msg in env_vars:
        console.print(f"  {var:>35s}: {msg}")

    console.print(
        "\n[dim]If issues persist, run `drt doctor` again after fixing the above.[/dim]\n"
    )
