"""Interactive drt init wizard — dbt/dlt style.

Prompts the user for project settings, then scaffolds:
  - drt_project.yml
  - syncs/example_sync.yml
  - .drt/.gitignore
  - ~/.drt/profiles.yml  (appended/created)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import typer
from jinja2 import Environment, PackageLoader

from drt.config.credentials import (
    BigQueryProfile,
    ClickHouseProfile,
    DuckDBProfile,
    PostgresProfile,
    ProfileConfig,
    RedshiftProfile,
    SQLiteProfile,
    save_profile,
)


@dataclass
class InitAnswers:
    project_name: str
    profile_name: str
    source_type: Literal["bigquery", "duckdb", "sqlite", "postgres", "redshift", "clickhouse"]
    # BigQuery
    gcp_project: str = ""
    dataset: str = ""
    location: str = "US"
    auth_method: Literal["application_default", "keyfile"] = "application_default"
    keyfile: str | None = None
    # DuckDB
    duckdb_database: str = ":memory:"
    # SQLite
    sqlite_database: str = "./data/local.db"
    # Postgres
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_dbname: str = ""
    pg_user: str = ""
    pg_password_env: str = "PG_PASSWORD"
    # Redshift
    rs_host: str = ""
    rs_port: int = 5439
    rs_dbname: str = ""
    rs_user: str = ""
    rs_password_env: str = "REDSHIFT_PASSWORD"
    rs_schema: str = "public"
    # ClickHouse
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_database: str = "default"
    ch_user: str = "default"
    ch_password_env: str = "CLICKHOUSE_PASSWORD"


def run_wizard() -> InitAnswers:
    """Run interactive prompts and return structured answers."""
    typer.echo("")
    typer.echo("  Welcome to drt! Let's set up your project.")
    typer.echo("")

    project_name = typer.prompt("  Project name", default=Path.cwd().name)
    profile_name = typer.prompt("  Profile name", default="dev")
    raw_source = typer.prompt(
        "  Source type [bigquery/duckdb/sqlite/postgres/redshift/clickhouse]",
        default="bigquery",
    )
    _valid = ("bigquery", "duckdb", "sqlite", "postgres", "redshift", "clickhouse")
    source_type: Literal["bigquery", "duckdb", "sqlite", "postgres", "redshift", "clickhouse"] = (
        raw_source if raw_source in _valid else "bigquery"
    )

    answers = InitAnswers(
        project_name=project_name,
        profile_name=profile_name,
        source_type=source_type,
    )

    if source_type == "bigquery":
        answers.gcp_project = typer.prompt("  GCP project ID")
        answers.dataset = typer.prompt("  BigQuery dataset")
        answers.location = typer.prompt("  Dataset location", default="US")
        raw_method = typer.prompt(
            "  Auth method [application_default/keyfile]",
            default="application_default",
        )
        answers.auth_method = "keyfile" if raw_method == "keyfile" else "application_default"
        if answers.auth_method == "keyfile":
            answers.keyfile = typer.prompt(
                "  Path to service account keyfile",
                default="~/.config/gcloud/service_account.json",
            )

    elif source_type == "duckdb":
        answers.duckdb_database = typer.prompt(
            "  DuckDB database path (:memory: for in-memory)",
            default=":memory:",
        )

    elif source_type == "sqlite":
        answers.sqlite_database = typer.prompt(
            "  SQLite database path",
            default="./data/local.db",
        )

    elif source_type == "postgres":
        answers.pg_host = typer.prompt("  Host", default="localhost")
        answers.pg_port = int(typer.prompt("  Port", default="5432"))
        answers.pg_dbname = typer.prompt("  Database name")
        answers.pg_user = typer.prompt("  User")
        answers.pg_password_env = typer.prompt("  Env var for password", default="PG_PASSWORD")

    elif source_type == "redshift":
        answers.rs_host = typer.prompt(
            "  Host (e.g., my-cluster.xxx.us-east-1.redshift.amazonaws.com)"
        )
        answers.rs_port = int(typer.prompt("  Port", default="5439"))
        answers.rs_dbname = typer.prompt("  Database name")
        answers.rs_user = typer.prompt("  User")
        answers.rs_password_env = typer.prompt(
            "  Env var for password", default="REDSHIFT_PASSWORD"
        )
        answers.rs_schema = typer.prompt("  Schema", default="public")

    elif source_type == "clickhouse":
        answers.ch_host = typer.prompt("  Host", default="localhost")
        answers.ch_port = int(typer.prompt("  Port", default="8123"))
        answers.ch_database = typer.prompt("  Database", default="default")
        answers.ch_user = typer.prompt("  User", default="default")
        answers.ch_password_env = typer.prompt(
            "  Env var for password", default="CLICKHOUSE_PASSWORD"
        )

    typer.echo("")
    return answers


def scaffold_project(answers: InitAnswers, project_dir: Path) -> list[str]:
    """Create project files and update ~/.drt/profiles.yml.

    Returns list of created file path strings (for display).
    """
    env = Environment(
        loader=PackageLoader("drt.cli", "templates"),
        keep_trailing_newline=True,
    )
    created: list[str] = []

    # --- drt_project.yml ---
    project_file = project_dir / "drt_project.yml"
    if not project_file.exists():
        tmpl = env.get_template("drt_project.yml.j2")
        project_file.write_text(
            tmpl.render(
                project_name=answers.project_name,
                profile_name=answers.profile_name,
            )
        )
        created.append(str(project_file))

    # --- syncs/ + example_sync.yml ---
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    example_sync = syncs_dir / "example_sync.yml"
    if not example_sync.exists():
        tmpl = env.get_template("example_sync.yml.j2")
        example_sync.write_text(tmpl.render(dataset=answers.dataset or "your_dataset"))
        created.append(str(example_sync))

    # --- .drt/.gitignore (keep state out of git) ---
    drt_dir = project_dir / ".drt"
    drt_dir.mkdir(exist_ok=True)
    drt_gitignore = drt_dir / ".gitignore"
    if not drt_gitignore.exists():
        drt_gitignore.write_text("*\n")
        created.append(str(drt_gitignore))

    # --- ~/.drt/profiles.yml ---
    profile: ProfileConfig
    if answers.source_type == "bigquery":
        profile = BigQueryProfile(
            type="bigquery",
            project=answers.gcp_project,
            dataset=answers.dataset,
            method=answers.auth_method,
            keyfile=answers.keyfile,
        )
    elif answers.source_type == "duckdb":
        profile = DuckDBProfile(
            type="duckdb",
            database=answers.duckdb_database,
        )
    elif answers.source_type == "sqlite":
        profile = SQLiteProfile(
            type="sqlite",
            database=answers.sqlite_database,
        )
    elif answers.source_type == "postgres":
        profile = PostgresProfile(
            type="postgres",
            host=answers.pg_host,
            port=answers.pg_port,
            dbname=answers.pg_dbname,
            user=answers.pg_user,
            password_env=answers.pg_password_env,
        )
    elif answers.source_type == "redshift":
        profile = RedshiftProfile(
            type="redshift",
            host=answers.rs_host,
            port=answers.rs_port,
            dbname=answers.rs_dbname,
            user=answers.rs_user,
            password_env=answers.rs_password_env,
            schema=answers.rs_schema,
        )
    else:
        profile = ClickHouseProfile(
            type="clickhouse",
            host=answers.ch_host,
            port=answers.ch_port,
            database=answers.ch_database,
            user=answers.ch_user,
            password_env=answers.ch_password_env,
        )
    profiles_path = save_profile(answers.profile_name, profile)
    created.append(str(profiles_path))

    return created
