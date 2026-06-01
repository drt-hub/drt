"""Curated starter templates for ``drt init --template <name>`` (#545).

Each template is a static, self-contained sync YAML that lands in
``syncs/<name>.yml`` and passes ``drt validate`` immediately. Per-
template next-steps text tells the user exactly which env vars / source
tables they need to set up before ``drt run`` will actually transmit
data.

Templates are deliberately *not* Jinja2-rendered at init time — the
file on disk is the file the user gets. Edits are then the user's
domain. This keeps the scaffolding step boring and predictable.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


@dataclass(frozen=True)
class TemplateInfo:
    """One curated sync template registered for ``drt init --template``."""

    name: str
    description: str
    next_steps: tuple[str, ...]

    def read_yaml(self) -> str:
        """Load the template's YAML content from the packaged templates dir.

        Anchored at ``drt.cli`` so we don't depend on ``templates/`` being a
        package — it's a data directory included via hatch's wheel target.
        """
        # ``Traversable.joinpath`` only accepts one segment per call (per typing
        # stubs); chain to walk into the syncs subdir.
        return (
            files("drt.cli")
            .joinpath("templates")
            .joinpath("syncs")
            .joinpath(f"{self.name}.yml")
            .read_text()
        )


TEMPLATES: dict[str, TemplateInfo] = {
    "duckdb_to_rest": TemplateInfo(
        name="duckdb_to_rest",
        description="DuckDB → REST API POST (httpbin.org for testing — no accounts needed)",
        next_steps=(
            "No env vars required — works out of the box once you have a DuckDB table.",
            'Seed sample data: `python -c "import duckdb; '
            "c=duckdb.connect('warehouse.duckdb'); "
            "c.execute('CREATE TABLE users AS SELECT * FROM "
            "(VALUES (1,\\'a\\',\\'a@x.com\\'),(2,\\'b\\',\\'b@x.com\\')) t(id,name,email)')\"`",
            "Dry-run: `drt run --dry-run`",
            "Run for real: `drt run`",
        ),
    ),
    "postgres_to_slack": TemplateInfo(
        name="postgres_to_slack",
        description="PostgreSQL → Slack webhook (operational alerts)",
        next_steps=(
            "Set the Slack webhook env var: `export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...`",
            "Configure a `postgres` profile in ~/.drt/profiles.yml — see `drt sources --detailed`",
            "Make sure your DB has an `alerts` table "
            "(or edit `model: ref('alerts')` in the sync file)",
            "Dry-run: `drt run --dry-run`",
        ),
    ),
    "duckdb_to_hubspot": TemplateInfo(
        name="duckdb_to_hubspot",
        description="DuckDB → HubSpot contacts upsert",
        next_steps=(
            "Create a HubSpot private app token and: `export HUBSPOT_TOKEN=pat-...`",
            "Seed a DuckDB `contacts` table with at least: email, firstname, lastname",
            "Dry-run: `drt run --dry-run`",
        ),
    ),
}


def write_template(name: str, project_dir: Path) -> Path:
    """Write the template's YAML to ``<project_dir>/syncs/<name>.yml``.

    Raises ``KeyError`` if ``name`` is not a registered template (let the
    CLI handler turn that into a user-facing error with the available
    list).
    """
    tmpl = TEMPLATES[name]
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    out_path = syncs_dir / f"{name}.yml"
    out_path.write_text(tmpl.read_yaml())
    return out_path
