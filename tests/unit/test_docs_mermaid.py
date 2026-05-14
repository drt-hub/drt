"""Tests for `drt docs generate --format mermaid` (P1 of #499)."""

from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.docs.builder import build_manifest
from drt.docs.manifest import (
    SCHEMA_VERSION,
    Destination,
    Edge,
    Manifest,
    Source,
    Sync,
)
from drt.docs.mermaid import render_mermaid

runner = CliRunner()


def _manifest(*, syncs=None, sources=None, destinations=None, edges=None) -> Manifest:
    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="test",
        syncs=list(syncs or []),
        sources=list(sources or []),
        destinations=list(destinations or []),
        edges=list(edges or []),
    )


def _write_project(project_dir: Path, profile: str = "bigquery_prod") -> None:
    (project_dir / "drt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "demo",
                "version": "0.1",
                "profile": profile,
                "source": {"type": "bigquery"},
            }
        )
    )


def _write_sync(project_dir: Path, filename: str, sync: dict[str, object]) -> None:
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    (syncs_dir / filename).write_text(yaml.safe_dump(sync))


def _postgres_sync(
    name: str,
    table: str,
    lookups: dict[str, object] | None = None,
) -> dict[str, object]:
    destination: dict[str, object] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "warehouse",
        "table": table,
        "upsert_key": ["id"],
    }
    if lookups:
        destination["lookups"] = lookups
    return {
        "name": name,
        "model": f"SELECT * FROM {name}",
        "destination": destination,
        "sync": {"mode": "upsert"},
    }


class TestRenderMermaid:
    def test_empty_project_renders_placeholder(self) -> None:
        out = render_mermaid(_manifest())
        assert out.startswith("graph LR")
        assert "No syncs found" in out

    def test_single_sync_has_three_subgraphs_and_two_edges(self) -> None:
        src = Source(name="bq_prod", type="bigquery")
        dst = Destination(name="dest_hubspot_hubspot__main", type="hubspot", label="hubspot (main)")
        sync = Sync(
            name="users_to_hubspot",
            source="bq_prod",
            destination=dst.name,
            mode="upsert",
        )
        m = _manifest(
            sources=[src],
            destinations=[dst],
            syncs=[sync],
            edges=[
                Edge(kind="source_to_sync", from_=src.name, to=sync.name),
                Edge(kind="sync_to_destination", from_=sync.name, to=dst.name),
            ],
        )
        out = render_mermaid(m)

        assert "subgraph Sources" in out
        assert "subgraph Syncs" in out
        assert "subgraph Destinations" in out
        assert "-->|extract|" in out
        assert "-->|load|" in out
        assert "upsert" in out
        assert "bigquery" in out

    def test_shared_source_between_syncs(self) -> None:
        src = Source(name="bq", type="bigquery")
        dst1 = Destination(name="dest_slack_a", type="slack", label="slack (#a)")
        dst2 = Destination(name="dest_slack_b", type="slack", label="slack (#b)")
        s1 = Sync(name="alpha", source="bq", destination=dst1.name, mode="full")
        s2 = Sync(name="beta", source="bq", destination=dst2.name, mode="full")
        m = _manifest(
            sources=[src],
            destinations=[dst1, dst2],
            syncs=[s1, s2],
            edges=[
                Edge(kind="source_to_sync", from_="bq", to="alpha"),
                Edge(kind="source_to_sync", from_="bq", to="beta"),
                Edge(kind="sync_to_destination", from_="alpha", to=dst1.name),
                Edge(kind="sync_to_destination", from_="beta", to=dst2.name),
            ],
        )
        out = render_mermaid(m)
        assert out.count("src_bq[") == 1
        assert out.count("-->|extract|") == 2

    def test_lookup_edge_uses_dashed_arrow(self) -> None:
        src = Source(name="bq", type="bigquery")
        dst1 = Destination(name="dest_pg_users", type="postgres", label="postgres (users)")
        dst2 = Destination(name="dest_pg_orders", type="postgres", label="postgres (orders)")
        s_users = Sync(name="users", source="bq", destination=dst1.name, mode="upsert")
        s_orders = Sync(name="orders", source="bq", destination=dst2.name, mode="upsert")
        m = _manifest(
            sources=[src],
            destinations=[dst1, dst2],
            syncs=[s_users, s_orders],
            edges=[
                Edge(kind="source_to_sync", from_="bq", to="users"),
                Edge(kind="source_to_sync", from_="bq", to="orders"),
                Edge(kind="sync_to_destination", from_="users", to=dst1.name),
                Edge(kind="sync_to_destination", from_="orders", to=dst2.name),
                Edge(kind="lookup", from_="users", to="orders"),
            ],
        )
        out = render_mermaid(m)
        assert "-.lookup.->" in out

    def test_destination_label_with_quotes_is_escaped(self) -> None:
        dst = Destination(
            name="dest_slack_with_quotes",
            type="slack",
            label='slack (#"weird")',
        )
        out = render_mermaid(
            _manifest(
                destinations=[dst],
                syncs=[Sync(name="x", source="s", destination=dst.name, mode="full")],
                sources=[Source(name="s", type="bq")],
            )
        )
        assert "&quot;" in out


class TestBuildManifest:
    def test_lookup_edge_matches_unqualified_table_alias(self, tmp_path: Path) -> None:
        _write_project(tmp_path, profile="postgres_replica")
        _write_sync(tmp_path, "users.yml", _postgres_sync("users", "public.users"))
        _write_sync(
            tmp_path,
            "accounts.yml",
            _postgres_sync(
                "accounts",
                "public.accounts",
                lookups={
                    "user_id": {
                        "table": "users",
                        "match": {"id": "user_id"},
                        "select": "id",
                    }
                },
            ),
        )

        manifest = build_manifest(tmp_path)

        assert Edge(kind="lookup", from_="users", to="accounts") in manifest.edges
        assert "sync_users -.lookup.-> sync_accounts" in render_mermaid(manifest)


class TestDocsGenerateCLI:
    def test_help_lists_generate_and_serve(self) -> None:
        result = runner.invoke(app, ["docs", "--help"])
        assert result.exit_code == 0
        assert "generate" in result.output
        assert "serve" in result.output

    def test_serve_raises_not_implemented(self) -> None:
        result = runner.invoke(app, ["docs", "serve"])
        # Typer surfaces uncaught exceptions; we just need a non-zero exit + message.
        assert result.exit_code != 0
        assert "v0.8.x" in str(result.exception) or "v0.8.x" in (result.output or "")

    def test_generate_html_not_yet_implemented(self) -> None:
        result = runner.invoke(app, ["docs", "generate", "--format", "html"])
        assert result.exit_code != 0
        assert "P3" in str(result.exception) or "P3" in (result.output or "")

    def test_generate_unknown_format_is_bad_param(self) -> None:
        result = runner.invoke(app, ["docs", "generate", "--format", "xml"])
        assert result.exit_code != 0

    def test_generate_mermaid_on_empty_project_prints_placeholder(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "drt_project.yml").write_text("name: empty\nprofile: default\n")
        result = runner.invoke(app, ["docs", "generate", "--format", "mermaid"])
        assert result.exit_code == 0
        assert "graph LR" in result.output
        assert "No syncs found" in result.output
