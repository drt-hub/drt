"""Tests for `drt docs generate --format html` (P3 of #499)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from drt.docs.html import render_html
from drt.docs.manifest import (
    SCHEMA_VERSION,
    Destination,
    Edge,
    Manifest,
    Project,
    Source,
    Sync,
    SyncStateSnapshot,
)


def _manifest() -> Manifest:
    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-06-28T00:00:00Z",
        project=Project(name="acme", profile="default"),
        sources=[Source(name="default", type="duckdb")],
        destinations=[
            Destination(name="dest_discord_x", type="discord", label="discord (webhook)"),
            Destination(name="dest_pg_users", type="postgres", label="postgres (public.users)"),
        ],
        syncs=[
            Sync(
                name="customers_to_discord",
                source="default",
                destination="dest_discord_x",
                mode="full",
                description="VIP customers to Discord",
                tags=("growth",),
                state=SyncStateSnapshot(
                    last_sync_at="2026-06-27T06:47:32Z",
                    last_cursor_value=None,
                    rows_synced=3,
                    last_status="success",
                    last_error=None,
                ),
            ),
            Sync(
                name="users_to_pg",
                source="default",
                destination="dest_pg_users",
                mode="upsert",
            ),
        ],
        edges=[Edge(kind="source_to_sync", from_="default", to="customers_to_discord")],
    )


@pytest.fixture
def site(tmp_path: Path) -> Path:
    out = tmp_path / "docs"
    render_html(_manifest(), out)
    return out


def test_output_tree_matches_layout(site: Path) -> None:
    for rel in [
        "index.html",
        "dag.html",
        "manifest.json",
        "assets/style.css",
        "assets/app.js",
        "assets/pygments-default.css",
        "sync/customers-to-discord.html",
        "sync/users-to-pg.html",
        "source/default.html",
        "destination/dest-discord-x.html",
        "destination/dest-pg-users.html",
    ]:
        assert (site / rel).exists(), f"missing {rel}"


def test_no_runtime_fetch_anywhere(site: Path) -> None:
    """file:// safety — no fetch() / XHR that would CORS-fail on local open."""
    for f in site.rglob("*.html"):
        assert "fetch(" not in f.read_text(encoding="utf-8")


def test_each_page_inlines_parseable_json(site: Path) -> None:
    for f in site.rglob("*.html"):
        m = re.search(r'id="drt-data">(.*?)</script>', f.read_text(encoding="utf-8"), re.S)
        assert m, f"{f.name} has no inline data block"
        json.loads(m.group(1))  # raises if malformed


def test_manifest_json_still_emitted(site: Path) -> None:
    data = json.loads((site / "manifest.json").read_text(encoding="utf-8"))
    assert data["schema_version"] == SCHEMA_VERSION
    assert len(data["syncs"]) == 2


def test_sync_page_has_highlighted_yaml_and_state(site: Path) -> None:
    html = (site / "sync/customers-to-discord.html").read_text(encoding="utf-8")
    assert 'class="highlight"' in html  # pygments output
    assert "success" in html  # state block
    assert "../source/default.html" in html  # cross-link
    assert "../destination/dest-discord-x.html" in html


def test_dag_uses_mermaid(site: Path) -> None:
    html = (site / "dag.html").read_text(encoding="utf-8")
    assert "mermaid" in html
    assert "graph LR" in html


def test_assets_referenced_with_correct_relative_paths(site: Path) -> None:
    assert 'href="assets/style.css"' in (site / "index.html").read_text(encoding="utf-8")
    sub = (site / "sync/customers-to-discord.html").read_text(encoding="utf-8")
    assert 'href="../assets/style.css"' in sub


def test_empty_project_renders_without_crashing(tmp_path: Path) -> None:
    empty = Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-06-28T00:00:00Z",
        project=Project(name="empty", profile="default"),
        sources=[Source(name="default", type="duckdb")],
    )
    out = tmp_path / "docs"
    render_html(empty, out)
    assert (out / "index.html").exists()
    assert "No syncs found" in (out / "dag.html").read_text(encoding="utf-8")
