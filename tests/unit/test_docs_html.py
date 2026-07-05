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


def _hostile() -> str:
    return "x</script><script>alert(1)</script>&<img src=y onerror=alert(2)>"


def test_manifest_strings_are_escaped_no_xss(tmp_path: Path) -> None:
    """Hostile names/description/error must not produce executable markup."""
    bad = _hostile()
    manifest = Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-06-28T00:00:00Z",
        project=Project(name=bad, profile="default"),
        sources=[Source(name="default", type="duckdb")],
        destinations=[Destination(name="dest_x", type="discord", label=bad)],
        syncs=[
            Sync(
                name=bad,
                source="default",
                destination="dest_x",
                mode="full",
                description=bad,
                tags=(bad,),
                state=SyncStateSnapshot(
                    last_sync_at="2026-06-27T00:00:00Z",
                    last_cursor_value=None,
                    rows_synced=0,
                    last_status="failed",
                    last_error=bad,
                ),
            )
        ],
    )
    out = tmp_path / "docs"
    render_html(manifest, out)
    for f in out.rglob("*.html"):
        text = f.read_text(encoding="utf-8")
        # No un-escaped hostile <script> anywhere in the markup.
        assert "<script>alert(1)</script>" not in text
        # The inline JSON island must not be broken out of by a "</script>".
        m = re.search(r'id="drt-data">(.*?)</script>', text, re.S)
        if m:
            assert "</script>" not in m.group(1)
            json.loads(m.group(1))  # still valid JSON


def test_removed_sync_leaves_no_orphan_page(tmp_path: Path) -> None:
    """Re-rendering with a sync removed must not leave its old page behind."""
    out = tmp_path / "docs"
    render_html(_manifest(), out)  # two syncs
    assert (out / "sync/users-to-pg.html").exists()

    one_sync = Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-06-28T00:00:00Z",
        project=Project(name="acme", profile="default"),
        sources=[Source(name="default", type="duckdb")],
        destinations=[
            Destination(name="dest_discord_x", type="discord", label="discord (webhook)")
        ],
        syncs=[
            Sync(
                name="customers_to_discord",
                source="default",
                destination="dest_discord_x",
                mode="full",
            )
        ],
    )
    render_html(one_sync, out)  # re-render with users-to-pg gone
    assert (out / "sync/customers-to-discord.html").exists()
    assert not (out / "sync/users-to-pg.html").exists()
    assert not (out / "destination/dest-pg-users.html").exists()


# --- #703 hardening: slug-collision, rmtree guard, ImportError hint ------------


def test_slug_collision_fails_fast(tmp_path: Path) -> None:
    """Two names that slugify to the same page must raise, not silently clobber."""
    manifest = Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-06-28T00:00:00Z",
        project=Project(name="p", profile="default"),
        sources=[Source(name="default", type="duckdb")],
        destinations=[Destination(name="d", type="discord", label="discord")],
        syncs=[
            Sync(name="a_b", source="default", destination="d", mode="full"),
            Sync(name="a__b", source="default", destination="d", mode="full"),
        ],
    )
    with pytest.raises(ValueError, match="slugify to the same page"):
        render_html(manifest, tmp_path / "docs")


def test_rmtree_guard_refuses_non_docs_directory(tmp_path: Path) -> None:
    """render_html must not wipe a populated directory that isn't a drt-docs build."""
    target = tmp_path / "precious"
    target.mkdir()
    keep = target / "thesis.txt"
    keep.write_text("do not delete", encoding="utf-8")
    with pytest.raises(ValueError, match="Refusing to delete"):
        render_html(_manifest(), target)
    assert keep.exists()  # untouched


def test_rmtree_guard_errors_on_file_target(tmp_path: Path) -> None:
    target = tmp_path / "afile"
    target.write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a directory"):
        render_html(_manifest(), target)


def test_rmtree_guard_allows_empty_and_prior_build(tmp_path: Path) -> None:
    # Empty dir is fine.
    empty = tmp_path / "docs"
    empty.mkdir()
    render_html(_manifest(), empty)
    assert (empty / "index.html").exists()
    # Re-rendering over a prior drt-docs build is fine (has index.html + assets/).
    render_html(_manifest(), empty)
    assert (empty / "index.html").exists()


def test_html_format_missing_extra_prints_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """--format html without the [docs] extra prints an install hint, not a traceback."""
    import sys

    from typer.testing import CliRunner

    from drt.cli.main import app

    # Force the deferred import to fail as if pygments/jinja2 weren't installed.
    monkeypatch.setitem(sys.modules, "drt.docs.html", None)
    result = CliRunner().invoke(app, ["docs", "generate", "--format", "html"])
    assert result.exit_code == 1
    assert "pip install drt-core[docs]" in result.output
