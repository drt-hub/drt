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


def test_dag_is_static_svg(site: Path) -> None:
    """The DAG page inlines the layout-engine SVG (#701) — no Mermaid, no CDN."""
    html = (site / "dag.html").read_text(encoding="utf-8")
    assert "<svg" in html
    assert "mermaid" not in html
    assert "cdn.jsdelivr.net" not in html


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
    target = tmp_path / "precious"
    target.mkdir()
    keep = target / "thesis.txt"
    keep.write_text("do not delete", encoding="utf-8")
    with pytest.raises(ValueError, match="Refusing to delete"):
        render_html(_manifest(), target)
    assert keep.exists()


def test_rmtree_guard_errors_on_file_target(tmp_path: Path) -> None:
    target = tmp_path / "afile"
    target.write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a directory"):
        render_html(_manifest(), target)


def test_rmtree_guard_allows_empty_and_prior_build(tmp_path: Path) -> None:
    empty = tmp_path / "docs"
    empty.mkdir()
    render_html(_manifest(), empty)
    assert (empty / "index.html").exists()
    render_html(_manifest(), empty)
    assert (empty / "index.html").exists()


def test_html_format_missing_extra_prints_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    from typer.testing import CliRunner

    from drt.cli.main import app

    monkeypatch.setitem(sys.modules, "drt.docs.html", None)
    result = CliRunner().invoke(app, ["docs", "generate", "--format", "html"])
    assert result.exit_code == 1
    assert "pip install drt-core[docs]" in result.output


def test_render_guard_surfaces_as_clean_cli_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A render_html guard (ValueError) must land as a clean CLI error + exit 1,
    not a traceback (#703 follow-up)."""
    import yaml
    from typer.testing import CliRunner

    from drt.cli.main import app

    project = tmp_path / "proj"
    project.mkdir()
    (project / "drt_project.yml").write_text(
        yaml.safe_dump(
            {"name": "demo", "version": "0.1", "profile": "p", "source": {"type": "bigquery"}}
        )
    )
    # populated, non-drt-docs directory → the rmtree guard must refuse
    target = project / "notdocs"
    target.mkdir()
    (target / "keep.txt").write_text("precious", encoding="utf-8")

    monkeypatch.chdir(project)
    result = CliRunner().invoke(
        app, ["docs", "generate", "--format", "html", "--output", str(target)]
    )
    assert result.exit_code == 1
    assert "Refusing to delete" in result.output
    # clean exit, not an unhandled ValueError traceback
    assert not isinstance(result.exception, ValueError)
    assert (target / "keep.txt").exists()  # guard protected the dir


def test_regeneration_is_byte_identical(tmp_path: Path) -> None:
    """#697's acceptance bar: same manifest -> same bytes, across the whole site."""
    m = _manifest()
    out1, out2 = tmp_path / "a", tmp_path / "b"
    render_html(m, out1)
    render_html(m, out2)
    files1 = sorted(p.relative_to(out1) for p in out1.rglob("*") if p.is_file())
    files2 = sorted(p.relative_to(out2) for p in out2.rglob("*") if p.is_file())
    assert files1 == files2
    for rel in files1:
        assert (out1 / rel).read_bytes() == (out2 / rel).read_bytes(), str(rel)
def test_yaml_tab_prefers_raw_text_and_notes_fallback(tmp_path: Path) -> None:
    """With sync_yaml_texts, the YAML tab shows the file as written (incl.
    model SQL); syncs without an entry keep the manifest view plus a note."""
    raw = (
        "name: customers_to_discord\n"
        'description: "VIP customers to Discord"\n'
        "model: ref('vip_customers')\n"
        "destination:\n  type: discord\n"
    )
    out = tmp_path / "docs"
    render_html(
        _manifest(), out, sync_yaml_texts={"customers_to_discord": ("syncs/customers.yml", raw)}
    )

    with_raw = (out / "sync" / "customers-to-discord.html").read_text(encoding="utf-8")
    assert "ref(&#39;vip_customers&#39;)" in with_raw or "vip_customers" in with_raw
    assert "Rendered from the manifest" not in with_raw
    assert "syncs/customers.yml" in with_raw  # code header shows the file path
    assert 'class="linenos"' in with_raw  # line numbers

    without_raw = (out / "sync" / "users-to-pg.html").read_text(encoding="utf-8")
    assert "Rendered from the manifest" in without_raw


def test_raw_yaml_is_escaped(tmp_path: Path) -> None:
    hostile = 'name: customers_to_discord\ndescription: "</script><script>alert(1)</script>"\n'
    out = tmp_path / "docs"
    render_html(
        _manifest(), out, sync_yaml_texts={"customers_to_discord": ("syncs/customers.yml", hostile)}
    )
    page = (out / "sync" / "customers-to-discord.html").read_text(encoding="utf-8")
    assert "<script>alert(1)</script>" not in page


def test_collect_sync_yaml_texts_best_effort(tmp_path: Path) -> None:
    from drt.docs.builder import collect_sync_yaml_texts

    syncs = tmp_path / "syncs"
    syncs.mkdir()
    (syncs / "good.yml").write_text("name: good_sync\nmode: full\n", encoding="utf-8")
    (syncs / "broken.yml").write_text("name: [unclosed\n", encoding="utf-8")
    (syncs / "nameless.yml").write_text("description: no name here\n", encoding="utf-8")

    texts = collect_sync_yaml_texts(tmp_path)
    assert texts == {"good_sync": ("syncs/good.yml", "name: good_sync\nmode: full")}
    # no syncs dir at all -> empty map, no crash
    assert collect_sync_yaml_texts(tmp_path / "nowhere") == {}
