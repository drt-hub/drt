"""Tests for the static DAG SVG emission (#701, phase 2)."""

from __future__ import annotations

from pathlib import Path

from drt.docs.dag import render_dag_svg
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


def _state(status: str) -> SyncStateSnapshot:
    return SyncStateSnapshot(
        last_sync_at="2026-07-01T00:00:00Z",
        last_cursor_value=None,
        rows_synced=10,
        last_status=status,
        last_error=None,
    )


def _manifest() -> Manifest:
    """Two sources, three syncs, three destinations, one lookup back-edge —
    built fresh on every call so byte-identity is tested across object graphs,
    not on a cached instance."""
    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-07-01T00:00:00Z",
        project=Project(name="acme", profile="default"),
        sources=[
            Source(name="bq_prod", type="bigquery"),
            Source(name="pg_replica", type="postgres"),
        ],
        destinations=[
            Destination(name="dest_pg_users", type="postgres", label="public.users"),
            Destination(name="dest_pg_orders", type="postgres", label="public.orders"),
            Destination(name="dest_slack_alerts", type="slack", label="#alerts"),
        ],
        syncs=[
            Sync(
                name="users_to_pg",
                source="bq_prod",
                destination="dest_pg_users",
                mode="incremental",
                state=_state("success"),
            ),
            Sync(
                name="orders_to_pg",
                source="bq_prod",
                destination="dest_pg_orders",
                mode="upsert",
                state=_state("partial"),
            ),
            Sync(
                name="alerts_to_slack",
                source="pg_replica",
                destination="dest_slack_alerts",
                mode="append",
                state=_state("failed"),
            ),
        ],
        edges=[
            Edge(kind="source_to_sync", from_="bq_prod", to="users_to_pg"),
            Edge(kind="source_to_sync", from_="bq_prod", to="orders_to_pg"),
            Edge(kind="source_to_sync", from_="pg_replica", to="alerts_to_slack"),
            Edge(kind="sync_to_destination", from_="users_to_pg", to="dest_pg_users"),
            Edge(kind="sync_to_destination", from_="orders_to_pg", to="dest_pg_orders"),
            Edge(kind="sync_to_destination", from_="alerts_to_slack", to="dest_slack_alerts"),
            # orders_to_pg looks up users_to_pg's destination table
            Edge(kind="lookup", from_="users_to_pg", to="orders_to_pg"),
        ],
    )


# --- determinism (#697 acceptance bar) --------------------------------------


def test_byte_identical_across_renders() -> None:
    """Same manifest -> same SVG bytes, across freshly built object graphs."""
    assert render_dag_svg(_manifest()) == render_dag_svg(_manifest())


def test_dag_page_byte_identical_across_site_builds(tmp_path: Path) -> None:
    render_html(_manifest(), tmp_path / "a")
    render_html(_manifest(), tmp_path / "b")
    assert (tmp_path / "a/dag.html").read_bytes() == (tmp_path / "b/dag.html").read_bytes()


# --- structure ---------------------------------------------------------------


def test_ownership_zones_present() -> None:
    svg = render_dag_svg(_manifest())
    assert "SOURCES" in svg
    assert "SYNCS" in svg
    assert "DESTINATIONS" in svg
    assert "managed by drt" in svg
    assert "external · read" in svg
    assert "external · write" in svg
    assert "var(--zone-drt-bg)" in svg
    assert "var(--zone-drt-line)" in svg


def test_forward_and_lookup_edges_present() -> None:
    svg = render_dag_svg(_manifest())
    # 6 forward edges as beziers with the forward marker
    assert svg.count('marker-end="url(#dag-arr)"') == 6
    assert 'stroke="var(--edge)"' in svg


def test_lookup_edge_has_lookup_styling_and_marker() -> None:
    svg = render_dag_svg(_manifest())
    assert svg.count('marker-end="url(#dag-arr-lk)"') == 1
    assert 'stroke="var(--edge-lookup)"' in svg
    assert 'stroke-dasharray="5 4"' in svg


def test_every_node_is_a_link() -> None:
    svg = render_dag_svg(_manifest())
    assert svg.count('<a href="sync/') == 3
    assert svg.count('<a href="source/') == 2
    assert svg.count('<a href="destination/') == 3
    assert '<a href="sync/users-to-pg.html">' in svg
    assert '<a href="source/bq-prod.html">' in svg
    assert '<a href="destination/dest-pg-users.html">' in svg


def test_sync_cards_show_status_dot_and_word() -> None:
    svg = render_dag_svg(_manifest())
    for word, var in [("success", "--success"), ("partial", "--warning"), ("failed", "--error")]:
        assert word in svg
        assert f"var({var})" in svg


def test_connector_cards_use_brand_badges() -> None:
    svg = render_dag_svg(_manifest())
    assert ">BQ</text>" in svg  # bigquery badge initials
    assert ">PG</text>" in svg  # postgres
    assert ">SL</text>" in svg  # slack


# --- file:// safety ----------------------------------------------------------


def test_no_runtime_layout_js_or_cdn(tmp_path: Path) -> None:
    out = tmp_path / "docs"
    render_html(_manifest(), out)
    html = (out / "dag.html").read_text(encoding="utf-8")
    assert "fetch(" not in html
    assert "mermaid" not in html
    assert "cdn." not in html
    assert "<svg" in html  # the SVG really is inline


# --- edge cases --------------------------------------------------------------


def test_empty_project_renders_without_crashing(tmp_path: Path) -> None:
    empty = Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-07-01T00:00:00Z",
        project=Project(name="empty", profile="default"),
        sources=[Source(name="default", type="duckdb")],
    )
    # the module itself must not crash on 0 syncs
    assert "<svg" in render_dag_svg(empty)
    # and the site falls back to the empty state on the DAG page
    out = tmp_path / "docs"
    render_html(empty, out)
    assert "No syncs found" in (out / "dag.html").read_text(encoding="utf-8")


def test_manifest_text_is_escaped() -> None:
    bad = 'x<script>alert(1)</script>&"'
    manifest = Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-07-01T00:00:00Z",
        project=Project(name="p", profile="default"),
        sources=[Source(name="default", type="duckdb")],
        destinations=[Destination(name="dest_x", type="discord", label=bad)],
        syncs=[Sync(name=bad, source="default", destination="dest_x", mode=bad)],
        edges=[
            Edge(kind="source_to_sync", from_="default", to=bad),
            Edge(kind="sync_to_destination", from_=bad, to="dest_x"),
        ],
    )
    svg = render_dag_svg(manifest)
    assert "<script>" not in svg
