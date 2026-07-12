"""Guard for examples/docs-demo — the showcase fixture must stay renderable."""

from __future__ import annotations

from pathlib import Path

from drt.docs.builder import build_manifest
from drt.docs.html import render_html

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "docs-demo"


def test_docs_demo_example_builds_and_renders(tmp_path: Path) -> None:
    manifest = build_manifest(EXAMPLE, include_state=True)

    assert len(manifest.syncs) == 10, "a sync stopped validating — check load_syncs_safe errors"
    assert [s.type for s in manifest.sources] == ["bigquery"]
    lookups = {(e.from_, e.to) for e in manifest.edges if e.kind == "lookup"}
    assert ("users_to_pg", "orders_to_pg") in lookups
    assert ("products_to_pg", "orders_to_pg") in lookups  # the two-into-one case
    assert sum(1 for s in manifest.syncs if s.state is not None) == 10
    statuses = {s.state.last_status for s in manifest.syncs if s.state}
    assert {"success", "partial", "failed"} <= statuses

    written = render_html(manifest, tmp_path / "docs")
    assert (tmp_path / "docs" / "index.html").exists()
    assert len(written) >= 20
