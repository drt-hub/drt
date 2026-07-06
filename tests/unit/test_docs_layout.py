"""Tests for the static DAG layout engine (#701).

Pure geometry, so everything here asserts against positions / edge paths /
crossing counts — no SVG or styling (that emission layer sits on top).
"""

from __future__ import annotations

import pytest

from drt.docs.layout import LayoutConfig, compute_layout
from drt.docs.manifest import (
    SCHEMA_VERSION,
    Destination,
    Edge,
    Manifest,
    Project,
    Source,
    Sync,
)


def _manifest(
    *,
    sources: list[Source],
    syncs: list[Sync],
    destinations: list[Destination],
    edges: list[Edge],
) -> Manifest:
    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version="9.9.9",
        generated_at="2026-06-28T00:00:00Z",
        project=Project(name="p", profile="default"),
        sources=sources,
        syncs=syncs,
        destinations=destinations,
        edges=edges,
    )


def _linear() -> Manifest:
    """warehouse -> s1 -> d1 (one clean line)."""
    return _manifest(
        sources=[Source(name="warehouse", type="duckdb")],
        syncs=[Sync(name="s1", source="warehouse", destination="d1", mode="full")],
        destinations=[Destination(name="d1", type="discord", label="discord")],
        edges=[
            Edge(kind="source_to_sync", from_="warehouse", to="s1"),
            Edge(kind="sync_to_destination", from_="s1", to="d1"),
        ],
    )


def _crossed() -> Manifest:
    """Name order crosses: sync `alpha` -> `z_dest`, `beta` -> `a_dest`.

    Alphabetical seeding puts dests [a_dest, z_dest] and syncs [alpha, beta], so
    the edges cross. A working optimizer must uncross them.
    """
    return _manifest(
        sources=[Source(name="warehouse", type="duckdb")],
        syncs=[
            Sync(name="alpha", source="warehouse", destination="z_dest", mode="full"),
            Sync(name="beta", source="warehouse", destination="a_dest", mode="full"),
        ],
        destinations=[
            Destination(name="a_dest", type="postgres", label="postgres"),
            Destination(name="z_dest", type="s3", label="s3"),
        ],
        edges=[
            Edge(kind="source_to_sync", from_="warehouse", to="alpha"),
            Edge(kind="source_to_sync", from_="warehouse", to="beta"),
            Edge(kind="sync_to_destination", from_="alpha", to="z_dest"),
            Edge(kind="sync_to_destination", from_="beta", to="a_dest"),
        ],
    )


def test_rank_is_node_kind() -> None:
    lay = compute_layout(_linear())
    by_id = {n.id: n for n in lay.nodes}
    assert by_id["warehouse"].rank == 0 and by_id["warehouse"].kind == "source"
    assert by_id["s1"].rank == 1 and by_id["s1"].kind == "sync"
    assert by_id["d1"].rank == 2 and by_id["d1"].kind == "destination"


def test_output_is_deterministic() -> None:
    m = _crossed()
    assert compute_layout(m) == compute_layout(m)  # byte-identical placement


def test_ordering_uncrosses_edges() -> None:
    # Naive alphabetical seeding crosses; the optimizer must drive it to zero.
    lay = compute_layout(_crossed())
    assert lay.crossings == 0


def test_both_strategies_run_and_are_deterministic() -> None:
    m = _crossed()
    for strategy in ("barycenter", "median"):
        a = compute_layout(m, strategy=strategy)
        b = compute_layout(m, strategy=strategy)
        assert a == b
        assert a.strategy == strategy
        assert a.crossings == 0  # both solve this one


def test_unknown_strategy_raises() -> None:
    with pytest.raises(ValueError, match="Unknown strategy"):
        compute_layout(_linear(), strategy="nope")


def test_forward_edges_are_four_point_beziers() -> None:
    lay = compute_layout(_linear())
    fwd = [e for e in lay.edges if e.kind == "forward"]
    assert fwd and all(len(e.points) == 4 and e.lane is None for e in fwd)


def test_lookups_get_distinct_lanes_and_ports() -> None:
    """N lookups into one sync must each get their own lane and a distinct
    top-edge entry port — never stacked into one curve."""
    m = _manifest(
        sources=[Source(name="wh", type="duckdb")],
        syncs=[
            Sync(name="consumer", source="wh", destination="d0", mode="full"),
            Sync(name="prod_a", source="wh", destination="d1", mode="full"),
            Sync(name="prod_b", source="wh", destination="d2", mode="full"),
        ],
        destinations=[
            Destination(name="d0", type="discord", label="discord"),
            Destination(name="d1", type="postgres", label="postgres"),
            Destination(name="d2", type="s3", label="s3"),
        ],
        edges=[
            Edge(kind="source_to_sync", from_="wh", to="consumer"),
            Edge(kind="source_to_sync", from_="wh", to="prod_a"),
            Edge(kind="source_to_sync", from_="wh", to="prod_b"),
            Edge(kind="sync_to_destination", from_="consumer", to="d0"),
            Edge(kind="sync_to_destination", from_="prod_a", to="d1"),
            Edge(kind="sync_to_destination", from_="prod_b", to="d2"),
            Edge(kind="lookup", from_="prod_a", to="consumer"),
            Edge(kind="lookup", from_="prod_b", to="consumer"),
        ],
    )
    lay = compute_layout(m)
    lookups = [e for e in lay.edges if e.kind == "lookup"]
    assert len(lookups) == 2
    lanes = [e.lane for e in lookups]
    assert lanes == [0, 1]  # distinct, deterministic
    entry_ports = [e.points[-1][0] for e in lookups]  # x where each enters consumer
    assert len(set(entry_ports)) == 2  # distinct ports, not stacked


def test_empty_project_does_not_crash() -> None:
    lay = compute_layout(
        _manifest(sources=[], syncs=[], destinations=[], edges=[])
    )
    assert lay.nodes == () and lay.edges == ()
    assert lay.width > 0 and lay.height > 0


def test_single_node_does_not_crash() -> None:
    lay = compute_layout(
        _manifest(
            sources=[Source(name="only", type="duckdb")],
            syncs=[],
            destinations=[],
            edges=[],
        )
    )
    assert len(lay.nodes) == 1 and lay.crossings == 0


def test_custom_config_scales_geometry() -> None:
    tight = compute_layout(_linear(), config=LayoutConfig(col_w=100.0))
    wide = compute_layout(_linear(), config=LayoutConfig(col_w=400.0))
    assert wide.width > tight.width
