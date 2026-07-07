"""Deterministic, dependency-free layout for the project DAG (#701).

Pure geometry: this module consumes a :class:`~drt.docs.manifest.Manifest` and
produces node positions + edge paths. It has **no** SVG, styling, or runtime JS
— the emission layer (design tokens, connector badges, ownership zones from the
docs UI work) sits on top and consumes this. Keeping the two apart is the whole
point: the look can change without touching the layout, and the layout can be
tested against pure data.

The approach is the one scoped in the #677 thread:

* **Ranks are free** — ``rank = node type`` (``source -> sync -> destination``).
  No longest-path layering needed; the node's kind *is* its column.
* **Within-rank ordering** — barycenter (or median) sweeps with a fixed
  iteration count and a stable name tiebreak, so the output is deterministic and
  byte-identical across builds (clean git diffs). ``strategy`` selects the
  heuristic so the two can be A/B'd on crossing count.
* **Lookup back-edges** (``sync -> sync``) — each gets its own lane above the
  graph and a distinct top-edge port on the target sync, so N lookups into one
  sync never collapse into a single curve.
"""

from __future__ import annotations

import statistics
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from drt.docs.manifest import Manifest

_COORD_PRECISION = 2  # round coords so output is byte-identical across builds


@dataclass(frozen=True)
class LayoutNode:
    """A placed node. ``order`` is its 0-based slot within its rank (top->bottom)."""

    id: str
    kind: str  # "source" | "sync" | "destination"
    rank: int
    order: int
    x: float
    y: float


@dataclass(frozen=True)
class LayoutEdge:
    """A routed edge. ``points`` are cubic-bezier control points (start, c1, c2,
    end for forward edges; a longer polyline for lane-routed back-edges).
    ``lane`` is set only for lookup back-edges."""

    src: str
    dst: str
    kind: str  # "forward" | "lookup"
    points: tuple[tuple[float, float], ...]
    lane: int | None = None


@dataclass(frozen=True)
class Layout:
    """The full placement. ``crossings`` counts forward-edge crossings under the
    chosen ordering — the number the barycenter/median A/B compares."""

    nodes: tuple[LayoutNode, ...]
    edges: tuple[LayoutEdge, ...]
    width: float
    height: float
    crossings: int
    strategy: str


@dataclass
class LayoutConfig:
    col_w: float = 240.0  # horizontal gap between ranks
    row_h: float = 96.0  # vertical gap between nodes in a rank
    node_w: float = 180.0  # node box width (for port geometry)
    node_h: float = 64.0  # node box height
    margin: float = 32.0  # outer padding
    lane_gap: float = 20.0  # vertical gap between back-edge lanes
    iterations: int = 4  # barycenter/median sweeps


# --------------------------------------------------------------------------- #
# graph extraction
# --------------------------------------------------------------------------- #


def _ranks(manifest: Manifest) -> dict[int, list[str]]:
    """Node ids per rank, each seeded in a stable (name-sorted) order."""
    return {
        0: sorted(s.name for s in manifest.sources),
        1: sorted(s.name for s in manifest.syncs),
        2: sorted(d.name for d in manifest.destinations),
    }


def _forward_edges(manifest: Manifest) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Forward edges split by rank-pair: (source->sync, sync->destination)."""
    src_sync = [(e.from_, e.to) for e in manifest.edges if e.kind == "source_to_sync"]
    sync_dst = [(e.from_, e.to) for e in manifest.edges if e.kind == "sync_to_destination"]
    return src_sync, sync_dst


def _lookup_edges(manifest: Manifest) -> list[tuple[str, str]]:
    """Lookup back-edges (producer sync -> consumer sync), stably ordered."""
    return sorted(
        (e.from_, e.to) for e in manifest.edges if e.kind == "lookup"
    )


# --------------------------------------------------------------------------- #
# ordering (the readability core)
# --------------------------------------------------------------------------- #

_Aggregator = Callable[[Sequence[float]], float]


def _barycenter(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _median(values: Sequence[float]) -> float:
    return float(statistics.median(values))


_STRATEGIES: dict[str, _Aggregator] = {
    "barycenter": _barycenter,
    "median": _median,
}


def _neighbor_positions(
    rank_ids: Sequence[str],
    adjacent_order: dict[str, int],
    edges: Iterable[tuple[str, str]],
    *,
    from_side: bool,
) -> dict[str, list[float]]:
    """For each node in ``rank_ids``, the order-indices of its neighbors in the
    adjacent rank. ``from_side`` picks which endpoint of each edge is *this* rank.
    """
    out: dict[str, list[float]] = {n: [] for n in rank_ids}
    for a, b in edges:
        here, there = (a, b) if from_side else (b, a)
        if here in out and there in adjacent_order:
            out[here].append(float(adjacent_order[there]))
    return out


def _reorder(
    rank_ids: Sequence[str],
    neighbor_pos: dict[str, list[float]],
    agg: _Aggregator,
) -> list[str]:
    """Sort a rank by the aggregated neighbor position. Nodes with no neighbors
    keep their current index; ties break by name for determinism."""
    keyed: list[tuple[float, str]] = []
    for i, node in enumerate(rank_ids):
        npos = neighbor_pos.get(node) or []
        key = agg(npos) if npos else float(i)
        keyed.append((key, node))
    keyed.sort(key=lambda t: (t[0], t[1]))
    return [n for _, n in keyed]


def _count_inversions(seq: Sequence[int]) -> int:
    """Number of out-of-order pairs — i.e. edge crossings for a bipartite layer.

    Intentionally the plain O(n^2) double loop, not an O(n log n) merge count:
    the docs DAG is the reverse-ETL last-mile subset (bounded small — dozens,
    low-hundreds worst case) and this is offline docs-gen, not a sync hot path,
    so the quadratic never bites. The binding constraint is byte-identical
    determinism; the double loop is trivially correct + deterministic with no
    tie-handling surface. Swap for a merge count only if a real project hurts.
    """
    total = 0
    for i in range(len(seq)):
        for j in range(i + 1, len(seq)):
            if seq[i] > seq[j]:
                total += 1
    return total


def _crossings_between(
    edges: Iterable[tuple[str, str]],
    order_top: Sequence[str],
    order_bottom: Sequence[str],
) -> int:
    pos_top = {n: i for i, n in enumerate(order_top)}
    pos_bottom = {n: i for i, n in enumerate(order_bottom)}
    placed = sorted(
        (pos_top[a], pos_bottom[b])
        for a, b in edges
        if a in pos_top and b in pos_bottom
    )
    return _count_inversions([b for _, b in placed])


def _total_crossings(
    order: dict[int, list[str]],
    src_sync: Sequence[tuple[str, str]],
    sync_dst: Sequence[tuple[str, str]],
) -> int:
    return _crossings_between(src_sync, order[0], order[1]) + _crossings_between(
        sync_dst, order[1], order[2]
    )


def _optimize_order(
    ranks: dict[int, list[str]],
    src_sync: Sequence[tuple[str, str]],
    sync_dst: Sequence[tuple[str, str]],
    agg: _Aggregator,
    iterations: int,
) -> dict[int, list[str]]:
    """Alternate down/up sweeps, keeping whichever ordering minimizes crossings.

    Deterministic: fixed iteration count, name tiebreaks, and ties in crossing
    count resolved by keeping the earliest (so the same input always wins the
    same way)."""
    order = {r: list(ids) for r, ids in ranks.items()}
    best = {r: list(ids) for r, ids in order.items()}
    best_cross = _total_crossings(order, src_sync, sync_dst)

    for _ in range(iterations):
        # down sweep: order ranks by their predecessors above
        pos0 = {n: i for i, n in enumerate(order[0])}
        order[1] = _reorder(
            order[1], _neighbor_positions(order[1], pos0, src_sync, from_side=False), agg
        )
        pos1 = {n: i for i, n in enumerate(order[1])}
        order[2] = _reorder(
            order[2], _neighbor_positions(order[2], pos1, sync_dst, from_side=False), agg
        )
        # up sweep: order ranks by their successors below
        pos2 = {n: i for i, n in enumerate(order[2])}
        order[1] = _reorder(
            order[1], _neighbor_positions(order[1], pos2, sync_dst, from_side=True), agg
        )
        pos1 = {n: i for i, n in enumerate(order[1])}
        order[0] = _reorder(
            order[0], _neighbor_positions(order[0], pos1, src_sync, from_side=True), agg
        )
        cross = _total_crossings(order, src_sync, sync_dst)
        if cross < best_cross:
            best_cross = cross
            best = {r: list(ids) for r, ids in order.items()}

    return best


# --------------------------------------------------------------------------- #
# coordinates + edge routing
# --------------------------------------------------------------------------- #


def _round(value: float) -> float:
    return round(value, _COORD_PRECISION)


def _place_nodes(
    order: dict[int, list[str]], kinds: dict[str, str], cfg: LayoutConfig
) -> tuple[dict[str, LayoutNode], float]:
    """Assign (x, y) to every node; ranks are vertically centered against the
    tallest one. Returns the placed nodes and the content height."""
    tallest = max((len(ids) for ids in order.values()), default=0)
    content_h = max(tallest, 1) * cfg.row_h
    placed: dict[str, LayoutNode] = {}
    for rank, ids in order.items():
        x = cfg.margin + rank * cfg.col_w + cfg.node_w / 2
        rank_h = len(ids) * cfg.row_h
        top = cfg.margin + (content_h - rank_h) / 2
        for i, node_id in enumerate(ids):
            y = top + i * cfg.row_h + cfg.node_h / 2
            placed[node_id] = LayoutNode(
                id=node_id, kind=kinds[node_id], rank=rank, order=i,
                x=_round(x), y=_round(y),
            )
    return placed, content_h


def _forward_path(
    src: LayoutNode, dst: LayoutNode, cfg: LayoutConfig
) -> tuple[tuple[float, float], ...]:
    """Cubic bezier from src's right port to dst's left port."""
    x1, y1 = src.x + cfg.node_w / 2, src.y
    x2, y2 = dst.x - cfg.node_w / 2, dst.y
    dx = (x2 - x1) * 0.5
    return (
        (_round(x1), _round(y1)),
        (_round(x1 + dx), _round(y1)),
        (_round(x2 - dx), _round(y2)),
        (_round(x2), _round(y2)),
    )


def _route_lookups(
    lookups: Sequence[tuple[str, str]],
    placed: dict[str, LayoutNode],
    cfg: LayoutConfig,
) -> tuple[list[LayoutEdge], float]:
    """Route each lookup as its own lane above the graph, entering the consumer
    sync through a distinct top-edge port. Returns the edges and the vertical
    space consumed by the lanes (so the caller can grow the canvas top)."""
    valid = [(p, c) for p, c in lookups if p in placed and c in placed]
    if not valid:
        return [], 0.0

    # Each back-edge gets its own lane (contract: N lookups never stack).
    lanes_total = len(valid)
    lanes_height = (lanes_total + 1) * cfg.lane_gap

    # Distinct top-edge ports: spread each consumer's incoming lookups across its
    # top edge so they don't share an entry point.
    incoming: dict[str, list[int]] = {}
    for idx, (_p, c) in enumerate(valid):
        incoming.setdefault(c, []).append(idx)

    port_x: dict[int, float] = {}
    for consumer, idxs in incoming.items():
        cx = placed[consumer].x
        span = cfg.node_w * 0.6
        n = len(idxs)
        for k, idx in enumerate(idxs):
            # even spread across [-span/2, +span/2]; single port lands at center
            frac = 0.0 if n == 1 else (k / (n - 1) - 0.5)
            port_x[idx] = cx + frac * span

    # Reserved gutter: the clear inter-column channel to the left of the sync
    # column (between the source and sync columns). Each lane rises vertically at
    # a distinct x *inside* this gutter, so risers never enter a node column —
    # they stay clear by construction rather than by luck of ordering.
    gutter_left = cfg.margin + cfg.node_w  # right edge of the rank-0 column
    gutter_right = cfg.margin + cfg.col_w  # left edge of the rank-1 column
    gutter_w = gutter_right - gutter_left

    edges: list[LayoutEdge] = []
    for lane, (producer, consumer) in enumerate(valid):
        pnode, cnode = placed[producer], placed[consumer]
        lane_y = cfg.margin + lanes_height - (lane + 1) * cfg.lane_gap
        gutter_x = gutter_left + (lane + 1) / (lanes_total + 1) * gutter_w
        p_top = pnode.y - cfg.node_h / 2  # producer top
        c_top = cnode.y - cfg.node_h / 2  # consumer top
        px = port_x[lane]
        points = (
            (_round(pnode.x), _round(p_top)),  # producer top
            (_round(gutter_x), _round(p_top)),  # jog into the reserved gutter
            (_round(gutter_x), _round(lane_y)),  # rise vertically in the gutter
            (_round(px), _round(lane_y)),  # across the lane band to the port
            (_round(px), _round(c_top)),  # down into the consumer top port
        )
        edges.append(
            LayoutEdge(src=producer, dst=consumer, kind="lookup", points=points, lane=lane)
        )
    return edges, lanes_height


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #


def compute_layout(
    manifest: Manifest,
    *,
    strategy: str = "barycenter",
    config: LayoutConfig | None = None,
) -> Layout:
    """Lay out *manifest* as a static DAG. Deterministic for a given manifest +
    strategy: identical input yields byte-identical coordinates.

    ``strategy`` is ``"barycenter"`` or ``"median"`` (the crossing-minimization
    heuristics to A/B). Raises ``ValueError`` for an unknown strategy.
    """
    if strategy not in _STRATEGIES:
        raise ValueError(
            f"Unknown strategy {strategy!r}; expected one of {sorted(_STRATEGIES)}."
        )
    cfg = config or LayoutConfig()
    agg = _STRATEGIES[strategy]

    kinds: dict[str, str] = {}
    for src in manifest.sources:
        kinds[src.name] = "source"
    for syn in manifest.syncs:
        kinds[syn.name] = "sync"
    for dst in manifest.destinations:
        kinds[dst.name] = "destination"

    ranks = _ranks(manifest)
    src_sync, sync_dst = _forward_edges(manifest)
    order = _optimize_order(ranks, src_sync, sync_dst, agg, cfg.iterations)
    crossings = _total_crossings(order, src_sync, sync_dst)

    placed, content_h = _place_nodes(order, kinds, cfg)

    # forward edges
    edges: list[LayoutEdge] = []
    for a, b in src_sync + sync_dst:
        if a in placed and b in placed:
            edges.append(
                LayoutEdge(
                    src=a, dst=b, kind="forward",
                    points=_forward_path(placed[a], placed[b], cfg),
                )
            )
    # lookup back-edges (lanes above the graph)
    lookup_edges, lanes_height = _route_lookups(_lookup_edges(manifest), placed, cfg)

    # shift everything down by the lane band so lanes have room at the top
    if lanes_height:
        placed = {
            nid: LayoutNode(
                id=n.id, kind=n.kind, rank=n.rank, order=n.order,
                x=n.x, y=_round(n.y + lanes_height),
            )
            for nid, n in placed.items()
        }
        edges = [
            LayoutEdge(
                src=e.src, dst=e.dst, kind=e.kind, lane=e.lane,
                points=tuple((x, _round(y + lanes_height)) for x, y in e.points),
            )
            for e in edges
        ]
        # lookup edges were routed in the lane band already; only shift their
        # graph-side endpoints, not the lane crossbar — recompute cleanly:
        lookup_edges, _ = _route_lookups(_lookup_edges(manifest), placed, cfg)

    all_nodes = tuple(
        sorted(placed.values(), key=lambda n: (n.rank, n.order))
    )
    all_edges = tuple(edges) + tuple(lookup_edges)

    width = cfg.margin * 2 + (len(ranks) - 1) * cfg.col_w + cfg.node_w
    height = cfg.margin * 2 + content_h + lanes_height

    return Layout(
        nodes=all_nodes,
        edges=all_edges,
        width=_round(width),
        height=_round(height),
        crossings=crossings,
        strategy=strategy,
    )
