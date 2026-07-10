"""Static SVG emission for the project DAG page (#701, phase 2).

Consumes the deterministic geometry from :mod:`drt.docs.layout` and emits one
inline, themeable SVG — the visual language of
``docs/design/drt-docs-lineage-mock.html``: ownership zone bands (sources ·
external read | syncs · managed by drt | destinations · external write),
forward edges as bezier curves, lookup back-edges as dashed "subway" lanes over
the top, and the #704 node cards / connector badges for every node, each linked
to its detail page.

No runtime layout JS, no CDN — the output works opened via ``file://``. Same
manifest in, same bytes out (#697): the layout engine rounds all coordinates,
node/edge iteration follows the layout's stable ordering, and nothing here
reads clocks, randomness, or unordered sets.
"""

from __future__ import annotations

from drt.docs._svg import _marker_defs, _node_card, _slug_map
from drt.docs.layout import LayoutConfig, LayoutEdge, compute_layout
from drt.docs.manifest import Manifest

_HEADER_H = 48.0  # band above the layout content, holding the zone titles
_ZONE_PAD_X = 12  # horizontal breathing room between a column and its zone edge
_ZONE_PAD_Y = 10  # gap between the SVG edge and the zone bands

# (title, title fill, secondary right-aligned label) per rank. The sync zone
# carries the "managed by drt" pill instead of a secondary label — the band is
# too narrow for both (the mock's "version-controlled YAML" tag is dropped).
_ZONES: tuple[tuple[str, str, str | None], ...] = (
    ("SOURCES", "var(--muted)", "external · read"),
    ("SYNCS", "var(--brand-700)", None),
    ("DESTINATIONS", "var(--muted)", "external · write"),
)


def _dag_config() -> LayoutConfig:
    """Layout geometry tuned to the mock's proportions. ``node_h`` matches the
    fixed 54px card height so edge ports land on card borders."""
    return LayoutConfig(
        col_w=380.0,  # rank pitch — leaves a 160px gutter for lookup risers
        row_h=96.0,
        node_w=220.0,
        node_h=54.0,
        margin=32.0,
        lane_gap=20.0,
    )


def _f(value: float) -> str:
    """Compact deterministic coordinate formatting (``84.0`` -> ``84``)."""
    return format(value, "g")


def _zone_bands(cfg: LayoutConfig, total_h: float) -> str:
    """The three ownership bands, drawn behind everything else."""
    parts: list[str] = []
    band_w = cfg.node_w + 2 * _ZONE_PAD_X
    band_h = total_h - 2 * _ZONE_PAD_Y
    for rank, (title, title_fill, secondary) in enumerate(_ZONES):
        left = cfg.margin + rank * cfg.col_w - _ZONE_PAD_X
        is_sync = rank == 1
        fill = "var(--zone-drt-bg)" if is_sync else "none"
        stroke = "var(--zone-drt-line)" if is_sync else "var(--line)"
        parts.append(
            f'<rect x="{_f(left)}" y="{_ZONE_PAD_Y}" width="{_f(band_w)}" '
            f'height="{_f(band_h)}" rx="10" fill="{fill}" stroke="{stroke}"/>'
            f'<text x="{_f(left + 14)}" y="32" font-size="11" letter-spacing="1.2" '
            f'fill="{title_fill}" font-weight="600">{title}</text>'
        )
        if is_sync:
            parts.append(
                f'<rect x="{_f(left + 66)}" y="19" width="92" height="18" rx="9" '
                'fill="var(--brand-600)"/>'
                f'<text x="{_f(left + 112)}" y="32" font-size="10.5" fill="#fff" '
                'text-anchor="middle" font-weight="600">managed by drt</text>'
            )
        elif secondary:
            parts.append(
                f'<text x="{_f(left + band_w - 14)}" y="32" font-size="11" '
                f'fill="var(--muted)" text-anchor="end">{secondary}</text>'
            )
    return "".join(parts)


def _edge_svg(edge: LayoutEdge) -> str:
    pts = edge.points
    if edge.kind == "forward":
        # 4 bezier control points from the layout: start, c1, c2, end.
        d = (
            f"M{_f(pts[0][0])},{_f(pts[0][1])} "
            f"C{_f(pts[1][0])},{_f(pts[1][1])} "
            f"{_f(pts[2][0])},{_f(pts[2][1])} "
            f"{_f(pts[3][0])},{_f(pts[3][1])}"
        )
        return (
            f'<path d="{d}" fill="none" stroke="var(--edge)" stroke-width="1.5" '
            'marker-end="url(#dag-arr)"/>'
        )
    # Lookup back-edge: orthogonal lane polyline over the top of the graph,
    # dashed, with a port dot where it enters the consumer's top edge.
    d = f"M{_f(pts[0][0])},{_f(pts[0][1])}" + "".join(
        f" L{_f(x)},{_f(y)}" for x, y in pts[1:]
    )
    end_x, end_y = pts[-1]
    return (
        f'<path d="{d}" fill="none" stroke="var(--edge-lookup)" stroke-width="1.5" '
        'stroke-dasharray="5 4" marker-end="url(#dag-arr-lk)"/>'
        f'<circle cx="{_f(end_x)}" cy="{_f(end_y)}" r="3" fill="var(--edge-lookup)"/>'
    )


def render_dag_svg(manifest: Manifest, *, strategy: str = "median") -> str:
    """Render *manifest* as one static, themeable DAG SVG.

    Deterministic: the same manifest (and strategy) always yields byte-identical
    markup. Every node is an ``<a>`` to its detail page, using the same slug
    scheme as the rest of the site; hrefs are relative to the site root (the
    DAG page lives at ``dag.html``).
    """
    cfg = _dag_config()
    layout = compute_layout(manifest, strategy=strategy, config=cfg)

    sync_slugs = _slug_map([s.name for s in manifest.syncs], "sync")
    source_slugs = _slug_map([s.name for s in manifest.sources], "source")
    dest_slugs = _slug_map([d.name for d in manifest.destinations], "destination")
    src_type = {s.name: s.type for s in manifest.sources}
    sync_by_name = {s.name: s for s in manifest.syncs}
    dest_by_id = {d.name: d for d in manifest.destinations}

    width = layout.width
    height = layout.height + _HEADER_H
    card_w = round(cfg.node_w)

    parts: list[str] = [
        f'<svg viewBox="0 0 {_f(width)} {_f(height)}" width="{_f(width)}" '
        f'height="{_f(height)}" role="img" aria-label="Lineage graph: sources feed '
        'drt-managed syncs which write to destinations">',
        _marker_defs("dag", size=7),
        # back to front: zones, then edges, then node cards
        _zone_bands(cfg, height),
        f'<g transform="translate(0,{_f(_HEADER_H)})">',
    ]
    parts.extend(_edge_svg(e) for e in layout.edges)

    for node in layout.nodes:
        x = round(node.x - cfg.node_w / 2)
        y = round(node.y - cfg.node_h / 2)
        if node.kind == "sync":
            sync = sync_by_name[node.id]
            parts.append(
                _node_card(
                    x,
                    y,
                    card_w,
                    "",
                    sync.name,
                    sync.mode,
                    f"sync/{sync_slugs[sync.name]}.html",
                    code=True,
                    status=sync.state.last_status if sync.state else None,
                )
            )
        elif node.kind == "source":
            conn_type = src_type.get(node.id, "source")
            parts.append(
                _node_card(
                    x,
                    y,
                    card_w,
                    conn_type,
                    node.id,
                    conn_type,
                    f"source/{source_slugs[node.id]}.html",
                )
            )
        else:
            dest = dest_by_id.get(node.id)
            conn_type = dest.type if dest else "destination"
            parts.append(
                _node_card(
                    x,
                    y,
                    card_w,
                    conn_type,
                    dest.label if dest else node.id,
                    conn_type,
                    f"destination/{dest_slugs[node.id]}.html",
                )
            )

    parts.append("</g></svg>")
    return "".join(parts)
