"""Render a `Manifest` as a Mermaid `graph LR` block."""

from __future__ import annotations

import re

from drt.docs.manifest import Manifest

_SLUG_RE = re.compile(r"[^A-Za-z0-9_]+")


def _node_id(prefix: str, name: str) -> str:
    return f"{prefix}_{_SLUG_RE.sub('_', name).strip('_') or 'x'}"


def _escape_label(text: str) -> str:
    """Mermaid label-safe — wrap in quotes and escape inner quotes."""
    return text.replace('"', "&quot;")


def render_mermaid(manifest: Manifest) -> str:
    """Render a Mermaid `graph LR` block from *manifest*.

    Layout:
      - Three subgraphs: Sources, Syncs, Destinations
      - source -> sync: solid arrow, label "extract"
      - sync -> destination: solid arrow, label "load"
      - sync -> sync (lookup): dashed arrow, label "lookup"
    """
    lines: list[str] = ["graph LR"]

    if not manifest.syncs:
        lines.append('    empty["No syncs found"]')
        return "\n".join(lines)

    # Sources subgraph
    lines.append("    subgraph Sources")
    for src in manifest.sources:
        nid = _node_id("src", src.name)
        label = f"{_escape_label(src.name)}<br/><i>{_escape_label(src.type)}</i>"
        lines.append(f'        {nid}["{label}"]')
    lines.append("    end")

    # Syncs subgraph (hexagon shape with mode label)
    lines.append("    subgraph Syncs")
    for sync in manifest.syncs:
        nid = _node_id("sync", sync.name)
        label = f"{_escape_label(sync.name)}<br/><i>{_escape_label(sync.mode)}</i>"
        lines.append(f'        {nid}{{{{"{label}"}}}}')
    lines.append("    end")

    # Destinations subgraph
    lines.append("    subgraph Destinations")
    for dst in manifest.destinations:
        nid = _node_id("dst", dst.label)
        label = f"{_escape_label(dst.label)}<br/><i>{_escape_label(dst.type)}</i>"
        lines.append(f'        {nid}["{label}"]')
    lines.append("    end")

    # Edges
    dst_by_id = {d.name: d for d in manifest.destinations}
    for edge in manifest.edges:
        if edge.kind == "source_to_sync":
            from_id = _node_id("src", edge.from_)
            to_id = _node_id("sync", edge.to)
            lines.append(f"    {from_id} -->|extract| {to_id}")
        elif edge.kind == "sync_to_destination":
            from_id = _node_id("sync", edge.from_)
            dst_match = dst_by_id.get(edge.to)
            to_id = _node_id("dst", dst_match.label) if dst_match else _node_id("dst", edge.to)
            lines.append(f"    {from_id} -->|load| {to_id}")
        elif edge.kind == "lookup":
            from_id = _node_id("sync", edge.from_)
            to_id = _node_id("sync", edge.to)
            lines.append(f"    {from_id} -.lookup.-> {to_id}")

    return "\n".join(lines)
