"""Render a `Manifest` as a Mermaid `graph LR` block."""

from __future__ import annotations

import re
from html import escape

from drt.docs.manifest import Manifest

_SLUG_RE = re.compile(r"[^A-Za-z0-9_]+")


def _node_id(prefix: str, name: str) -> str:
    return f"{prefix}_{_SLUG_RE.sub('_', name).strip('_') or 'x'}"


def _node_ids(prefix: str, names: list[str]) -> dict[str, str]:
    """Allocate collision-free Mermaid ids within one node kind."""
    groups: dict[str, list[str]] = {}
    for name in names:
        groups.setdefault(_node_id(prefix, name), []).append(name)

    ids: dict[str, str] = {}
    for base, members in groups.items():
        for index, name in enumerate(sorted(members), start=1):
            ids[name] = base if index == 1 else f"{base}_{index}"
    return ids


def _escape_label(text: str) -> str:
    """Escape every Mermaid/HTML label delimiter before quoted emission."""
    return escape(text, quote=True)


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

    source_ids = _node_ids("src", [src.name for src in manifest.sources])
    sync_ids = _node_ids("sync", [sync.name for sync in manifest.syncs])
    destination_ids = _node_ids("dst", [dst.name for dst in manifest.destinations])

    # Sources subgraph
    lines.append("    subgraph Sources")
    for src in manifest.sources:
        nid = source_ids[src.name]
        label = f"{_escape_label(src.name)}<br/><i>{_escape_label(src.type)}</i>"
        lines.append(f'        {nid}["{label}"]')
    lines.append("    end")

    # Syncs subgraph (hexagon shape with mode label)
    lines.append("    subgraph Syncs")
    for sync in manifest.syncs:
        nid = sync_ids[sync.name]
        label = f"{_escape_label(sync.name)}<br/><i>{_escape_label(sync.mode)}</i>"
        lines.append(f'        {nid}{{{{"{label}"}}}}')
    lines.append("    end")

    # Destinations subgraph
    lines.append("    subgraph Destinations")
    for dst in manifest.destinations:
        nid = destination_ids[dst.name]
        label = f"{_escape_label(dst.label)}<br/><i>{_escape_label(dst.type)}</i>"
        lines.append(f'        {nid}["{label}"]')
    lines.append("    end")

    # Edges
    for edge in manifest.edges:
        if edge.kind == "source_to_sync":
            from_id = source_ids.get(edge.from_, _node_id("src", edge.from_))
            to_id = sync_ids.get(edge.to, _node_id("sync", edge.to))
            lines.append(f"    {from_id} -->|extract| {to_id}")
        elif edge.kind == "sync_to_destination":
            from_id = sync_ids.get(edge.from_, _node_id("sync", edge.from_))
            to_id = destination_ids.get(edge.to, _node_id("dst", edge.to))
            lines.append(f"    {from_id} -->|load| {to_id}")
        elif edge.kind == "lookup":
            from_id = sync_ids.get(edge.from_, _node_id("sync", edge.from_))
            to_id = sync_ids.get(edge.to, _node_id("sync", edge.to))
            lines.append(f"    {from_id} -.lookup.-> {to_id}")

    return "\n".join(lines)
