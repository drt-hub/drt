"""Mermaid renderer for drt documentation manifests."""

from __future__ import annotations

from drt.docs.manifest import Edge, Manifest


def render_mermaid(manifest: Manifest) -> str:
    """Render a project manifest as a Mermaid left-to-right DAG."""
    lines = ["graph LR"]

    lines.append("  subgraph Sources")
    for source in manifest.sources:
        lines.append(f"    {source.id}[{_label(source.name)}]")
    lines.append("  end")

    lines.append("  subgraph Syncs")
    for sync in manifest.syncs:
        lines.append(f"    {sync.id}{{{{{_label(sync.name)}<br/>{_label(sync.mode)}}}}}")
    lines.append("  end")

    lines.append("  subgraph Destinations")
    for destination in manifest.destinations:
        lines.append(f"    {destination.id}[{_label(destination.name)}]")
    lines.append("  end")

    for edge in manifest.edges:
        lines.append(_render_edge(edge))

    return "\n".join(lines) + "\n"


def _render_edge(edge: Edge) -> str:
    if edge.kind == "source_to_sync":
        return f"  {edge.from_id} -->|extract| {edge.to_id}"
    if edge.kind == "sync_to_destination":
        return f"  {edge.from_id} -->|load| {edge.to_id}"
    return f"  {edge.from_id} -.lookup.-> {edge.to_id}"


def _label(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("[", "&#91;")
        .replace("]", "&#93;")
        .replace("{", "&#123;")
        .replace("}", "&#125;")
        .replace("\n", " ")
    )
