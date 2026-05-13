"""Build a `Manifest` from a drt project on disk."""

from __future__ import annotations

import re
from pathlib import Path

from drt import __version__
from drt.config.models import SyncConfig
from drt.config.parser import load_project, load_syncs_safe
from drt.docs.manifest import (
    SCHEMA_VERSION,
    Destination,
    Edge,
    Manifest,
    Source,
    Sync,
)

_SLUG_RE = re.compile(r"[^A-Za-z0-9_]+")


def _slug(value: str) -> str:
    """Mermaid-safe node id."""
    return _SLUG_RE.sub("_", value).strip("_") or "node"


def _destination_id(sync: SyncConfig) -> str:
    """Synthesize a stable destination node id; shared across syncs that target the same one."""
    dest = sync.destination
    # describe() is type-specific (e.g. "postgres (public.users)", "slack (#alerts)")
    return f"dest_{_slug(dest.type)}_{_slug(dest.describe())}"


def _destination_table(sync: SyncConfig) -> str | None:
    """SQL-shaped destinations expose `.table`; non-SQL ones don't."""
    return getattr(sync.destination, "table", None)


def _destination_lookup_tables(sync: SyncConfig) -> list[str]:
    """Tables referenced via `lookups` on the destination, if any."""
    lookups = getattr(sync.destination, "lookups", None)
    if not lookups:
        return []
    return [cfg.table for cfg in lookups.values()]


def build_manifest(project_dir: Path = Path(".")) -> Manifest:
    """Build a `Manifest` from sync YAMLs + project config under *project_dir*.

    State is NOT included in P1 (Mermaid output doesn't render it; `--no-state`
    is parsed but no-op in P1).
    """
    project = load_project(project_dir)
    syncs_result = load_syncs_safe(project_dir)

    # Source: project.profile is authoritative; type from inline ProjectConfig.source if present,
    # else "configured" (operator can resolve to a concrete type later via profiles.yml).
    source_type = project.source.type if project.source else "configured"
    source = Source(name=project.profile, type=source_type)

    destinations: dict[str, Destination] = {}
    syncs: list[Sync] = []
    edges: list[Edge] = []

    for sync_cfg in syncs_result.syncs:
        dest_id = _destination_id(sync_cfg)
        if dest_id not in destinations:
            destinations[dest_id] = Destination(
                name=dest_id,
                type=sync_cfg.destination.type,
                label=sync_cfg.destination.describe(),
            )

        syncs.append(
            Sync(
                name=sync_cfg.name,
                source=source.name,
                destination=dest_id,
                mode=sync_cfg.sync.mode,
                description=sync_cfg.description,
                tags=tuple(sync_cfg.tags),
            )
        )

        edges.append(Edge(kind="source_to_sync", from_=source.name, to=sync_cfg.name))
        edges.append(Edge(kind="sync_to_destination", from_=sync_cfg.name, to=dest_id))

    # Heuristic lookup edges: if sync A's destination has lookups pointing at a table
    # that matches sync B's destination.table, draw a `B -> A` lookup edge (A depends on B).
    sync_by_dest_table: dict[str, str] = {}
    for sync_cfg in syncs_result.syncs:
        table = _destination_table(sync_cfg)
        if table:
            sync_by_dest_table[table] = sync_cfg.name

    for sync_cfg in syncs_result.syncs:
        for lookup_table in _destination_lookup_tables(sync_cfg):
            producer = sync_by_dest_table.get(lookup_table)
            if producer and producer != sync_cfg.name:
                edges.append(Edge(kind="lookup", from_=producer, to=sync_cfg.name))

    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version=__version__,
        syncs=syncs,
        sources=[source],
        destinations=list(destinations.values()),
        edges=edges,
    )
