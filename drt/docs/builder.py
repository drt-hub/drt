"""Build a `Manifest` from a drt project on disk."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from drt import __version__
from drt.config.models import SyncConfig
from drt.config.parser import load_project, load_syncs_safe
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
from drt.state.manager import StateManager, SyncState

_SLUG_RE = re.compile(r"[^A-Za-z0-9_]+")


def _slug(value: str) -> str:
    """Mermaid-safe node id."""
    return _SLUG_RE.sub("_", value).strip("_") or "node"


def _destination_id(sync: SyncConfig) -> str:
    """Synthesize a stable destination node id; shared across syncs that target the same one."""
    dest = sync.destination
    return f"dest_{_slug(dest.type)}_{_slug(dest.describe())}"


def _destination_table(sync: SyncConfig) -> str | None:
    """SQL-shaped destinations expose `.table`; non-SQL ones do not."""
    table = getattr(sync.destination, "table", None)
    return str(table) if table else None


def _destination_lookup_tables(sync: SyncConfig) -> list[str]:
    """Tables referenced via `lookups` on the destination, if any."""
    lookups = getattr(sync.destination, "lookups", None)
    if not lookups:
        return []
    return [str(cfg.table) for cfg in lookups.values()]


def _table_aliases(table: str) -> tuple[str, ...]:
    aliases = [table]
    if "." in table:
        aliases.append(table.rsplit(".", maxsplit=1)[-1])
    return tuple(dict.fromkeys(aliases))


def _state_snapshot(state: SyncState) -> SyncStateSnapshot:
    """Rename internal SyncState fields to the public manifest schema (v1)."""
    return SyncStateSnapshot(
        last_sync_at=state.last_run_at,
        last_cursor_value=state.last_cursor_value,
        rows_synced=state.records_synced,
        last_status=state.status,
        last_error=state.error,
    )


def build_manifest(project_dir: Path = Path("."), include_state: bool = False) -> Manifest:
    """Build a `Manifest` from sync YAMLs + project config under *project_dir*.

    When *include_state* is true, each sync's latest persisted state from
    ``.drt/state.json`` is attached as :class:`SyncStateSnapshot` under the
    sync's ``state`` field. Syncs that have never run are emitted with
    ``state=None`` (and the public schema omits the ``state`` block).
    """
    project = load_project(project_dir)
    syncs_result = load_syncs_safe(project_dir)

    states: dict[str, SyncState] = {}
    if include_state:
        states = StateManager(project_dir).get_all()

    # Source: project.profile is authoritative; type from inline ProjectConfig.source if present,
    # else "configured" because profiles.yml resolves the concrete type later.
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

        sync_state = states.get(sync_cfg.name)
        syncs.append(
            Sync(
                name=sync_cfg.name,
                source=source.name,
                destination=dest_id,
                mode=sync_cfg.sync.mode,
                description=sync_cfg.description,
                tags=tuple(sync_cfg.tags),
                state=_state_snapshot(sync_state) if sync_state else None,
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
            for alias in _table_aliases(table):
                sync_by_dest_table.setdefault(alias, sync_cfg.name)

    lookup_edges_seen: set[tuple[str, str]] = set()
    for sync_cfg in syncs_result.syncs:
        for lookup_table in _destination_lookup_tables(sync_cfg):
            producer = None
            for alias in _table_aliases(lookup_table):
                producer = sync_by_dest_table.get(alias)
                if producer:
                    break
            if not producer or producer == sync_cfg.name:
                continue
            edge_key = (producer, sync_cfg.name)
            if edge_key in lookup_edges_seen:
                continue
            lookup_edges_seen.add(edge_key)
            edges.append(Edge(kind="lookup", from_=producer, to=sync_cfg.name))

    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version=__version__,
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        project=Project(name=project.name, profile=project.profile),
        syncs=syncs,
        sources=[source],
        destinations=list(destinations.values()),
        edges=edges,
    )
