"""Build project documentation manifests from drt config files."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from drt.config.models import ProjectConfig
from drt.config.parser import load_project, load_syncs
from drt.docs.manifest import Destination, Edge, Manifest, Source, Sync

_NODE_ID_PATTERN = re.compile(r"[^0-9a-zA-Z_]+")


def build_manifest(project_dir: Path = Path("."), include_state: bool = False) -> Manifest:
    """Build a documentation manifest for a drt project.

    ``include_state`` is accepted for the future state overlay flag; current P1
    graph output is based only on static project and sync configuration.
    """
    _ = include_state
    project = _load_project_if_present(project_dir)
    sync_configs = load_syncs(project_dir)
    project_name = project.name if project else project_dir.resolve().name

    source_name = project.profile if project else "default"
    source_type = project.source.type if project and project.source else source_name
    used_ids: set[str] = set()
    source_id = _node_id("src", source_name, used_ids)

    syncs: list[Sync] = []
    destinations_by_key: dict[tuple[str, str], Destination] = {}
    destination_consumers: dict[str, list[str]] = {}
    edges: list[Edge] = []
    table_to_sync_id: dict[str, str] = {}

    for sync_config in sync_configs:
        destination_type = _destination_type(sync_config.destination)
        destination_name = _destination_name(sync_config.destination)
        destination_key = (destination_name, destination_type)
        destination = destinations_by_key.get(destination_key)
        if destination is None:
            destination = Destination(
                id=_node_id("dst", f"{destination_type}_{destination_name}", used_ids),
                name=destination_name,
                type=destination_type,
            )
            destinations_by_key[destination_key] = destination
            destination_consumers[destination.id] = []

        sync = Sync(
            id=_node_id("sync", sync_config.name, used_ids),
            name=sync_config.name,
            mode=sync_config.sync.mode,
            source_id=source_id,
            destination_id=destination.id,
            tags=tuple(sync_config.tags),
        )
        syncs.append(sync)
        destination_consumers[destination.id].append(sync.id)
        edges.append(Edge(kind="source_to_sync", from_id=source_id, to_id=sync.id))
        edges.append(Edge(kind="sync_to_destination", from_id=sync.id, to_id=destination.id))

        for table_name in _destination_table_aliases(sync_config.destination):
            table_to_sync_id.setdefault(table_name, sync.id)

    lookup_edges_seen: set[tuple[str, str]] = set()
    for sync, sync_config in zip(syncs, sync_configs, strict=True):
        for lookup_table in _lookup_tables(sync_config.destination):
            target_sync_id = _find_lookup_target(lookup_table, table_to_sync_id)
            if target_sync_id is None or target_sync_id == sync.id:
                continue
            edge_key = (sync.id, target_sync_id)
            if edge_key in lookup_edges_seen:
                continue
            lookup_edges_seen.add(edge_key)
            edges.append(Edge(kind="lookup", from_id=sync.id, to_id=target_sync_id))

    source = Source(
        id=source_id,
        name=source_name,
        type=source_type,
        consumed_by=tuple(sync.id for sync in syncs),
    )
    destinations = tuple(
        Destination(
            id=destination.id,
            name=destination.name,
            type=destination.type,
            produced_by=tuple(destination_consumers[destination.id]),
        )
        for destination in destinations_by_key.values()
    )

    return Manifest(
        project_name=project_name,
        project_root=str(project_dir.resolve()),
        sources=(source,),
        syncs=tuple(syncs),
        destinations=destinations,
        edges=tuple(edges),
    )


def _load_project_if_present(project_dir: Path) -> ProjectConfig | None:
    try:
        return load_project(project_dir)
    except FileNotFoundError:
        return None


def _node_id(prefix: str, raw: str, used_ids: set[str]) -> str:
    slug = _NODE_ID_PATTERN.sub("_", raw.lower()).strip("_")
    if not slug:
        slug = prefix
    if slug[0].isdigit():
        slug = f"{prefix}_{slug}"
    if not slug.startswith(f"{prefix}_"):
        slug = f"{prefix}_{slug}"

    candidate = slug
    suffix = 2
    while candidate in used_ids:
        candidate = f"{slug}_{suffix}"
        suffix += 1
    used_ids.add(candidate)
    return candidate


def _destination_type(destination: Any) -> str:
    return str(getattr(destination, "type", "destination"))


def _destination_name(destination: Any) -> str:
    table = getattr(destination, "table", None)
    if table:
        return str(table)

    object_type = getattr(destination, "object_type", None)
    if object_type:
        return f"{_destination_type(destination)}_{object_type}"

    sheet = getattr(destination, "sheet", None)
    if sheet:
        return f"google_sheets_{sheet}"

    path = getattr(destination, "path", None)
    if path:
        return str(path)

    url = getattr(destination, "url", None)
    if url:
        parsed = urlparse(str(url))
        return f"{parsed.netloc}{parsed.path}" if parsed.netloc else str(url)

    owner = getattr(destination, "owner", None)
    repo = getattr(destination, "repo", None)
    if owner and repo:
        return f"{owner}/{repo}"

    database_id = getattr(destination, "database_id", None)
    if database_id:
        return "notion_database"

    customer_id = getattr(destination, "customer_id", None)
    if customer_id:
        return f"google_ads_{customer_id}"

    from_number = getattr(destination, "from_number", None)
    if from_number:
        return f"twilio_{from_number}"

    return _destination_type(destination)


def _destination_table_aliases(destination: Any) -> tuple[str, ...]:
    table = getattr(destination, "table", None)
    if not table:
        return ()
    return tuple(_table_aliases(str(table)))


def _lookup_tables(destination: Any) -> tuple[str, ...]:
    lookups = getattr(destination, "lookups", None)
    if not lookups:
        return ()
    return tuple(str(lookup.table) for lookup in lookups.values())


def _find_lookup_target(lookup_table: str, table_to_sync_id: dict[str, str]) -> str | None:
    for alias in _table_aliases(lookup_table):
        target = table_to_sync_id.get(alias)
        if target:
            return target
    return None


def _table_aliases(table: str) -> Iterable[str]:
    yield table
    if "." in table:
        yield table.rsplit(".", maxsplit=1)[-1]
