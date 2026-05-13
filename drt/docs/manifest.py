"""Manifest dataclasses for `drt docs generate`.

These describe the catalog of a drt project — syncs, sources, destinations,
and the edges between them. Schema is versioned (`schema_version`) and
considered a public surface; see ADR-0001 in #500.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

SCHEMA_VERSION = 1

EdgeKind = Literal["source_to_sync", "sync_to_destination", "lookup"]


@dataclass(frozen=True)
class Source:
    name: str
    type: str


@dataclass(frozen=True)
class Destination:
    name: str  # synthesized node id (stable across runs)
    type: str
    label: str  # human-readable describe(), e.g. "slack (#alerts)"


@dataclass(frozen=True)
class Sync:
    name: str
    source: str  # Source.name
    destination: str  # Destination.name
    mode: str  # SyncOptions.mode
    description: str = ""
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Edge:
    kind: EdgeKind
    from_: str
    to: str


@dataclass
class Manifest:
    schema_version: int
    drt_version: str
    syncs: list[Sync] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
    destinations: list[Destination] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
