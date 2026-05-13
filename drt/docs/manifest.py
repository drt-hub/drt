"""Serializable project documentation manifest types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Source:
    """A source node consumed by one or more syncs."""

    id: str
    name: str
    type: str
    consumed_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class Sync:
    """A configured sync node."""

    id: str
    name: str
    mode: str
    source_id: str
    destination_id: str
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Destination:
    """A destination node produced by one or more syncs."""

    id: str
    name: str
    type: str
    produced_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class Edge:
    """A relationship between documentation graph nodes."""

    kind: Literal["source_to_sync", "sync_to_destination", "lookup"]
    from_id: str
    to_id: str


@dataclass(frozen=True)
class Manifest:
    """A project-level graph manifest used by docs renderers."""

    project_name: str
    project_root: str
    sources: tuple[Source, ...]
    syncs: tuple[Sync, ...]
    destinations: tuple[Destination, ...]
    edges: tuple[Edge, ...]
