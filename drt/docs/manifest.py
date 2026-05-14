"""Manifest dataclasses for `drt docs generate`.

These describe the catalog of a drt project — syncs, sources, destinations,
edges, and (optionally) per-sync run state. Schema is versioned
(`SCHEMA_VERSION`) and considered a public surface; see ADR #500 and
sub-issue #507.

Public field names are stable across drt versions per VERSIONING.md.
Breaking the JSON shape requires bumping ``SCHEMA_VERSION`` and a migration
note. Internal :class:`drt.state.manager.SyncState` field names are
intentionally renamed in the public ``state`` block; see ``SyncStateSnapshot``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

SCHEMA_VERSION = 1

EdgeKind = Literal["source_to_sync", "sync_to_destination", "lookup"]


@dataclass(frozen=True)
class Project:
    name: str
    profile: str


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
class SyncStateSnapshot:
    """Public snapshot of the latest run state for a sync.

    Field names are part of the public schema and renamed from
    :class:`drt.state.manager.SyncState` so the manifest stays stable
    even when the runtime persistence layer evolves.
    """

    last_sync_at: str
    last_cursor_value: str | None
    rows_synced: int
    last_status: str
    last_error: str | None


@dataclass(frozen=True)
class Sync:
    name: str
    source: str  # Source.name
    destination: str  # Destination.name
    mode: str  # SyncOptions.mode
    description: str = ""
    tags: tuple[str, ...] = ()
    state: SyncStateSnapshot | None = None


@dataclass(frozen=True)
class Edge:
    kind: EdgeKind
    from_: str
    to: str


@dataclass
class Manifest:
    schema_version: int
    drt_version: str
    generated_at: str = ""  # ISO-8601 UTC, set by the builder
    project: Project | None = None
    syncs: list[Sync] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
    destinations: list[Destination] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict matching schema v1."""
        return {
            "schema_version": self.schema_version,
            "drt_version": self.drt_version,
            "generated_at": self.generated_at,
            "project": (
                {"name": self.project.name, "profile": self.project.profile}
                if self.project is not None
                else None
            ),
            "syncs": [_sync_to_dict(s) for s in self.syncs],
            "sources": [{"name": s.name, "type": s.type} for s in self.sources],
            "destinations": [
                {"name": d.name, "type": d.type, "label": d.label} for d in self.destinations
            ],
            "edges": [{"kind": e.kind, "from": e.from_, "to": e.to} for e in self.edges],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Manifest:
        """Reconstruct a Manifest from a schema-v1 dict (round-trip safe)."""
        project_data = data.get("project")
        project = Project(**project_data) if project_data else None
        return cls(
            schema_version=data["schema_version"],
            drt_version=data["drt_version"],
            generated_at=data.get("generated_at", ""),
            project=project,
            syncs=[_sync_from_dict(s) for s in data.get("syncs", [])],
            sources=[Source(**s) for s in data.get("sources", [])],
            destinations=[Destination(**d) for d in data.get("destinations", [])],
            edges=[
                Edge(kind=e["kind"], from_=e["from"], to=e["to"]) for e in data.get("edges", [])
            ],
        )


def _sync_to_dict(s: Sync) -> dict[str, Any]:
    d: dict[str, Any] = {
        "name": s.name,
        "source": s.source,
        "destination": s.destination,
        "mode": s.mode,
        "description": s.description,
        "tags": list(s.tags),
    }
    if s.state is not None:
        d["state"] = {
            "last_sync_at": s.state.last_sync_at,
            "last_cursor_value": s.state.last_cursor_value,
            "rows_synced": s.state.rows_synced,
            "last_status": s.state.last_status,
            "last_error": s.state.last_error,
        }
    return d


def _sync_from_dict(d: dict[str, Any]) -> Sync:
    state_data = d.get("state")
    state = SyncStateSnapshot(**state_data) if state_data else None
    return Sync(
        name=d["name"],
        source=d["source"],
        destination=d["destination"],
        mode=d["mode"],
        description=d.get("description", ""),
        tags=tuple(d.get("tags", [])),
        state=state,
    )
