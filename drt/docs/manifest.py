"""Manifest dataclasses for `drt docs generate`.

These describe the catalog of a drt project — syncs, sources, destinations,
edges, and (optionally) per-sync run state and history. Schema is versioned
(`SCHEMA_VERSION`) and considered a public surface; see ADR 0001 (#500,
sub-issue #507) for v1 and ADR 0003 (#698) for v2.

Public field names are stable across drt versions per VERSIONING.md.
Breaking the JSON shape requires bumping ``SCHEMA_VERSION`` and a migration
note. Internal :class:`drt.state.manager.SyncState` field names are
intentionally renamed in the public ``state`` block; see ``SyncStateSnapshot``.

Schema v2 (#698) is a pure superset of v1 — three optional additions on each
sync, nothing renamed or removed, so v1 consumers keep working unchanged:

- ``runs``: recent execution history (public shape of
  :class:`drt.state.history.HistoryEntry`), newest first.
- ``fields``: declared write-side column facts from ``sync.field_mappings``
  and ``sync.mask`` — the column-level lineage source (#808).
- ``dlq_depth``: current Dead Letter Queue depth for the sync.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

SCHEMA_VERSION = 2

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
class SyncRun:
    """One historical execution of a sync (schema v2, #698).

    Public shape of :class:`drt.state.history.HistoryEntry`. ``sync_name`` is
    dropped (runs nest under their sync) and ``dry_run`` is dropped (always
    False on disk — reserved). ``errors`` text is redacted by the builder
    unless ``--full-labels`` (#696 policy, see ADR 0003).
    """

    started_at: str  # ISO-8601 UTC
    completed_at: str  # ISO-8601 UTC
    duration_seconds: float
    status: str  # "success" | "partial" | "failed"
    records_synced: int
    records_failed: int
    errors: tuple[str, ...] = ()
    cursor_value_used: str | None = None


@dataclass(frozen=True)
class SyncField:
    """A declared write-side column fact (schema v2, #808 column lineage).

    Built from ``sync.field_mappings`` (source column → destination field)
    and ``sync.mask`` (whose keys reference the post-rename name). Only
    *declared* columns appear — drt does not parse model SQL, so this is
    never the full column set.
    """

    name: str  # destination-side field name (post-rename)
    source_name: str  # pre-rename column; equals ``name`` when not renamed
    mask: str | None = None  # mask strategy ("hash" | "redact" | "truncate")


@dataclass(frozen=True)
class Sync:
    name: str
    source: str  # Source.name
    destination: str  # Destination.name
    mode: str  # SyncOptions.mode
    description: str = ""
    tags: tuple[str, ...] = ()
    state: SyncStateSnapshot | None = None
    runs: tuple[SyncRun, ...] = ()  # newest first; empty when state excluded
    fields: tuple[SyncField, ...] = ()  # declared columns only (v2)
    dlq_depth: int | None = None  # None when state excluded


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
    if s.runs:
        d["runs"] = [
            {
                "started_at": r.started_at,
                "completed_at": r.completed_at,
                "duration_seconds": r.duration_seconds,
                "status": r.status,
                "records_synced": r.records_synced,
                "records_failed": r.records_failed,
                "errors": list(r.errors),
                "cursor_value_used": r.cursor_value_used,
            }
            for r in s.runs
        ]
    if s.fields:
        d["fields"] = [
            {"name": f.name, "source_name": f.source_name, "mask": f.mask} for f in s.fields
        ]
    if s.dlq_depth is not None:
        d["dlq_depth"] = s.dlq_depth
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
        runs=tuple(
            SyncRun(**{**r, "errors": tuple(r.get("errors", []))}) for r in d.get("runs", [])
        ),
        fields=tuple(SyncField(**f) for f in d.get("fields", [])),
        dlq_depth=d.get("dlq_depth"),
    )
