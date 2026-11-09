"""Manifest dataclasses for `drt docs generate`.

These describe the catalog of a drt project — syncs, sources, destinations,
edges, and (optionally) per-sync run state and history. Schema is versioned
(`SCHEMA_VERSION`) and considered a public surface; see ADR 0001 (#500,
sub-issue #507) for v1, ADR 0003 (#698) for v2, and ADR 0006 (#772) for v3.

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

Schema v3 (#772) is a pure superset of v2 — one addition on each sync,
nothing renamed or removed, so v2 consumers keep working unchanged:

- ``config_hash``: stable fingerprint of the sync definition for baseline
  comparison by the ``state:modified`` selector. Like every other
  optional v2+ field (``state``, ``runs``, ``fields``, ``dlq_depth``), it
  is omitted from the serialized dict rather than emitted as ``null`` when
  absent — this keeps a re-serialized v1/v2 manifest (loaded via
  :meth:`Manifest.from_dict` with no ``config_hash`` in the source data)
  free of a schema-v3-only key its declared ``schema_version`` doesn't
  actually support.

Schema v4 (#1222, ADR 0013) is a pure superset of v3 — three optional keys on
each ``runs[]`` entry, nothing renamed or removed, omitted (never ``null``)
when absent:

- ``plan_id``: the plan a ``drt apply`` run applied (an opaque digest id).
- ``guards``: the run-time guard outcome, ``{"tripped": [...], "forced": bool}``.
- ``approved_by``: who approved the apply. A person's name, so — like the other
  labels redacted by default (#696) — it is emitted only with ``--full-labels``.

Schema v5 (#1226, ADR 0015) is a pure superset of v4 — two optional keys on each
sync, omitted (never ``null``) when absent:

- ``meta``: ``{owner, owner_email, maturity}`` from the sync's ``meta`` block.
  ``owner_email`` is personal data and appears only with ``--full-labels``.
- ``freshness``: the *declared* ``alerts.on_stale`` contract
  (``{max_age_seconds, never_run}``). Deliberately not a verdict: a fresh/stale
  judgement depends on the clock and would make regeneration non-deterministic (#697).

Schema v6 (#761, ADR 0017) is a pure superset of v5 — one optional key on each
sync, omitted when absent:

- ``columns``: the model-output declaration as ``{name, type, description?}``.
  This is repository configuration, not run state, so regeneration remains
  byte-identical. Manifests from v1-v5 continue to load with no columns.

Schema v7 (ADR 0019) is a pure superset of v6. All additions are optional and
omitted when absent, so manifests from v1-v6 continue to load unchanged:

- ``runs[].exact`` records exact-replay applies.
- ``destinations[].plan`` carries the deterministic declaration from
  :mod:`drt.engine.plan_coverage`.
- ``syncs[].plan`` applies sync-level planner blockers to that destination
  declaration.
- ``syncs[].columns_source`` distinguishes dbt-derived and hand-written
  column declarations.
- ``syncs[].manage_table`` / ``on_schema_change`` carry explicitly
  authored managed-table settings.

Schema v8 (#1045, ADR 0020) is a pure superset of v7. Its optional
``syncs[].sources`` list carries every source node discovered from a supported
raw-SQL model. The older scalar ``syncs[].source`` remains the deterministic
first input for consumers that only understand one source. ``sources`` is
omitted when no SQL-derived lineage is available, so v1-v7 manifests keep
their serialized shape when they are loaded and written again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

SCHEMA_VERSION = 8

EdgeKind = Literal["source_to_sync", "sync_to_destination", "lookup"]
PlanStatus = Literal["available", "partial", "api-reads", "needs-api-diff", "unavailable"]
ColumnsSource = Literal["dbt", "declared"]
SchemaChangePolicy = Literal["fail", "ignore", "append_new_columns"]


@dataclass(frozen=True)
class Project:
    name: str
    profile: str


@dataclass(frozen=True)
class Source:
    name: str
    type: str


@dataclass(frozen=True)
class DestinationPlan:
    """Declared ``drt plan`` coverage for a built-in destination (schema v7)."""

    status: PlanStatus
    reason: str = ""


@dataclass(frozen=True)
class Destination:
    name: str  # synthesized node id (stable across runs)
    type: str
    label: str  # human-readable describe(), e.g. "slack (#alerts)"
    plan: DestinationPlan | None = None


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
    # Schema v4 (#1222, ADR 0013); see the module docstring.
    plan_id: str | None = None
    guards: dict[str, Any] | None = None
    approved_by: str | None = None  # only populated with --full-labels
    exact: bool | None = None  # schema v7; omitted for ordinary runs


@dataclass(frozen=True)
class SyncMeta:
    """Catalog metadata from a sync's ``meta`` block (schema v5, #1226)."""

    owner: str | None = None
    owner_email: str | None = None  # only populated with --full-labels
    maturity: str = "high"  # "high" | "medium" | "low"


@dataclass(frozen=True)
class SyncFreshness:
    """The declared ``alerts.on_stale`` contract (schema v5, #1226): not a verdict."""

    max_age_seconds: int
    never_run: str = "warn"  # "warn" | "error"


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
class SyncColumn:
    """A portable model-output column declaration (schema v6, #761)."""

    name: str
    type: str
    description: str | None = None


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
    # Defensive None: normal filesystem-backed syncs have a fingerprint.
    config_hash: str | None = None
    meta: SyncMeta | None = None  # schema v5
    freshness: SyncFreshness | None = None  # schema v5
    columns: tuple[SyncColumn, ...] = ()  # schema v6; declaration order
    columns_source: ColumnsSource | None = None  # schema v7; only with columns
    plan: DestinationPlan | None = None  # schema v7; destination coverage + sync blockers
    manage_table: bool | None = None  # schema v7; explicitly authored only
    on_schema_change: SchemaChangePolicy | None = None  # schema v7; explicitly authored only
    # Schema v8: complete, ordered input relation list for parsed raw SQL.
    # ``source`` remains the deterministic first input for legacy consumers.
    sources: tuple[str, ...] = ()


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
        """Serialize to a JSON-compatible, schema-versioned dict."""
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
            "destinations": [_destination_to_dict(d) for d in self.destinations],
            "edges": [{"kind": e.kind, "from": e.from_, "to": e.to} for e in self.edges],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Manifest:
        """Reconstruct a Manifest from a schema-versioned dict (round-trip safe)."""
        project_data = data.get("project")
        project = Project(**project_data) if project_data else None
        return cls(
            schema_version=data["schema_version"],
            drt_version=data["drt_version"],
            generated_at=data.get("generated_at", ""),
            project=project,
            syncs=[_sync_from_dict(s) for s in data.get("syncs", [])],
            sources=[Source(**s) for s in data.get("sources", [])],
            destinations=[_destination_from_dict(d) for d in data.get("destinations", [])],
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
    if s.config_hash is not None:
        d["config_hash"] = s.config_hash
    if s.sources:
        d["sources"] = list(s.sources)
    if s.state is not None:
        d["state"] = {
            "last_sync_at": s.state.last_sync_at,
            "last_cursor_value": s.state.last_cursor_value,
            "rows_synced": s.state.rows_synced,
            "last_status": s.state.last_status,
            "last_error": s.state.last_error,
        }
    if s.runs:
        d["runs"] = [_run_to_dict(r) for r in s.runs]
    if s.fields:
        d["fields"] = [
            {"name": f.name, "source_name": f.source_name, "mask": f.mask} for f in s.fields
        ]
    if s.dlq_depth is not None:
        d["dlq_depth"] = s.dlq_depth
    if s.meta is not None:
        d["meta"] = {
            key: value
            for key, value in (
                ("owner", s.meta.owner),
                ("owner_email", s.meta.owner_email),
                ("maturity", s.meta.maturity),
            )
            if value is not None
        }
    if s.freshness is not None:
        d["freshness"] = {
            "max_age_seconds": s.freshness.max_age_seconds,
            "never_run": s.freshness.never_run,
        }
    if s.columns:
        d["columns"] = [
            {
                key: value
                for key, value in (
                    ("name", column.name),
                    ("type", column.type),
                    ("description", column.description),
                )
                if value is not None
            }
            for column in s.columns
        ]
    if s.columns_source is not None:
        d["columns_source"] = s.columns_source
    if s.plan is not None:
        d["plan"] = {"status": s.plan.status, "reason": s.plan.reason}
    if s.manage_table is not None:
        d["manage_table"] = s.manage_table
    if s.on_schema_change is not None:
        d["on_schema_change"] = s.on_schema_change
    return d


def _run_to_dict(r: SyncRun) -> dict[str, Any]:
    d: dict[str, Any] = {
        "started_at": r.started_at,
        "completed_at": r.completed_at,
        "duration_seconds": r.duration_seconds,
        "status": r.status,
        "records_synced": r.records_synced,
        "records_failed": r.records_failed,
        "errors": list(r.errors),
        "cursor_value_used": r.cursor_value_used,
    }
    # v4 keys: omitted rather than null, so a re-serialized v1-v3 manifest does
    # not grow keys its declared schema_version does not support.
    if r.plan_id is not None:
        d["plan_id"] = r.plan_id
    if r.guards is not None:
        d["guards"] = r.guards
    if r.approved_by is not None:
        d["approved_by"] = r.approved_by
    if r.exact is not None:
        d["exact"] = r.exact
    return d


def _destination_to_dict(destination: Destination) -> dict[str, Any]:
    data: dict[str, Any] = {
        "name": destination.name,
        "type": destination.type,
        "label": destination.label,
    }
    if destination.plan is not None:
        data["plan"] = {
            "status": destination.plan.status,
            "reason": destination.plan.reason,
        }
    return data


def _destination_from_dict(data: dict[str, Any]) -> Destination:
    unknown = set(data) - {
        "name",
        "type",
        "label",
        "plan",
    }
    if unknown:
        raise ValueError(f"manifest destinations[] has unknown key(s): {sorted(unknown)}")
    plan_data = data.get("plan")
    if plan_data is not None:
        if (
            not isinstance(plan_data, dict)
            or set(plan_data) != {"status", "reason"}
            or plan_data.get("status")
            not in {"available", "partial", "api-reads", "needs-api-diff", "unavailable"}
            or not isinstance(plan_data.get("reason"), str)
        ):
            raise ValueError("manifest destinations[].plan is malformed")
        plan = DestinationPlan(**plan_data)
    else:
        plan = None
    return Destination(
        name=data["name"],
        type=data["type"],
        label=data["label"],
        plan=plan,
    )


def _meta_from_dict(data: Any) -> SyncMeta | None:
    if data is None:
        return None
    if not isinstance(data, dict):
        raise ValueError("manifest syncs[].meta must be an object")
    unknown = set(data) - {"owner", "owner_email", "maturity"}
    if unknown:
        raise ValueError(f"manifest syncs[].meta has unknown key(s): {sorted(unknown)}")
    for key in ("owner", "owner_email"):
        if data.get(key) is not None and not isinstance(data[key], str):
            raise ValueError(f"manifest syncs[].meta.{key} must be a string")
    if data.get("maturity", "high") not in {"high", "medium", "low"}:
        raise ValueError("manifest syncs[].meta.maturity must be high, medium or low")
    return SyncMeta(**data)


def _freshness_from_dict(data: Any) -> SyncFreshness | None:
    if data is None:
        return None
    if (
        not isinstance(data, dict)
        or set(data) - {"max_age_seconds", "never_run"}
        or not isinstance(data.get("max_age_seconds"), int)
        or isinstance(data.get("max_age_seconds"), bool)
        or data.get("never_run", "warn") not in {"warn", "error"}
    ):
        raise ValueError("manifest syncs[].freshness is malformed")
    return SyncFreshness(**data)


def _sync_from_dict(d: dict[str, Any]) -> Sync:
    state_data = d.get("state")
    state = SyncStateSnapshot(**state_data) if state_data else None
    columns_source = d.get("columns_source")
    if columns_source is not None and columns_source not in {"dbt", "declared"}:
        raise ValueError("manifest syncs[].columns_source must be dbt or declared")
    plan_data = d.get("plan")
    if plan_data is not None:
        if (
            not isinstance(plan_data, dict)
            or set(plan_data) != {"status", "reason"}
            or plan_data.get("status")
            not in {"available", "partial", "api-reads", "needs-api-diff", "unavailable"}
            or not isinstance(plan_data.get("reason"), str)
        ):
            raise ValueError("manifest syncs[].plan is malformed")
        plan = DestinationPlan(**plan_data)
    else:
        plan = None
    manage_table = d.get("manage_table")
    if manage_table is not None and not isinstance(manage_table, bool):
        raise ValueError("manifest syncs[].manage_table must be a boolean")
    on_schema_change = d.get("on_schema_change")
    if on_schema_change is not None and on_schema_change not in {
        "fail",
        "ignore",
        "append_new_columns",
    }:
        raise ValueError("manifest syncs[].on_schema_change is malformed")
    sources = d.get("sources", [])
    if not isinstance(sources, list) or not all(isinstance(source, str) for source in sources):
        raise ValueError("manifest syncs[].sources must be an array of strings")
    runs: list[SyncRun] = []
    for run_data in d.get("runs", []):
        if "exact" in run_data and not isinstance(run_data["exact"], bool):
            raise ValueError("manifest syncs[].runs[].exact must be a boolean")
        runs.append(SyncRun(**{**run_data, "errors": tuple(run_data.get("errors", []))}))
    return Sync(
        name=d["name"],
        source=d["source"],
        destination=d["destination"],
        mode=d["mode"],
        description=d.get("description", ""),
        tags=tuple(d.get("tags", [])),
        state=state,
        runs=tuple(runs),
        fields=tuple(SyncField(**f) for f in d.get("fields", [])),
        dlq_depth=d.get("dlq_depth"),
        config_hash=d.get("config_hash"),
        meta=_meta_from_dict(d.get("meta")),
        freshness=_freshness_from_dict(d.get("freshness")),
        columns=tuple(SyncColumn(**column) for column in d.get("columns", [])),
        columns_source=columns_source,
        plan=plan,
        manage_table=manage_table,
        on_schema_change=on_schema_change,
        sources=tuple(sources),
    )
