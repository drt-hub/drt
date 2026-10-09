"""Reviewable change-set artifact behind ``drt plan`` (#1216, epic #1227).

A plan is the *complete* key-level result of the same read-only diff that backs
``drt run --dry-run --diff`` (``drt/engine/diff.py``), serialized as a
versioned, deterministic document. This module is pure: it turns a
:class:`~drt.engine.diff.DiffResult` into a :class:`Plan` and renders it. It
never touches the destination or run state.

Determinism: entries are sorted, JSON is canonical, and ``digest`` /
``plan_id`` derive from content only. ``created_at`` is the one wall-clock
field (``drt apply --max-age`` needs it) and is excluded from both.

Redaction: row *values* never appear. Key columns are shown so a reviewer can
tell which record changes, except columns covered by ``sync.mask`` (or all key
columns under ``redact_keys``), which are replaced by an unsalted SHA-256
prefix. That hash supports comparing two plans; it does not make a
low-entropy key (an email address) unguessable.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from drt.engine.diff import DiffResult

PLAN_SCHEMA_VERSION = 1

# Review order: destructive actions last, so a reader meets them at the end of
# a long list rather than buried in the middle.
_ACTION_ORDER = {"create": 0, "insert": 1, "update": 2, "replace": 3, "delete": 4}


@dataclass
class PlanEntry:
    key: dict[str, Any]
    action: str
    changed_columns: list[str] = field(default_factory=list)
    delete_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"key": self.key, "action": self.action}
        if self.changed_columns:
            out["changed_columns"] = self.changed_columns
        if self.delete_reason:
            out["delete_reason"] = self.delete_reason
        return out


@dataclass
class Plan:
    sync_name: str
    sync_mode: str
    match_policy: str
    destination: str
    config_hash: str
    drt_version: str
    created_at: str
    cursor_hash: str | None = None
    available: bool = True
    unavailable_reason: str | None = None
    total_source_rows: int = 0
    total_destination_rows: int | None = None
    entries: list[PlanEntry] = field(default_factory=list)

    @property
    def summary(self) -> dict[str, int]:
        counts = {action: 0 for action in _ACTION_ORDER}
        for entry in self.entries:
            counts[entry.action] += 1
        return counts

    @property
    def has_changes(self) -> bool:
        return bool(self.entries)

    @property
    def digest(self) -> str:
        """Hash of the action set (what would change), not of the metadata."""
        body = {
            "sync": self.sync_name,
            "config_hash": self.config_hash,
            "entries": [e.to_dict() for e in self.entries],
        }
        return "sha256:" + _sha256(_canonical(body))

    @property
    def plan_id(self) -> str:
        seed = _canonical({"digest": self.digest, "cursor": self.cursor_hash})
        return "plan-" + _sha256(seed)[:16]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "plan_id": self.plan_id,
            "created_at": self.created_at,
            "drt_version": self.drt_version,
            "sync": {
                "name": self.sync_name,
                "mode": self.sync_mode,
                "match_policy": self.match_policy,
            },
            "destination": self.destination,
            "fingerprints": {
                "config_hash": self.config_hash,
                "cursor_hash": self.cursor_hash,
            },
            "status": {
                "available": self.available,
                "reason": self.unavailable_reason,
            },
            "summary": {
                **self.summary,
                "total_source_rows": self.total_source_rows,
                "total_destination_rows": self.total_destination_rows,
            },
            "digest": self.digest,
            "entries": [e.to_dict() for e in self.entries],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str) + "\n"


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_value(value: Any) -> str:
    return "sha256:" + _sha256(_canonical(value))[:16]


def config_hash(sync_config: Any) -> str:
    """Fingerprint of the parsed sync definition (a changed sync invalidates a plan)."""
    return "sha256:" + _sha256(_canonical(sync_config.model_dump(mode="json")))


def _key_of(
    record: dict[str, Any],
    key_columns: list[str],
    hashed_columns: set[str],
    redact_keys: bool,
) -> dict[str, Any]:
    key: dict[str, Any] = {}
    for column in key_columns:
        value = record.get(column)
        if redact_keys or column in hashed_columns:
            key[column] = hash_value(value)
        else:
            key[column] = value
    return key


def _entry_sort_key(entry: PlanEntry) -> tuple[int, str, str, str]:
    # Full tie-breaker: two entries for one key (a source with duplicate keys)
    # must not depend on the order an unordered query returned them in.
    return (
        _ACTION_ORDER[entry.action],
        _canonical(entry.key),
        _canonical(entry.changed_columns),
        entry.delete_reason or "",
    )


def build_plan(
    diff: DiffResult,
    *,
    sync_name: str,
    sync_mode: str,
    match_policy: str,
    destination: str,
    config_fingerprint: str,
    drt_version: str,
    key_columns: list[str],
    mask_columns: set[str] | None = None,
    redact_keys: bool = False,
    cursor_value: str | None = None,
    created_at: str | None = None,
) -> Plan:
    """Build a :class:`Plan` from a *complete* (un-truncated) diff.

    A diff that could not enumerate the change set yields an *unavailable* plan
    with a reason and no entries, never a partial one: a reviewer must not
    mistake "we could not look" for "nothing changes".
    """
    plan = Plan(
        sync_name=sync_name,
        sync_mode=sync_mode,
        match_policy=match_policy,
        destination=destination,
        config_hash=config_fingerprint,
        drt_version=drt_version,
        created_at=created_at or datetime.now(timezone.utc).isoformat(),
        cursor_hash=hash_value(cursor_value) if cursor_value is not None else None,
        total_source_rows=diff.total_source_rows,
        total_destination_rows=diff.total_destination_rows,
    )

    if not diff.supported:
        plan.available = False
        plan.unavailable_reason = diff.fallback_reason or "destination cannot report its contents"
        return plan
    if diff.delete_preview_unavailable_reason:
        plan.available = False
        plan.unavailable_reason = (
            f"the set of rows to delete could not be determined: "
            f"{diff.delete_preview_unavailable_reason}"
        )
        return plan
    if diff.truncated:
        plan.available = False
        plan.unavailable_reason = "the change set was truncated; a plan must be complete"
        return plan
    if not key_columns:
        plan.available = False
        plan.unavailable_reason = "destination.upsert_key is required to identify changed rows"
        return plan

    hashed = mask_columns or set()

    def key(record: dict[str, Any]) -> dict[str, Any]:
        return _key_of(record, key_columns, hashed, redact_keys)

    entries: list[PlanEntry] = []
    for record in diff.added:
        entries.append(PlanEntry(key=key(record), action="create"))
    for record in diff.inserted:
        entries.append(PlanEntry(key=key(record), action="insert"))
    for old, new in diff.updated:
        columns = sorted(DiffResult.changed_fields(old, new))
        entries.append(PlanEntry(key=key(new), action="update", changed_columns=columns))
    for old, new in diff.replaced:
        columns = sorted(DiffResult.changed_fields(old, new, include_removed=True))
        entries.append(PlanEntry(key=key(new), action="replace", changed_columns=columns))
    for record in diff.deleted:
        entries.append(
            PlanEntry(key=key(record), action="delete", delete_reason=diff.delete_reason)
        )

    plan.entries = sorted(entries, key=_entry_sort_key)
    return plan


def unsupported_reason(sync: Any) -> str | None:
    """Why this sync cannot be planned *yet*, checked before anything runs.

    These are cases where a plan would be wrong or would not be read-only, so
    the answer is "unavailable, with a reason" rather than a best effort.
    """
    options = sync.sync
    policy = getattr(options, "match_policy", "upsert")
    if policy != "upsert":
        return (
            f"sync.match_policy: {policy} is not supported by drt plan yet "
            "(the plan would list creates/updates the destination skips)"
        )
    if (
        options.mode == "incremental"
        and getattr(options, "incremental_strategy", "cursor") == "diff"
    ):
        return (
            "incremental_strategy: diff is not supported by drt plan yet "
            "(snapshot extraction writes scratch tables, so it is not read-only)"
        )
    metadata = getattr(options, "metadata_columns", None)
    if metadata is not None:
        targets = {
            getattr(metadata, name)
            for name in ("synced_at", "run_id", "sync_name")
            if getattr(metadata, name, None)
        }
        overlap = sorted(targets & set(getattr(sync.destination, "upsert_key", None) or []))
        if overlap:
            return (
                f"upsert_key includes engine-written metadata column(s) {overlap}; "
                "their values differ between a plan and a real run"
            )
    return None


PLAN_JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "drt plan",
    "type": "object",
    "required": [
        "schema_version",
        "plan_id",
        "created_at",
        "drt_version",
        "sync",
        "destination",
        "fingerprints",
        "status",
        "summary",
        "digest",
        "entries",
    ],
    "properties": {
        "schema_version": {"const": PLAN_SCHEMA_VERSION},
        "plan_id": {"type": "string", "pattern": "^plan-[0-9a-f]{16}$"},
        "created_at": {"type": "string"},
        "drt_version": {"type": "string"},
        "sync": {
            "type": "object",
            "required": ["name", "mode", "match_policy"],
            "properties": {
                "name": {"type": "string"},
                "mode": {"type": "string"},
                "match_policy": {"type": "string"},
            },
        },
        "destination": {"type": "string"},
        "fingerprints": {
            "type": "object",
            "required": ["config_hash", "cursor_hash"],
            "properties": {
                "config_hash": {"type": "string"},
                "cursor_hash": {"type": ["string", "null"]},
            },
        },
        "status": {
            "type": "object",
            "required": ["available", "reason"],
            "properties": {
                "available": {"type": "boolean"},
                "reason": {"type": ["string", "null"]},
            },
        },
        "summary": {
            "type": "object",
            "required": [*_ACTION_ORDER, "total_source_rows", "total_destination_rows"],
            "properties": {
                **{action: {"type": "integer", "minimum": 0} for action in _ACTION_ORDER},
                "total_source_rows": {"type": "integer", "minimum": 0},
                "total_destination_rows": {"type": ["integer", "null"], "minimum": 0},
            },
        },
        "digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
        "entries": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["key", "action"],
                "properties": {
                    "key": {"type": "object"},
                    "action": {"enum": list(_ACTION_ORDER)},
                    "changed_columns": {"type": "array", "items": {"type": "string"}},
                    "delete_reason": {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
    },
}

_MARKDOWN_ENTRY_LIMIT = 50


def render_markdown(plan: Plan) -> str:
    """Markdown for a PR comment or a CI job summary (the JSON stays the artifact)."""
    if not plan.available:
        return (
            f"### drt plan: `{plan.sync_name}`\n\n**Plan unavailable.** {plan.unavailable_reason}\n"
        )
    summary = plan.summary
    lines = [f"### drt plan: `{plan.sync_name}` -> {plan.destination}", ""]
    if not plan.entries:
        lines.append("No changes.")
    else:
        lines += ["| Action | Count |", "|---|---:|"]
        lines += [f"| {a} | {summary[a]} |" for a in _ACTION_ORDER if summary[a]]
        lines += ["", "| Action | Key | Changed columns |", "|---|---|---|"]
        for entry in plan.entries[:_MARKDOWN_ENTRY_LIMIT]:
            key = ", ".join(f"{k}={v}" for k, v in entry.key.items())
            columns = ", ".join(entry.changed_columns) or "-"
            lines.append(f"| {entry.action} | `{_md_escape(key)}` | {_md_escape(columns)} |")
        hidden = len(plan.entries) - _MARKDOWN_ENTRY_LIMIT
        if hidden > 0:
            lines.append(f"\n_{hidden} more in plan.json._")
    lines += ["", f"`{plan.plan_id}` · digest `{plan.digest[:23]}`", ""]
    return "\n".join(lines)


def _md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("`", "'").replace("\n", " ")


def render_text(plan: Plan) -> str:
    """Human summary for the terminal (the JSON file is the reviewable artifact)."""
    lines = [f"Plan for sync '{plan.sync_name}' -> {plan.destination}"]
    if not plan.available:
        lines.append(f"  Plan unavailable: {plan.unavailable_reason}")
        return "\n".join(lines) + "\n"
    summary = plan.summary
    parts = [f"{summary[a]} to {a}" for a in _ACTION_ORDER if summary[a]]
    lines.append("  " + (", ".join(parts) if parts else "No changes."))
    lines.append(f"  Source rows: {plan.total_source_rows}")
    if plan.total_destination_rows is not None:
        lines.append(f"  Destination rows read: {plan.total_destination_rows}")
    lines.append(f"  plan_id: {plan.plan_id}")
    lines.append(f"  digest:  {plan.digest}")
    return "\n".join(lines) + "\n"
