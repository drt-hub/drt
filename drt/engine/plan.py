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
import hmac
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from drt.engine.diff import DiffResult
from drt.engine.guards import GuardTrip, evaluate_guards

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
    # Salted hash of the row that would be written, so a changed *value* is
    # drift even when key, action and changed column names are unchanged.
    value_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"key": self.key, "action": self.action}
        if self.value_hash:
            out["value_hash"] = self.value_hash
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
    environment_hash: str
    drt_version: str
    created_at: str
    cursor_hash: str | None = None
    redact_keys: bool = False
    key_id: str = ""
    available: bool = True
    unavailable_reason: str | None = None
    total_source_rows: int = 0
    total_destination_rows: int | None = None
    delete_baseline: int | None = None
    guards_configured: dict[str, Any] | None = None
    guard_trips: list[GuardTrip] = field(default_factory=list)
    entries: list[PlanEntry] = field(default_factory=list)
    plan_key: bytes = field(default=b"", repr=False)

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
        return digest_of(
            self.sync_name,
            self.config_hash,
            self.environment_hash,
            [e.to_dict() for e in self.entries],
        )

    @property
    def plan_id(self) -> str:
        """Unique per plan *artifact* (the digest is the deterministic part), so a
        later plan over the same drift is not mistaken for an already-applied one."""
        return plan_id_of(self.digest, self.cursor_hash, self.created_at)

    def to_dict(self) -> dict[str, Any]:
        document = self._body()
        document["seal"] = seal_of(document, self.plan_key)
        return document

    def _body(self) -> dict[str, Any]:
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
            "options": {"redact_keys": self.redact_keys, "key_id": self.key_id},
            "fingerprints": {
                "config_hash": self.config_hash,
                "environment_hash": self.environment_hash,
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
            "guards": {
                "configured": self.guards_configured,
                "tripped": [t.to_dict() for t in self.guard_trips],
            },
            "digest": self.digest,
            "entries": [e.to_dict() for e in self.entries],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str) + "\n"


def digest_of(
    sync_name: str,
    config_fingerprint: str,
    environment_fingerprint: str,
    entries: list[dict[str, Any]],
) -> str:
    body = {
        "sync": sync_name,
        "config_hash": config_fingerprint,
        "environment_hash": environment_fingerprint,
        "entries": entries,
    }
    return "sha256:" + _sha256(_canonical(body))


def plan_id_of(digest: str, cursor_hash: str | None, created_at: str) -> str:
    seed = _canonical({"digest": digest, "cursor": cursor_hash, "created_at": created_at})
    return "plan-" + _sha256(seed)[:16]


def seal_of(document: dict[str, Any], plan_key: bytes) -> str:
    """HMAC-SHA256 over the whole document except the seal itself.

    Keyed with the plan key, so nobody who can read or write the file but does
    not hold ``DRT_PLAN_KEY`` can refresh ``created_at`` (to dodge ``--max-age``),
    mint a new ``plan_id`` (to dodge single use) or edit anything else and still
    produce a valid seal. ``drt apply`` additionally re-verifies the plan against
    the live world.
    """
    body = {k: v for k, v in document.items() if k != "seal"}
    mac = hmac.new(plan_key, _canonical(body).encode("utf-8"), hashlib.sha256)
    return "hmac-sha256:" + mac.hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_value(value: Any, plan_key: bytes) -> str:
    """Keyed hash (HMAC-SHA256) of a value.

    Keyed, not salted with public data: anyone holding a ``plan.json`` could
    otherwise test guesses for a low-entropy email, status or password offline.
    The key never appears in the artifact.
    """
    mac = hmac.new(plan_key, _canonical(value).encode("utf-8"), hashlib.sha256)
    return "hmac-sha256:" + mac.hexdigest()[:32]


def key_id_of(plan_key: bytes) -> str:
    """Identifies which key made a plan without revealing it (so apply can say why)."""
    return hmac.new(plan_key, b"drt-plan-key-id", hashlib.sha256).hexdigest()[:16]


def config_hash(sync_config: Any) -> str:
    """Fingerprint of the parsed sync definition (a changed sync invalidates a plan)."""
    return "sha256:" + _sha256(_canonical(sync_config.model_dump(mode="json")))


def _key_of(
    record: dict[str, Any],
    key_columns: list[str],
    hashed_columns: set[str],
    redact_keys: bool,
    plan_key: bytes,
) -> dict[str, Any]:
    key: dict[str, Any] = {}
    for column in key_columns:
        value = record.get(column)
        if redact_keys or column in hashed_columns:
            key[column] = hash_value(value, plan_key)
        else:
            key[column] = value
    return key


def _entry_sort_key(entry: PlanEntry) -> tuple[int, str, str, str, str]:
    # Full tie-breaker: two entries for one key (a source with duplicate keys)
    # must not depend on the order an unordered query returned them in.
    return (
        _ACTION_ORDER[entry.action],
        _canonical(entry.key),
        _canonical(entry.changed_columns),
        entry.delete_reason or "",
        entry.value_hash or "",
    )


def build_plan(
    diff: DiffResult,
    *,
    sync_name: str,
    sync_mode: str,
    match_policy: str,
    destination: str,
    config_fingerprint: str,
    environment_fingerprint: str,
    plan_key: bytes,
    drt_version: str,
    key_columns: list[str],
    mask_columns: set[str] | None = None,
    redact_keys: bool = False,
    exclude_columns: set[str] | None = None,
    guards: Any = None,
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
        environment_hash=environment_fingerprint,
        drt_version=drt_version,
        created_at=created_at or datetime.now(timezone.utc).isoformat(),
        cursor_hash=hash_value(cursor_value, plan_key) if cursor_value is not None else None,
        redact_keys=redact_keys,
        key_id=key_id_of(plan_key),
        plan_key=plan_key,
        total_source_rows=diff.total_source_rows,
        total_destination_rows=diff.total_destination_rows,
        delete_baseline=diff.delete_baseline,
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
        return _key_of(record, key_columns, hashed, redact_keys, plan_key)

    skipped_columns = exclude_columns or set()

    def value(record: dict[str, Any]) -> str:
        # Engine-written bookkeeping columns (synced_at, run_id) differ between
        # a plan and the run it describes by construction; they are not drift.
        row = {k: v for k, v in record.items() if k not in skipped_columns}
        return hash_value({"sync": sync_name, "row": row}, plan_key)

    entries: list[PlanEntry] = []
    for record in diff.added:
        entries.append(PlanEntry(key=key(record), action="create", value_hash=value(record)))
    for record in diff.inserted:
        entries.append(PlanEntry(key=key(record), action="insert", value_hash=value(record)))
    for old, new in diff.updated:
        columns = sorted(DiffResult.changed_fields(old, new))
        entries.append(
            PlanEntry(key=key(new), action="update", changed_columns=columns, value_hash=value(new))
        )
    for old, new in diff.replaced:
        columns = sorted(DiffResult.changed_fields(old, new, include_removed=True))
        entries.append(
            PlanEntry(
                key=key(new), action="replace", changed_columns=columns, value_hash=value(new)
            )
        )
    for record in diff.deleted:
        entries.append(
            PlanEntry(key=key(record), action="delete", delete_reason=diff.delete_reason)
        )

    plan.entries = sorted(entries, key=_entry_sort_key)
    if guards is not None:
        plan.guards_configured = guards.model_dump(exclude_none=True)
        counts = plan.summary
        plan.guard_trips = evaluate_guards(
            guards,
            creates=counts["create"] + counts["insert"],
            updates=counts["update"] + counts["replace"],
            deletes=counts["delete"],
            source_rows=plan.total_source_rows,
            delete_baseline=plan.delete_baseline,
        )
    return plan


class PlanDocumentError(ValueError):
    """A plan file that must not be applied (unreadable, tampered or unusable)."""


def load_plan_document(text: str, plan_key: bytes) -> dict[str, Any]:
    """Parse and authenticate a ``plan.json``.

    The keyed seal, digest and ``plan_id`` are recomputed, so a plan that was
    edited by hand, truncated, or made with another plan key is rejected.
    """
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as e:
        raise PlanDocumentError(f"plan file is not valid JSON: {e}") from e
    if not isinstance(doc, dict):
        raise PlanDocumentError("plan file must contain a JSON object")
    try:
        version = doc["schema_version"]
        if version != PLAN_SCHEMA_VERSION:
            raise PlanDocumentError(
                f"unsupported plan schema_version {version!r} "
                f"(this drt reads {PLAN_SCHEMA_VERSION})"
            )
        if not doc["status"]["available"]:
            raise PlanDocumentError("the plan is marked unavailable and cannot be applied")
        if doc["options"]["key_id"] != key_id_of(plan_key):
            raise PlanDocumentError(
                "the plan was made with a different plan key, so it cannot be verified here"
            )
        if not hmac.compare_digest(seal_of(doc, plan_key), str(doc["seal"])):
            raise PlanDocumentError("plan seal does not match its contents (file was modified)")
        fingerprints = doc["fingerprints"]
        digest = digest_of(
            doc["sync"]["name"],
            fingerprints["config_hash"],
            fingerprints["environment_hash"],
            doc["entries"],
        )
        if digest != doc["digest"]:
            raise PlanDocumentError("plan digest does not match its entries (file was modified)")
        if plan_id_of(digest, fingerprints["cursor_hash"], doc["created_at"]) != doc["plan_id"]:
            raise PlanDocumentError("plan_id does not match the plan contents")
        doc["options"]["redact_keys"]
        doc["options"]["key_id"]
        doc["drt_version"]
    except (KeyError, TypeError) as e:
        raise PlanDocumentError(f"plan file is missing or malformed field: {e}") from e
    return doc


def drift_report(planned: list[dict[str, Any]], current: list[dict[str, Any]]) -> dict[str, Any]:
    """Compare two entry lists as multisets (duplicate keys count each time).

    ``removed`` are planned entries that no longer exist, ``added`` are entries
    that were not planned; an entry whose action or values changed appears in
    both. ``drifted`` is the larger of the two, so a changed entry counts once.
    """
    old = Counter(_canonical(e) for e in planned)
    new = Counter(_canonical(e) for e in current)
    removed = [json.loads(k) for k in sorted((old - new).elements())]
    added = [json.loads(k) for k in sorted((new - old).elements())]
    return {"removed": removed, "added": added, "drifted": max(len(removed), len(added))}


MANIFEST_SCHEMA_VERSION = 1


def build_manifest(
    syncs: list[dict[str, Any]], plan_key: bytes, created_at: str | None = None
) -> dict[str, Any]:
    """The authenticated index of one ``drt plan --all`` run.

    Every sync appears with a status (``planned`` / ``unavailable`` / ``error``),
    so a directory of plans that is missing a sync, holds a stale file, or comes
    from a run where a sync failed to plan can be recognised by ``drt apply``.
    """
    document: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": "drt-plan-manifest",
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        "key_id": key_id_of(plan_key),
        "syncs": syncs,
    }
    document["seal"] = seal_of(document, plan_key)
    return document


def load_manifest(text: str, plan_key: bytes) -> dict[str, Any]:
    try:
        doc = json.loads(text)
        if not isinstance(doc, dict) or doc.get("kind") != "drt-plan-manifest":
            raise PlanDocumentError("not a drt plan manifest")
        if doc["schema_version"] != MANIFEST_SCHEMA_VERSION:
            raise PlanDocumentError(
                f"unsupported manifest schema_version {doc['schema_version']!r}"
            )
        if doc["key_id"] != key_id_of(plan_key):
            raise PlanDocumentError(
                "the manifest was made with a different plan key, so it cannot be verified here"
            )
        if not hmac.compare_digest(seal_of(doc, plan_key), str(doc["seal"])):
            raise PlanDocumentError("manifest seal does not match its contents (file was modified)")
        if not isinstance(doc["syncs"], list):
            raise PlanDocumentError("manifest syncs must be a list")
    except json.JSONDecodeError as e:
        raise PlanDocumentError(f"manifest is not valid JSON: {e}") from e
    except (KeyError, TypeError) as e:
        raise PlanDocumentError(f"manifest is missing or malformed field: {e}") from e
    return doc


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
        "options",
        "fingerprints",
        "status",
        "summary",
        "guards",
        "digest",
        "seal",
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
        "options": {
            "type": "object",
            "required": ["redact_keys", "key_id"],
            "properties": {"redact_keys": {"type": "boolean"}, "key_id": {"type": "string"}},
        },
        "fingerprints": {
            "type": "object",
            "required": ["config_hash", "environment_hash", "cursor_hash"],
            "properties": {
                "config_hash": {"type": "string"},
                "environment_hash": {"type": "string"},
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
        "guards": {
            "type": "object",
            "required": ["configured", "tripped"],
            "properties": {
                "configured": {"type": ["object", "null"]},
                "tripped": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["guard", "limit", "observed", "message"],
                        "properties": {
                            "guard": {"type": "string"},
                            "limit": {"type": "number"},
                            "observed": {"type": ["number", "null"]},
                            "message": {"type": "string"},
                        },
                    },
                },
            },
        },
        "digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
        "seal": {"type": "string", "pattern": "^hmac-sha256:[0-9a-f]{64}$"},
        "entries": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["key", "action"],
                "properties": {
                    "key": {"type": "object"},
                    "action": {"enum": list(_ACTION_ORDER)},
                    "value_hash": {"type": "string"},
                    "changed_columns": {"type": "array", "items": {"type": "string"}},
                    "delete_reason": {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
    },
}

_MARKDOWN_ENTRY_LIMIT = 50
_MARKDOWN_CELL_LIMIT = 120
_MARKDOWN_REASON_LIMIT = 500
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f\u2028\u2029]")


def _code(text: str, limit: int = _MARKDOWN_CELL_LIMIT) -> str:
    """Render untrusted text as one inline code span that cannot escape itself.

    Keys and column names are user data. Inside a code span HTML, links and
    emphasis are inert; control characters (newlines would end the table row or
    the paragraph) are shown as visible ``\\xNN`` escapes, the delimiter is
    longer than any backtick run in the text so distinct values stay distinct,
    and a ``|`` is escaped so it cannot split a table cell.
    """
    shown = _CONTROL_CHARS.sub(lambda m: f"\\x{ord(m.group()):02x}", text)
    if len(shown) > limit:
        shown = f"{shown[:limit]}... (+{len(shown) - limit} chars)"
    longest = max((len(run) for run in re.findall(r"`+", shown)), default=0)
    fence = "`" * (longest + 1)
    pad = " " if shown.startswith("`") or shown.endswith("`") else ""
    return f"{fence}{pad}{shown.replace('|', chr(92) + '|')}{pad}{fence}"


def render_markdown(plan: Plan) -> str:
    """Markdown for a PR comment or a CI job summary (the JSON stays the artifact).

    Every value that comes from data or configuration goes through :func:`_code`.
    """
    if not plan.available:
        reason = _code(plan.unavailable_reason or "", _MARKDOWN_REASON_LIMIT)
        return f"### drt plan: {_code(plan.sync_name)}\n\n**Plan unavailable.** {reason}\n"
    summary = plan.summary
    lines = [f"### drt plan: {_code(plan.sync_name)} -> {_code(plan.destination)}", ""]
    if not plan.entries:
        lines.append("No changes.")
    else:
        lines += ["| Action | Count |", "|---|---:|"]
        lines += [f"| {a} | {summary[a]} |" for a in _ACTION_ORDER if summary[a]]
        lines += ["", "| Action | Key | Changed columns |", "|---|---|---|"]
        for entry in plan.entries[:_MARKDOWN_ENTRY_LIMIT]:
            key = ", ".join(f"{k}={v}" for k, v in entry.key.items())
            columns = ", ".join(_code(c, 60) for c in entry.changed_columns) or "-"
            lines.append(f"| {entry.action} | {_code(key)} | {columns} |")
        hidden = len(plan.entries) - _MARKDOWN_ENTRY_LIMIT
        if hidden > 0:
            lines.append(
                f"\n_{hidden} more entries not shown; the full list is in the JSON plan "
                "(`drt plan ... --out plan.json`)._"
            )
    for trip in plan.guard_trips:
        lines += ["", f"**Guard tripped:** {_code(trip.message, _MARKDOWN_REASON_LIMIT)}"]
    lines += ["", f"{_code(plan.plan_id)} - digest {_code(plan.digest[:23])}", ""]
    return "\n".join(lines)


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
    for trip in plan.guard_trips:
        lines.append(f"  GUARD TRIPPED: {trip.message}")
    lines.append(f"  plan_id: {plan.plan_id}")
    lines.append(f"  digest:  {plan.digest}")
    return "\n".join(lines) + "\n"
