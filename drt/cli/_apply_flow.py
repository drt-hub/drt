"""The guarded apply, as a function (#1217, #1220).

``drt apply`` and the MCP ``drt_apply`` tool both call :func:`apply_plan`, so a
plan is verified, claimed and written the same way whichever door it comes
through. This module prints nothing and never exits: a refusal is an
:class:`ApplyRefused`, and the caller decides how to show it.

Verify-by-replan: the plan is recomputed through the same code ``drt plan``
uses and compared before anything is written. A plan can be applied once: a
claim file under ``.drt/applied_plans/`` is created atomically (``O_EXCL``)
before the write and updated with the outcome. The claim is local to the
workspace, so it stops concurrent and repeated applies there; coordinating
applies across machines needs a shared claim store (not built yet).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import tempfile
import time
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from drt import __version__

_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DRIFT_SHOWN = 10
_CLOCK_SKEW = timedelta(minutes=5)
_DRIFT_ITEM_CHARS = 300
PLAN_ID_PATTERN = re.compile(r"plan-[0-9a-f]{16}")  # use fullmatch(): `$` allows a trailing newline


class ApplyRefused(Exception):
    """Nothing was written, for the reason in the message."""


class ApplyAborted(ApplyRefused):
    """The person declined the confirmation: nothing was written, and it is not an error."""


@dataclass
class ApplyOutcome:
    plan_id: str
    sync: str
    applied: bool
    message: str = ""
    run_id: str | None = None
    entry: dict[str, Any] = field(default_factory=dict)
    had_error: bool = False
    guards_forced: list[dict[str, Any]] = field(default_factory=list)
    drifted: int = 0
    run_ids: list[str] = field(default_factory=list)
    canary: dict[str, Any] | None = None
    halted: bool = False


def parse_duration(text: str) -> timedelta:
    match = _DURATION.match(text.strip())
    if not match:
        raise ValueError("use a number and a unit: 30m, 24h, 7d")
    return timedelta(seconds=int(match.group(1)) * _UNITS[match.group(2)])


def normalize_canary_waves(waves: list[float | int]) -> list[float]:
    """Validate cumulative canary percentages and append 100 when absent."""
    if not waves:
        raise ValueError("canary waves must contain at least one percentage")
    if any(isinstance(wave, bool) or not isinstance(wave, int | float) for wave in waves):
        raise ValueError("canary waves must contain only numbers (not booleans or strings)")
    normalized = [float(wave) for wave in waves]
    if any(not math.isfinite(wave) or not 0 < wave <= 100 for wave in normalized):
        raise ValueError("canary waves must each be greater than 0 and at most 100")
    if any((Decimal(str(wave)) * 100) % 1 for wave in normalized):
        raise ValueError("canary waves must be multiples of 0.01 (the bucket resolution)")
    if any(left >= right for left, right in zip(normalized, normalized[1:], strict=False)):
        raise ValueError("canary waves must be strictly increasing without duplicates")
    if normalized[-1] != 100:
        normalized.append(100.0)
    return normalized


def parse_canary_waves(text: str) -> list[float]:
    """Parse the CLI's comma-separated numeric wave list strictly."""
    parts = [part.strip() for part in text.split(",")]
    if not parts or any(not part for part in parts):
        raise ValueError("canary waves must be a comma-separated list of numbers")
    try:
        values = [float(part) for part in parts]
    except ValueError as e:
        raise ValueError("canary waves must contain only numbers") from e
    return normalize_canary_waves(values)


def _canonical_key_value(value: Any) -> bytes:
    """Encode one destination key value with an unambiguous, stable type tag."""
    if value is None:
        return b"null:"
    if isinstance(value, bool | int | float | Decimal):
        number = Decimal(int(value)) if isinstance(value, bool) else Decimal(str(value))
        if not number.is_finite():
            raise ValueError(f"Unsupported non-finite canary key number: {value!r}")
        if number == 0:
            number = Decimal(0)
        text = format(number.normalize(), "f")
        return f"number:{text}".encode()
    if isinstance(value, str):
        return b"string:" + unicodedata.normalize("NFC", value).encode("utf-8")
    if isinstance(value, datetime):
        aware = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        return b"datetime:" + aware.astimezone(timezone.utc).isoformat().encode()
    if isinstance(value, date):
        return b"date:" + value.isoformat().encode()
    if isinstance(value, bytes):
        return b"bytes:" + value.hex().encode()
    raise ValueError(
        "Unsupported canary key type "
        f"{type(value).__name__}; use null, bool/number, string, date/datetime, or bytes."
    )


def canary_bucket(record: dict[str, Any], upsert_key: list[str] | None = None) -> int:
    """Return the stable 0..9999 rollout bucket for destination key values."""
    if not upsert_key:
        raise ValueError("Canary apply requires destination.upsert_key for stable wave keys.")
    encoded: list[bytes] = []
    for column in upsert_key:
        if column not in record:
            raise ValueError(
                f"Canary key column {column!r} is missing after transforms; missing is not null."
            )
        token = _canonical_key_value(record[column])
        encoded.append(len(token).to_bytes(8, "big") + token)
    return int.from_bytes(hashlib.sha256(b"".join(encoded)).digest(), "big") % 10_000


def canary_filter(
    lower_pct: float,
    upper_pct: float,
    upsert_key: list[str] | None,
    field_mappings: dict[str, str] | None = None,
    computed_fields: dict[str, str] | None = None,
    mask: dict[str, Any] | None = None,
) -> Callable[[dict[str, Any]], bool]:
    """Build an exclusive stable bucket-range predicate for one wave."""

    def selected(record: dict[str, Any]) -> bool:
        keyed = record
        if upsert_key:
            from drt.engine.computed_fields import apply_computed_fields
            from drt.engine.field_mappings import apply_field_mappings
            from drt.engine.masking import apply_mask

            # Only derive fields that can become a destination key here. Other
            # computed fields must run in the engine so on_error: skip can count
            # them as error-skipped rows in this wave's health check.
            key_fields = {
                name: template
                for name, template in (computed_fields or {}).items()
                if (field_mappings or {}).get(name, name) in upsert_key
            }
            transformed, _errors = apply_computed_fields([dict(record)], key_fields, "fail")
            transformed = apply_field_mappings(transformed, field_mappings)
            transformed = apply_mask(transformed, mask)
            keyed = transformed[0]
        bucket = canary_bucket(keyed, upsert_key)
        return lower_pct * 100 <= bucket < upper_pct * 100

    return selected


def _canary_health(
    entry: dict[str, Any], *, had_error: bool, on_error: str, max_error_pct: float
) -> tuple[int, int, int, float, bool]:
    """Return rows, successes, error rows, error percentage, and wave verdict."""
    row_error_value = entry.pop("_row_error_count", 0)
    row_errors = int(row_error_value) if isinstance(row_error_value, int | float) else 0
    ok_value = entry.get("rows_synced", 0)
    failed_value = entry.get("rows_failed", 0)
    rows_value = entry.get("rows_extracted", 0)
    ok = int(ok_value) if isinstance(ok_value, int | float) else 0
    destination_failed = int(failed_value) if isinstance(failed_value, int | float) else 0
    rows = int(rows_value) if isinstance(rows_value, int | float) else 0
    error_rows = max(destination_failed, row_errors)
    rate = 0.0 if rows == 0 else error_rows * 100.0 / rows
    fatal_destination_error = had_error or (on_error == "fail" and destination_failed > 0)
    return rows, ok, error_rows, rate, not fatal_destination_error and rate <= max_error_pct


def _validate_canary_compatibility(sync: Any, destination: Any) -> list[str]:
    """Fail closed unless this is the stateless, keyed per-row v1 path."""
    from drt.destinations.base import StagedDestination

    if isinstance(destination, StagedDestination):
        raise ApplyRefused(
            "Canary apply v1 supports only non-staged per-row destinations; "
            "remove --canary/sync.rollout or use a non-staged destination."
        )
    upsert_key = getattr(sync.destination, "upsert_key", None)
    if sync.sync.mode not in {"full", "upsert"}:
        raise ApplyRefused(
            "Canary apply v1 supports only non-incremental sync.mode: full or upsert; "
            f"mode: {sync.sync.mode} can change watermark/finalize state across waves. "
            "Remove --canary/sync.rollout or use a plain full/upsert sync."
        )
    if sync.sync.incremental_strategy == "diff":
        raise ApplyRefused(
            "Canary apply v1 does not support incremental_strategy: diff because each wave "
            "would mutate the snapshot baseline. Remove --canary/sync.rollout or use the "
            "plain non-incremental write path."
        )
    if not upsert_key:
        raise ApplyRefused(
            "Canary apply v1 requires destination.upsert_key so rows have stable wave keys "
            "and a re-plan after a halt can identify what remains."
        )
    lookup_outputs = {
        (sync.sync.field_mappings or {}).get(name, name)
        for name in (getattr(sync.destination, "lookups", None) or {})
    }
    metadata = sync.sync.metadata_columns
    metadata_outputs = {
        value
        for value in (
            metadata.synced_at if metadata is not None else None,
            metadata.run_id if metadata is not None else None,
            metadata.sync_name if metadata is not None else None,
        )
        if value is not None
    }
    unstable_keys = set(upsert_key) & (lookup_outputs | metadata_outputs)
    if unstable_keys:
        raise ApplyRefused(
            "Canary apply v1 cannot use an upsert_key produced by a destination lookup or "
            f"run metadata because it is not stable before each wave: {sorted(unstable_keys)}. "
            "Choose source/computed key columns or remove canary rollout."
        )
    return list(upsert_key)


def describe_drift(report: dict[str, Any]) -> str:
    lines = ["The world no longer matches the plan:"]
    for label, items in (
        ("planned but no longer the case", report["removed"]),
        ("not in the plan", report["added"]),
    ):
        if not items:
            continue
        lines.append(f"  {len(items)} entr{'y' if len(items) == 1 else 'ies'} {label}:")
        for item in items[:_DRIFT_SHOWN]:
            shown = {"action": item.get("action"), "key": item.get("key")}
            text = json.dumps(shown, default=str, sort_keys=True)
            if len(text) > _DRIFT_ITEM_CHARS:
                text = f"{text[:_DRIFT_ITEM_CHARS]}... (+{len(text) - _DRIFT_ITEM_CHARS} chars)"
            lines.append(f"    - {text}")
        if len(items) > _DRIFT_SHOWN:
            lines.append(f"    ... and {len(items) - _DRIFT_SHOWN} more")
    return "\n".join(lines)


def claims_dir(project_dir: Path) -> Path:
    return project_dir / ".drt" / "applied_plans"


def _claim_path(project_dir: Path, plan_id: str) -> Path:
    return claims_dir(project_dir) / f"{plan_id}.json"


def _read_claim(project_dir: Path, plan_id: str) -> dict[str, Any] | None:
    try:
        text = _claim_path(project_dir, plan_id).read_text(encoding="utf-8")
        return json.loads(text)  # type: ignore[no-any-return]
    except (OSError, ValueError):
        return None


def _refuse_if_claimed(project_dir: Path, plan_id: str) -> None:
    from drt.cli._plan_runner import PlanCliError

    claim = _read_claim(project_dir, plan_id)
    if claim is None and not _claim_path(project_dir, plan_id).exists():
        return
    state = (claim or {}).get("state", "unknown")
    when = (claim or {}).get("claimed_at", "?")
    if state == "success":
        message = f"Plan {plan_id} was already applied ({when})."
    elif state == "pending":
        message = (
            f"Plan {plan_id} is being applied, or an earlier apply stopped part-way "
            f"(claimed {when}). Check the destination before doing anything else; "
            f"{_claim_path(project_dir, plan_id)} records the attempt."
        )
    else:
        message = f"Plan {plan_id} was already attempted (state: {state}, {when})."
    raise PlanCliError(f"{message} A plan is single-use; run `drt plan` again.")


def _claim(
    project_dir: Path,
    plan_id: str,
    sync_name: str,
    run_id: str,
    forced: list[str] | None = None,
    approved_by: str | None = None,
    waves: list[float] | None = None,
) -> None:
    """Atomically take the plan. Raises ``FileExistsError`` if someone already did."""
    claims_dir(project_dir).mkdir(parents=True, exist_ok=True)
    fd = os.open(_claim_path(project_dir, plan_id), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "plan_id": plan_id,
                "sync": sync_name,
                "run_id": run_id,
                "state": "pending",
                "status": "pending",
                "claimed_at": datetime.now(timezone.utc).isoformat(),
                **(
                    {
                        "waves": waves,
                        "wave_results": [],
                        "run_ids": [],
                    }
                    if waves is not None
                    else {}
                ),
                **({"guards_forced": forced} if forced else {}),
                **({"approved_by": approved_by} if approved_by else {}),
            },
            handle,
        )


def _finish_claim(project_dir: Path, plan_id: str, state: str, **updates: Any) -> None:
    """Record the outcome. Never raises: by now the destination may already have
    been written, and a bookkeeping failure must not hide that (or an earlier error)."""
    log = logging.getLogger(__name__)
    try:
        claim = _read_claim(project_dir, plan_id) or {"plan_id": plan_id}
        claim.update(state=state, status=state, **updates)
        if state in {"success", "failed", "halted"}:
            claim["finished_at"] = datetime.now(timezone.utc).isoformat()
        # Private, exclusive temp file in the same directory (mkstemp is 0600 and
        # does not follow a pre-existing symlink), then an atomic rename.
        fd, tmp_name = tempfile.mkstemp(
            dir=claims_dir(project_dir), prefix=f".{plan_id}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(claim, handle)
            os.replace(tmp_name, _claim_path(project_dir, plan_id))
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
    except BaseException as e:  # noqa: BLE001 - see docstring; must never mask interruption
        try:
            log.warning(
                "Could not record the outcome of plan %s (%s); %s may still say 'pending'.",
                plan_id,
                e,
                _claim_path(project_dir, plan_id),
            )
        except BaseException:  # logging is also best-effort on this terminal path
            pass


def preflight_document(
    plan_text: str,
    *,
    project_dir: Path = Path("."),
    max_age: timedelta = timedelta(hours=24),
) -> dict[str, Any]:
    """Every check on a plan that needs no source or destination access.

    Authenticity (keyed seal, digest, plan id), drt major version, age, and the
    single-use claim. ``drt apply <dir>`` runs this on every plan *before* the
    first write, so a plan that is already refusable cannot leave a deployment
    half applied.
    """
    from drt.cli._plan_runner import PlanCliError, load_plan_key
    from drt.engine.plan import PlanDocumentError, load_plan_document

    try:
        doc = load_plan_document(plan_text, load_plan_key(project_dir))
    except PlanDocumentError as e:
        message = f"Cannot apply the plan: {e}"
        if "plan key" in str(e):
            message += (
                ". Set the same DRT_PLAN_KEY where you plan and where you apply, or apply "
                "from the workspace that made the plan."
            )
        raise ApplyRefused(message) from e

    plan_major = str(doc["drt_version"]).split(".")[0]
    if plan_major != __version__.split(".")[0]:
        raise ApplyRefused(
            f"The plan was made by drt {doc['drt_version']} and cannot be applied by "
            f"drt {__version__} (different major version). Re-run `drt plan`."
        )

    try:
        created = datetime.fromisoformat(doc["created_at"])
    except ValueError as e:
        raise ApplyRefused("The plan's created_at is not a valid timestamp.") from e
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - created
    if age < -_CLOCK_SKEW:
        raise ApplyRefused(
            "The plan's created_at is in the future; refusing it. Re-run `drt plan`."
        )
    if age > max_age:
        raise ApplyRefused(
            f"The plan is {age} old, older than --max-age {max_age}. Re-run `drt plan`."
        )
    try:
        _refuse_if_claimed(project_dir, doc["plan_id"])
    except PlanCliError as e:
        raise ApplyRefused(str(e)) from e
    return doc


MANIFEST_NAME = "manifest.json"
_PLAN_FILE = re.compile(r"\d{3,}-[A-Za-z0-9._-]+\.json")


def load_plan_directory(
    directory: Path, *, project_dir: Path = Path("."), max_age: timedelta = timedelta(hours=24)
) -> list[tuple[str, str]]:
    """The plans in a ``drt plan --all`` directory, in apply order, fully vetted.

    A directory is only trusted through its authenticated manifest. It must come
    from a run in which every sync was planned or knowingly unavailable (a sync
    that *failed* to plan means the run was incomplete), hold exactly the files
    the manifest lists, and each file must be the plan the manifest names. Every
    plan then passes the offline checks before anything is written.
    """
    from drt.cli._plan_runner import load_plan_key
    from drt.engine.plan import PlanDocumentError, load_manifest

    try:
        manifest = load_manifest(
            (directory / MANIFEST_NAME).read_text(encoding="utf-8"), load_plan_key(project_dir)
        )
    except FileNotFoundError as e:
        raise ApplyRefused(
            f"{directory} has no {MANIFEST_NAME}. Plans are applied from a directory written "
            "by `drt plan --all`, which records every sync it planned."
        ) from e
    except (OSError, PlanDocumentError) as e:
        message = f"Cannot apply {directory}: {e}"
        if "plan key" in str(e):
            message += (
                ". Set the same DRT_PLAN_KEY secret where you plan and where you apply "
                "(an unset or empty secret makes each runner invent its own key)."
            )
        raise ApplyRefused(message) from e

    failed = [e["sync"] for e in manifest["syncs"] if e.get("status") == "error"]
    if failed:
        raise ApplyRefused(
            "The plan run was incomplete: "
            + ", ".join(str(n) for n in failed)
            + " could not be planned. Nothing was applied; fix the cause and re-run the plan."
        )

    planned = [e for e in manifest["syncs"] if e.get("status") == "planned"]
    listed = {str(e["file"]) for e in planned}
    present = {p.name for p in directory.glob("*.json") if p.name != MANIFEST_NAME}
    if present != listed or any(not _PLAN_FILE.fullmatch(name) for name in listed):
        extra, missing = sorted(present - listed), sorted(listed - present)
        raise ApplyRefused(
            "The plan files do not match the manifest"
            + (f"; not in the manifest: {', '.join(extra)}" if extra else "")
            + (f"; missing: {', '.join(missing)}" if missing else "")
            + ". Re-run `drt plan --all` into an empty directory."
        )

    plans: list[tuple[str, str]] = []
    for entry in planned:
        name = str(entry["file"])
        text = (directory / name).read_text(encoding="utf-8")
        doc = preflight_document(text, project_dir=project_dir, max_age=max_age)
        if doc["plan_id"] != entry.get("plan_id") or doc["sync"]["name"] != entry["sync"]:
            raise ApplyRefused(f"{name} is not the plan the manifest recorded; refusing it.")
        plans.append((name, text))
    return plans


def _apply_plan_once(
    plan_text: str,
    *,
    project_dir: Path = Path("."),
    approved: bool = False,
    approved_by: str | None = None,
    confirm: Callable[[str], bool] | None = None,
    max_age: timedelta = timedelta(hours=24),
    allow_drift_pct: float = 0.0,
    force_guards: bool = False,
    cursor_value: str | None = None,
    cli_vars: dict[str, Any] | None = None,
    profile_name: str | None = None,
    json_output: bool = False,
    notify: Callable[[str], None] | None = None,
    expect_plan_id: str | None = None,
) -> ApplyOutcome:
    """Verify a plan against the live world and, only if it still matches, write it.

    ``approved`` is the caller's assertion that a human said yes (``--auto-approve``,
    or the MCP client's permission prompt); without it ``confirm`` is asked, and
    with neither nothing is written. ``approved_by`` is recorded, not checked.
    """
    from drt._identifiers import new_run_id
    from drt.cli._helpers import get_source
    from drt.cli._plan_runner import PlanCliError, compute_plan
    from drt.cli.commands.run import _run_one, _RunContext, _write_run_results
    from drt.engine.plan import drift_report

    say = notify or (lambda _message: None)

    if not math.isfinite(allow_drift_pct) or allow_drift_pct < 0 or allow_drift_pct > 100:
        raise ApplyRefused("allow_drift_pct must be between 0 and 100.")

    doc = preflight_document(plan_text, project_dir=project_dir, max_age=max_age)
    plan_id = doc["plan_id"]
    sync_name = doc["sync"]["name"]
    if expect_plan_id is not None and plan_id != expect_plan_id:
        # The caller asked for one plan and was handed another (a file swapped
        # under a different name); applying it would bypass the approval that
        # named the requested one.
        raise ApplyRefused(
            f"The stored plan is {plan_id}, not the requested {expect_plan_id}; refusing it. "
            "Call drt_plan again."
        )

    def preflight(_project: Any, _state_bundle: Any, _sync: Any) -> None:
        _refuse_if_claimed(project_dir, plan_id)

    try:
        ctx_plan = compute_plan(
            sync_name,
            redact_keys=bool(doc["options"]["redact_keys"]),
            cursor_value=cursor_value,
            cli_vars=cli_vars,
            profile_name=profile_name,
            project_dir=project_dir,
            preflight=preflight,
        )
    except PlanCliError as e:
        raise ApplyRefused(str(e)) from e

    current = ctx_plan.plan
    if not current.available:
        raise ApplyRefused(f"Cannot verify the plan: {current.unavailable_reason}")
    if current.config_hash != doc["fingerprints"]["config_hash"]:
        raise ApplyRefused(
            "The sync definition (or the model SQL it references) changed since the plan "
            "was made. Re-run `drt plan`."
        )
    if current.environment_hash != doc["fingerprints"]["environment_hash"]:
        raise ApplyRefused(
            "The environment differs from the one the plan was made in (profile, project "
            "vars, environment variables or the resolved destination). Re-run `drt plan` "
            "here, or apply from the environment that made the plan."
        )
    if current.cursor_hash != doc["fingerprints"]["cursor_hash"]:
        raise ApplyRefused(
            "The incremental watermark moved since the plan was made, so the plan covers a "
            "different window. Re-run `drt plan`."
        )

    drifted = 0
    if current.digest != doc["digest"]:
        report = drift_report(doc["entries"], [e.to_dict() for e in current.entries])
        drifted = report["drifted"]
        pct = 100.0 * drifted / max(len(doc["entries"]), 1)
        word = "y" if drifted == 1 else "ies"
        # Zero tolerance means any difference at all; the percentage is only
        # consulted when the operator opted into some drift.
        if allow_drift_pct == 0 or pct > allow_drift_pct:
            raise ApplyRefused(
                describe_drift(report)
                + f"\n  {drifted} entr{word} drifted ({pct:.1f}% of the plan; allowed "
                f"{allow_drift_pct}%). Nothing was written."
            )
        say(f"Proceeding despite drift: {drifted} entr{word} ({pct:.1f}% <= {allow_drift_pct}%).")

    summary = current.summary
    parts = [f"{summary[a]} {a}" for a in summary if summary[a]]
    if not current.has_changes:
        return ApplyOutcome(
            plan_id, sync_name, applied=False, message="The plan has no changes; nothing to apply."
        )

    # Guards are judged on the freshly recomputed plan, not on the file: the file
    # records what was true when it was made, this is what would be written now.
    tripped = [t.to_dict() for t in current.guard_trips]
    if tripped and not force_guards:
        raise ApplyRefused(
            "Change guards tripped; nothing was written:\n"
            + "\n".join(f"  - {t['message']}" for t in tripped)
            + "\nFix the cause, or review and re-run with --force-guards."
        )
    if tripped:
        say(
            "Applying despite tripped guards (--force-guards): "
            + "; ".join(str(t["message"]) for t in tripped)
        )

    if not approved:
        if confirm is None:
            raise ApplyRefused("Approval is required to apply a plan.")
        question = f"Apply plan {plan_id} to {current.destination}: {', '.join(parts)}."
        if not confirm(question):
            raise ApplyAborted("Aborted. Nothing was written.")

    project = ctx_plan.project
    state_bundle = ctx_plan.state_bundle
    run_id = new_run_id()
    try:
        _claim(
            project_dir,
            plan_id,
            sync_name,
            run_id,
            [str(t["guard"]) for t in tripped],
            approved_by,
        )
    except FileExistsError as e:
        raise ApplyRefused(
            f"Plan {plan_id} was just taken by another apply. A plan is single-use; "
            "nothing was written by this command."
        ) from e

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    ctx = _RunContext(
        source=get_source(ctx_plan.profile),
        state_mgr=state_bundle.state,
        history_mgr=state_bundle.history,
        dlq_store=state_bundle.dlq,
        history_retention_days=project.history.retention_days,
        json_mode=json_output,
        dry_run=False,
        verbose=False,
        quiet=json_output,
        log_json=False,
        # The window the plan was verified over, not whatever the watermark says now.
        cursor_value=ctx_plan.cursor_value_used,
        vars=ctx_plan.project_vars,
        query_tagging=project.query_tagging,
        run_id=run_id,
        idempotency_ledger=state_bundle.ledger,
        audit_trail=state_bundle.audit_trail,
        audit_fields=project.state.audit_trail.fields,
        audit_retain_days=project.state.audit_trail.retain_days or 30,
        project_dir=project_dir,
    )
    try:
        _name, entry, had_error = _run_one(ctx_plan.sync, ctx, ctx_plan.profile)
    except BaseException:
        _finish_claim(project_dir, plan_id, "failed")  # never raises: cannot mask this error
        raise
    _finish_claim(project_dir, plan_id, "failed" if had_error else "success")
    entry["plan_id"] = plan_id
    if approved_by:
        entry["approved_by"] = approved_by
    if tripped:
        entry["guards_forced"] = tripped
    _write_run_results(
        project_dir / "target" / "drt",
        run_id=run_id,
        started_at=started_at,
        results=[entry],
        succeeded=0 if had_error else 1,
        failed=1 if had_error else 0,
        skipped=0,
        total_duration=round(time.monotonic() - t0, 2),
        exit_code=1 if had_error else 0,
    )
    return ApplyOutcome(
        plan_id,
        sync_name,
        applied=True,
        run_id=run_id,
        entry=entry,
        had_error=had_error,
        guards_forced=tripped,
        drifted=drifted,
    )


def _rollout_from_config(
    plan_text: str,
    project_dir: Path,
    cli_vars: dict[str, Any] | None,
) -> Any:
    """Read only the rollout block to decide whether apply uses canary mode."""
    from drt.config.parser import load_project, load_syncs
    from drt.config.vars import resolve_vars

    try:
        sync_name = json.loads(plan_text)["sync"]["name"]
        project = load_project(project_dir)
        variables = resolve_vars(project.vars, cli_vars)
        sync = next(
            item for item in load_syncs(project_dir, vars=variables) if item.name == sync_name
        )
    except (KeyError, StopIteration, TypeError, ValueError):
        return None
    return sync.sync.rollout


def apply_plan(
    plan_text: str,
    *,
    project_dir: Path = Path("."),
    approved: bool = False,
    approved_by: str | None = None,
    confirm: Callable[[str], bool] | None = None,
    max_age: timedelta = timedelta(hours=24),
    allow_drift_pct: float = 0.0,
    force_guards: bool = False,
    cursor_value: str | None = None,
    cli_vars: dict[str, Any] | None = None,
    profile_name: str | None = None,
    json_output: bool = False,
    notify: Callable[[str], None] | None = None,
    expect_plan_id: str | None = None,
    canary_waves: list[float | int] | None = None,
    canary_max_error_pct: float | None = None,
    canary_soak: timedelta | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ApplyOutcome:
    """Apply once, or progressively when CLI/config selects a canary rollout."""
    rollout = _rollout_from_config(plan_text, project_dir, cli_vars)
    if (
        canary_waves is None
        and rollout is None
        and (canary_max_error_pct is not None or canary_soak is not None)
    ):
        raise ApplyRefused("--canary-max-error-pct/--canary-soak require --canary or sync.rollout.")
    if canary_waves is None and rollout is None:
        return _apply_plan_once(
            plan_text,
            project_dir=project_dir,
            approved=approved,
            approved_by=approved_by,
            confirm=confirm,
            max_age=max_age,
            allow_drift_pct=allow_drift_pct,
            force_guards=force_guards,
            cursor_value=cursor_value,
            cli_vars=cli_vars,
            profile_name=profile_name,
            json_output=json_output,
            notify=notify,
            expect_plan_id=expect_plan_id,
        )
    return _apply_plan_canary(
        plan_text,
        project_dir=project_dir,
        approved=approved,
        approved_by=approved_by,
        confirm=confirm,
        max_age=max_age,
        allow_drift_pct=allow_drift_pct,
        force_guards=force_guards,
        cursor_value=cursor_value,
        cli_vars=cli_vars,
        profile_name=profile_name,
        json_output=json_output,
        notify=notify,
        expect_plan_id=expect_plan_id,
        canary_waves=canary_waves,
        canary_max_error_pct=canary_max_error_pct,
        canary_soak=canary_soak,
        sleep=sleep,
        configured_rollout=rollout,
    )


def _apply_plan_canary(
    plan_text: str,
    *,
    project_dir: Path,
    approved: bool,
    approved_by: str | None,
    confirm: Callable[[str], bool] | None,
    max_age: timedelta,
    allow_drift_pct: float,
    force_guards: bool,
    cursor_value: str | None,
    cli_vars: dict[str, Any] | None,
    profile_name: str | None,
    json_output: bool,
    notify: Callable[[str], None] | None,
    expect_plan_id: str | None,
    canary_waves: list[float | int] | None,
    canary_max_error_pct: float | None,
    canary_soak: timedelta | None,
    sleep: Callable[[float], None],
    configured_rollout: Any,
) -> ApplyOutcome:
    """Verified progressive apply whose halt is terminal for this plan."""
    from drt._identifiers import new_run_id
    from drt.cli._helpers import get_destination, get_source
    from drt.cli._plan_runner import PlanCliError, compute_plan
    from drt.cli.commands.run import _run_one, _RunContext, _write_run_results
    from drt.config.duration import parse_duration as parse_config_duration
    from drt.engine.plan import drift_report

    say = notify or (lambda _message: None)
    if not math.isfinite(allow_drift_pct) or not 0 <= allow_drift_pct <= 100:
        raise ApplyRefused("allow_drift_pct must be between 0 and 100.")
    if canary_max_error_pct is not None and (
        not math.isfinite(canary_max_error_pct) or not 0 <= canary_max_error_pct <= 100
    ):
        raise ApplyRefused("canary_max_error_pct must be between 0 and 100.")

    doc = preflight_document(plan_text, project_dir=project_dir, max_age=max_age)
    plan_id = str(doc["plan_id"])
    sync_name = str(doc["sync"]["name"])
    if expect_plan_id is not None and plan_id != expect_plan_id:
        raise ApplyRefused(
            f"The stored plan is {plan_id}, not the requested {expect_plan_id}; refusing it. "
            "Call drt_plan again."
        )

    def claim_preflight(_project: Any, _state: Any, _sync: Any) -> None:
        _refuse_if_claimed(project_dir, plan_id)

    try:
        ctx_plan = compute_plan(
            sync_name,
            redact_keys=bool(doc["options"]["redact_keys"]),
            cursor_value=cursor_value,
            cli_vars=cli_vars,
            profile_name=profile_name,
            project_dir=project_dir,
            preflight=claim_preflight,
        )
    except PlanCliError as e:
        raise ApplyRefused(str(e)) from e
    current = ctx_plan.plan
    if not current.available:
        raise ApplyRefused(f"Cannot verify the plan: {current.unavailable_reason}")
    fingerprints = doc["fingerprints"]
    comparisons = (
        (
            "config_hash",
            current.config_hash,
            "The sync definition changed since the plan was made.",
        ),
        (
            "environment_hash",
            current.environment_hash,
            "The apply environment differs from the plan.",
        ),
        ("cursor_hash", current.cursor_hash, "The incremental cursor differs from the plan."),
    )
    for name, actual, message in comparisons:
        if actual != fingerprints[name]:
            raise ApplyRefused(f"{message} Re-run `drt plan`.")

    drifted = 0
    if current.digest != doc["digest"]:
        report = drift_report(doc["entries"], [entry.to_dict() for entry in current.entries])
        drifted = int(report["drifted"])
        pct = 100.0 * drifted / max(len(doc["entries"]), 1)
        if allow_drift_pct == 0 or pct > allow_drift_pct:
            raise ApplyRefused(
                describe_drift(report)
                + f"\n  {drifted} entries drifted ({pct:.1f}% of the plan; allowed "
                f"{allow_drift_pct}%). Nothing was written."
            )
        say(f"Proceeding despite drift: {drifted} entries ({pct:.1f}% <= {allow_drift_pct}%).")

    destination = get_destination(ctx_plan.sync)
    upsert_key = _validate_canary_compatibility(ctx_plan.sync, destination)

    if not current.has_changes:
        return ApplyOutcome(
            plan_id, sync_name, applied=False, message="The plan has no changes; nothing to apply."
        )
    summary = current.summary
    tripped = [trip.to_dict() for trip in current.guard_trips]
    if tripped and not force_guards:
        raise ApplyRefused(
            "Change guards tripped; nothing was written:\n"
            + "\n".join(f"  - {trip['message']}" for trip in tripped)
            + "\nFix the cause, or review and re-run with --force-guards."
        )
    if not approved:
        if confirm is None:
            raise ApplyRefused("Approval is required to apply a plan.")
        parts = [f"{summary[action]} {action}" for action in summary if summary[action]]
        if not confirm(f"Apply plan {plan_id} to {current.destination}: {', '.join(parts)}."):
            raise ApplyAborted("Aborted. Nothing was written.")

    configured_waves = configured_rollout.waves if configured_rollout is not None else None
    waves = normalize_canary_waves(canary_waves or configured_waves or [])
    max_error_pct = configured_rollout.max_error_pct if configured_rollout is not None else 2.0
    soak_seconds = 0.0
    if configured_rollout is not None and configured_rollout.soak is not None:
        try:
            soak_delta = parse_duration(configured_rollout.soak)
        except ValueError:
            soak_delta = parse_config_duration(
                configured_rollout.soak, field_name="sync.rollout.soak"
            )
        soak_seconds = soak_delta.total_seconds()
    if canary_max_error_pct is not None:
        max_error_pct = canary_max_error_pct
    if canary_soak is not None:
        soak_seconds = canary_soak.total_seconds()

    first_run_id = new_run_id()
    try:
        _claim(
            project_dir,
            plan_id,
            sync_name,
            first_run_id,
            [str(trip["guard"]) for trip in tripped],
            approved_by,
            waves=waves,
        )
    except FileExistsError as e:
        raise ApplyRefused(f"Plan {plan_id} was just taken by another apply.") from e
    _finish_claim(
        project_dir,
        plan_id,
        "pending",
        max_error_pct=max_error_pct,
        soak_seconds=soak_seconds,
    )

    wave_results: list[dict[str, Any]] = []
    run_ids: list[str] = []
    project = ctx_plan.project
    state_bundle = ctx_plan.state_bundle
    source = get_source(ctx_plan.profile)
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()

    def context(
        run_id: str,
        predicate: Callable[[dict[str, Any]], bool],
    ) -> _RunContext:
        return _RunContext(
            source=source,
            state_mgr=state_bundle.state,
            history_mgr=state_bundle.history,
            dlq_store=state_bundle.dlq,
            history_retention_days=project.history.retention_days,
            json_mode=True,
            dry_run=False,
            verbose=False,
            quiet=True,
            log_json=False,
            cursor_value=ctx_plan.cursor_value_used,
            vars=ctx_plan.project_vars,
            query_tagging=project.query_tagging,
            run_id=run_id,
            idempotency_ledger=state_bundle.ledger,
            audit_trail=state_bundle.audit_trail,
            audit_fields=project.state.audit_trail.fields,
            audit_retain_days=project.state.audit_trail.retain_days or 30,
            project_dir=project_dir,
            row_filter=predicate,
        )

    halted = False
    reason: str | None = None
    try:
        for index, upper in enumerate(waves):
            lower = 0.0 if index == 0 else waves[index - 1]
            predicate = canary_filter(
                lower,
                upper,
                upsert_key,
                ctx_plan.sync.sync.field_mappings,
                ctx_plan.sync.sync.computed_fields,
                ctx_plan.sync.sync.mask,
            )
            run_id = first_run_id if not run_ids else new_run_id()
            wave_ctx = context(run_id, predicate)
            _name, entry, had_error = _run_one(ctx_plan.sync, wave_ctx, ctx_plan.profile)
            rows, ok, error_rows, rate, passed = _canary_health(
                entry,
                had_error=had_error,
                on_error=ctx_plan.sync.sync.on_error,
                max_error_pct=max_error_pct,
            )
            verdict = "PASS" if passed else "HALT"
            wave_result = {
                "index": index,
                "wave": index + 1,
                "total_waves": len(waves),
                "cumulative_pct": upper,
                "rows": rows,
                "ok": ok,
                "failed": error_rows,
                "error_pct": round(rate, 4),
                "verdict": verdict.lower(),
                "run_id": run_id,
            }
            wave_results.append(wave_result)
            run_ids.append(run_id)
            say(
                f"wave {index + 1}/{len(waves)} ({upper:g}% cumulative): "
                f"{rows} rows, {ok} ok, {error_rows} failed — {verdict}"
            )
            if not passed:
                halted = True
                reason = (
                    str(entry.get("error", "destination failure under on_error: fail"))
                    if had_error or (ctx_plan.sync.sync.on_error == "fail" and error_rows > 0)
                    else f"wave error rate {rate:.2f}% exceeded {max_error_pct:.2f}%"
                )
            _finish_claim(
                project_dir,
                plan_id,
                "halted" if halted else "pending",
                wave_results=wave_results,
                run_ids=run_ids,
                halted_at=index if halted else None,
                reason=reason,
            )
            if halted:
                break
            if index < len(waves) - 1 and soak_seconds > 0:
                sleep(soak_seconds)
    except BaseException:
        _finish_claim(
            project_dir,
            plan_id,
            "halted",
            wave_results=wave_results,
            run_ids=run_ids,
            reason="interrupted",
        )
        raise

    canary = {
        "waves": waves,
        "results": wave_results,
        "status": "halted" if halted else "success",
        "halted_at": wave_results[-1]["index"] if halted and wave_results else None,
        "reason": reason,
        "run_ids": run_ids,
    }
    message = ""
    if halted:
        halted_wave = int(wave_results[-1]["wave"]) if wave_results else 1
        message = (
            f"HALTED after wave {halted_wave}: {reason}. This plan is terminal; "
            "run `drt plan` again, review the remaining changes, and apply the new plan."
        )
        say(message)
    else:
        _finish_claim(
            project_dir,
            plan_id,
            "success",
            wave_results=wave_results,
            run_ids=run_ids,
            halted_at=None,
            reason=None,
        )
    entry = {
        "name": sync_name,
        "status": "halted" if halted else "success",
        "plan_id": plan_id,
        "canary": canary,
    }
    if approved_by:
        entry["approved_by"] = approved_by
    if tripped:
        entry["guards_forced"] = tripped
    artifact_run_id = run_ids[0] if run_ids else first_run_id
    _write_run_results(
        project_dir / "target" / "drt",
        run_id=artifact_run_id,
        started_at=started_at,
        results=[entry],
        succeeded=0 if halted else 1,
        failed=1 if halted else 0,
        skipped=0,
        total_duration=round(time.monotonic() - t0, 2),
        exit_code=1 if halted else 0,
    )
    return ApplyOutcome(
        plan_id,
        sync_name,
        applied=True,
        message=message,
        run_id=artifact_run_id,
        entry=entry,
        had_error=halted,
        guards_forced=tripped,
        drifted=drifted,
        run_ids=run_ids,
        canary=canary,
        halted=halted,
    )
