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
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from drt import __version__

_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DRIFT_SHOWN = 10
_CLOCK_SKEW = timedelta(minutes=5)
_DRIFT_ITEM_CHARS = 300
_DEFAULT_MAX_SPOOL_ROWS = 1_000_000
_DEFAULT_MAX_SPOOL_BYTES = 1_073_741_824
PLAN_ID_PATTERN = re.compile(r"plan-[0-9a-f]{16}")  # use fullmatch(): `$` allows a trailing newline


class ApplyRefused(Exception):
    """Nothing was written, for the reason in the message."""


class ApplyAborted(ApplyRefused):
    """The person declined the confirmation: nothing was written, and it is not an error."""


class CanaryWaves(list[int]):
    """Validated cumulative wave bounds in integer hundredths of a percent."""


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


def normalize_canary_waves(waves: Sequence[Decimal | float | int]) -> CanaryWaves:
    """Return exact cumulative hundredths-of-a-percent, appending 100%."""
    if not waves:
        raise ValueError("canary waves must contain at least one percentage")
    if any(isinstance(wave, bool) or not isinstance(wave, Decimal | int | float) for wave in waves):
        raise ValueError("canary waves must contain only numbers (not booleans or strings)")
    decimals = [Decimal(str(wave)) for wave in waves]
    if any(not wave.is_finite() or not 0 < wave <= 100 for wave in decimals):
        raise ValueError("canary waves must each be greater than 0 and at most 100")
    scaled = [wave * 100 for wave in decimals]
    if any(wave != wave.to_integral_value() for wave in scaled):
        raise ValueError("canary waves must be multiples of 0.01 (the bucket resolution)")
    normalized = [int(wave) for wave in scaled]
    if any(left >= right for left, right in zip(normalized, normalized[1:], strict=False)):
        raise ValueError("canary waves must be strictly increasing without duplicates")
    if normalized[-1] != 10_000:
        normalized.append(10_000)
    return CanaryWaves(normalized)


def parse_canary_waves(text: str) -> CanaryWaves:
    """Parse the CLI's comma-separated numeric wave list strictly."""
    parts = [part.strip() for part in text.split(",")]
    if not parts or any(not part for part in parts):
        raise ValueError("canary waves must be a comma-separated list of numbers")
    try:
        values = [Decimal(part) for part in parts]
    except Exception as e:
        raise ValueError("canary waves must contain only numbers") from e
    return normalize_canary_waves(values)


def _decimal_key_text(number: Decimal) -> str:
    """Canonical finite Decimal without consulting the mutable decimal context."""
    if number.is_zero():
        return "0"
    parts = number.as_tuple()
    digits = list(parts.digits)
    exponent = int(parts.exponent)
    while digits and digits[-1] == 0:
        digits.pop()
        exponent += 1
    coefficient = "".join(str(digit) for digit in digits) or "0"
    return f"{'-' if parts.sign else ''}{coefficient}e{exponent}"


def _canonical_key_value(value: Any) -> bytes:
    """Encode one destination key value with an unambiguous, stable type tag."""
    if value is None:
        raise ValueError("Canary key components cannot be null.")
    if isinstance(value, bool | int | float | Decimal):
        number = Decimal(int(value)) if isinstance(value, bool) else Decimal(str(value))
        if not number.is_finite():
            raise ValueError(f"Unsupported non-finite canary key number: {value!r}")
        text = _decimal_key_text(number)
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
    if isinstance(value, UUID):
        return b"uuid:" + value.hex.encode()
    raise ValueError(
        "Unsupported canary key type "
        f"{type(value).__name__}; use bool/number, string, date/datetime, UUID, or bytes."
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
    lower_hundredths: int,
    upper_hundredths: int,
    upsert_key: list[str] | None,
    field_mappings: dict[str, str] | None = None,
    computed_fields: dict[str, str] | None = None,
    mask: dict[str, Any] | None = None,
) -> Callable[[dict[str, Any]], bool]:
    """Build an exclusive stable bucket-range predicate for one wave."""

    def selected(record: dict[str, Any]) -> bool:
        bucket = _raw_record_bucket(record, upsert_key, field_mappings, computed_fields, mask)
        return lower_hundredths <= bucket < upper_hundredths

    return selected


def _raw_record_bucket(
    record: dict[str, Any],
    upsert_key: list[str] | None,
    field_mappings: dict[str, str] | None = None,
    computed_fields: dict[str, str] | None = None,
    mask: dict[str, Any] | None = None,
) -> int:
    """Compute a bucket from the destination-facing key without mutating raw input."""
    keyed = record
    if upsert_key:
        from drt.engine.computed_fields import apply_computed_fields
        from drt.engine.field_mappings import apply_field_mappings
        from drt.engine.masking import apply_mask

        key_fields = {
            name: template
            for name, template in (computed_fields or {}).items()
            if (field_mappings or {}).get(name, name) in upsert_key
        }
        transformed, _errors = apply_computed_fields([dict(record)], key_fields, "fail")
        transformed = apply_field_mappings(transformed, field_mappings)
        transformed = apply_mask(transformed, mask)
        keyed = transformed[0]
    return canary_bucket(keyed, upsert_key)


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
    fatal_destination_error = on_error == "fail" and (had_error or destination_failed > 0)
    return rows, ok, error_rows, rate, not fatal_destination_error and rate <= max_error_pct


def _validate_canary_compatibility(sync: Any, destination: Any) -> list[str]:
    """Fail closed unless this is the stateless, keyed per-row v2 path."""
    from drt.destinations.base import KeyedUpsertCapable, StagedDestination

    if isinstance(destination, StagedDestination):
        raise ApplyRefused(
            "Canary apply supports only non-staged per-row destinations; "
            "remove --canary/sync.rollout or use a non-staged destination."
        )
    upsert_key = getattr(sync.destination, "upsert_key", None)
    if sync.sync.mode not in {"full", "upsert"}:
        raise ApplyRefused(
            "Canary apply supports only non-incremental sync.mode: full or upsert; "
            f"mode: {sync.sync.mode} can change watermark/finalize state across waves. "
            "Remove --canary/sync.rollout or use a plain full/upsert sync."
        )
    if sync.sync.incremental_strategy == "diff":
        raise ApplyRefused(
            "Canary apply does not support incremental_strategy: diff because each wave "
            "would mutate the snapshot baseline. Remove --canary/sync.rollout or use the "
            "plain non-incremental write path."
        )
    if not upsert_key:
        raise ApplyRefused(
            "Canary apply requires destination.upsert_key so rows have stable wave keys "
            "and a re-plan after a halt can identify what remains."
        )
    destination_name = getattr(sync.destination, "type", type(destination).__name__)
    if not isinstance(destination, KeyedUpsertCapable) or not destination.supports_keyed_upsert(
        sync.destination
    ):
        hint = (
            " Set destination.mode: merge."
            if destination_name in {"snowflake", "databricks", "bigquery"}
            else " Use PostgreSQL/MySQL or a merge-mode Snowflake/Databricks/BigQuery destination."
        )
        raise ApplyRefused(
            f"Canary apply requires a keyed-upsert destination; {destination_name!r} does not "
            f"provably overwrite the same upsert_key row.{hint}"
        )
    if sync.sync.metadata_columns is not None:
        raise ApplyRefused(
            "Canary apply cannot be combined with sync.metadata_columns: run-specific values "
            "break the guarantee that a new plan lists only what remains. Remove "
            "metadata_columns or the rollout."
        )
    if sync.sync.match_policy != "upsert":
        raise ApplyRefused(
            "Canary apply requires sync.match_policy: upsert; update_only/create_only cannot "
            "guarantee that a new plan lists only what remains."
        )
    masked_keys = []
    for key in upsert_key:
        rule = (sync.sync.mask or {}).get(key)
        strategy = rule if isinstance(rule, str) else getattr(rule, "strategy", None)
        if strategy not in {None, "hash"}:
            masked_keys.append(key)
    if masked_keys:
        raise ApplyRefused(
            "Canary apply requires injective key handling; upsert_key columns cannot use "
            f"redact/truncate masking: {sorted(masked_keys)}. Use hash or no key mask."
        )
    lookup_outputs = {
        (sync.sync.field_mappings or {}).get(name, name)
        for name in (getattr(sync.destination, "lookups", None) or {})
    }
    unstable_keys = set(upsert_key) & lookup_outputs
    if unstable_keys:
        raise ApplyRefused(
            "Canary apply cannot use an upsert_key produced by a destination lookup because "
            f"it is not stable before each wave: {sorted(unstable_keys)}. "
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
    elif state in {"pending", "running"}:
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


def _finish_claim(project_dir: Path, plan_id: str, state: str, **updates: Any) -> bool:
    """Best-effort claim update; return false on ordinary bookkeeping failures.

    ``BaseException`` deliberately propagates.  In particular, a second Ctrl-C
    while writing a claim must never be swallowed.
    """
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
    except Exception as e:  # noqa: BLE001 - terminal bookkeeping is best-effort
        try:
            log.warning(
                "Could not record the outcome of plan %s (%s); %s may still say 'pending'.",
                plan_id,
                e,
                _claim_path(project_dir, plan_id),
            )
        except Exception:  # noqa: BLE001 - logging is also best-effort here
            pass
        return False
    return True


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
    canary_waves: Sequence[Decimal | float | int] | None = None,
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
    canary_waves: Sequence[Decimal | float | int] | None,
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
    requested_waves = canary_waves or configured_waves or []
    waves = (
        requested_waves
        if isinstance(requested_waves, CanaryWaves)
        else normalize_canary_waves(requested_waves)
    )
    displayed_waves = [wave / 100 for wave in waves]
    max_error_pct = configured_rollout.max_error_pct if configured_rollout is not None else 2.0
    max_spool_rows = (
        configured_rollout.max_spool_rows
        if configured_rollout is not None
        else _DEFAULT_MAX_SPOOL_ROWS
    )
    max_spool_bytes = (
        configured_rollout.max_spool_bytes
        if configured_rollout is not None
        else _DEFAULT_MAX_SPOOL_BYTES
    )
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
            waves=displayed_waves,
        )
    except FileExistsError as e:
        raise ApplyRefused(f"Plan {plan_id} was just taken by another apply.") from e
    if not _finish_claim(
        project_dir,
        plan_id,
        "pending",
        max_error_pct=max_error_pct,
        soak_seconds=soak_seconds,
    ):
        raise ApplyRefused("Could not initialize the canary claim; nothing was written.")

    wave_results: list[dict[str, Any]] = []
    run_ids: list[str] = []
    project = ctx_plan.project
    state_bundle = ctx_plan.state_bundle
    source = get_source(ctx_plan.profile)
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    spool_path = claims_dir(project_dir) / f"{plan_id}.spool.jsonl"

    def context(
        run_id: str,
        run_source: Any,
        predicate: Callable[[dict[str, Any]], bool] | None,
        *,
        dry_run: bool = False,
    ) -> _RunContext:
        return _RunContext(
            source=run_source,
            state_mgr=state_bundle.state,
            history_mgr=state_bundle.history,
            dlq_store=state_bundle.dlq,
            history_retention_days=project.history.retention_days,
            json_mode=True,
            dry_run=dry_run,
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

    def write_artifact(status: str, *, exit_code: int) -> None:
        result_entry: dict[str, Any] = {
            "name": sync_name,
            "status": status,
            "plan_id": plan_id,
            "canary": {
                "waves": displayed_waves,
                "results": wave_results,
                "status": status,
                "run_ids": run_ids,
            },
        }
        if approved_by:
            result_entry["approved_by"] = approved_by
        if tripped:
            result_entry["guards_forced"] = tripped
        _write_run_results(
            project_dir / "target" / "drt",
            run_id=run_ids[0] if run_ids else first_run_id,
            started_at=started_at,
            results=[result_entry],
            succeeded=1 if status == "success" else 0,
            failed=0 if status == "success" else 1,
            skipped=0,
            total_duration=round(time.monotonic() - t0, 2),
            exit_code=exit_code,
        )

    halted = False
    reason: str | None = None
    writer: Any = None
    rollout_started = False
    try:
        from drt.cli._canary_spool import (
            SnapshotSpoolSource,
            SnapshotSpoolWriter,
            SpoolError,
            verify_spool,
        )

        bucket_of = lambda record: _raw_record_bucket(  # noqa: E731 - callback with captured config
            record,
            upsert_key,
            ctx_plan.sync.sync.field_mappings,
            ctx_plan.sync.sync.computed_fields,
            ctx_plan.sync.sync.mask,
        )
        try:
            writer = SnapshotSpoolWriter(
                spool_path,
                max_rows=max_spool_rows,
                max_bytes=max_spool_bytes,
                bucket_of=bucket_of,
            )
            _name, snapshot_entry, snapshot_error = _run_one(
                ctx_plan.sync,
                context(first_run_id, source, writer.append, dry_run=True),
                ctx_plan.profile,
            )
            spool_rows, spool_bytes, spool_sha256 = writer.close()
            if snapshot_error:
                raise SpoolError(
                    str(snapshot_entry.get("error", "source extraction did not complete"))
                )
            extracted_value = snapshot_entry.get("rows_extracted", spool_rows)
            extracted_rows = (
                int(extracted_value) if isinstance(extracted_value, int | float) else -1
            )
            if extracted_rows != spool_rows:
                raise SpoolError("Canary snapshot row count did not match completed extraction.")
            if not _finish_claim(
                project_dir,
                plan_id,
                "pending",
                spool={
                    "rows": spool_rows,
                    "bytes": spool_bytes,
                    "sha256": spool_sha256,
                },
            ):
                raise SpoolError("Could not record the canary snapshot in the claim.")
        except Exception as exc:
            if writer is not None:
                writer.abort()
            _finish_claim(project_dir, plan_id, "failed", reason="snapshot_failed")
            raise ApplyRefused(f"Could not create the canary snapshot: {exc}") from exc

        replay_source = SnapshotSpoolSource(spool_path)
        rollout_started = True
        for index, upper in enumerate(waves):
            lower = 0 if index == 0 else waves[index - 1]
            predicate = canary_filter(
                lower,
                upper,
                upsert_key,
                ctx_plan.sync.sync.field_mappings,
                ctx_plan.sync.sync.computed_fields,
                ctx_plan.sync.sync.mask,
            )
            run_id = first_run_id if not run_ids else new_run_id()
            selected = sum(writer.buckets[lower:upper])
            run_ids.append(run_id)
            if not _finish_claim(
                project_dir,
                plan_id,
                "running",
                wave_results=wave_results,
                run_ids=run_ids,
                current_wave=index,
                current_run_id=run_id,
            ):
                raise ApplyRefused(
                    "Could not record the current canary wave in the claim; refusing to write it."
                )
            try:
                verify_spool(spool_path, spool_sha256)
            except SpoolError as exc:
                raise ApplyRefused(str(exc)) from exc
            wave_ctx = context(run_id, replay_source, predicate)
            _name, entry, had_error = _run_one(ctx_plan.sync, wave_ctx, ctx_plan.profile)
            if had_error and "rows_extracted" not in entry:
                halted = True
                reason = "wave raised an exception"
                wave_result = {
                    "index": index,
                    "wave": index + 1,
                    "total_waves": len(waves),
                    "cumulative_pct": upper / 100,
                    "selected": selected,
                    "outcome": "error",
                    "exception": str(entry.get("error_type", "Exception")),
                    "failed": None,
                    "verdict": "halt",
                    "run_id": run_id,
                }
                wave_results.append(wave_result)
                if not _finish_claim(
                    project_dir,
                    plan_id,
                    "halted",
                    wave_results=wave_results,
                    run_ids=run_ids,
                    halted_at=index,
                    reason=reason,
                ):
                    raise ApplyRefused("Could not record the terminal canary outcome.")
                write_artifact("halted", exit_code=1)
                break
            health_entry = dict(entry)
            health_entry["rows_extracted"] = selected
            rows, ok, error_rows, rate, passed = _canary_health(
                health_entry,
                had_error=had_error,
                on_error=ctx_plan.sync.sync.on_error,
                max_error_pct=max_error_pct,
            )
            verdict = "PASS" if passed else "HALT"
            wave_result = {
                "index": index,
                "wave": index + 1,
                "total_waves": len(waves),
                "cumulative_pct": upper / 100,
                "selected": selected,
                "outcome": "completed",
                "rows": rows,
                "ok": ok,
                "failed": error_rows,
                "error_pct": round(rate, 4),
                "verdict": verdict.lower(),
                "run_id": run_id,
            }
            wave_results.append(wave_result)
            say(
                f"wave {index + 1}/{len(waves)} ({upper / 100:g}% cumulative): "
                f"{rows} rows, {ok} ok, {error_rows} failed — {verdict}"
            )
            if not passed:
                halted = True
                reason = (
                    "destination failure under on_error: fail"
                    if ctx_plan.sync.sync.on_error == "fail" and error_rows > 0
                    else f"wave error rate {rate:.2f}% exceeded {max_error_pct:.2f}%"
                )
            if not _finish_claim(
                project_dir,
                plan_id,
                "halted" if halted else "running",
                wave_results=wave_results,
                run_ids=run_ids,
                halted_at=index if halted else None,
                reason=reason,
                current_wave=None,
                current_run_id=None,
            ):
                raise ApplyRefused(
                    "Could not update the canary claim; aborting before the next wave."
                )
            write_artifact("halted" if halted else "running", exit_code=1 if halted else 0)
            if halted:
                break
            if index < len(waves) - 1 and soak_seconds > 0:
                sleep(soak_seconds)
    except BaseException as exc:
        if not rollout_started:
            raise
        if run_ids and (not wave_results or wave_results[-1].get("run_id") != run_ids[-1]):
            current_index = len(run_ids) - 1
            lower = 0 if current_index == 0 else waves[current_index - 1]
            upper = waves[current_index]
            wave_results.append(
                {
                    "index": current_index,
                    "wave": current_index + 1,
                    "total_waves": len(waves),
                    "cumulative_pct": upper / 100,
                    "selected": sum(writer.buckets[lower:upper]),
                    "outcome": "error",
                    "exception": type(exc).__name__,
                    "failed": None,
                    "verdict": "halt",
                    "run_id": run_ids[-1],
                }
            )
        _finish_claim(
            project_dir,
            plan_id,
            "halted",
            wave_results=wave_results,
            run_ids=run_ids,
            reason="interrupted",
        )
        try:
            write_artifact("halted", exit_code=1)
        except Exception:  # noqa: BLE001 - interruption must retain priority
            pass
        raise
    finally:
        try:
            spool_path.unlink(missing_ok=True)
        except OSError:
            logging.getLogger(__name__).warning(
                "Could not delete canary snapshot %s; remove it manually.", spool_path
            )

    canary = {
        "waves": displayed_waves,
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
        if not _finish_claim(
            project_dir,
            plan_id,
            "success",
            wave_results=wave_results,
            run_ids=run_ids,
            halted_at=None,
            reason=None,
        ):
            raise ApplyRefused("The writes completed, but the canary claim could not be finalized.")
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
    write_artifact("halted" if halted else "success", exit_code=1 if halted else 0)
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
