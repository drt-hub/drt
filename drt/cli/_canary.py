"""Progressive canary apply orchestration and artifacts."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from drt.cli._canary_spool import (
    SnapshotSpoolSource,
    SnapshotSpoolWriter,
    SpoolError,
    verify_spool,
)
from drt.engine.canary import (
    CanaryWaves,
    canary_bucket,
    canary_filter,
    canary_health,
    compatibility_problem,
    normalize_canary_waves,
)

_DEFAULT_MAX_SPOOL_ROWS = 1_000_000
_DEFAULT_MAX_SPOOL_BYTES = 1_073_741_824

if TYPE_CHECKING:
    from drt.cli._apply_flow import ApplyOutcome


def _configured_sync(plan_text: str, project_dir: Path, cli_vars: dict[str, Any] | None) -> Any:
    import json

    from drt.config.parser import load_project, load_syncs
    from drt.config.vars import resolve_vars

    sync_name = json.loads(plan_text)["sync"]["name"]
    project = load_project(project_dir)
    variables = resolve_vars(project.vars, cli_vars)
    return next(item for item in load_syncs(project_dir, vars=variables) if item.name == sync_name)


def rollout_from_config(plan_text: str, project_dir: Path, cli_vars: dict[str, Any] | None) -> Any:
    """Discover a configured rollout without instantiating a connector."""
    try:
        return _configured_sync(plan_text, project_dir, cli_vars).sync.rollout
    except (KeyError, StopIteration, TypeError, ValueError):
        return None


def apply_plan_canary(
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
    """Verify and progressively apply the exact post-transform plan snapshot."""
    from drt._identifiers import new_run_id
    from drt.cli._apply_claims import (
        UnsafeApplyArtifacts,
        claims_dir,
    )
    from drt.cli._apply_claims import (
        claim as _claim,
    )
    from drt.cli._apply_claims import (
        finish_claim as _finish_claim,
    )
    from drt.cli._apply_claims import (
        sweep_canary_spools as _sweep_canary_spools,
    )
    from drt.cli._apply_flow import (
        ApplyAborted,
        ApplyOutcome,
        ApplyRefused,
        describe_drift,
        parse_duration,
        preflight_document,
    )
    from drt.cli._helpers import get_destination
    from drt.cli._plan_runner import compute_plan
    from drt.cli.commands.run import _run_one, _RunContext, _write_run_results
    from drt.config.duration import parse_duration as parse_config_duration
    from drt.destinations.base import KeyedUpsertVerifiable
    from drt.engine.plan import drift_report

    del json_output
    say = notify or (lambda _message: None)
    try:
        _sweep_canary_spools(project_dir, notify=say)
    except UnsafeApplyArtifacts as exc:
        raise ApplyRefused(str(exc)) from exc
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

    try:
        configured_sync = _configured_sync(plan_text, project_dir, cli_vars)
    except (KeyError, StopIteration, TypeError, ValueError) as exc:
        raise ApplyRefused("Could not resolve the canary sync configuration.") from exc
    destination = get_destination(configured_sync)
    upsert_key, problem = compatibility_problem(configured_sync, destination)
    if problem is not None or upsert_key is None:
        raise ApplyRefused(problem or "Canary apply is incompatible with this sync.")

    configured_waves = configured_rollout.waves if configured_rollout is not None else None
    requested_waves = canary_waves or configured_waves or []
    waves = (
        CanaryWaves(int(wave) for wave in requested_waves)
        if requested_waves.__class__.__name__ == "CanaryWaves"
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
    spool_path = claims_dir(project_dir) / f"{plan_id}.spool.jsonl"
    try:
        _claim(
            project_dir,
            plan_id,
            sync_name,
            first_run_id,
            approved_by=approved_by,
            waves=displayed_waves,
            spool_path=spool_path,
        )
    except FileExistsError as exc:
        raise ApplyRefused(f"Plan {plan_id} was just taken by another apply.") from exc
    if not _finish_claim(
        project_dir,
        plan_id,
        "pending",
        max_error_pct=max_error_pct,
        soak_seconds=soak_seconds,
    ):
        _finish_claim(project_dir, plan_id, "failed", reason="claim_write_failed")
        raise ApplyRefused("Could not initialize the canary claim; nothing was written.")

    writer: SnapshotSpoolWriter | None = None
    wave_results: list[dict[str, Any]] = []
    run_ids: list[str] = []
    halted = False
    reason: str | None = None
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    ctx_plan: Any = None
    artifact_writer: Callable[[str], None] | None = None

    def finish_failure(exc: BaseException, stage_reason: str | None = None) -> None:
        nonlocal reason
        reason = stage_reason or (
            "interrupted" if not isinstance(exc, Exception) else f"error:{type(exc).__name__}"
        )
        _finish_claim(
            project_dir,
            plan_id,
            "halted",
            wave_results=wave_results,
            run_ids=run_ids,
            reason=reason,
        )

    try:
        try:
            writer = SnapshotSpoolWriter(
                spool_path,
                max_rows=max_spool_rows,
                max_bytes=max_spool_bytes,
                bucket_of=lambda record: canary_bucket(record, upsert_key),
            )
            ctx_plan = compute_plan(
                sync_name,
                redact_keys=bool(doc["options"]["redact_keys"]),
                cursor_value=cursor_value,
                cli_vars=cli_vars,
                profile_name=profile_name,
                project_dir=project_dir,
                record_sink=writer.append,
            )
            spool_rows, spool_bytes, spool_sha256 = writer.close()
        except Exception as exc:
            if writer is not None:
                writer.abort()
            finish_failure(exc, "snapshot_failed")
            root = exc
            while isinstance(root.__cause__, Exception):
                root = root.__cause__
            raise ApplyRefused(
                f"Canary snapshot verification failed at snapshot stage ({type(root).__name__})."
            ) from exc

        if not _finish_claim(
            project_dir,
            plan_id,
            "pending",
            spool={"rows": spool_rows, "bytes": spool_bytes, "sha256": spool_sha256},
        ):
            finish_failure(RuntimeError(), "claim_write_failed")
            raise ApplyRefused("Could not record the canary snapshot in the claim.")

        current = ctx_plan.plan
        if not current.available:
            raise ApplyRefused(f"Cannot verify the plan: {current.unavailable_reason}")
        if isinstance(current.total_source_rows, int) and current.total_source_rows != spool_rows:
            raise ApplyRefused(
                "Canary snapshot row count did not match completed plan verification."
            )
        for name, actual, message in (
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
        ):
            if actual != doc["fingerprints"][name]:
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
        checked_key, problem = compatibility_problem(ctx_plan.sync, destination)
        if problem is not None or checked_key != upsert_key:
            raise ApplyRefused(problem or "Canary key changed during verification.")
        if not current.has_changes:
            if not _finish_claim(project_dir, plan_id, "success", reason="no_changes"):
                reason = "claim_write_failed"
                raise ApplyRefused("Could not finalize the no-change canary claim.")
            return ApplyOutcome(
                plan_id,
                sync_name,
                applied=False,
                message="The plan has no changes; nothing to apply.",
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

        if isinstance(destination, KeyedUpsertVerifiable):
            try:
                verification_problem = destination.verify_keyed_upsert(ctx_plan.sync.destination)
            except Exception as exc:
                reason = f"error:{type(exc).__name__}"
                raise ApplyRefused(
                    "Could not verify the destination's keyed-upsert constraint "
                    f"({type(exc).__name__})."
                ) from exc
            if verification_problem is not None:
                raise ApplyRefused(verification_problem)

        project = ctx_plan.project
        state_bundle = ctx_plan.state_bundle
        replay_source = SnapshotSpoolSource(writer.fd)

        def context(run_id: str, predicate: Callable[[dict[str, Any]], bool]) -> _RunContext:
            return _RunContext(
                source=replay_source,
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
                replay_records=replay_source.extract("", ctx_plan.profile),
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

        def interrupted_artifact(status: str) -> None:
            write_artifact(status, exit_code=1)

        artifact_writer = interrupted_artifact

        for index, upper in enumerate(waves):
            lower = 0 if index == 0 else waves[index - 1]
            predicate = canary_filter(lower, upper, upsert_key)
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
                reason = "claim_write_failed"
                raise ApplyRefused(
                    "Could not record the current canary wave in the claim; refusing to write it."
                )
            try:
                verify_spool(writer.fd, spool_sha256)
            except SpoolError as exc:
                reason = "spool_tampered"
                raise ApplyRefused(str(exc)) from exc
            _name, entry, had_error = _run_one(
                ctx_plan.sync, context(run_id, predicate), ctx_plan.profile
            )
            if had_error and "rows_extracted" not in entry:
                halted = True
                reason = f"error:{entry.get('error_type', 'Exception')}"
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
                    reason = "claim_write_failed"
                    raise ApplyRefused("Could not record the terminal canary outcome.")
                write_artifact("halted", exit_code=1)
                break
            health_entry = dict(entry)
            health_entry["rows_extracted"] = selected
            rows, ok, error_rows, rate, passed = canary_health(
                health_entry,
                had_error=had_error,
                on_error=ctx_plan.sync.sync.on_error,
                max_error_pct=max_error_pct,
            )
            verdict = "PASS" if passed else "HALT"
            wave_results.append(
                {
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
            )
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
                reason = "claim_write_failed"
                raise ApplyRefused(
                    "Could not update the canary claim; aborting before the next wave."
                )
            write_artifact("halted" if halted else "running", exit_code=1 if halted else 0)
            if halted:
                break
            if index < len(waves) - 1 and soak_seconds > 0:
                sleep(soak_seconds)

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
        elif not _finish_claim(
            project_dir,
            plan_id,
            "success",
            wave_results=wave_results,
            run_ids=run_ids,
            halted_at=None,
            reason=None,
        ):
            reason = "claim_write_failed"
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
        write_artifact("halted" if halted else "success", exit_code=1 if halted else 0)
        return ApplyOutcome(
            plan_id,
            sync_name,
            applied=True,
            message=message,
            run_id=run_ids[0] if run_ids else first_run_id,
            entry=entry,
            had_error=halted,
            guards_forced=tripped,
            drifted=drifted,
            run_ids=run_ids,
            canary=canary,
            halted=halted,
        )
    except BaseException as exc:
        if run_ids and (not wave_results or wave_results[-1].get("run_id") != run_ids[-1]):
            current_index = len(run_ids) - 1
            lower = 0 if current_index == 0 else waves[current_index - 1]
            upper = waves[current_index]
            selected = sum(writer.buckets[lower:upper]) if writer is not None else 0
            wave_results.append(
                {
                    "index": current_index,
                    "wave": current_index + 1,
                    "total_waves": len(waves),
                    "cumulative_pct": upper / 100,
                    "selected": selected,
                    "outcome": "error",
                    "exception": type(exc).__name__,
                    "failed": None,
                    "verdict": "halt",
                    "run_id": run_ids[-1],
                }
            )
        finish_failure(exc, reason)
        if artifact_writer is not None:
            try:
                artifact_writer("halted")
            except Exception:
                pass
        raise
    finally:
        if writer is not None:
            try:
                removed = writer.unlink_owned()
                if not removed:
                    say(f"Canary snapshot path changed; preserved replacement at {spool_path}.")
            except OSError:
                logging.getLogger(__name__).warning(
                    "Could not delete canary snapshot %s; remove it manually.", spool_path
                )
            writer.dispose()
