"""Shared path that computes a plan, used by ``drt plan`` and ``drt apply``.

``drt apply`` verifies a plan by recomputing it, so both commands must go
through exactly the same code or "the plan still matches" would mean nothing.
"""

from __future__ import annotations

import dataclasses
import os
import secrets
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from drt import __version__
from drt.cli._helpers import (
    get_destination,
    get_source,
    get_watermark_storage,
    resolve_profile_name,
)
from drt.config.models import SyncConfig
from drt.engine.plan import Plan, build_plan, config_hash, hash_value, unsupported_reason


class PlanCliError(Exception):
    """A user-facing failure; the command prints the message and exits 1."""


@dataclass
class PlanContext:
    plan: Plan
    sync: SyncConfig
    project: Any
    profile: Any
    state_bundle: Any
    project_vars: dict[str, Any] | None
    # In-memory only (never written to plan.json): the window the plan was
    # computed over, so `drt apply` can run the same window.
    cursor_value_used: str | None = None


def sync_fingerprint(sync: SyncConfig) -> str:
    """Hash of the sync file *and* the model SQL it references (#772's fingerprint)."""
    from drt.config.fingerprint import sync_fingerprints

    from_files = sync_fingerprints(Path(".")).get(sync.name)
    return f"sha256:{from_files}" if from_files else config_hash(sync)


def load_plan_key() -> bytes:
    """The secret that keys every hash in a plan.

    ``DRT_PLAN_KEY`` wins (set the same secret in the jobs that plan and apply);
    otherwise a random key is created once in ``.drt/plan.key`` (mode 0600), so a
    plan applies in the workspace that made it. The key is never written to a plan.
    """
    env = os.environ.get("DRT_PLAN_KEY")
    if env:
        return env.encode("utf-8")
    path = Path(".drt") / "plan.key"
    try:
        return path.read_bytes()
    except FileNotFoundError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    key = secrets.token_hex(32).encode("ascii")
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:  # another process created it first
        return path.read_bytes()
    with os.fdopen(fd, "wb") as handle:
        handle.write(key)
    return key


def _canonical_profile(profile: Any) -> Any:
    if hasattr(profile, "model_dump"):
        return profile.model_dump(mode="json")
    if dataclasses.is_dataclass(profile) and not isinstance(profile, type):
        return dataclasses.asdict(profile)
    if hasattr(profile, "__dict__"):
        return vars(profile)
    raise PlanCliError(
        f"The profile type {type(profile).__name__} cannot be fingerprinted for a plan; "
        "it must be a Pydantic model or a dataclass."
    )


def environment_fingerprint(
    sync: SyncConfig, project_vars: dict[str, Any] | None, profile: Any, plan_key: bytes
) -> str:
    """Keyed hash of what the sync *resolves to* in this environment.

    The file fingerprint deliberately ignores environment variables, project
    vars and the profile; for a plan those decide where and what gets written,
    so a plan made in one environment must not verify in another. The inputs
    include resolved secrets, so the hash is keyed and only the hash is stored.
    It is deliberately conservative: any change to the resolved config, any
    project var or the profile (including a rotated literal credential) is
    treated as a different environment.
    """
    body = {
        "sync": sync.model_dump(mode="json"),
        "vars": project_vars or {},
        "profile": _canonical_profile(profile),
    }
    return hash_value(body, plan_key)


def _engine_written_columns(sync: SyncConfig) -> set[str]:
    metadata = getattr(sync.sync, "metadata_columns", None)
    if metadata is None:
        return set()
    return {
        target
        for name in ("synced_at", "run_id", "sync_name")
        if (target := getattr(metadata, name, None))
    }


def compute_plan(
    sync_name: str,
    *,
    redact_keys: bool = False,
    cursor_value: str | None = None,
    vars_raw: str | None = None,
    profile_name: str | None = None,
    preflight: Callable[[Any, Any, SyncConfig], None] | None = None,
) -> PlanContext:
    """Plan one sync, read-only. Raises :class:`PlanCliError` when it cannot run at all.

    A sync that simply cannot be planned (unsupported destination, failed
    extraction) is not an exception: the returned plan has ``available=False``
    and a reason, so callers can render it.

    ``preflight(project, state_bundle, sync)`` may raise :class:`PlanCliError` to
    stop before anything is extracted.
    """
    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.config.vars import VarError, parse_cli_vars, resolve_vars
    from drt.engine.sync import run_sync
    from drt.state.factory import build_state_bundle

    try:
        project = load_project(Path("."))
    except FileNotFoundError as e:
        raise PlanCliError(str(e)) from e

    try:
        profile = load_profile(resolve_profile_name(profile_name, project.profile))
    except (FileNotFoundError, KeyError, ValueError) as e:
        raise PlanCliError(str(e)) from e

    try:
        cli_vars = parse_cli_vars(vars_raw) if vars_raw else None
        project_vars = resolve_vars(project.vars, cli_vars)
        syncs = load_syncs(Path("."), vars=project_vars)
    except VarError as e:
        raise PlanCliError(str(e)) from e

    matches = [s for s in syncs if s.name == sync_name]
    if not matches:
        raise PlanCliError(f"Sync '{sync_name}' not found in syncs/.")
    sync = matches[0]

    blocker = unsupported_reason(sync)
    if blocker is not None:
        raise PlanCliError(f"Plan unavailable for '{sync.name}': {blocker}.")

    state_bundle = build_state_bundle(project, Path("."))
    if preflight is not None:
        # Cheap checks that must run before the (possibly expensive) extraction.
        preflight(project, state_bundle, sync)
    try:
        result = run_sync(
            sync,
            get_source(profile),
            get_destination(sync),
            profile,
            Path("."),
            True,
            state_bundle.state,
            watermark_storage=get_watermark_storage(sync, Path(".")),
            cursor_value_override=cursor_value if sync.sync.mode == "incremental" else None,
            compute_diff=True,
            # Not a sample: a plan needs every changed key.
            diff_limit=sys.maxsize,
            vars=project_vars,
            query_tagging=project.query_tagging,
        )
    except Exception as e:
        raise PlanCliError(f"Could not compute a plan for '{sync_name}': {e}") from e

    if result.diff is None:
        raise PlanCliError(f"No diff was produced for '{sync_name}'.")
    if result.failed or result.interrupted:
        # A diff over a partial extraction would read as "nothing else changes".
        raise PlanCliError(
            f"Plan unavailable for '{sync_name}': extraction did not complete "
            f"({result.failed} failed row(s)"
            f"{', interrupted' if result.interrupted else ''})."
        )

    plan_key = load_plan_key()
    destination = sync.destination
    plan = build_plan(
        result.diff,
        sync_name=sync.name,
        sync_mode=sync.sync.mode,
        match_policy=sync.sync.match_policy,
        destination=getattr(destination, "describe_safe", lambda: str(destination.type))(),
        config_fingerprint=sync_fingerprint(sync),
        environment_fingerprint=environment_fingerprint(sync, project_vars, profile, plan_key),
        plan_key=plan_key,
        drt_version=__version__,
        key_columns=list(getattr(destination, "upsert_key", None) or []),
        mask_columns=set(sync.sync.mask or {}),
        redact_keys=redact_keys,
        exclude_columns=_engine_written_columns(sync),
        cursor_value=result.cursor_value_used,
    )
    return PlanContext(
        plan, sync, project, profile, state_bundle, project_vars, result.cursor_value_used
    )
