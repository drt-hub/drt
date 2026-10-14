"""Filesystem-safe apply claim primitives shared by plain and canary apply."""

from __future__ import annotations

import json
import logging
import os
import socket
import stat
import tempfile
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_UNCLAIMED_SPOOL_MAX_AGE = timedelta(hours=24)


class UnsafeApplyArtifacts(ValueError):
    """The apply artifact directory does not meet ownership/safety rules."""


def claims_dir(project_dir: Path) -> Path:
    return project_dir / ".drt" / "applied_plans"


def ensure_claims_dir(project_dir: Path) -> Path:
    directory = claims_dir(project_dir)
    directory.parent.mkdir(parents=True, exist_ok=True)
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or directory.is_symlink():
        raise UnsafeApplyArtifacts(
            f"Refusing unsafe apply artifact path {directory}: not a real directory."
        )
    if info.st_uid != os.getuid():
        raise UnsafeApplyArtifacts(f"Refusing unsafe apply artifact path {directory}: wrong owner.")
    if info.st_mode & 0o022:
        raise UnsafeApplyArtifacts(
            f"Refusing unsafe apply artifact path {directory}: group/other writable. "
            f"Run `chmod 700 {directory}`."
        )
    return directory


def claim_path(project_dir: Path, plan_id: str) -> Path:
    return claims_dir(project_dir) / f"{plan_id}.json"


def read_claim(project_dir: Path, plan_id: str) -> dict[str, Any] | None:
    try:
        return json.loads(claim_path(project_dir, plan_id).read_text(encoding="utf-8"))  # type: ignore[no-any-return]
    except (OSError, ValueError):
        return None


def claim(
    project_dir: Path,
    plan_id: str,
    sync_name: str,
    run_id: str,
    forced: list[str] | None = None,
    approved_by: str | None = None,
    waves: list[float] | None = None,
    spool_path: Path | None = None,
) -> None:
    ensure_claims_dir(project_dir)
    now = datetime.now(timezone.utc).isoformat()
    fd = os.open(
        claim_path(project_dir, plan_id),
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "plan_id": plan_id,
                "sync": sync_name,
                "run_id": run_id,
                "state": "pending",
                "status": "pending",
                "claimed_at": now,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "started_at": now,
                **({"spool_path": str(spool_path)} if spool_path is not None else {}),
                **(
                    {"waves": waves, "wave_results": [], "run_ids": []} if waves is not None else {}
                ),
                **({"guards_forced": forced} if forced else {}),
                **({"approved_by": approved_by} if approved_by else {}),
            },
            handle,
        )


def finish_claim(project_dir: Path, plan_id: str, state: str, **updates: Any) -> bool:
    log = logging.getLogger(__name__)
    try:
        ensure_claims_dir(project_dir)
        current = read_claim(project_dir, plan_id) or {"plan_id": plan_id}
        current.update(state=state, status=state, **updates)
        if state in {"success", "failed", "halted"}:
            current["finished_at"] = datetime.now(timezone.utc).isoformat()
        fd, tmp_name = tempfile.mkstemp(
            dir=claims_dir(project_dir), prefix=f".{plan_id}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(current, handle)
            os.replace(tmp_name, claim_path(project_dir, plan_id))
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
    except Exception as exc:
        try:
            log.warning(
                "Could not record the outcome of plan %s (%s); %s may still say 'pending'.",
                plan_id,
                exc,
                claim_path(project_dir, plan_id),
            )
        except Exception:
            pass
        return False
    return True


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def sweep_canary_spools(
    project_dir: Path,
    *,
    notify: Callable[[str], None] | None = None,
    now: datetime | None = None,
) -> None:
    directory = ensure_claims_dir(project_dir)
    say = notify or (lambda _message: None)
    host = socket.gethostname()
    current_time = now or datetime.now(timezone.utc)
    for spool in directory.glob("*.spool.jsonl"):
        plan_id = spool.name.removesuffix(".spool.jsonl")
        current = read_claim(project_dir, plan_id)
        if current is None:
            try:
                modified = datetime.fromtimestamp(spool.stat().st_mtime, timezone.utc)
            except OSError:
                continue
            if current_time - modified > _UNCLAIMED_SPOOL_MAX_AGE:
                spool.unlink(missing_ok=True)
            else:
                say(f"Preserving unclaimed recent canary snapshot {spool}.")
            continue
        state = str(current.get("state", "unknown"))
        if state in {"halted", "success", "failed"}:
            spool.unlink(missing_ok=True)
            continue
        lease_host = current.get("hostname")
        lease_pid = current.get("pid")
        if state in {"pending", "running"} and lease_host == host and isinstance(lease_pid, int):
            if not pid_is_alive(lease_pid):
                finish_claim(project_dir, plan_id, "halted", reason="orphaned")
                spool.unlink(missing_ok=True)
                say(f"Removed orphaned canary snapshot {spool} (dead pid {lease_pid}).")
                continue
            say(f"Preserving live canary snapshot {spool} (pid {lease_pid} on {lease_host}).")
            continue
        say(
            f"Preserving canary snapshot {spool} owned by "
            f"{lease_host or 'unknown host'} pid {lease_pid or 'unknown'}."
        )
