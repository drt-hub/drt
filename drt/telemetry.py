"""Anonymous opt-in usage telemetry.

drt collects no telemetry by default. When a user opts in, a single
``sync_completed`` event is sent per ``drt run`` invocation containing only
the fields enumerated in :func:`build_sync_completed_payload`.

Privacy posture:

- Allow-list payload schema enforced by the function signature; sync names,
  model SQL, destination URLs, credentials, and project paths never enter
  the telemetry path.
- ``DO_NOT_TRACK=1`` short-circuits to disabled regardless of opt-in state.
- Errors during send are swallowed; telemetry must never affect the user's
  command exit code or latency.

Backend wire format follows PostHog's capture endpoint
(``POST /i/v0/e/`` with ``{"api_key", "event", "distinct_id", "properties",
"timestamp"}``) so the same code works against PostHog Cloud and a
self-hosted PostHog without conditional logic.
"""

from __future__ import annotations

import atexit
import functools
import json
import logging
import os
import platform
import sys
import threading
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from drt import __version__

logger = logging.getLogger("drt.telemetry")

_DEFAULT_ENDPOINT = "https://us.i.posthog.com/i/v0/e/"
_DEFAULT_API_KEY: str | None = None  # populated by maintainer pre-release

_TIMEOUT_SECONDS = 2.0


def _user_dir() -> Path:
    """User-level drt directory (`~/.drt`). Single point of redirection for tests."""
    return Path.home() / ".drt"


def _config_path() -> Path:
    return _user_dir() / "telemetry.json"


def _anon_id_path() -> Path:
    return _user_dir() / ".anonymous_id"


@functools.lru_cache(maxsize=1)
def _load_config_cached() -> dict[str, Any]:
    """Cached read of telemetry.json. Invalidated by `set_enabled` / `unset_enabled`."""
    path = _config_path()
    if not path.exists():
        return {}
    try:
        with path.open() as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _write_config(data: dict[str, Any]) -> None:
    _user_dir().mkdir(parents=True, exist_ok=True)
    with _config_path().open("w") as f:
        json.dump(data, f, indent=2)
    _load_config_cached.cache_clear()


def _env_truthy(name: str) -> bool | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_endpoint() -> str:
    return os.environ.get("DRT_TELEMETRY_ENDPOINT") or _DEFAULT_ENDPOINT


def _resolve_api_key() -> str | None:
    return os.environ.get("DRT_TELEMETRY_API_KEY") or _DEFAULT_API_KEY


def is_enabled() -> bool:
    """Decide whether to send telemetry for the current invocation.

    Precedence: DO_NOT_TRACK > DRT_TELEMETRY env > config file > default off.
    Also requires an effective API key (env override or compiled-in default).
    """
    if _env_truthy("DO_NOT_TRACK"):
        return False
    if _resolve_api_key() is None:
        return False
    env_pref = _env_truthy("DRT_TELEMETRY")
    if env_pref is not None:
        return env_pref
    return bool(_load_config_cached().get("enabled", False))


def set_enabled(value: bool) -> None:
    """Persist user preference to ~/.drt/telemetry.json."""
    cfg = dict(_load_config_cached())
    cfg["enabled"] = bool(value)
    _write_config(cfg)


def unset_enabled() -> None:
    """Remove preference so default (off) applies."""
    cfg = dict(_load_config_cached())
    cfg.pop("enabled", None)
    _write_config(cfg)


def get_anonymous_id() -> str:
    """Return a stable per-machine anonymous UUID, generating it on first call."""
    path = _anon_id_path()
    if path.exists():
        try:
            existing = path.read_text().strip()
            if existing:
                return existing
        except OSError:
            pass
    new_id = str(uuid.uuid4())
    try:
        _user_dir().mkdir(parents=True, exist_ok=True)
        path.write_text(new_id + "\n")
    except OSError:
        pass
    return new_id


def build_sync_completed_payload(
    *,
    distinct_id: str,
    sync_mode: str,
    source_type: str,
    destination_type: str,
    rows_synced: int,
    duration_seconds: float,
    status: str,
) -> dict[str, Any]:
    """Build the PostHog-shaped capture body for a sync_completed event.

    Pure: no disk reads, no network. Keyword-only arguments form the allow-list
    — adding a field requires a signature change.
    """
    py = sys.version_info
    properties = {
        "drt_version": __version__,
        "python_version": f"{py.major}.{py.minor}",
        "os": platform.system().lower(),
        "source_type": source_type,
        "destination_type": destination_type,
        "sync_mode": sync_mode,
        "rows_synced": int(rows_synced),
        "duration_seconds": float(duration_seconds),
        "status": status,
    }
    return {
        "api_key": _resolve_api_key() or "",
        "event": "sync_completed",
        "distinct_id": distinct_id,
        "properties": properties,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _send(payload: dict[str, Any]) -> None:
    """Best-effort POST. Any exception is swallowed (logged at DEBUG)."""
    try:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            _resolve_endpoint(),
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS):
            pass
    except Exception as exc:
        logger.debug("telemetry send failed: %s", exc)


def track_sync_completed(
    *,
    sync_mode: str,
    source_type: str,
    destination_type: str,
    rows_synced: int,
    duration_seconds: float,
    status: str,
) -> None:
    """Fire-and-forget tracking. No-op when disabled."""
    if not is_enabled():
        return
    payload = build_sync_completed_payload(
        distinct_id=get_anonymous_id(),
        sync_mode=sync_mode,
        source_type=source_type,
        destination_type=destination_type,
        rows_synced=rows_synced,
        duration_seconds=duration_seconds,
        status=status,
    )
    t = threading.Thread(target=_send, args=(payload,), daemon=True)
    t.start()
    atexit.register(t.join, _TIMEOUT_SECONDS)
