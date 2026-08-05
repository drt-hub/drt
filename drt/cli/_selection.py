"""Shared sync-selection resolver for run / test / validate (#771).

Selector grammar (per token):

- ``*`` / ``all``            — every sync (explicit sentinel, back-compat)
- ``tag:<pattern>``          — syncs with a tag matching the pattern
- ``destination:<pattern>``  — syncs whose destination ``type`` matches
- anything else              — the sync name (glob patterns supported)

Patterns use ``fnmatch`` semantics (``*``, ``?``, ``[seq]``), so exact names
keep working unchanged. Repeated ``--select`` values union; ``--exclude``
subtracts with the same grammar. Definition order is preserved and results
are deduplicated.

``source:`` is deliberately **not** a method: syncs share the project
profile (one source per run), so there is nothing per-sync to select on.
Revisit if per-sync sources ever land.
"""

from __future__ import annotations

from collections.abc import Sequence
from fnmatch import fnmatchcase
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from drt.config.models import SyncConfig

_METHODS = ("tag", "destination")


class SelectionError(ValueError):
    """A --select token matched nothing, or used an unknown method."""


def is_glob(token: str) -> bool:
    """True when the token contains fnmatch metacharacters."""
    return any(ch in token for ch in "*?[")


def matches(sync: SyncConfig, token: str) -> bool:
    """Does one selector token match one sync?"""
    if token in ("*", "all"):
        return True
    if token.startswith("tag:"):
        pattern = token[len("tag:") :]
        return any(fnmatchcase(tag, pattern) for tag in getattr(sync, "tags", []))
    if token.startswith("destination:"):
        pattern = token[len("destination:") :]
        return fnmatchcase(sync.destination.type, pattern)
    if ":" in token:
        method = token.split(":", 1)[0]
        raise SelectionError(
            f"Unknown selector method '{method}:'. Available methods: "
            + ", ".join(f"{m}:" for m in _METHODS)
            + " — or a bare sync name / glob."
        )
    return fnmatchcase(sync.name, token)


def _no_match_message(token: str) -> str:
    if token.startswith("tag:"):
        return f"No syncs with tag '{token[len('tag:'):]}' found."
    if token.startswith("destination:"):
        return f"No syncs with destination '{token[len('destination:'):]}' found."
    if is_glob(token):
        return f"No syncs matching '{token}' found."
    return f"No sync named '{token}' found."


def select_syncs(
    syncs: Sequence[SyncConfig],
    select: Sequence[str] | None,
    exclude: Sequence[str] | None = None,
) -> list[SyncConfig]:
    """Resolve ``--select`` / ``--exclude`` tokens against the sync list.

    Every ``select`` token must match at least one sync (raises
    ``SelectionError`` naming the dud token — a typo should never silently
    run nothing). ``exclude`` tokens may match nothing. The caller decides
    what an empty final selection means for its command.
    """
    if select:
        matched_names: set[str] = set()
        for token in select:
            hits = [s for s in syncs if matches(s, token)]
            if not hits:
                raise SelectionError(_no_match_message(token))
            matched_names.update(s.name for s in hits)
        selected = [s for s in syncs if s.name in matched_names]
    else:
        selected = list(syncs)

    for token in exclude or ():
        selected = [s for s in selected if not matches(s, token)]
    return selected


def complete_selector(incomplete: str) -> list[str]:
    """Best-effort shell completion for --select/--exclude values.

    Loads the project's syncs from the current directory; any failure
    (not in a project, YAML error) silently completes nothing — completion
    must never crash the shell.
    """
    try:
        from drt.config.parser import load_syncs

        syncs = load_syncs(Path("."))
    except Exception:  # noqa: BLE001 — completion is strictly best-effort
        return []
    values: list[str] = [s.name for s in syncs]
    values += sorted({f"tag:{t}" for s in syncs for t in getattr(s, "tags", [])})
    values += sorted({f"destination:{s.destination.type}" for s in syncs})
    return [v for v in values if v.startswith(incomplete)]
