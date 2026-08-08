"""Shared sync-selection resolver for run / test / validate (#771).

Selector grammar (per token):

- ``*`` / ``all``            — every sync (explicit sentinel, back-compat)
- ``tag:<pattern>``          — syncs with a tag matching the pattern
- ``destination:<pattern>``  — syncs whose destination ``type`` matches
- ``state:modified`` / ``state:new`` — syncs changed or added since a baseline
- anything else              — the sync name (glob patterns supported)

Patterns use ``fnmatch`` semantics (``*``, ``?``, ``[seq]``), so exact names
keep working unchanged. Repeated ``--select`` values union; ``--exclude``
subtracts with the same grammar. Definition order is preserved and results
are deduplicated.

State selectors require the caller to supply a pre-computed baseline diff;
using one without a baseline is an error.

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
    from drt.cli._state_selection import StateDiff
    from drt.config.models import SyncConfig

_METHODS = ("tag", "destination", "state")
_STATE_SELECTORS = ("state:modified", "state:new")


class SelectionError(ValueError):
    """A --select token matched nothing, or used an unknown method."""


def is_glob(token: str) -> bool:
    """True when the token contains fnmatch metacharacters."""
    return any(ch in token for ch in "*?[")


def matches(sync: SyncConfig, token: str, *, state_diff: StateDiff | None = None) -> bool:
    """Does one selector token match one sync?"""
    if token in ("*", "all"):
        return True
    if token.startswith("tag:"):
        pattern = token[len("tag:") :]
        return any(fnmatchcase(tag, pattern) for tag in getattr(sync, "tags", []))
    if token.startswith("destination:"):
        pattern = token[len("destination:") :]
        return fnmatchcase(sync.destination.type, pattern)
    if token.startswith("state:"):
        if token not in _STATE_SELECTORS:
            raise SelectionError(
                f"Unknown state selector '{token}'. Available state selectors: "
                + ", ".join(_STATE_SELECTORS)
                + "."
            )
        if state_diff is None:
            raise SelectionError(
                f"Selector '{token}' requires a baseline manifest to compare against."
            )
        names = state_diff.new if token == "state:new" else state_diff.modified
        return sync.name in names
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
    if token == "state:modified":
        return "No modified syncs found relative to the baseline manifest."
    if token == "state:new":
        return "No new syncs found relative to the baseline manifest."
    if is_glob(token):
        return f"No syncs matching '{token}' found."
    return f"No sync named '{token}' found."


def select_syncs(
    syncs: Sequence[SyncConfig],
    select: Sequence[str] | None,
    exclude: Sequence[str] | None = None,
    *,
    state_diff: StateDiff | None = None,
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
            hits = [s for s in syncs if matches(s, token, state_diff=state_diff)]
            if not hits:
                raise SelectionError(_no_match_message(token))
            matched_names.update(s.name for s in hits)
        selected = [s for s in syncs if s.name in matched_names]
    else:
        selected = list(syncs)

    for token in exclude or ():
        selected = [s for s in selected if not matches(s, token, state_diff=state_diff)]
    return selected


def complete_selector(incomplete: str) -> list[str]:
    """Best-effort shell completion for --select/--exclude values.

    Loads the project's syncs from the current directory; any failure (such as
    a YAML error) silently completes nothing — completion must never crash the
    shell. Literal state selectors remain available when the project has no
    sync definitions because they do not depend on project contents.
    """
    try:
        from drt.config.parser import load_syncs

        syncs = load_syncs(Path("."))
    except Exception:  # noqa: BLE001 — completion is strictly best-effort
        return []
    values: list[str] = [s.name for s in syncs]
    values += sorted({f"tag:{t}" for s in syncs for t in getattr(s, "tags", [])})
    values += sorted({f"destination:{s.destination.type}" for s in syncs})
    values += list(_STATE_SELECTORS)
    return [v for v in values if v.startswith(incomplete)]
