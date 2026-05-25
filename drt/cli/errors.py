"""ErrorFormatter — context-rich rendering for ``drt run`` failures.

Wraps a raw exception with **sync name**, **stage** (source / destination /
engine / state), **error type + message**, and a **suggested next step**
so users don't have to reverse-engineer "what was happening when this
broke?" from a single ``Error: connection refused`` line.

Stage detection is a traceback walk today — it inspects each frame's
``co_filename`` to find which ``drt/<area>/`` module raised. This is a
heuristic that will be replaced by an engine-emitted ``Stage`` tag
once #527 (OTel span context) and #548 (engine I/O boundary protocol)
land; both expose a cleaner observation surface that this module can
plug into without keeping the traceback walk forever.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class Stage(str, Enum):
    SOURCE = "source"
    DESTINATION = "destination"
    ENGINE = "engine"
    STATE = "state"
    UNKNOWN = "unknown"


# Path fragment → stage. Ordered: state checked before engine so files like
# ``drt/state/...`` are not swallowed by a future ``drt/engine/state/...``.
_PATH_TO_STAGE: tuple[tuple[str, Stage], ...] = (
    (f"{os.sep}drt{os.sep}sources{os.sep}", Stage.SOURCE),
    (f"{os.sep}drt{os.sep}destinations{os.sep}", Stage.DESTINATION),
    (f"{os.sep}drt{os.sep}state{os.sep}", Stage.STATE),
    (f"{os.sep}drt{os.sep}engine{os.sep}", Stage.ENGINE),
)


@dataclass
class FormattedError:
    sync_name: str
    stage: Stage
    error_type: str
    message: str
    suggestion: str | None

    def to_dict(self) -> dict[str, str | None]:
        """Machine-readable form for ``--format json`` consumers."""
        return {
            "sync_name": self.sync_name,
            "stage": self.stage.value,
            "error_type": self.error_type,
            "message": self.message,
            "suggestion": self.suggestion,
        }


def classify_filename(filename: str) -> Stage:
    """Map a source filename to its pipeline stage. Pure function for tests."""
    for fragment, stage in _PATH_TO_STAGE:
        if fragment in filename:
            return stage
    return Stage.UNKNOWN


def infer_stage(exc: BaseException) -> Stage:
    """Return the pipeline stage that produced ``exc``.

    Resolution order:

    1. **Engine-emitted ``_drt_stage`` attribute** (#544 retrofit). The
       engine wraps each phase call in a ``_stage_ctx`` context manager
       (``drt.engine.sync._stage_ctx``) that attaches a string tag
       (``"source"`` / ``"destination"`` / ``"state"`` / ``"engine"``)
       to any exception bubbling through. First writer wins so a source-
       raised error tagged at the source site survives intermediate
       destination/engine frames. This is the authoritative signal —
       prefer it over the heuristic when present.

    2. **Traceback walk fallback**. For exceptions raised outside any
       engine ``_stage_ctx`` block (library callers using ``run_sync``
       directly, future code paths not yet wrapped) we walk the
       traceback for the deepest ``drt/<area>/`` frame. Same heuristic
       as before #544 — preserves back-compat for the 26 ErrorFormatter
       tests that depend on it.
    """
    # 1. Engine-emitted tag (preferred)
    tag = getattr(exc, "_drt_stage", None)
    if tag is not None:
        try:
            # Stage subclasses str so Stage("source") round-trips fine.
            return Stage(tag) if not isinstance(tag, Stage) else tag
        except ValueError:
            # Unknown tag string (e.g. drifted between engine and cli) —
            # fall through to the traceback walk rather than crash.
            pass

    # 2. Traceback walk fallback
    stage = Stage.UNKNOWN
    tb = exc.__traceback__
    while tb is not None:
        candidate = classify_filename(tb.tb_frame.f_code.co_filename)
        if candidate is not Stage.UNKNOWN:
            stage = candidate
        tb = tb.tb_next
    return stage


def suggest(stage: Stage, exc: BaseException) -> str | None:
    """Tiny rule table for actionable next-step hints.

    Conservative on purpose — a misleading suggestion is worse than none.
    Returns ``None`` when no rule fires; the formatter just omits the
    "Suggestion:" line in that case.
    """
    msg = str(exc).lower()

    if stage is Stage.SOURCE:
        if any(k in msg for k in ("connection", "refused", "could not connect", "timeout")):
            return "Verify source connectivity with: drt validate --check-connection"
        if any(k in msg for k in ("auth", "credential", "401", "403", "permission denied")):
            return "Check the source profile's auth env vars are set"
        return "Check the source profile and the query/table reference"

    if stage is Stage.DESTINATION:
        if any(k in msg for k in ("401", "403", "unauthorized", "forbidden")):
            return "Check the destination's auth env vars (token / api key)"
        if any(k in msg for k in ("rate", "429", "too many requests")):
            return "Reduce sync.rate_limit.requests_per_second or add retry config"
        if any(k in msg for k in ("timeout", "connection")):
            return "Check destination network reachability"
        return "Check the destination config (url, auth, headers)"

    if stage is Stage.STATE:
        return "Inspect .drt/state.json; if corrupted, delete it to reset (you lose watermarks)"

    # ENGINE / UNKNOWN: usually a programming bug — let the traceback speak.
    return None


def format_error(sync_name: str, exc: BaseException) -> FormattedError:
    """Build a FormattedError from a sync name and the caught exception."""
    stage = infer_stage(exc)
    return FormattedError(
        sync_name=sync_name,
        stage=stage,
        error_type=type(exc).__name__,
        message=str(exc),
        suggestion=suggest(stage, exc),
    )


def render_to_console(fe: FormattedError) -> None:
    """Print a context-rich panel for ``fe`` via the project console.

    Rendered shape::

        ╭─ Sync failed: my_sync ──────────────────────────╮
        │ Stage: source                                    │
        │ ConnectionError: connection refused              │
        │                                                  │
        │ Suggestion: Verify source connectivity with:     │
        │   drt validate --check-connection                │
        ╰──────────────────────────────────────────────────╯

    Suggestion block is omitted when there is no hint, so engine /
    unknown failures don't carry a misleading "try X" line.
    """
    # Local import so this module stays importable without rich (rare in
    # this repo, but it keeps coupling tight) and so JSON-mode callers
    # who never invoke this function don't pull rich either.
    from rich.panel import Panel

    from drt.cli.output import console

    lines: list[str] = [
        f"[bold]Stage:[/bold] {fe.stage.value}",
        f"[bold red]{fe.error_type}:[/bold red] {fe.message}",
    ]
    if fe.suggestion:
        lines.extend(["", f"[bold yellow]Suggestion:[/bold yellow] {fe.suggestion}"])

    console.print(
        Panel(
            "\n".join(lines),
            title=f"[bold red]Sync failed:[/bold red] {fe.sync_name}",
            border_style="red",
            expand=False,
        )
    )
