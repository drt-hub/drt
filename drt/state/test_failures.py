"""Failing-row samples for `drt test --store-failures` (#779).

DLQ-style (one JSONL file per key, under ``.drt/``), but a debugging
**snapshot of the current run** rather than an accumulating queue: each run
overwrites ``.drt/test_failures/<sync_name>/<test_id>.jsonl`` with the rows
that failed *this* time, and removes the file for a test that just passed —
so a stale sample can never linger once the assertion holds again. This
differs from :mod:`drt.state.dlq`, which intentionally accumulates until
``drt retry`` drains it; there is no equivalent "replay" concept here.

Privacy: rows must already be masked (:func:`drt.engine.masking.apply_mask`,
reusing ``sync.mask`` — #427) by the caller **before** they reach this module.
This module never sees an unmasked row, so it cannot leak one.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

_lock = threading.Lock()


def _path(project_dir: Path, sync_name: str, test_id: str) -> Path:
    return project_dir / ".drt" / "test_failures" / sync_name / f"{test_id}.jsonl"


def write_test_failures(
    project_dir: Path,
    sync_name: str,
    test_id: str,
    rows: list[dict[str, Any]],
) -> Path:
    """Overwrite the failure sample for one test with *rows*.

    *rows* must already be masked and already capped by the caller — this
    function performs no further transformation, only the write. Returns the
    path written (parents created as needed).
    """
    path = _path(project_dir, sync_name, test_id)
    with _lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = "\n".join(json.dumps(row, default=str) for row in rows)
        path.write_text(lines + ("\n" if rows else ""))
    return path


def clear_test_failures(project_dir: Path, sync_name: str, test_id: str) -> None:
    """Remove a test's failure sample, if any (the test passed on this run —
    a stale failing-rows file would be actively misleading)."""
    with _lock:
        _path(project_dir, sync_name, test_id).unlink(missing_ok=True)
