"""Construct the configured state stores behind one shared factory (#756)."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

from drt.config.base import ProjectConfig
from drt.state.dlq import DlqBackend, LocalDlqStore
from drt.state.history import HistoryStore, LocalHistoryManager
from drt.state.manager import LocalStateManager, StateStore


@dataclass(frozen=True)
class StateBundle:
    """The three persistence surfaces selected by ``project.state``."""

    state: StateStore
    history: HistoryStore
    dlq: DlqBackend


_CacheKey = tuple[Path, str, str | None, str | None]
_bundle_cache: dict[_CacheKey, StateBundle] = {}
_bundle_lock = threading.Lock()


def build_state_bundle(project: ProjectConfig, project_dir: Path) -> StateBundle:
    """Return the process-shared stores for one project/backend configuration.

    The bundle is cached for correctness, not merely construction cost. As the
    existing ``drt.cli.server`` precedent explains, one state store must be
    shared across every run: ``LocalStateManager``'s thread-safety is an
    instance lock, so per-request instances would race load-modify-save on
    ``state.json`` once different syncs run concurrently. The same reasoning
    applies to ``drt run --threads N`` and to future shared remote clients.

    Step 1 of #756 supports only the existing local implementations, making
    this function a pure indirection layer until remote backends land.
    """
    backend = project.state.backend
    if backend != "local":
        raise NotImplementedError(
            f"State backend '{backend}' is not implemented; remote backends "
            "land in follow-up PRs for #756."
        )

    resolved_dir = project_dir.resolve()
    key: _CacheKey = (
        resolved_dir,
        backend,
        project.state.bucket,
        project.state.prefix,
    )
    with _bundle_lock:
        bundle = _bundle_cache.get(key)
        if bundle is None:
            bundle = StateBundle(
                state=LocalStateManager(resolved_dir),
                history=LocalHistoryManager(resolved_dir),
                dlq=LocalDlqStore(resolved_dir),
            )
            _bundle_cache[key] = bundle
        return bundle
