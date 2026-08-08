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


_CacheKey = tuple[Path, str, str | None, str | None, int]
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

    GCS bundles share one client as well as one instance lock per store. The
    lock handles threads in this process; generation preconditions handle
    independent processes.
    """
    backend = project.state.backend
    if backend not in {"local", "gcs"}:
        raise NotImplementedError(
            f"State backend '{backend}' is not implemented; supported backends "
            "for this stage of #756 are 'local' and 'gcs'."
        )

    resolved_dir = project_dir.resolve()
    key: _CacheKey = (
        resolved_dir,
        backend,
        project.state.bucket,
        project.state.prefix,
        project.history.max_entries,
    )
    with _bundle_lock:
        bundle = _bundle_cache.get(key)
        if bundle is None:
            if backend == "local":
                bundle = StateBundle(
                    state=LocalStateManager(resolved_dir),
                    history=LocalHistoryManager(resolved_dir),
                    dlq=LocalDlqStore(resolved_dir),
                )
            else:
                from drt.state._objectstore import (
                    ObjectStoreDlqBackend,
                    ObjectStoreHistoryStore,
                    ObjectStoreStateStore,
                )
                from drt.state.gcs import GCSObjectClient

                # Pydantic's validator guarantees this for real configs. The
                # assertion also narrows the optional type for strict mypy.
                assert project.state.bucket is not None
                client = GCSObjectClient(project.state.bucket)
                bundle = StateBundle(
                    state=ObjectStoreStateStore(client, prefix=project.state.prefix),
                    history=ObjectStoreHistoryStore(
                        client,
                        prefix=project.state.prefix,
                        max_entries=project.history.max_entries,
                    ),
                    dlq=ObjectStoreDlqBackend(client, prefix=project.state.prefix),
                )
            _bundle_cache[key] = bundle
        return bundle
