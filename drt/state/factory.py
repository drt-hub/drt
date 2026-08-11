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


_CacheKey = tuple[
    Path,
    str,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    int,
]
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

    Remote bundles share one client as well as one instance lock per store.
    The lock handles threads in this process; generation / ETag preconditions
    handle independent processes.
    """
    backend = project.state.backend
    if backend not in {"local", "gcs", "s3"}:
        raise NotImplementedError(
            f"State backend '{backend}' is not implemented; supported backends "
            "for this stage of #756 are 'local', 'gcs', and 's3'."
        )

    resolved_dir = project_dir.resolve()
    key: _CacheKey = (
        resolved_dir,
        backend,
        project.state.bucket,
        project.state.prefix,
        project.state.region,
        project.state.endpoint_url,
        project.state.aws_profile,
        project.state.aws_access_key_id_env,
        project.state.aws_secret_access_key_env,
        project.state.aws_session_token_env,
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
                    ObjectClient,
                    ObjectStoreDlqBackend,
                    ObjectStoreHistoryStore,
                    ObjectStoreStateStore,
                )

                # Pydantic's validator guarantees this for real configs. The
                # assertion also narrows the optional type for strict mypy.
                assert project.state.bucket is not None
                client: ObjectClient
                if backend == "gcs":
                    from drt.state.gcs import GCSObjectClient

                    client = GCSObjectClient(project.state.bucket)
                else:
                    from drt.state.s3 import S3ObjectClient

                    client = S3ObjectClient(
                        project.state.bucket,
                        region=project.state.region,
                        endpoint_url=project.state.endpoint_url,
                        aws_profile=project.state.aws_profile,
                        aws_access_key_id_env=project.state.aws_access_key_id_env,
                        aws_secret_access_key_env=project.state.aws_secret_access_key_env,
                        aws_session_token_env=project.state.aws_session_token_env,
                    )
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
