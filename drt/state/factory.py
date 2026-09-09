"""Construct the configured state stores behind one shared factory (#756)."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

from drt.config.base import ProjectConfig
from drt.state.audit_trail import ComplianceAuditTrail
from drt.state.dlq import DlqBackend, LocalDlqStore
from drt.state.history import HistoryStore, LocalHistoryManager
from drt.state.idempotency import IdempotencyLedger
from drt.state.manager import LocalStateManager, StateStore

_SUPPORTED_BACKENDS = {"local", "gcs", "s3", "warehouse"}


@dataclass(frozen=True)
class StateBundle:
    """The persistence surfaces selected by ``project.state``.

    ``ledger`` (#1099) and ``audit_trail`` (#1100) are ``None`` for every
    backend except ``warehouse`` with, respectively, ``state.idempotency:
    true`` / ``state.audit_trail.enabled: true`` — every other
    configuration has neither available, matching how ``history`` is
    already ``None``-able per ``history.enabled`` at call sites.
    """

    state: StateStore
    history: HistoryStore
    dlq: DlqBackend
    ledger: IdempotencyLedger | None = None
    audit_trail: ComplianceAuditTrail | None = None


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
    str | None,
    int,
    bool,
    bool,
    int | None,
    tuple[str, ...],
]
_bundle_cache: dict[_CacheKey, StateBundle] = {}
_bundle_lock = threading.Lock()


def _reject_unsupported_ledger_and_audit_trail(
    project: ProjectConfig, dialect_name: str, issue_ref: str
) -> None:
    """Fail loudly rather than silently build a bundle with ``ledger=None``/
    ``audit_trail=None`` for a dialect that hasn't implemented them yet
    (currently every warehouse dialect except Postgres, #1099/#1100)."""
    if project.state.idempotency:
        raise NotImplementedError(
            f"state.idempotency: true is not yet supported on the {dialect_name} "
            f"warehouse backend ({issue_ref}) — Postgres only today. Set "
            "state.idempotency: false or use a Postgres connection_profile."
        )
    if project.state.audit_trail.enabled:
        raise NotImplementedError(
            "state.audit_trail.enabled: true is not yet supported on the "
            f"{dialect_name} warehouse backend ({issue_ref}) — Postgres only "
            "today. Set state.audit_trail.enabled: false or use a Postgres "
            "connection_profile."
        )


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
    if backend not in _SUPPORTED_BACKENDS:
        raise NotImplementedError(
            f"State backend '{backend}' is not implemented; supported backends "
            "are 'local', 'gcs', 's3' (#756), and 'warehouse' (#920)."
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
        project.state.connection_profile,
        project.history.max_entries,
        project.state.idempotency,
        project.state.audit_trail.enabled,
        project.state.audit_trail.retain_days,
        tuple(project.state.audit_trail.fields),
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
            elif backend == "warehouse":
                from drt.config.credentials import (
                    DatabricksProfile,
                    PostgresProfile,
                    SnowflakeProfile,
                    load_profile,
                )

                # Pydantic's validator guarantees this for real configs.
                assert project.state.connection_profile is not None
                profile = load_profile(project.state.connection_profile)
                if isinstance(profile, PostgresProfile):
                    from drt.state.warehouse import (
                        PostgresComplianceAuditTrail,
                        PostgresWarehouseDlqBackend,
                        PostgresWarehouseHistoryStore,
                        PostgresWarehouseIdempotencyLedger,
                        PostgresWarehouseStateStore,
                    )

                    bundle = StateBundle(
                        state=PostgresWarehouseStateStore(profile),
                        history=PostgresWarehouseHistoryStore(profile),
                        dlq=PostgresWarehouseDlqBackend(profile),
                        ledger=(
                            PostgresWarehouseIdempotencyLedger(profile)
                            if project.state.idempotency
                            else None
                        ),
                        audit_trail=(
                            PostgresComplianceAuditTrail(profile)
                            if project.state.audit_trail.enabled
                            else None
                        ),
                    )
                elif isinstance(profile, SnowflakeProfile):
                    _reject_unsupported_ledger_and_audit_trail(project, "Snowflake", "#1106")
                    from drt.state.warehouse_snowflake import (
                        SnowflakeWarehouseDlqBackend,
                        SnowflakeWarehouseHistoryStore,
                        SnowflakeWarehouseStateStore,
                    )

                    bundle = StateBundle(
                        state=SnowflakeWarehouseStateStore(profile),
                        history=SnowflakeWarehouseHistoryStore(profile),
                        dlq=SnowflakeWarehouseDlqBackend(profile),
                    )
                elif isinstance(profile, DatabricksProfile):
                    _reject_unsupported_ledger_and_audit_trail(project, "Databricks", "#1108")
                    from drt.state.warehouse_databricks import (
                        DatabricksWarehouseDlqBackend,
                        DatabricksWarehouseHistoryStore,
                        DatabricksWarehouseStateStore,
                    )

                    bundle = StateBundle(
                        state=DatabricksWarehouseStateStore(profile),
                        history=DatabricksWarehouseHistoryStore(profile),
                        dlq=DatabricksWarehouseDlqBackend(profile),
                    )
                else:
                    raise NotImplementedError(
                        f"state.backend: warehouse only supports Postgres (#920), "
                        f"Snowflake (#1106), and Databricks (#1108) profiles today; "
                        f"'{project.state.connection_profile}' is a {profile.type} "
                        "profile. Other dialects are tracked as follow-up issues."
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
