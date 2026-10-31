"""YAML config parser for drt project and sync definitions."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from drt.config.models import ColumnConfig, ProjectConfig, SyncConfig
from drt.config.vars import expand_vars, has_var_template, render_vars, resolve_vars

_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env_vars_in_str(value: str) -> str:
    """Expand ``${VAR}`` placeholders in a single string.

    Raises ``ValueError`` if a referenced variable is not set.
    """

    def _replace(match: re.Match[str]) -> str:
        var = match.group(1)
        val = os.environ.get(var)
        if val is None:
            raise ValueError(f"Environment variable ${{{var}}} is not set")
        return val

    return _ENV_VAR_PATTERN.sub(_replace, value)


def expand_env_vars(data: Any) -> Any:
    """Recursively expand ``${VAR}`` in all string values of a parsed YAML tree.

    Dicts, lists, and nested structures are walked. Non-string leaves
    are returned unchanged.  Raises ``ValueError`` for unset variables.
    """
    if isinstance(data, str):
        return _expand_env_vars_in_str(data)
    if isinstance(data, dict):
        return {k: expand_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [expand_env_vars(item) for item in data]
    return data


@dataclass
class SyncLoadResult:
    """Result of loading sync YAML files — valid syncs, errors, and warnings."""

    syncs: list[SyncConfig] = field(default_factory=list)
    errors: dict[str, list[str]] = field(default_factory=dict)
    deprecations: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    warnings: dict[str, list[str]] = field(default_factory=dict)


def _manifest_path(project_dir: Path, manifest_path: Path | None) -> Path:
    if manifest_path is None:
        return project_dir / "target" / "manifest.json"
    return manifest_path if manifest_path.is_absolute() else project_dir / manifest_path


def _dbt_disagreement(
    model_name: str, authored: list[ColumnConfig], derived: list[ColumnConfig]
) -> str | None:
    """Describe a hand-written/dbt disagreement; authored columns still win."""
    authored_by_name = {column.name: column for column in authored}
    derived_by_name = {column.name: column for column in derived}
    details: list[str] = []
    only_authored = sorted(authored_by_name.keys() - derived_by_name.keys())
    only_dbt = sorted(derived_by_name.keys() - authored_by_name.keys())
    if only_authored:
        details.append("only hand-written: " + ", ".join(only_authored))
    if only_dbt:
        details.append("only in dbt: " + ", ".join(only_dbt))
    for name in sorted(authored_by_name.keys() & derived_by_name.keys()):
        authored_column = authored_by_name[name]
        dbt_column = derived_by_name[name]
        if authored_column.type != dbt_column.type:
            details.append(
                f"{name} type is {authored_column.type} (hand-written) vs {dbt_column.type} (dbt)"
            )
        if authored_column.description != dbt_column.description:
            details.append(f"{name} description differs")
    if not details:
        return None
    return (
        f"hand-written columns override dbt model {model_name!r}, but disagree with its "
        f"manifest declaration ({'; '.join(details)})"
    )


def _resolve_dbt_contract(
    sync: SyncConfig, project_dir: Path, manifest_path: Path | None
) -> tuple[SyncConfig, str | None]:
    """Resolve ``contract.source: dbt`` once into ordinary ``columns``."""
    if sync.contract is None or sync.contract.source != "dbt":
        return sync, None

    from drt.engine.resolver import parse_ref
    from drt.integrations.dbt import resolve_contract_from_manifest

    model_name = parse_ref(sync.model)
    if model_name is None:
        raise ValueError(
            f"contract.source: dbt requires model: ref('model_name'); got {sync.model!r}."
        )
    if (project_dir / "syncs" / "models" / f"{model_name}.sql").is_file():
        raise ValueError(
            f"contract.source: dbt cannot be used with ref({model_name!r}): the local SQL "
            f"override syncs/models/{model_name}.sql runs instead of the dbt model, so the "
            "dbt contract would describe a different query. Write columns: by hand."
        )
    path = _manifest_path(project_dir, manifest_path)
    try:
        dbt_model = resolve_contract_from_manifest(model_name, path)
    except FileNotFoundError as exc:
        raise ValueError(str(exc)) from exc

    derived = [
        ColumnConfig(name=column.name, type=column.type, description=column.description)
        for column in dbt_model.columns
    ]
    names = [column.name for column in derived]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(
            f"dbt model {model_name!r} declares duplicate column name(s): "
            + ", ".join(repr(name) for name in duplicates)
        )

    authored = sync.columns
    warning = _dbt_disagreement(model_name, authored, derived) if authored is not None else None
    resolved = sync.model_copy(update={"columns": authored or derived})
    resolved._dbt_relation_name = dbt_model.relation_name
    resolved._dbt_contract_resolved = True
    return resolved, warning


def _format_validation_errors(exc: ValidationError) -> list[str]:
    """Convert Pydantic ValidationError into human-readable messages."""
    messages: list[str] = []
    for err in exc.errors():
        loc = " → ".join(str(part) for part in err["loc"]) if err["loc"] else "(root)"
        messages.append(f"{loc}: {err['msg']}")
    return messages


def _check_deprecated_keys(data: dict[str, Any]) -> list[dict[str, str]]:
    """Check for deprecated sync keys in the raw YAML data.

    Returns a list of deprecation warnings for the given sync.
    """
    from drt.deprecations import DEPRECATED_SYNC_KEYS

    warnings: list[dict[str, str]] = []

    # Check top-level sync options
    sync_config = data.get("sync", {})
    if isinstance(sync_config, dict):
        for deprecated_key, feature in DEPRECATED_SYNC_KEYS.items():
            if deprecated_key in sync_config:
                warnings.append(
                    {
                        "key": f"sync.{deprecated_key}",
                        "replacement": feature.replacement,
                        "removed_in": feature.removed_in,
                        "docs_link": feature.docs_link or "",
                    }
                )

    return warnings


def _emit_config_changed(*, source: str, project_dir: Path, **details: Any) -> None:
    """Emit a ``config_changed`` audit event (#299, ADR 0008) — no-op under
    the OSS default (``NoOpAuditLogger``).

    Wrapped in a broad try/except: a logging failure — an unreachable
    audit sink, say — must never turn successful config loading into a
    CLI failure. This catch is the enforcement of ``AuditLogger.log_event``'s
    documented best-effort contract (caught in Codex review on this PR: the
    unguarded call previously let an audit-sink error break the operation
    being audited).
    """
    from datetime import datetime, timezone

    from drt.observability.audit import AuditEvent, get_audit_logger

    try:
        get_audit_logger().log_event(
            AuditEvent(
                event_type="config_changed",
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"source": source, "project_dir": str(project_dir), **details},
            )
        )
    except Exception as exc:  # noqa: BLE001 — fire-and-forget contract
        logging.getLogger(__name__).warning("audit log_event failed: %s", exc)


def load_project(project_dir: Path = Path(".")) -> ProjectConfig:
    """Load and validate drt_project.yml."""
    config_path = project_dir / "drt_project.yml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"drt_project.yml not found in {project_dir}. Run `drt init` first."
        )
    with config_path.open() as f:
        data = yaml.safe_load(f)
    project = ProjectConfig.model_validate(data)

    # Enterprise audit extension point (#299, ADR 0008) — ADR 0008 names
    # both load_syncs and load_project as config_changed's trigger points;
    # this is the load_project half, covering direct callers that never go
    # through load_syncs (e.g. `drt status`'s own load_project() call,
    # caught missing in Codex review on this PR). Also fires from
    # project_vars()'s best-effort call below — a config read is a config
    # read, regardless of how many CLI paths lead to it.
    _emit_config_changed(source="load_project", project_dir=project_dir)
    return project


def expand_sync_vars(data: Any, variables: dict[str, Any]) -> Any:
    """Render ``var()`` in a sync's YAML string fields, leaving ``model:`` alone.

    Model SQL shares its template surface with ``{{ cursor_value }}`` /
    ``{{ watermark }}``, which only :func:`drt.engine.resolver.resolve_model_ref`
    can supply. Rendering the model here would resolve ``var()`` but blank the
    cursor template (undefined at load time), silently breaking an incremental
    predicate — so the resolver owns the whole SQL template surface, and this
    owns the rest of the YAML.
    """
    if not isinstance(data, dict):
        return expand_vars(data, variables)
    return {
        key: value if key == "model" else expand_vars(value, variables)
        for key, value in data.items()
    }


def project_vars(
    project_dir: Path = Path("."), cli_vars: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Resolve the project's vars (#783): ``vars:`` block < ``DRT_VAR_*`` < *cli_vars*.

    Best-effort on the project file: a directory with syncs but no (or an
    invalid) ``drt_project.yml`` still resolves vars from the environment and
    ``cli_vars``, so loading syncs never starts requiring a project file.
    """
    declared: dict[str, Any] = {}
    try:
        declared = load_project(project_dir).vars
    except (FileNotFoundError, ValidationError, ValueError, yaml.YAMLError):
        pass  # no/invalid project file — env + CLI vars still apply
    return resolve_vars(declared, cli_vars)


def load_syncs(
    project_dir: Path = Path("."),
    vars: dict[str, Any] | None = None,
    manifest_path: Path | None = None,
) -> list[SyncConfig]:
    """Load and validate all sync YAML files from syncs/.

    Raises ``ValidationError`` on the first invalid file (original behaviour).
    ``vars`` overrides the resolved project vars (#783) — pass the ``--vars``
    result; omit it to resolve from ``drt_project.yml`` + ``DRT_VAR_*``.
    """
    syncs_dir = project_dir / "syncs"
    if not syncs_dir.exists():
        return []
    resolved = vars if vars is not None else project_vars(project_dir)
    syncs = []
    for path in sorted(syncs_dir.glob("*.yml")):
        with path.open() as f:
            data = yaml.safe_load(f)
        data = expand_env_vars(data)
        data = expand_sync_vars(data, resolved)
        sync = SyncConfig.model_validate(data)
        sync, _warning = _resolve_dbt_contract(sync, project_dir, manifest_path)
        syncs.append(sync)

    # Enterprise audit extension point (#299, ADR 0008) — fires once per
    # successful load, not per file. project_vars(project_dir)'s own
    # best-effort load_project() call above may have already fired a
    # separate "load_project" event if drt_project.yml exists; this one is
    # the "load_syncs" half, distinguished by `source` in details.
    _emit_config_changed(source="load_syncs", project_dir=project_dir, sync_count=len(syncs))
    return syncs


def load_syncs_safe(
    project_dir: Path = Path("."),
    vars: dict[str, Any] | None = None,
    manifest_path: Path | None = None,
) -> SyncLoadResult:
    """Load sync YAML files, collecting errors instead of raising.

    An undefined ``var()`` without a default lands in ``errors`` for that file
    (``VarError`` subclasses ``ValueError``), so ``drt validate`` reports it.
    """
    syncs_dir = project_dir / "syncs"
    if not syncs_dir.exists():
        return SyncLoadResult()
    resolved = vars if vars is not None else project_vars(project_dir)
    result = SyncLoadResult()
    for path in sorted(syncs_dir.glob("*.yml")):
        with path.open() as f:
            data = yaml.safe_load(f)
        try:
            data = expand_env_vars(data)
            data = expand_sync_vars(data, resolved)
            # Check for deprecated keys before validation
            deprecations = _check_deprecated_keys(data)
            sync = SyncConfig.model_validate(data)
            sync, dbt_warning = _resolve_dbt_contract(sync, project_dir, manifest_path)
            # `model:` is rendered by the resolver at run time (it owns
            # cursor_value too), so trial-render it here purely to surface an
            # undefined var now rather than mid-run — the output is discarded.
            if has_var_template(sync.model):
                render_vars(sync.model, resolved)
            if deprecations:
                # Store deprecations using the actual sync name, not the file name
                result.deprecations[sync.name] = deprecations
            if dbt_warning is not None:
                result.warnings.setdefault(sync.name, []).append(dbt_warning)
            result.syncs.append(sync)
        except (ValidationError, ValueError) as e:
            if isinstance(e, ValidationError):
                result.errors[path.stem] = _format_validation_errors(e)
            else:
                result.errors[path.stem] = [str(e)]
    return result
