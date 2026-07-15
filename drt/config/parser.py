"""YAML config parser for drt project and sync definitions."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from drt.config.models import ProjectConfig, SyncConfig
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
    """Result of loading sync YAML files — valid syncs + per-file errors + deprecations."""

    syncs: list[SyncConfig] = field(default_factory=list)
    errors: dict[str, list[str]] = field(default_factory=dict)
    deprecations: dict[str, list[dict[str, str]]] = field(default_factory=dict)


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
                warnings.append({
                    "key": f"sync.{deprecated_key}",
                    "replacement": feature.replacement,
                    "removed_in": feature.removed_in,
                    "docs_link": feature.docs_link or "",
                })
    
    return warnings


def load_project(project_dir: Path = Path(".")) -> ProjectConfig:
    """Load and validate drt_project.yml."""
    config_path = project_dir / "drt_project.yml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"drt_project.yml not found in {project_dir}. Run `drt init` first."
        )
    with config_path.open() as f:
        data = yaml.safe_load(f)
    return ProjectConfig.model_validate(data)


def _expand_sync_vars(data: Any, variables: dict[str, Any]) -> Any:
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
    project_dir: Path = Path("."), vars: dict[str, Any] | None = None
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
        data = _expand_sync_vars(data, resolved)
        syncs.append(SyncConfig.model_validate(data))
    return syncs


def load_syncs_safe(
    project_dir: Path = Path("."), vars: dict[str, Any] | None = None
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
            data = _expand_sync_vars(data, resolved)
            # Check for deprecated keys before validation
            deprecations = _check_deprecated_keys(data)
            sync = SyncConfig.model_validate(data)
            # `model:` is rendered by the resolver at run time (it owns
            # cursor_value too), so trial-render it here purely to surface an
            # undefined var now rather than mid-run — the output is discarded.
            if has_var_template(sync.model):
                render_vars(sync.model, resolved)
            if deprecations:
                # Store deprecations using the actual sync name, not the file name
                result.deprecations[sync.name] = deprecations
            result.syncs.append(sync)
        except (ValidationError, ValueError) as e:
            if isinstance(e, ValidationError):
                result.errors[path.stem] = _format_validation_errors(e)
            else:
                result.errors[path.stem] = [str(e)]
    return result
