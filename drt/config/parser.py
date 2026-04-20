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
    """Result of loading sync YAML files — valid syncs + per-file errors."""

    syncs: list[SyncConfig] = field(default_factory=list)
    errors: dict[str, list[str]] = field(default_factory=dict)


def _format_validation_errors(exc: ValidationError) -> list[str]:
    """Convert Pydantic ValidationError into human-readable messages."""
    messages: list[str] = []
    for err in exc.errors():
        loc = " → ".join(str(part) for part in err["loc"]) if err["loc"] else "(root)"
        messages.append(f"{loc}: {err['msg']}")
    return messages


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


def load_syncs(project_dir: Path = Path(".")) -> list[SyncConfig]:
    """Load and validate all sync YAML files from syncs/.

    Raises ``ValidationError`` on the first invalid file (original behaviour).
    """
    syncs_dir = project_dir / "syncs"
    if not syncs_dir.exists():
        return []
    syncs = []
    for path in sorted(syncs_dir.glob("*.yml")):
        with path.open() as f:
            data = yaml.safe_load(f)
        data = expand_env_vars(data)
        syncs.append(SyncConfig.model_validate(data))
    return syncs


def load_syncs_safe(project_dir: Path = Path(".")) -> SyncLoadResult:
    """Load sync YAML files, collecting errors instead of raising."""
    syncs_dir = project_dir / "syncs"
    if not syncs_dir.exists():
        return SyncLoadResult()
    result = SyncLoadResult()
    for path in sorted(syncs_dir.glob("*.yml")):
        with path.open() as f:
            data = yaml.safe_load(f)
        try:
            data = expand_env_vars(data)
            result.syncs.append(SyncConfig.model_validate(data))
        except (ValidationError, ValueError) as e:
            if isinstance(e, ValidationError):
                result.errors[path.stem] = _format_validation_errors(e)
            else:
                result.errors[path.stem] = [str(e)]
    return result
