"""YAML config parser for drt project and sync definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pydantic import ValidationError

from drt.config.models import ProjectConfig, SyncConfig


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
            result.syncs.append(SyncConfig.model_validate(data))
        except ValidationError as e:
            result.errors[path.stem] = _format_validation_errors(e)
    return result
