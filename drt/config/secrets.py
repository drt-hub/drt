"""Hardcoded secret detection for project YAML files."""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_SECRET_FIELD_SUFFIXES = ("_key", "_token", "_password", "_secret")
_NON_SECRET_SUFFIXES = ("_env", "_path")
_KNOWN_SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("OpenAI-style token", re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b")),
    ("Slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{16,}\b")),
    ("Google API key", re.compile(r"\bAIza[A-Za-z0-9_-]{20,}\b")),
    ("GitHub token", re.compile(r"\bgh[opsru]_[A-Za-z0-9_]{20,}\b")),
    ("AWS access key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("Stripe live secret key", re.compile(r"\bsk_live_[A-Za-z0-9]{16,}\b")),
)
_MIN_ENTROPY_SECRET_LENGTH = 20
_SHANNON_ENTROPY_THRESHOLD = 3.5


@dataclass(frozen=True)
class SecretFinding:
    """A likely hardcoded secret in a sync YAML file."""

    sync_name: str
    file: str
    path: str
    reason: str

    @property
    def message(self) -> str:
        return f"{self.path}: looks like a hardcoded secret; use ${{ENV_VAR}} instead"

    def to_dict(self) -> dict[str, str]:
        return {
            "sync": self.sync_name,
            "file": self.file,
            "path": self.path,
            "reason": self.reason,
            "message": self.message,
        }


def find_hardcoded_secrets(project_dir: Path = Path(".")) -> list[SecretFinding]:
    """Return likely hardcoded secret values in ``syncs/*.yml`` files.

    The scanner reads raw YAML before environment expansion so ``${ENV_VAR}``
    references are treated as safe even when the environment value is secret.
    """
    syncs_dir = project_dir / "syncs"
    if not syncs_dir.exists():
        return []

    findings: list[SecretFinding] = []
    for path in sorted(syncs_dir.glob("*.yml")):
        try:
            with path.open() as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError:
            continue
        if not isinstance(data, dict):
            continue
        sync_name = str(data.get("name") or path.stem)
        findings.extend(_find_in_value(data, sync_name, path.name, ()))
    return findings


def _find_in_value(
    value: Any,
    sync_name: str,
    filename: str,
    path_parts: tuple[str, ...],
) -> list[SecretFinding]:
    if isinstance(value, dict):
        findings: list[SecretFinding] = []
        for key, child in value.items():
            key_str = str(key)
            child_path = (*path_parts, key_str)
            if _is_secret_field(key_str) and isinstance(child, str):
                reason = _secret_reason(child)
                if reason is not None:
                    findings.append(
                        SecretFinding(
                            sync_name=sync_name,
                            file=filename,
                            path=".".join(child_path),
                            reason=reason,
                        )
                    )
            findings.extend(_find_in_value(child, sync_name, filename, child_path))
        return findings

    if isinstance(value, list):
        findings = []
        for index, child in enumerate(value):
            findings.extend(_find_in_value(child, sync_name, filename, (*path_parts, f"[{index}]")))
        return findings

    return []


def _is_secret_field(field_name: str) -> bool:
    lower = field_name.lower()
    if lower.endswith(_NON_SECRET_SUFFIXES):
        return False
    return (
        lower.startswith("api_")
        or lower in {"password", "secret", "token", "api_key", "auth_token"}
        or lower.endswith(_SECRET_FIELD_SUFFIXES)
    )


def _secret_reason(value: str) -> str | None:
    if "${" in value:
        return None
    stripped = value.strip()
    if not stripped:
        return None

    for name, pattern in _KNOWN_SECRET_PATTERNS:
        if pattern.search(stripped):
            return name

    if _looks_high_entropy(stripped):
        return f"high entropy ({_shannon_entropy(stripped):.2f})"

    return None


def _looks_high_entropy(value: str) -> bool:
    if len(value) < _MIN_ENTROPY_SECRET_LENGTH:
        return False
    if any(char.isspace() for char in value):
        return False
    return _shannon_entropy(value) >= _SHANNON_ENTROPY_THRESHOLD


def _shannon_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts = Counter(value)
    length = len(value)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())
