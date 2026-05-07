"""Deprecation registry for drt.

Each entry defines a deprecated feature, its replacement,
the version it was announced, when it will be removed,
and a link to the migration guide.
"""

from dataclasses import dataclass


@dataclass
class DeprecatedFeature:
    """Represents a deprecated feature in drt."""

    key: str
    replacement: str
    announced_in: str
    removed_in: str
    docs_link: str | None = None


DEPRECATED_SYNC_KEYS: dict[str, DeprecatedFeature] = {
    "batch_size": DeprecatedFeature(
        key="batch_size",
        replacement="sync.batch_config.size",
        announced_in="v0.5.0",
        removed_in="v0.7.0",
        docs_link="docs/migration/v0.6-to-v0.7.md",
    ),
}
