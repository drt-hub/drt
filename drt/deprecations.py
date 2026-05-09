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


# No active sync-config deprecations as of v0.7.x.
# Register entries here when announcing new deprecations.
# Example structure:
#     DEPRECATED_SYNC_KEYS = {
#         "old_key": DeprecatedFeature(
#             key="old_key",
#             replacement="new.key.path",
#             announced_in="v0.X.0",
#             removed_in="v0.Y.0",
#             docs_link="docs/migration/vX-to-vY.md",
#         ),
#     }
DEPRECATED_SYNC_KEYS: dict[str, DeprecatedFeature] = {}
