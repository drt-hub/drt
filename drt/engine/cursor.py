"""Cursor value comparison shared between the extraction engine and observers.

Split out of ``drt/engine/sync.py`` (#1074 / #1083 review) so
``StatePersistingObserver`` can apply the same comparison when deciding
whether to persist a watermark, without ``drt/engine/observer.py`` importing
from ``drt/engine/sync.py`` (which itself imports from ``observer.py`` —
that direction would be circular).
"""

from __future__ import annotations


def cursor_gt(new: str, current: str) -> bool:
    """Return True if new > current, using numeric comparison when both are numeric."""
    try:
        return float(new) > float(current)
    except (ValueError, TypeError):
        return new > current
