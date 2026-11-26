"""Every registered destination must be classified in docs/guides/retry-safety.md.

A new connector that ships without saying how it behaves on a lost response silently
inherits whatever the retry helper does. Forcing a row here makes that a review item.
"""

from __future__ import annotations

import re
from pathlib import Path

from drt.connectors.registry import registered_destination_types

_DOC = Path(__file__).resolve().parents[2] / "docs" / "guides" / "retry-safety.md"


def _indexed_types() -> list[str]:
    text = _DOC.read_text()
    section = text.split("## Every registered destination", 1)[1].split("\n## ", 1)[0]
    return re.findall(r"^\| `([a-z0-9_]+)` \|", section, flags=re.MULTILINE)


def test_every_registered_destination_has_a_retry_safety_row() -> None:
    indexed = set(_indexed_types())
    missing = sorted(set(registered_destination_types()) - indexed)

    assert not missing, (
        f"Add these destination types to the index in {_DOC.name}: {', '.join(missing)}"
    )


def test_the_index_lists_no_unknown_or_duplicate_types() -> None:
    indexed = _indexed_types()

    assert len(indexed) == len(set(indexed)), "duplicate rows in the retry-safety index"
    assert set(indexed) <= set(registered_destination_types()), (
        "the retry-safety index names a type that is not registered"
    )


def test_every_row_has_a_status() -> None:
    text = _DOC.read_text()
    section = text.split("## Every registered destination", 1)[1].split("\n## ", 1)[0]
    rows = [line for line in section.splitlines() if re.match(r"^\| `[a-z0-9_]+` \|", line)]

    assert rows and all(line.count("|") == 5 for line in rows), "each row needs 4 columns"
