"""Current Meta guidance must not promise server-to-server retry deduplication.

Meta documents ``event_id`` + ``event_name`` deduplication between the browser Pixel and the
server API; it does not document it between two server submissions, so a retry after a lost
response is unproven (ADR 0023 amendment 2026-11-28). This guards the current surfaces
against reintroducing the claim.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SURFACES = [
    "docs/connectors/meta-conversions.md",
    "docs/llm/API_REFERENCE.md",
    "drt/destinations/meta_conversions.py",
    "drt/config/destinations_saas.py",
]
_STALE_PHRASES = [
    "stable retry dedup id",
    "required to deduplicate retried conversions",
    "so Meta can deduplicate a retry",
    "retries need a stable deduplication id",
    "Required so retries can be deduplicated",
]


def test_no_current_surface_promises_server_retry_deduplication() -> None:
    offenders = [
        f"{path}: {phrase!r}"
        for path in _SURFACES
        for phrase in _STALE_PHRASES
        if phrase in (_ROOT / path).read_text()
    ]

    assert not offenders, "stale Meta retry-deduplication claims: " + "; ".join(offenders)


def test_the_adr_records_that_meta_is_not_classified() -> None:
    adr = (_ROOT / "docs/adr/0023-non-idempotent-create-retries.md").read_text()

    assert "Amendment 2026-11-28 (Meta Conversions)" in adr
    assert "not between two server submissions" in adr

    inventory_line = next(
        line for line in adr.splitlines() if line.startswith("| Meta Conversions |")
    )
    assert "Superseded by the 2026-11-28 amendment" in inventory_line
    assert "Unclassified" in inventory_line

    rollout = adr[
        adr.index("### 4. Roll out by duplicate cost and current exposure") : adr.index(
            "### 5. Test the commit-then-drop failure"
        )
    ]
    assert "former Meta keyed rollout reference" in rollout
    assert "superseded by the\n   2026-11-28 amendment" in rollout
    assert "Meta remains unclassified with unchanged retries" in rollout

    plan_line = next(line for line in adr.splitlines() if line.startswith("| 5 | Append"))
    assert "keyed rollout superseded by the 2026-11-28 amendment" in plan_line
    assert "leave unclassified" in plan_line


def test_historical_changelog_describes_meta_dedup_scope_inline() -> None:
    changelog = (_ROOT / "CHANGELOG.md").read_text()
    entry = next(
        line
        for line in changelog.splitlines()
        if line.startswith("- **Meta (Facebook) Conversions API destination**")
    )

    assert "event_id_field so ambiguous retries can be deduplicated" not in entry
    assert "support documented Pixel-to-server deduplication" in entry
    assert "does not make an ambiguous server-to-server retry proven safe" in entry
