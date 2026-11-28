"""The LLM-facing API reference must keep Zendesk's user and organization identity sets apart.

Regression guard for a review finding: a union comment ("id/external_id/email") led readers to
believe a user ``id`` or an organization ``email`` makes a replay safe, which Zendesk's
endpoint contracts do not promise.
"""

from __future__ import annotations

from pathlib import Path

_REFERENCE = Path(__file__).resolve().parents[2] / "docs" / "llm" / "API_REFERENCE.md"


def _zendesk_section() -> str:
    text = _REFERENCE.read_text()
    section = text.split("### `type: zendesk`", 1)[1]
    return section.split("\n### ", 1)[0]


def test_no_union_identity_claim_for_zendesk() -> None:
    section = _zendesk_section()

    assert "no id/external_id/email" not in section
    assert "id/external_id/email" not in section


def test_user_and_organization_identities_are_stated_separately() -> None:
    section = _zendesk_section()

    assert "A **user** request is replay-safe only when every row" in section
    assert "non-blank string `email` or `external_id`" in section
    assert "a user `id`, including one copied by" in section
    assert "An **organization** is replay-safe" in section
    assert "positive numeric `id` or a non-blank string `external_id`" in section
