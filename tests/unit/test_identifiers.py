"""Unit tests for the run-id generator (#762)."""

from __future__ import annotations

import uuid

from drt._identifiers import new_run_id


def test_returns_a_valid_uuid4_string() -> None:
    value = new_run_id()
    parsed = uuid.UUID(value)
    assert parsed.version == 4
    assert str(parsed) == value


def test_successive_calls_are_unique() -> None:
    assert len({new_run_id() for _ in range(100)}) == 100
