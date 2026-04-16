"""Shared fixtures for integration tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig


class FakeSource:
    """A Source that yields pre-defined rows — no BigQuery required."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


@pytest.fixture
def profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="test_project", dataset="test_dataset")


@pytest.fixture
def fake_source() -> FakeSource:
    return FakeSource(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Carol", "email": "carol@example.com"},
        ]
    )
