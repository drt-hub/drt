"""Shared pytest fixtures for drt tests."""

import pytest

from drt.sources.fake import FakeSource


@pytest.fixture
def sample_row() -> dict:
    return {"name": "Alice", "email": "alice@example.com", "id": 42}


@pytest.fixture
def fake_source() -> FakeSource:
    """An empty ``FakeSource`` — override ``rows`` per-test as needed.

    Yields no rows by default; tests that need a configured source
    typically construct ``FakeSource(rows=[...])`` inline rather than
    use this fixture. Kept here so engine integration tests can write
    ``def test_x(fake_source): ...`` for the no-data case without an
    import dance (#364).
    """
    return FakeSource()
