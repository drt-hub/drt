"""Shared pytest fixtures for drt tests."""

import pytest

from drt.destinations.rate_limiter import _reset_limiter_registry
from drt.sources.fake import FakeSource


@pytest.fixture(autouse=True)
def _isolate_limiter_registry():
    """Give every test a cold rate-limiter registry (#769).

    The registry is deliberately process-global — one bucket per destination
    endpoint, outliving individual ``load()`` calls so it can pace across
    them. That makes it shared state between tests: without this, the first
    test to sync to an endpoint registers a limiter and every later test
    reusing that config silently receives the cached instance instead of
    constructing one. Tests that patch ``drt.destinations.<name>.RateLimiter``
    and count ``acquire()`` calls would then assert against a mock that was
    never wired in, passing alone and failing in a suite (or vice versa).

    Autouse rather than opt-in: any test that runs a destination ``load()``
    touches the registry, which is far more of the suite than the tests
    naming a limiter directly.
    """
    _reset_limiter_registry()
    yield
    _reset_limiter_registry()


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
