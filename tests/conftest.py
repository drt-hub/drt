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


def public_methods(cls: type) -> set[str]:
    """Public callables on ``cls``, **including inherited ones**.

    Used by the Protocol-coverage tests in ``test_state.py`` / ``test_history.py``
    / ``test_dlq_store.py`` to assert a Protocol declares exactly what its local
    implementation exposes.

    Walks the MRO rather than reading ``vars(cls)`` directly: the three state
    stores have no base class today, but a shared base for the remote backends
    (#756) is a plausible next step, and an inherited public method must not
    escape the check — a Protocol that silently stops covering its
    implementation is the exact failure this guard exists to catch.
    """
    names: set[str] = set()
    for klass in cls.__mro__:
        if klass is object:
            continue
        names |= {name for name in vars(klass) if not name.startswith("_")}
    return {name for name in names if callable(getattr(cls, name))}
