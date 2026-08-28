"""Entry-point based plugin discovery (#297).

Third-party packages can extend five of drt's registries — ``PermissionChecker``
(:mod:`drt.security`), ``AuditLogger`` (:mod:`drt.observability.audit`),
extra :class:`~drt.engine.observer.SyncObserver`\\ s, and
:class:`~drt.config.secret_providers.base.SecretProvider` schemes, and
rate-limiter backends (:mod:`drt.destinations.rate_limiter`) — without an
explicit import anywhere in drt-core or in the operator's own code. ADR
0008 named this as an existing gap shared by the original four; this module
closes it via standard ``importlib.metadata`` entry points, the same mechanism
pytest, sqlalchemy, and Flask extensions use.

``drt.sources`` / ``drt.destinations`` are covered too: a connector registered
through one of these is nameable in a sync YAML exactly like a built-in. That
was not true when this module landed — ``SyncConfig.destination`` and
``load_profile()` both validated ``type`` against a closed, hand-enumerated set
*before* the connector registry in :mod:`drt.connectors.registry` was consulted,
so a connector could register itself and still be permanently unreachable. #997
closed that; `ADR 0009 <../docs/adr/0009-plugin-config-union-blocker.md>`_
records both the blocker and the fix.

``drt.rate_limiter_backends`` is a single-active-backend registry, like
``drt.permission_checkers`` and ``drt.audit_loggers``. It is not keyed like
the source, destination, and secret-provider registries; registering a second
backend replaces the first for that process.

Contract for a third-party package: expose a zero-argument callable under
one of the groups below and have it perform its own registration as a side
effect (call ``register_permission_checker(...)``, ``register(scheme,
provider)``, etc.). Example ``pyproject.toml``::

    [project.entry-points."drt.audit_loggers"]
    my_audit_logger = "my_package:register"

    # my_package/__init__.py
    def register() -> None:
        from drt.observability import register_audit_logger
        from .audit import MyAuditLogger
        register_audit_logger(MyAuditLogger())

The single-active rate-limiter backend follows the same side-effect contract::

    [project.entry-points."drt.rate_limiter_backends"]
    shared = "my_rate_limits:register"

    # my_rate_limits/__init__.py
    def register() -> None:
        from drt.destinations.rate_limiter import register_rate_limiter_backend
        from .backend import build_limiter
        register_rate_limiter_backend(build_limiter)
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Sequence
from dataclasses import dataclass, replace
from importlib.metadata import EntryPoint, entry_points

PLUGIN_GROUPS: tuple[str, ...] = (
    "drt.sources",
    "drt.destinations",
    "drt.secret_providers",
    "drt.permission_checkers",
    "drt.audit_loggers",
    "drt.rate_limiter_backends",
    "drt.observers",
)

# The two connector groups. Kept as a named constant for callers that want to
# tell connector plugins from the rest; it no longer marks a limitation —
# #997 made a registered connector nameable in a sync YAML like any built-in.
CONNECTOR_GROUPS = frozenset({"drt.sources", "drt.destinations"})

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveredPlugin:
    """One installed entry point under a ``drt.*`` plugin group."""

    group: str
    name: str
    value: str
    dist_name: str | None
    dist_version: str | None
    author: str | None
    loaded: bool = False
    error: str | None = None


_lock = threading.Lock()
_loaded = False
_last_result: list[DiscoveredPlugin] = []
# Reentrancy guard (#921): a registration callback that itself asks "what's
# currently registered" (e.g. a rate-limiter backend wrapping the existing
# factory) re-enters load_plugins() from inside the loop below, on the same
# thread, before ``_loaded`` is set. ``threading.Lock`` is not reentrant, so
# that call would deadlock the thread against itself trying to acquire
# ``_lock`` a second time. Per-thread (not a plain module-level flag): a
# *different* thread genuinely loading concurrently must still block on
# ``_lock`` normally rather than being mistaken for a reentrant call and
# handed a still-empty result.
_thread_state = threading.local()


def _describe(group: str, ep: EntryPoint) -> DiscoveredPlugin:
    dist = ep.dist
    author = None
    if dist is not None:
        # PEP 621 authors[] is flattened by build backends into either
        # "Author" (name only) or "Author-email" ("Name <email>") —
        # neither is guaranteed, so fall back through both. typeshed's
        # `PackageMetadata` Protocol omits `.get` even though every concrete
        # implementation (an `email.message.Message` subclass) has it —
        # `__getitem__` returning `None` on a missing header is deprecated
        # (see CPython bpo-45716), so `.get` is the correct call, not a
        # workaround.
        author = dist.metadata.get("Author") or dist.metadata.get(  # type: ignore[attr-defined]
            "Author-email"
        )
    return DiscoveredPlugin(
        group=group,
        name=ep.name,
        value=ep.value,
        dist_name=dist.name if dist is not None else None,
        dist_version=dist.version if dist is not None else None,
        author=author,
    )


def discover_plugins(groups: Sequence[str] = PLUGIN_GROUPS) -> list[DiscoveredPlugin]:
    """Enumerate installed entry points under ``groups`` without invoking them."""
    return [_describe(group, ep) for group in groups for ep in entry_points(group=group)]


def load_plugins(
    groups: Sequence[str] = PLUGIN_GROUPS, *, force: bool = False
) -> list[DiscoveredPlugin]:
    """Discover and invoke each entry point's registration callable.

    Idempotent per process (cached after the first call) unless
    ``force=True`` — safe to call unconditionally from the CLI startup
    callback on every invocation. One broken plugin's exception is caught
    and recorded on its own entry rather than propagated, so a bad
    third-party package can't take down unrelated commands.

    Safely reentrant on the same thread (#921): if a registration callback
    itself calls something that funnels back through here (e.g. a
    rate-limiter backend's ``register()`` reading
    :func:`drt.destinations.rate_limiter.get_rate_limiter_backend` to wrap
    the existing factory), that nested call returns immediately with
    whatever has been discovered so far instead of trying to acquire
    ``_lock`` a second time (a plain, non-reentrant lock — this thread
    already holds it) or re-invoking every entry point's callable again,
    including the one currently running.
    """
    global _loaded, _last_result
    if getattr(_thread_state, "loading", False):
        return _last_result
    with _lock:
        if _loaded and not force:
            return _last_result
        _thread_state.loading = True
        try:
            results: list[DiscoveredPlugin] = []
            for group in groups:
                for ep in entry_points(group=group):
                    entry = _describe(group, ep)
                    try:
                        target = ep.load()
                        if not callable(target):
                            raise TypeError(
                                f"entry point target {entry.value!r} is not callable "
                                f"(got {type(target).__name__}); the contract requires a "
                                f"zero-argument callable that performs its own registration"
                            )
                        target()
                        entry = replace(entry, loaded=True)
                    except Exception as exc:  # noqa: BLE001 — isolate one broken plugin from the rest
                        _log.warning(
                            "Failed to load drt plugin %r (group %r, %s): %s",
                            entry.name,
                            entry.group,
                            entry.value,
                            exc,
                        )
                        entry = replace(entry, error=str(exc))
                    results.append(entry)
            _loaded = True
            _last_result = results
        finally:
            _thread_state.loading = False
        return results


def _reset_plugin_state() -> None:
    """Test hook — clear the load cache so ``load_plugins()`` re-runs."""
    global _loaded, _last_result
    with _lock:
        _loaded = False
        _last_result = []
