"""Entry-point based plugin discovery (#297).

Third-party packages can extend four of drt's registries — ``PermissionChecker``
(:mod:`drt.security`), ``AuditLogger`` (:mod:`drt.observability.audit`),
extra :class:`~drt.engine.observer.SyncObserver`\\ s, and
:class:`~drt.config.secret_providers.base.SecretProvider` schemes — without
an explicit import anywhere in drt-core or in the operator's own code. ADR
0008 named this as an existing gap shared by all four; this module closes it
via standard ``importlib.metadata`` entry points, the same mechanism
pytest, sqlalchemy, and Flask extensions use.

**Connector entry points are a partial exception.** ``drt.sources`` /
``drt.destinations`` are discovered and reported here for visibility, but a
connector registered this way is not yet reachable from a sync YAML —
``SyncConfig.destination`` and ``load_profile()`` both validate against a
closed, hand-enumerated set of ``type`` values *before* the connector
registry in :mod:`drt.connectors.registry` is ever consulted. See
`ADR 0009 <../docs/adr/0009-plugin-config-union-blocker.md>`_ for why, and
what would need to change to lift this. Registering a destination or source
here still has value (the type participates in ``get_destination()`` /
``get_source()`` lookups for any code path that constructs a sync
programmatically rather than through YAML), so the group is not withheld —
just labeled accurately.

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
    "drt.observers",
)

# Groups where a successfully loaded entry point still can't be named in a
# sync YAML today — see the module docstring and ADR 0009.
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
    """
    global _loaded, _last_result
    with _lock:
        if _loaded and not force:
            return _last_result
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
        return results


def _reset_plugin_state() -> None:
    """Test hook — clear the load cache so ``load_plugins()`` re-runs."""
    global _loaded, _last_result
    with _lock:
        _loaded = False
        _last_result = []
