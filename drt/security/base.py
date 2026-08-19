"""Permission-check Protocol and registry for the Enterprise RBAC boundary (#298).

Design-only per #298: this module defines the seam and an always-permitting
OSS default. No enforcement logic ships here — a separately-installed
Enterprise package registers a real `PermissionChecker` via
:func:`register_permission_checker`. See ADR 0008.
"""

from __future__ import annotations

import threading
from enum import Enum
from typing import Protocol, runtime_checkable


class PermissionAction(str, Enum):
    """The three verbs #298 names: "who can run/edit/view which syncs"."""

    RUN = "run"
    EDIT = "edit"
    VIEW = "view"


class PermissionDeniedError(PermissionError):
    """Raised by a `PermissionChecker` when `principal` may not `action` on `sync_name`."""


@runtime_checkable
class PermissionChecker(Protocol):
    """Enterprise RBAC extension point (#298, ADR 0008).

    A new, separate Protocol per ADR 0007 — nothing in drt-core's engine or
    CLI has an existing permission seam this could reuse.
    """

    def check(
        self,
        action: PermissionAction,
        sync_name: str | None,
        *,
        principal: str | None = None,
    ) -> None:
        """Raise :class:`PermissionDeniedError` if `principal` may not
        `action` on `sync_name`.

        `sync_name` is ``None`` for project-wide actions (e.g. ``drt run``
        with no ``--select``). `principal` is ``None`` when the caller has
        no identity concept — the OSS default, and any CLI invocation
        before an Enterprise identity layer resolves one. Implementations
        MUST treat an absent principal as unauthenticated, not trusted,
        though the OSS no-op checker permits it regardless (#298's stated
        OSS default: all permissions granted).

        Raises:
            PermissionDeniedError: `principal` may not perform `action`.
        """
        ...


class AllowAllPermissionChecker:
    """OSS default (#298): every action is permitted, unconditionally.

    `CLAUDE.md`'s "no RBAC" line stays true of the OSS product's actual
    behavior — this checker never denies anything. Only a registered
    Enterprise checker changes that.
    """

    def check(
        self,
        action: PermissionAction,
        sync_name: str | None,
        *,
        principal: str | None = None,
    ) -> None:
        return None


_lock = threading.Lock()
_checker: PermissionChecker = AllowAllPermissionChecker()


def register_permission_checker(checker: PermissionChecker) -> None:
    """Install `checker` as the active `PermissionChecker` for this process.

    Unlike `SecretProvider`'s per-scheme registry, there is exactly one
    active policy per process — a second call replaces the first rather
    than erroring, so a caller (e.g. a test fixture) can reset to the OSS
    default via ``register_permission_checker(AllowAllPermissionChecker())``.
    """
    global _checker
    with _lock:
        _checker = checker


def get_permission_checker() -> PermissionChecker:
    """Return the currently active `PermissionChecker` (the OSS default,
    `AllowAllPermissionChecker`, unless an Enterprise package registered
    its own via :func:`register_permission_checker`)."""
    with _lock:
        return _checker


def _reset_permission_checker() -> None:
    """Restore the OSS default. Test hook only — not called by production
    code, which registers at most once per process."""
    register_permission_checker(AllowAllPermissionChecker())
