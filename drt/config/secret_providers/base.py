"""Provider protocol and scheme registry for URI-scheme secret references (#782).

A provider resolves a URI like ``aws-sm://prod/drt/snowflake#password`` to a
live secret value. Registered by scheme so :func:`resolve_provider_uri` can
dispatch without importing every provider's SDK — only the scheme actually
referenced in a profile pays the lazy-import cost (each provider module
imports its client library inside the method that needs it, not at module
level, matching the existing ``s3``/``bigquery`` destination pattern).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlparse


@dataclass(frozen=True)
class SecretRef:
    """A parsed provider URI: everything after the scheme, and an optional
    ``#fragment`` naming a key inside a JSON-valued secret."""

    path: str
    key: str | None


def parse_secret_uri(uri: str) -> SecretRef:
    """Split a ``scheme://netloc/path#fragment`` URI into a :class:`SecretRef`.

    ``netloc`` and ``path`` are rejoined — the scheme authority ends at the
    first ``/`` in a URI, but every provider's identifier here (an ARN-ish
    path, a Vault mount + path) is meant to read as one continuous string,
    e.g. ``aws-sm://prod/drt/snowflake`` -> ``prod/drt/snowflake``, not just
    ``drt/snowflake``.
    """
    parsed = urlparse(uri)
    path = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path.lstrip("/")
    return SecretRef(path=path, key=parsed.fragment or None)


def select_field(payload: dict[str, Any], ref: SecretRef, *, scheme: str) -> str:
    """Pull ``ref.key`` out of an already-parsed secret payload.

    Shared by every provider whose secret is (or can be) a field map holding
    several related values under one id (a DB user + password together,
    say) rather than one id per field — the ``#key`` fragment selects one.
    ``scheme`` (e.g. ``"aws-sm"``) only shapes the error messages, so a
    failure names the provider a user actually configured.

    ``ref.key`` must not be ``None`` — callers with a payload that might
    also just *be* the scalar value (AWS, GCP: the secret string itself,
    with no wrapping object) branch on that before parsing far enough to
    have a ``dict`` at all; see :func:`extract_key`. Vault's payload is
    always a field map, so it requires a key from the start rather than
    calling this.
    """
    if ref.key not in payload:
        raise LookupError(f"{scheme}: key '{ref.key}' not found in secret '{ref.path}'")
    found = payload[ref.key]
    if found is None or isinstance(found, dict | list):
        # str(None) == "None", str({...}) == a Python repr — both would
        # silently hand back a plausible-looking but wrong credential
        # instead of failing.
        raise LookupError(
            f"{scheme}: key '{ref.key}' in secret '{ref.path}' isn't a plain "
            f"value (got {type(found).__name__})"
        )
    return str(found)


def extract_key(raw: str, ref: SecretRef, *, scheme: str) -> str:
    """Return ``raw`` unchanged if ``ref.key`` is ``None``, else parse it as
    a JSON object and pull that field out via :func:`select_field`.

    For providers (AWS, GCP) whose secret is a single string that *may*
    additionally be a JSON blob — as opposed to Vault, whose secret is
    always already a field map and so never needs the JSON-parsing step.
    """
    if ref.key is None:
        return raw

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise LookupError(
            f"{scheme}: '{ref.path}#{ref.key}' requested a key, but the secret "
            "value isn't JSON"
        ) from e
    if not isinstance(payload, dict):
        raise LookupError(f"{scheme}: key '{ref.key}' not found in secret '{ref.path}'")
    return select_field(payload, ref, scheme=scheme)


@runtime_checkable
class SecretProvider(Protocol):
    """Resolves a parsed provider URI to a secret value.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Implementations do their own SDK import lazily (inside ``fetch``) so
    installing drt-core without the relevant extra never fails at import
    time — only when a profile actually references that provider's scheme.
    """

    def fetch(self, ref: SecretRef) -> str:
        """Return the secret value, or raise if it can't be resolved.

        Raises:
            ImportError: the provider's extra isn't installed.
            LookupError: the secret (or the ``ref.key`` field within it)
                doesn't exist.
        """
        ...


_registry: dict[str, SecretProvider] = {}

# Cache provider values by full URI for 300 seconds by default. A run commonly
# resolves the same credential more than once (validation pass, connection
# test, the real connection), and unlike an env var or secrets.toml lookup, a
# provider fetch is a network call. The TTL lets long-lived `drt serve`
# processes pick up secret rotation without repeating that call on every sync.
# Set DRT_SECRET_CACHE_TTL_SECONDS to another duration, or to a non-positive
# value to disable caching. It is read at lookup time so runtime environment
# changes take effect without restarting the process.
_value_cache: dict[str, tuple[str, float]] = {}
_value_cache_lock = threading.Lock()

_DEFAULT_CACHE_TTL_SECONDS = 300.0
_CACHE_TTL_ENV_VAR = "DRT_SECRET_CACHE_TTL_SECONDS"


def _cache_ttl_seconds() -> float:
    return float(os.environ.get(_CACHE_TTL_ENV_VAR, _DEFAULT_CACHE_TTL_SECONDS))


def register(scheme: str, provider: SecretProvider) -> None:
    """Register a provider for a URI scheme (e.g. ``"aws-sm"``).

    Raises:
        ValueError: ``scheme`` is already registered — each scheme must map
            to exactly one provider.
    """
    if scheme in _registry:
        raise ValueError(f"Secret provider scheme '{scheme}' already registered.")
    _registry[scheme] = provider


def _emit_secret_accessed(*, scheme: str, path: str) -> None:
    """Emit a ``secret_accessed`` audit event (#299, ADR 0008) — no-op
    under the OSS default (``NoOpAuditLogger``).

    Called *after* ``provider.fetch`` returns and, critically, after
    ``resolve_provider_uri`` has released ``_value_cache_lock`` — an
    Enterprise ``AuditLogger.log_event`` implementation may itself need to
    resolve a secret (e.g. sink credentials) via ``resolve_provider_uri``,
    and that non-reentrant lock would deadlock if this ran while still
    held (caught in Codex review on this PR). Wrapped in a broad
    try/except: a logging failure — an unreachable audit sink, say — must
    never fail the secret resolution it's auditing, matching
    ``AuditLogger.log_event``'s documented best-effort contract. This
    catch is the enforcement of that contract; it cannot be left to every
    future ``AuditLogger`` implementation to get right on its own.

    Never logs the resolved value, only the scheme/path that identifies
    which secret was read.
    """
    from datetime import datetime, timezone

    from drt.observability.audit import AuditEvent, get_audit_logger

    try:
        get_audit_logger().log_event(
            AuditEvent(
                event_type="secret_accessed",
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"scheme": scheme, "path": path},
            )
        )
    except Exception as exc:  # noqa: BLE001 — fire-and-forget contract
        logging.getLogger(__name__).warning("audit log_event failed: %s", exc)


def resolve_provider_uri(uri: str) -> str | None:
    """Resolve a ``scheme://...`` secret reference.

    Returns ``None`` when ``uri``'s scheme isn't a registered provider, so
    callers (``resolve_env``) can fall through rather than treat every
    unresolved string as an error — most values reaching this point are
    plain env var names, not provider URIs at all.
    """
    scheme = urlparse(uri).scheme
    provider = _registry.get(scheme)
    if provider is None:
        return None

    ref = parse_secret_uri(uri)
    ttl = _cache_ttl_seconds()
    if ttl <= 0:
        _value_cache.pop(uri, None)
        value = provider.fetch(ref)
        _emit_secret_accessed(scheme=scheme, path=ref.path)
        return value

    cached = _value_cache.get(uri)
    if cached is not None:
        value, fetched_at = cached
        if time.monotonic() - fetched_at <= ttl:
            return value

    fetched = False
    with _value_cache_lock:
        cached = _value_cache.get(uri)
        if cached is not None:
            value, fetched_at = cached
            if time.monotonic() - fetched_at <= ttl:
                return value

        value = provider.fetch(ref)
        _value_cache[uri] = (value, time.monotonic())
        fetched = True

    # Audit emission happens after the lock is released (see
    # _emit_secret_accessed's docstring for why — an Enterprise AuditLogger
    # resolving its own credentials via this same function would otherwise
    # deadlock on this non-reentrant lock).
    if fetched:
        _emit_secret_accessed(scheme=scheme, path=ref.path)
    return value


def clear_cache() -> None:
    """Drop all cached values — test isolation; not called in production."""
    _value_cache.clear()
