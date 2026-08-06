"""Provider protocol and scheme registry for URI-scheme secret references (#782).

A provider resolves a URI like ``aws-sm://prod/drt/snowflake#password`` to a
live secret value. Registered by scheme so :func:`resolve_provider_uri` can
dispatch without importing every provider's SDK — only the scheme actually
referenced in a profile pays the lazy-import cost (each provider module
imports its client library inside the method that needs it, not at module
level, matching the existing ``s3``/``bigquery`` destination pattern).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
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


class SecretProvider(Protocol):
    """Resolves a parsed provider URI to a secret value.

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

# Process-lifetime cache, keyed by the full URI. A run commonly resolves the
# same credential more than once (validation pass, connection test, the real
# connection), and unlike an env var or secrets.toml lookup, a provider fetch
# is a network call — so unlike those two steps, this one is worth not
# repeating.
#
# Unbounded for the process's life: fine for a `drt run` invocation (exits in
# seconds to minutes), but `drt serve` is a long-lived process that re-enters
# this path on every triggered sync — a secret resolved once is held until
# the server restarts, with no TTL and no re-fetch on rotation. Known gap,
# not yet addressed — tracked as #929 — rather than assuming rotation is
# picked up here.
_value_cache: dict[str, str] = {}


def register(scheme: str, provider: SecretProvider) -> None:
    """Register a provider for a URI scheme (e.g. ``"aws-sm"``).

    Raises:
        ValueError: ``scheme`` is already registered — each scheme must map
            to exactly one provider.
    """
    if scheme in _registry:
        raise ValueError(f"Secret provider scheme '{scheme}' already registered.")
    _registry[scheme] = provider


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
    if uri not in _value_cache:
        _value_cache[uri] = provider.fetch(parse_secret_uri(uri))
    return _value_cache[uri]


def clear_cache() -> None:
    """Drop all cached values — test isolation; not called in production."""
    _value_cache.clear()
