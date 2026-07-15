"""Project vars — the ``vars:`` block, ``--vars`` override, and ``var()`` (#783).

Completes the parameterization layer that ``${ENV}`` substitution (#385/#240)
started: anything project-shaped (a default lookback window, a campaign tag, a
sandbox-vs-prod pipeline id) gets a reviewed default in the repo instead of an
environment variable documented per runner.

``{{ var('name') }}`` / ``{{ var('name', default) }}`` joins the deliberately
tiny Jinja surface (``cursor_value`` / ``watermark``) in model SQL, and works in
YAML string fields alongside ``${ENV}``.

Precedence, highest first:

1. ``--vars 'name: value'`` (CLI)
2. ``DRT_VAR_<NAME>`` environment variables (escape hatch)
3. the project ``vars:`` block in ``drt_project.yml``

An undefined var with no default raises :class:`VarError` at load time, so
``drt validate`` surfaces it rather than a run failing halfway. Var values
interpolate into SQL text exactly like ``${ENV}`` does — see the security note
in the docs; they are project config, not user input.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from typing import Any

import yaml
from jinja2 import BaseLoader, Environment

# Only ``var(`` calls count as a var template — a bare ``{{ foo }}`` is left to
# whatever else renders it, so adding vars never changes existing SQL.
_VAR_TEMPLATE_PATTERN = re.compile(r"\{\{\s*var\s*\(")

ENV_VAR_PREFIX = "DRT_VAR_"


class VarError(ValueError):
    """An undefined var without a default, or an unparseable ``--vars`` value.

    Subclasses ``ValueError`` so ``load_syncs_safe`` collects it per-file the
    same way it collects env-substitution failures (``drt validate`` coverage).
    """


def parse_cli_vars(raw: str) -> dict[str, Any]:
    """Parse ``--vars`` into a mapping.

    Accepts a YAML flow mapping with or without braces, so both of these work::

        --vars 'lookback_days: 1, hubspot_pipeline: sandbox'
        --vars '{lookback_days: 1, hubspot_pipeline: sandbox}'
    """
    text = raw.strip()
    if not text:
        return {}
    if not text.startswith("{"):
        if ":" not in text:
            # Wrapping a bare word in braces yields {word: None} — a typo would
            # silently become a null var. Demand the documented name: value form.
            raise VarError(
                f"--vars must be a mapping, e.g. --vars 'lookback_days: 1, tag: crm' "
                f"(got {raw.strip()!r})."
            )
        text = "{" + text + "}"
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise VarError(
            f"--vars is not valid YAML: {e}. Expected 'name: value, other: value'."
        ) from e
    if not isinstance(data, dict):
        raise VarError(
            "--vars must be a mapping, e.g. --vars 'lookback_days: 1, tag: crm'."
        )
    return data


def env_vars() -> dict[str, Any]:
    """``DRT_VAR_LOOKBACK_DAYS=7`` -> ``{"lookback_days": "7"}``.

    The name is lowercased: env vars are conventionally upper-case while
    ``vars:`` keys are not. Values are always strings (the environment has no
    types) — a var consumed as a number should carry its default in ``vars:``.
    """
    prefix_len = len(ENV_VAR_PREFIX)
    return {
        key[prefix_len:].lower(): value
        for key, value in os.environ.items()
        if key.startswith(ENV_VAR_PREFIX) and len(key) > prefix_len
    }


def resolve_vars(
    project_vars: dict[str, Any] | None = None,
    cli_vars: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge the three var sources by precedence: project < env < CLI."""
    merged: dict[str, Any] = dict(project_vars or {})
    merged.update(env_vars())
    merged.update(cli_vars or {})
    return merged


def has_var_template(text: str) -> bool:
    """True when *text* contains a ``{{ var(...) }}`` call."""
    return bool(_VAR_TEMPLATE_PATTERN.search(text))


class _Missing:
    """Sentinel — distinguishes "no default given" from ``default=None``."""


_MISSING = _Missing()


def make_var(variables: dict[str, Any]) -> Callable[..., Any]:
    """Build the ``var()`` callable bound to *variables*, for a Jinja global."""

    def var(name: str, default: Any = _MISSING) -> Any:
        if name in variables:
            return variables[name]
        if not isinstance(default, _Missing):
            return default
        raise VarError(
            f"Undefined var {name!r} and no default given. Define it under `vars:` in "
            f"drt_project.yml, pass --vars '{name}: <value>', set "
            f"{ENV_VAR_PREFIX}{name.upper()}=<value>, or supply a default: "
            f"{{{{ var('{name}', 'fallback') }}}}."
        )

    return var


def var_environment(variables: dict[str, Any] | None = None) -> Environment:
    """A Jinja environment with ``var()`` bound to *variables*.

    Undefined handling is Jinja's default on purpose: strictness lives in
    ``var()`` itself (an unknown var raises), so adding this layer can't change
    how an existing stray ``{{ token }}`` renders.
    """
    # autoescape stays off: this renders SQL and YAML, never HTML. Escaping
    # would corrupt values (``O'Brien`` -> ``O&#39;Brien``) and break queries.
    # Var values are reviewed project config interpolated into SQL text — the
    # same trust posture as ``${ENV}`` substitution, documented alongside it.
    # (This is the environment that previously lived in engine/resolver.py for
    # the cursor template; it moved here rather than being newly introduced.)
    env = Environment(  # lgtm[py/jinja2/autoescape-false]  # noqa: S701
        loader=BaseLoader(),
        autoescape=False,
    )
    env.globals["var"] = make_var(variables or {})
    return env


def render_vars(text: str, variables: dict[str, Any] | None = None) -> str:
    """Render ``{{ var(...) }}`` in a single string."""
    return var_environment(variables).from_string(text).render()


def expand_vars(data: Any, variables: dict[str, Any] | None = None) -> Any:
    """Recursively render ``{{ var(...) }}`` in every string of a YAML tree.

    Mirrors :func:`drt.config.parser.expand_env_vars`. Strings without a
    ``var()`` call are returned untouched, so this is a no-op for projects that
    don't use vars. Rendering yields a string; pydantic coerces it back to the
    field's declared type (``{{ var('lookback_days') }}`` -> ``"7"`` -> ``7``).
    """
    if isinstance(data, str):
        return render_vars(data, variables) if has_var_template(data) else data
    if isinstance(data, dict):
        return {k: expand_vars(v, variables) for k, v in data.items()}
    if isinstance(data, list):
        return [expand_vars(item, variables) for item in data]
    return data
