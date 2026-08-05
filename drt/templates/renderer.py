"""Template renderer using Jinja2.

Future: replace with MiniJinja (Rust) via PyO3 for zero-dependency binary.
Interface is intentionally simple to make the swap transparent.

Two rendering entry points, differing only in what they return:

* :func:`render_template` — always a ``str``. Used where the result is spliced
  into a larger text payload (REST ``body_template``, HubSpot / Klaviyo /
  Mixpanel / Zendesk / Linear property templates), so a string is the only
  sensible answer.
* :func:`render_value` — a Python value when the template is a **single
  output node**, otherwise a ``str`` (#763). Used where the result becomes a
  *field* in a record that a typed destination is about to write, so
  ``{{ row.n * 1000 }}`` must reach a BIGINT column as ``5000`` and not
  ``"5000"``.

Both share one Environment per mode and cache compiled templates, because
every caller renders **per record**: the previous code built a fresh
``Environment`` and re-parsed the template inside the row loop.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from functools import lru_cache
from itertools import chain, islice
from typing import Any
from uuid import UUID

from jinja2 import BaseLoader, Environment, StrictUndefined, Template, Undefined
from jinja2.exceptions import UndefinedError
from jinja2.nativetypes import NativeEnvironment, NativeTemplate

# Compiled templates are cached by source text. Template sources come from
# config, so the working set is bounded by project size; the cap only exists
# so a pathological config cannot grow the cache without limit (a miss just
# re-parses). Jinja templates are safe to render concurrently — each render
# builds its own context — which matters because `drt run --threads N` renders
# from several threads against one cache.
_TEMPLATE_CACHE_SIZE = 512


def _json_default(obj: Any) -> Any:
    """JSON encoder fallback for non-JSON-serializable Python types.

    Handles datetime/date/time → ISO 8601, Decimal/UUID → str. Anything else
    raises TypeError to preserve `json.dumps` semantics.
    """
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def tojson_safe(value: Any) -> str:
    """Jinja2 filter: like `tojson` but tolerant of datetime / Decimal / UUID."""
    return json.dumps(value, default=_json_default, ensure_ascii=False)


def _single_node_concat(values: Any) -> Any:
    """Join rendered output nodes, preserving the value of a lone node (#763).

    This is the whole of the "when does a template keep its type" rule, and it
    is deliberately expressed in terms of **output nodes** rather than the
    rendered text:

    * one node  → that node's value, untouched. ``{{ row.n }}`` yields ``5``;
      ``{{ row.zip }}`` over ``"01"`` yields ``"01"``; a template with no
      expression at all (``"drt-prod"``) is a single literal node and stays
      the string it was written as.
    * many nodes → ``str`` of each, concatenated. ``"{{ a }}-{{ b }}"`` is
      text assembly, so text is what it produces.

    Jinja's own :func:`jinja2.nativetypes.native_concat` is **not** usable
    here: it runs ``literal_eval`` over the result, so a column holding the
    string ``"123"`` would silently arrive at the destination as the integer
    ``123``. That is data-dependent — the same config would behave differently
    per row — which is exactly the failure this rule exists to avoid.
    """
    head = list(islice(values, 2))
    if not head:
        return ""
    if len(head) == 1:
        value = head[0]
        if isinstance(value, Undefined):
            # The native code generator never stringifies an output node, so
            # StrictUndefined has nothing to trip over and an Undefined would
            # otherwise be written into the record as-is. Force it.
            str(value)
        return value
    return "".join(str(v) for v in chain(head, values))


class _ValueTemplate(NativeTemplate):
    """A NativeTemplate that concatenates via :func:`_single_node_concat`.

    ``NativeTemplate.render`` reaches for ``self.environment_class.concat``,
    i.e. the concat of the *declared* class rather than of the environment
    that produced the template — so overriding ``concat`` on an Environment
    subclass alone silently has no effect.
    """

    def render(self, *args: Any, **kwargs: Any) -> Any:
        ctx = self.new_context(dict(*args, **kwargs))
        try:
            return _single_node_concat(self.root_render_func(ctx))
        except Exception:
            return self.environment.handle_exception()


class _ValueEnvironment(NativeEnvironment):
    """Native environment whose templates return values, not repr-able text."""

    template_class = _ValueTemplate
    concat = staticmethod(_single_node_concat)


def _build_env(env: Environment) -> Environment:
    env.filters["tojson_safe"] = tojson_safe
    return env


_STRING_ENV = _build_env(Environment(loader=BaseLoader(), undefined=StrictUndefined))
_VALUE_ENV = _build_env(_ValueEnvironment(loader=BaseLoader(), undefined=StrictUndefined))


@lru_cache(maxsize=_TEMPLATE_CACHE_SIZE)
def _compile_string(template_str: str) -> Template:
    return _STRING_ENV.from_string(template_str)


@lru_cache(maxsize=_TEMPLATE_CACHE_SIZE)
def _compile_value(template_str: str) -> Template:
    return _VALUE_ENV.from_string(template_str)


def render_template(template_str: str, row: dict[str, Any]) -> str:
    """Render a Jinja2 template string with a single row of data.

    Variables are accessed as {{ row.field_name }}.
    Raises ValueError on missing variables (strict mode).
    """
    try:
        return _compile_string(template_str).render(row=row)
    except UndefinedError as e:
        raise ValueError(f"Template error: {e}") from e


def render_value(template_str: str, row: dict[str, Any]) -> Any:
    """Render a template to a Python value rather than to text (#763).

    Same environment, filters and strictness as :func:`render_template`; the
    only difference is that a template consisting of a **single output node**
    returns that node's value with its type intact — see
    :func:`_single_node_concat` for the rule and why Jinja's native concat is
    not used.

    Raises ValueError on missing variables (strict mode).
    """
    try:
        return _compile_value(template_str).render(row=row)
    except UndefinedError as e:
        raise ValueError(f"Template error: {e}") from e
