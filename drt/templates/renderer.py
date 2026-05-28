"""Template renderer using Jinja2.

Future: replace with MiniJinja (Rust) via PyO3 for zero-dependency binary.
Interface is intentionally simple to make the swap transparent.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from jinja2 import BaseLoader, Environment, StrictUndefined
from jinja2.exceptions import UndefinedError


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


def render_template(template_str: str, row: dict[str, Any]) -> str:
    """Render a Jinja2 template string with a single row of data.

    Variables are accessed as {{ row.field_name }}.
    Raises ValueError on missing variables (strict mode).
    """
    env = Environment(loader=BaseLoader(), undefined=StrictUndefined)
    env.filters["tojson_safe"] = tojson_safe
    try:
        tmpl = env.from_string(template_str)
        return tmpl.render(row=row)
    except UndefinedError as e:
        raise ValueError(f"Template error: {e}") from e
