"""JSON-lines logging for ``drt run --log-format json``.

Extracted from ``drt.cli.commands.run`` so the command module holds the run
command, not generic logging infrastructure. ``run.py`` re-imports these names
(and ``drt.cli.main`` re-exports them), so the public import surface is
unchanged.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

# Attributes present on a bare LogRecord — anything else on a record came in via
# the ``extra=`` kwarg and should be merged into the JSON payload.
_STANDARD_LOG_FIELDS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)))


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON object (JSON Lines format)."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload: dict[str, object] = {
            "ts": ts,
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        # Merge any extra fields passed via the `extra` kwarg
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_FIELDS and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload)


def _configure_json_logging() -> None:
    """Replace root logger handlers with a stderr JSON handler."""
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.INFO)
