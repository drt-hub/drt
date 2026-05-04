"""Generic HTTP webhook alert sender."""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

from drt.config.models import WebhookAlertConfig

logger = logging.getLogger(__name__)


def _resolve_url(cfg: WebhookAlertConfig) -> str | None:
    if cfg.url:
        return cfg.url
    if cfg.url_env:
        return os.environ.get(cfg.url_env)
    return None


def send_webhook_alert(cfg: WebhookAlertConfig, context: dict[str, Any]) -> None:
    url = _resolve_url(cfg)
    if not url:
        logger.warning("Webhook alert: no url resolved; skipping")
        return
    if cfg.body_template:
        body = cfg.body_template.format(**context).encode()
    else:
        body = json.dumps(context, default=str).encode()
    headers = dict(cfg.headers)
    headers.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=body, headers=headers, method=cfg.method)
    try:
        urllib.request.urlopen(req, timeout=10).close()
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.warning("Webhook alert send failed: %s", exc)
