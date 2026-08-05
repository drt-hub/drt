"""Slack incoming-webhook alert sender."""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

from drt.config.models import SlackAlertConfig

logger = logging.getLogger(__name__)


def _resolve_url(cfg: SlackAlertConfig) -> str | None:
    if cfg.webhook_url:
        return cfg.webhook_url
    if cfg.webhook_url_env:
        return os.environ.get(cfg.webhook_url_env)
    return None


def send_slack_alert(cfg: SlackAlertConfig, context: dict[str, Any]) -> None:
    url = _resolve_url(cfg)
    if not url:
        logger.warning("Slack alert: no webhook_url resolved; skipping")
        return
    text = cfg.message.format(**context)
    payload = json.dumps({"text": text}).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10).close()
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.warning("Slack alert send failed: %s", exc)
