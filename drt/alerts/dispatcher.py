"""Routes AlertItems to per-type senders. Best-effort: never raises."""
from __future__ import annotations

import logging
from typing import Any

from drt.alerts.slack import send_slack_alert
from drt.alerts.webhook import send_webhook_alert
from drt.config.models import (
    AlertsConfig,
    SlackAlertConfig,
    WebhookAlertConfig,
)
from drt.destinations.base import SyncResult

logger = logging.getLogger(__name__)


def build_context(
    sync_name: str,
    result: SyncResult,
    duration_s: float,
    started_at: str,
    exception: BaseException | None = None,
) -> dict[str, Any]:
    if exception is not None:
        error = f"{type(exception).__name__}: {exception}"
    elif result.errors:
        error = result.errors[0]
    else:
        error = "<no error message>"
    return {
        "sync_name": sync_name,
        "error": error,
        "rows_processed": result.success + result.failed,
        "duration_s": duration_s,
        "started_at": started_at,
    }


def dispatch_alerts(
    alerts: AlertsConfig | None,
    event: str,
    context: dict[str, Any],
) -> None:
    if alerts is None:
        return
    targets = getattr(alerts, event, []) or []
    for target in targets:
        try:
            if isinstance(target, SlackAlertConfig):
                send_slack_alert(target, context)
            elif isinstance(target, WebhookAlertConfig):
                send_webhook_alert(target, context)
            else:
                logger.warning("Unknown alert type: %r", type(target).__name__)
        except Exception as exc:  # noqa: BLE001  — dispatch is best-effort
            logger.warning("Alert dispatch failed (%s): %s", type(target).__name__, exc)
