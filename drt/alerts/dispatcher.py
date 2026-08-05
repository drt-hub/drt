"""Routes AlertItems to per-type senders. Best-effort: never raises."""
from __future__ import annotations

import logging
from typing import Any

from drt.alerts.slack import send_slack_alert
from drt.alerts.webhook import send_webhook_alert
from drt.config.models import (
    AlertItem,
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


def build_degraded_context(
    sync_name: str,
    result: SyncResult,
    duration_s: float,
    started_at: str,
    tripped: list[Any],
) -> dict[str, Any]:
    """Context for an ``on_degraded`` alert (#784) — the tripped conditions.

    ``error`` reuses the existing message templates' field so a channel's
    default message renders a readable summary; ``conditions_tripped`` carries
    the structured detail for webhook JSON bodies.
    """
    summary = ", ".join(
        f"{t.metric} {t.actual} ({t.operator} {t.threshold})" for t in tripped
    )
    return {
        "sync_name": sync_name,
        "status": "degraded",
        "error": f"degraded — {summary}",
        "conditions_tripped": [
            {
                "metric": t.metric,
                "operator": t.operator,
                "threshold": t.threshold,
                "actual": t.actual,
            }
            for t in tripped
        ],
        "rows_processed": result.success + result.failed,
        "duration_s": duration_s,
        "started_at": started_at,
    }


def dispatch_targets(targets: list[AlertItem], context: dict[str, Any]) -> None:
    """Send *context* to each configured channel. Best-effort — never raises.

    Shared by ``on_failure`` and ``on_degraded`` (#784) so both events run
    through identical per-channel sending.
    """
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


def dispatch_alerts(
    alerts: AlertsConfig | None,
    event: str,
    context: dict[str, Any],
) -> None:
    if alerts is None:
        return
    dispatch_targets(getattr(alerts, event, []) or [], context)
