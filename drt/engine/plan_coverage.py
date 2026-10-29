"""Which destinations ``drt plan`` can plan, declared per destination type (#1223).

``drt plan`` is only as honest as its coverage: a destination that cannot report
what it currently holds (or whose writes drt cannot preview) answers *plan
unavailable, with a reason* instead of a partial plan. This module is the one
declared place that says which is which, so the answer is visible in the docs and
a new destination cannot ship without stating it.

This describes the *destination's* capability to answer "what exists today". Every
plan additionally needs ``destination.upsert_key`` and a sync that a plan can
represent (see "When a plan is unavailable" in ``docs/guides/plan.md``); a keyless
sync is reported unavailable whatever the destination.

Status values:

``available``
    The destination can be read back, so every sync mode (append, upsert/merge,
    mirror, replace) can be planned with a true create/update/delete change set.
``partial``
    Only some write shapes can be planned (an append-only ``mode: insert``, whose
    write is known exactly without reading the target).
``needs-api-diff``
    The destination has an API that could answer "what exists today"; planning it
    is the API-based diff work of #472. Not planned today.
``unavailable``
    There is no target state to diff (a message, event, file or object sink).
    Not planned, and not expected to be.

The declaration is checked against the real code paths by
``tests/unit/test_plan_coverage.py``: it cannot say ``available`` for a destination
whose rows ``drt`` cannot read. ``scripts/check_drift.sh`` and that test fail when a
built-in destination has no entry. Third-party (plugin) destinations need no entry
and are not listed; whether one can be planned depends on what it implements.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

PlanStatus = Literal["available", "partial", "needs-api-diff", "unavailable"]

PLAN_STATUSES: tuple[PlanStatus, ...] = ("available", "partial", "needs-api-diff", "unavailable")


@dataclass(frozen=True)
class PlanCoverage:
    status: PlanStatus
    reason: str = ""  # required for everything except ``available``


_SQL_READ = PlanCoverage("available")
_EVENT = "Event/message sink: there is no stored target state to diff."
_OBJECT = "Writes whole files or objects; there are no keyed target rows to diff."
_API = "Records live behind an API that could be read back by key (API-based diff, #472)."

PLAN_COVERAGE: dict[str, PlanCoverage] = {
    "postgres": _SQL_READ,
    "mysql": _SQL_READ,
    "snowflake": _SQL_READ,
    "databricks": _SQL_READ,
    "clickhouse": _SQL_READ,
    "bigquery": PlanCoverage(
        "partial",
        "Only `mode: insert` (append-only) can be planned. `merge`, mirror and replace need a "
        "target read that BigQuery does not implement yet.",
    ),
    "airtable": PlanCoverage("needs-api-diff", _API),
    "elasticsearch": PlanCoverage("needs-api-diff", _API),
    "google_sheets": PlanCoverage("needs-api-diff", _API),
    "hubspot": PlanCoverage("needs-api-diff", _API),
    "intercom": PlanCoverage("needs-api-diff", _API),
    "jira": PlanCoverage("needs-api-diff", _API),
    "linear": PlanCoverage("needs-api-diff", _API),
    "notion": PlanCoverage("needs-api-diff", _API),
    "salesforce_bulk": PlanCoverage("needs-api-diff", _API),
    "zendesk": PlanCoverage("needs-api-diff", _API),
    "amplitude": PlanCoverage("unavailable", _EVENT),
    "mixpanel": PlanCoverage("unavailable", _EVENT),
    "klaviyo": PlanCoverage("unavailable", _EVENT),
    "meta_conversions": PlanCoverage("unavailable", _EVENT),
    "google_ads": PlanCoverage("unavailable", "Conversion upload: " + _EVENT),
    "discord": PlanCoverage("unavailable", _EVENT),
    "slack": PlanCoverage("unavailable", _EVENT),
    "teams": PlanCoverage("unavailable", _EVENT),
    "email_smtp": PlanCoverage("unavailable", _EVENT),
    "sendgrid": PlanCoverage("unavailable", _EVENT),
    "twilio": PlanCoverage("unavailable", _EVENT),
    "sqs": PlanCoverage("unavailable", "Queue: " + _EVENT),
    "kafka": PlanCoverage("unavailable", "Topic: " + _EVENT),
    "github_actions": PlanCoverage("unavailable", "Triggers a workflow run; " + _EVENT),
    "rest_api": PlanCoverage(
        "unavailable", "Generic HTTP endpoint: drt has no read-back contract for it."
    ),
    "staged_upload": PlanCoverage("unavailable", "Staged file upload: " + _OBJECT),
    "s3": PlanCoverage("unavailable", _OBJECT),
    "gcs": PlanCoverage("unavailable", _OBJECT),
    "azure_blob": PlanCoverage("unavailable", _OBJECT),
    "parquet": PlanCoverage("unavailable", _OBJECT),
    "file": PlanCoverage("unavailable", _OBJECT),
}


def plan_coverage(destination_type: str) -> PlanCoverage | None:
    """The declared coverage of a built-in destination type, or ``None`` (undeclared).

    ``None`` does not mean "cannot be planned": the planner decides from the
    destination itself. It only means the matrix says nothing about this type.
    """
    return PLAN_COVERAGE.get(destination_type)
