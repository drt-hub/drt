"""Example rows for Jira destination smoke testing.

This file intentionally avoids any direct Jira API calls.
Use these rows to populate a source table (e.g. DuckDB table: jira_test_rows),
then run drt with --dry-run for a safe local verification.
"""

from __future__ import annotations

from typing import Any

EXAMPLE_ROWS: list[dict[str, Any]] = [
    {
        # No issue_id -> create path when running non-dry mode
        "issue_id": None,
        "metric": "cpu_usage",
        "value": 95,
        "threshold": 80,
    },
    {
        # Has issue_id -> update path when running non-dry mode
        "issue_id": "ENG-123",
        "metric": "memory_usage",
        "value": 88,
        "threshold": 75,
    },
]


def print_rows() -> None:
    """Quick preview helper."""
    for row in EXAMPLE_ROWS:
        print(row)


if __name__ == "__main__":
    print_rows()
