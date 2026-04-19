"""SQL utility functions for row count operations.

Provides consistent interface for querying row counts across SQL destinations.
"""

from __future__ import annotations

from typing import Any

from drt.config.models import (
    ClickHouseDestinationConfig,
    DestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
)


def get_row_count_for_destination(
    destination: Any,
    config: DestinationConfig,
) -> int | None:
    """Get current row count from a SQL destination table.

    Args:
        destination: Destination instance (must be a SQL destination class).
        config: Destination configuration with table name.

    Returns:
        Row count as integer, or None if unable to determine (e.g., non-SQL destination).

    Raises:
        Exception: If connection or query fails (should be caught by caller).
    """
    if isinstance(config, PostgresDestinationConfig):
        return destination.get_row_count(config)
    elif isinstance(config, MySQLDestinationConfig):
        return destination.get_row_count(config)
    elif isinstance(config, ClickHouseDestinationConfig):
        return destination.get_row_count(config)
    # Non-SQL destinations (REST API, Slack, etc.) don't support row count
    return None
