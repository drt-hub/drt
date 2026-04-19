"""Tests for --dry-run row count diff feature (issue #339)."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from drt.cli.output import _print_row_count_diff, print_dry_run_summary
from drt.config.credentials import PostgresProfile
from drt.config.models import (
    DestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
    SyncOptions,
)


@pytest.fixture
def mock_sync_config() -> SyncConfig:
    """Create a mock SyncConfig for testing."""
    config = SyncConfig(
        name="test_sync",
        model="public.test_table",
        destination=MySQLDestinationConfig(
            type="mysql",
            host="localhost",
            dbname="testdb",
            user="testuser",
            table="destination_table",
            upsert_key=["id"],
        ),
        sync=SyncOptions(mode="replace"),
    )
    return config


@pytest.fixture
def mock_postgres_config() -> SyncConfig:
    """Create a SyncConfig with Postgres destination."""
    config = SyncConfig(
        name="test_sync",
        model="public.test_table",
        destination=PostgresDestinationConfig(
            type="postgres",
            host="localhost",
            dbname="testdb",
            user="testuser",
            table="public.destination_table",
            upsert_key=["id"],
        ),
        sync=SyncOptions(mode="replace"),
    )
    return config


@pytest.fixture
def mock_profile() -> PostgresProfile:
    """Create a mock profile for testing."""
    profile = Mock(spec=PostgresProfile)
    profile.describe.return_value = "postgres (localhost:5432)"
    return profile


class MockMySQLDestination:
    """Mock MySQL destination for testing row count."""

    def __init__(self, row_count: int = 100):
        self.row_count = row_count

    def get_row_count(self, config: DestinationConfig) -> int:
        """Return the configured row count."""
        return self.row_count


class MockPostgresDestination:
    """Mock Postgres destination for testing row count."""

    def __init__(self, row_count: int = 100):
        self.row_count = row_count

    def get_row_count(self, config: DestinationConfig) -> int:
        """Return the configured row count."""
        return self.row_count


def test_print_row_count_diff_positive_diff(
    mock_sync_config: SyncConfig,
    capsys,
) -> None:
    """Test row count diff when new rows > current rows (positive diff)."""
    destination = MockMySQLDestination(row_count=1000)

    _print_row_count_diff(mock_sync_config, destination, new_rows=1500)

    captured = capsys.readouterr()
    assert "Current destination rows: 1000" in captured.out
    assert "→ New: 1500" in captured.out
    assert "+500" in captured.out


def test_print_row_count_diff_negative_diff(
    mock_sync_config: SyncConfig,
    capsys,
) -> None:
    """Test row count diff when new rows < current rows (negative diff)."""
    destination = MockMySQLDestination(row_count=2000)

    _print_row_count_diff(mock_sync_config, destination, new_rows=500)

    captured = capsys.readouterr()
    assert "Current destination rows: 2000" in captured.out
    assert "→ New: 500" in captured.out
    assert "-1500" in captured.out


def test_print_row_count_diff_zero_diff(
    mock_sync_config: SyncConfig,
    capsys,
) -> None:
    """Test row count diff when new rows == current rows (zero diff)."""
    destination = MockMySQLDestination(row_count=1000)

    _print_row_count_diff(mock_sync_config, destination, new_rows=1000)

    captured = capsys.readouterr()
    assert "Current destination rows: 1000" in captured.out
    assert "→ New: 1000" in captured.out


def test_print_row_count_diff_handles_connection_error(
    mock_sync_config: SyncConfig,
    capsys,
) -> None:
    """Test that connection errors are handled gracefully."""
    # Mock destination that raises an exception
    bad_destination = Mock()
    bad_destination.get_row_count.side_effect = ConnectionError("Connection failed")

    _print_row_count_diff(mock_sync_config, bad_destination, new_rows=100)

    captured = capsys.readouterr()
    assert "Could not retrieve current row count" in captured.out
    assert "ConnectionError" in captured.out


def test_print_dry_run_summary_includes_row_count_for_replace_mode(
    mock_sync_config: SyncConfig,
    mock_profile,
    capsys,
) -> None:
    """Test that dry run summary includes row count diff for replace mode."""
    destination = MockMySQLDestination(row_count=1180)

    print_dry_run_summary(mock_sync_config, mock_profile, rows=1234, destination=destination)

    captured = capsys.readouterr()
    assert "Dry run summary:" in captured.out
    assert "Sync mode: replace" in captured.out
    assert "⚠ replace mode will TRUNCATE" in captured.out
    assert "Current destination rows: 1180" in captured.out
    assert "→ New: 1234" in captured.out
    assert "+54" in captured.out


def test_print_dry_run_summary_no_row_count_without_destination(
    mock_sync_config: SyncConfig,
    mock_profile,
    capsys,
) -> None:
    """Test that row count diff is not shown when destination is None."""
    print_dry_run_summary(mock_sync_config, mock_profile, rows=1234, destination=None)

    captured = capsys.readouterr()
    assert "Dry run summary:" in captured.out
    assert "Current destination rows:" not in captured.out


def test_print_dry_run_summary_replace_mode_zero_source_rows(
    mock_sync_config: SyncConfig,
    mock_profile,
    capsys,
) -> None:
    """Test warning when replace mode would result in 0 rows (dangerous scenario)."""
    destination = MockMySQLDestination(row_count=5000)

    print_dry_run_summary(mock_sync_config, mock_profile, rows=0, destination=destination)

    captured = capsys.readouterr()
    assert "Current destination rows: 5000" in captured.out
    assert "→ New: 0" in captured.out
    assert "-5000" in captured.out


def test_get_row_count_for_postgres_destination(
    mock_postgres_config: SyncConfig,
    capsys,
) -> None:
    """Test row count diff for PostgreSQL destination."""
    destination = MockPostgresDestination(row_count=2500)

    _print_row_count_diff(mock_postgres_config, destination, new_rows=3000)

    captured = capsys.readouterr()
    assert "Current destination rows: 2500" in captured.out
    assert "→ New: 3000" in captured.out
    assert "+500" in captured.out


def test_print_dry_run_summary_full_output(
    mock_sync_config: SyncConfig,
    mock_profile,
    capsys,
) -> None:
    """Test full dry run summary output with all components."""
    destination = MockMySQLDestination(row_count=100)

    print_dry_run_summary(mock_sync_config, mock_profile, rows=150, destination=destination)

    captured = capsys.readouterr()
    # Verify all expected components
    assert "Dry run summary:" in captured.out
    assert "Source: postgres (localhost:5432)" in captured.out
    assert "Destination: mysql" in captured.out
    assert "Rows to sync: 150" in captured.out
    assert "Sync mode: replace" in captured.out
    assert "⚠ replace mode will TRUNCATE" in captured.out
    assert "Current destination rows: 100" in captured.out
    assert "→ New: 150" in captured.out
    assert "+50" in captured.out
