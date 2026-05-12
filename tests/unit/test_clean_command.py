"""Unit tests for drt clean command (#447)."""

from __future__ import annotations

from unittest import mock

import pytest
from typer.testing import CliRunner

from drt.cli.main import app


class TestCleanCommand:
    """Test drt clean --orphans command."""

    def test_clean_orphans_dry_run(self):
        """Dry-run should not call drop methods."""
        runner = CliRunner()

        mock_sync = mock.Mock()
        mock_sync.name = "test_sync"
        mock_sync.destination = mock.Mock()

        mock_dest = mock.Mock()
        mock_dest.list_orphan_swap_tables = mock.Mock(
            return_value=["public.orphan__drt_swap"]
        )

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync])
            mock_get_dest.return_value = mock_dest
            with mock.patch("drt.cli.main.isinstance", return_value=True):
                result = runner.invoke(app, ["clean", "--orphans"])

        # Should not drop without --execute
        mock_dest.drop_orphan_swap_tables.assert_not_called()
        assert result.exit_code == 0

    def test_clean_orphans_with_execute(self):
        """Should call drop when --execute flag provided."""
        runner = CliRunner()

        mock_sync = mock.Mock()
        mock_sync.name = "test_sync"
        mock_sync.destination = mock.Mock()

        mock_dest = mock.Mock()
        mock_dest.list_orphan_swap_tables = mock.Mock(
            return_value=["public.orphan__drt_swap"]
        )
        mock_dest.drop_orphan_swap_tables = mock.Mock(
            return_value=(["public.orphan__drt_swap"], [])
        )

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync])
            mock_get_dest.return_value = mock_dest
            with mock.patch("drt.cli.main.isinstance", return_value=True):
                result = runner.invoke(app, ["clean", "--orphans", "--execute"])

        # Should call drop with --execute
        mock_dest.drop_orphan_swap_tables.assert_called_once()
        assert result.exit_code == 0

    def test_clean_help_shows_options(self):
        """Help should document --orphans and --execute."""
        runner = CliRunner()
        result = runner.invoke(app, ["clean", "--help"])
        assert result.exit_code == 0
        assert "--orphans" in result.stdout
        assert "--execute" in result.stdout

    def test_deduplicates_across_syncs(self):
        """Should deduplicate orphans found across multiple syncs."""
        runner = CliRunner()

        mock_sync1 = mock.Mock()
        mock_sync1.name = "sync1"
        mock_sync1.destination = mock.Mock()

        mock_sync2 = mock.Mock()
        mock_sync2.name = "sync2"
        mock_sync2.destination = mock.Mock()

        mock_dest = mock.Mock()
        # Both syncs return same orphan table
        mock_dest.list_orphan_swap_tables = mock.Mock(
            return_value=["public.orphan__drt_swap"]
        )

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync1, mock_sync2])
            mock_get_dest.side_effect = [mock_dest, mock_dest]
            with mock.patch("drt.cli.main.isinstance", return_value=True):
                result = runner.invoke(app, ["clean", "--orphans"])

        # Should show 1 table (deduplicated), not 2
        assert "Found 1 orphan" in result.stdout or "1 orphan" in result.stdout.lower()

    def test_no_orphans_found_message(self):
        """Should show success when no orphans found."""
        runner = CliRunner()

        mock_sync = mock.Mock()
        mock_sync.name = "test_sync"
        mock_sync.destination = mock.Mock()

        mock_dest = mock.Mock()
        mock_dest.list_orphan_swap_tables = mock.Mock(return_value=[])

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync])
            mock_get_dest.return_value = mock_dest
            with mock.patch("drt.cli.main.isinstance", return_value=True):
                result = runner.invoke(app, ["clean", "--orphans"])

        assert "No orphan" in result.stdout or "0 orphan" in result.stdout.lower()
