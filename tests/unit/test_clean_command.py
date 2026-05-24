"""Unit tests for drt clean command (#447)."""

from __future__ import annotations

from unittest import mock

from typer.testing import CliRunner

from drt.cli.main import app


class FakeOrphanCleanup:
    """Minimal fake that satisfies the OrphanCleanup Protocol."""

    def __init__(self, tables_by_base_table: dict[str, list[str]] | None = None) -> None:
        self.tables_by_base_table = tables_by_base_table or {}
        self.list_calls: list[tuple[str, str, float | None]] = []
        self.drop_calls: list[tuple[str, list[str]]] = []

    def list_orphan_swap_tables(
        self,
        config,
        base_table: str,
        older_than: float | None = None,
    ) -> list[str]:
        self.list_calls.append((config.table, base_table, older_than))
        return self.tables_by_base_table.get(base_table, [])

    def drop_orphan_swap_tables(self, config, tables: list[str]) -> tuple[list[str], list[str]]:
        dropped = list(tables)
        self.drop_calls.append((config.table, dropped))
        return dropped, []


class TestCleanCommand:
    """Test drt clean --orphans command."""

    def test_clean_dry_run_does_not_drop_tables(self):
        """Dry-run should report tables without dropping them."""
        runner = CliRunner()

        mock_sync = mock.Mock()
        mock_sync.name = "test_sync"
        mock_sync.destination = mock.Mock(table="public.users")

        fake_dest = FakeOrphanCleanup(
            {"public.users": ["public.users__drt_swap"]}
        )

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync])
            mock_get_dest.return_value = fake_dest
            result = runner.invoke(app, ["clean", "--orphans"])

        assert fake_dest.list_calls == [("public.users", "public.users", None)]
        assert fake_dest.drop_calls == []
        assert "[DRY RUN] Would drop: public.users__drt_swap" in result.stdout
        assert "Run with --execute to apply." in result.stdout
        assert result.exit_code == 0

    def test_clean_execute_drops_tables_for_current_sync_only(self):
        """Execute should drop only the current sync's orphan table."""
        runner = CliRunner()

        mock_sync = mock.Mock()
        mock_sync.name = "test_sync"
        mock_sync.destination = mock.Mock(table="public.users")

        fake_dest = FakeOrphanCleanup(
            {"public.users": ["public.users__drt_swap"]}
        )

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync])
            mock_get_dest.return_value = fake_dest
            result = runner.invoke(app, ["clean", "--orphans", "--execute"])

        assert fake_dest.drop_calls == [("public.users", ["public.users__drt_swap"])]
        assert "Dropped: 1" in result.stdout
        assert result.exit_code == 0

    def test_clean_help_shows_options(self):
        """Help should document --orphans and --execute."""
        import re as _re
        runner = CliRunner()
        result = runner.invoke(app, ["clean", "--help"])
        assert result.exit_code == 0
        stdout = _re.sub(r"\x1b\[[0-9;]*m", "", result.stdout)
        assert "--orphans" in stdout
        assert "--execute" in stdout

    def test_clean_deduplicates_orphan_tables_across_syncs(self):
        """Duplicate orphan tables should be dropped only once."""
        runner = CliRunner()

        mock_sync1 = mock.Mock()
        mock_sync1.name = "sync1"
        mock_sync1.destination = mock.Mock(table="public.users")

        mock_sync2 = mock.Mock()
        mock_sync2.name = "sync2"
        mock_sync2.destination = mock.Mock(table="public.users")

        fake_dest = FakeOrphanCleanup(
            {"public.users": ["public.users__drt_swap"]}
        )

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync1, mock_sync2])
            mock_get_dest.side_effect = [fake_dest, fake_dest]
            result = runner.invoke(app, ["clean", "--orphans"])

        assert fake_dest.list_calls == [
            ("public.users", "public.users", None),
            ("public.users", "public.users", None),
        ]
        assert "Found 1 orphan swap table(s)." in result.stdout

    def test_clean_no_orphans_found_reports_success(self):
        """No matching orphans should produce the empty-state message."""
        runner = CliRunner()

        mock_sync = mock.Mock()
        mock_sync.name = "test_sync"
        mock_sync.destination = mock.Mock(table="public.users")

        fake_dest = FakeOrphanCleanup({"public.users": []})

        with mock.patch("drt.config.parser.load_syncs_safe") as mock_load, \
             mock.patch("drt.cli.main._get_destination") as mock_get_dest:
            mock_load.return_value = mock.Mock(syncs=[mock_sync])
            mock_get_dest.return_value = fake_dest
            result = runner.invoke(app, ["clean", "--orphans"])

        assert "No orphan swap tables found." in result.stdout
        assert fake_dest.drop_calls == []
