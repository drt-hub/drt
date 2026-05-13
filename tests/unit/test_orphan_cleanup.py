"""Unit tests for orphan swap table cleanup feature (#447)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest import mock

from drt.config.models import PostgresDestinationConfig
from drt.destinations.postgres import PostgresDestination


def _config(**overrides: Any) -> PostgresDestinationConfig:
    """Create a test PostgresDestinationConfig."""
    defaults: dict[str, Any] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "public.test_table",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return PostgresDestinationConfig(**defaults)


class TestListOrphanSwapTables:
    """Test orphan swap table discovery via PostgreSQL."""

    def test_list_orphan_tables_returns_only_matching_shadow_name(self):
        """Should return only the current sync's shadow table."""
        dest = PostgresDestination()
        config = _config(table="public.users")
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [
            ("public", "users__drt_swap"),
            ("public", "orders__drt_swap"),
        ]

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            result = dest.list_orphan_swap_tables(config, "public.users")

        assert result == ["public.users__drt_swap"]

        sql, params = mock_cur.execute.call_args.args
        assert "table_name = %s" in sql
        assert params == ("users__drt_swap", "public")

    def test_list_orphan_tables_filters_false_positive_rows(self):
        """Should defensively filter false positives returned by the catalog."""
        dest = PostgresDestination()
        config = _config(table="public.users")
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur
        # Database returns some false positives from LIKE wildcard
        mock_cur.fetchall.return_value = [
            ("public", "users__drt_swap"),
            ("public", "test_drt_swap"),  # Should be filtered by Python
            ("public", "orders__drt_swap"),
        ]

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            result = dest.list_orphan_swap_tables(config, "public.users")

        assert result == ["public.users__drt_swap"]

    def test_list_orphan_tables_logs_older_than_as_best_effort(self):
        """Should log that older_than is best-effort for Postgres."""
        dest = PostgresDestination()
        config = _config(table="public.users")
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = []

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            with mock.patch("logging.getLogger") as mock_logger:
                dest.list_orphan_swap_tables(
                    config,
                    "public.users",
                    older_than=timedelta(hours=24),
                )
                mock_logger.return_value.info.assert_called_once()
                call_msg = mock_logger.return_value.info.call_args[0][0]
                assert "older_than" in call_msg.lower()


class TestDropOrphanSwapTables:
    """Test orphan swap table deletion with transaction safety."""

    def test_drop_orphan_tables_drops_valid_entries(self):
        """Should successfully drop valid schema.table entries."""
        dest = PostgresDestination()
        config = _config()
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            dropped, failed = dest.drop_orphan_swap_tables(
                config, ["public.users__drt_swap", "public.orders__drt_swap"]
            )

        assert dropped == ["public.users__drt_swap", "public.orders__drt_swap"]
        assert failed == []
        assert mock_conn.commit.call_count == 2

    def test_drop_orphan_tables_commits_each_table_independently(self):
        """Should commit each successful drop independently."""
        dest = PostgresDestination()
        config = _config()
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur

        # First DROP succeeds, second fails
        def execute_side_effect(*args, **kwargs):
            if mock_cur.execute.call_count == 1:
                return
            raise Exception("DROP TABLE failed")

        mock_cur.execute.side_effect = execute_side_effect

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            dropped, failed = dest.drop_orphan_swap_tables(
                config, ["public.users__drt_swap", "public.orders__drt_swap"]
            )

        assert dropped == ["public.users__drt_swap"]
        assert failed == ["public.orders__drt_swap"]
        assert mock_conn.commit.call_count == 1
        assert mock_conn.rollback.call_count == 1

    def test_drop_orphan_tables_rejects_malformed_entries(self):
        """Should reject entries without schema.table format."""
        dest = PostgresDestination()
        config = _config()
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            dropped, failed = dest.drop_orphan_swap_tables(
                config,
                [
                    "public.users__drt_swap",
                    "no_schema",
                    "public.orders__drt_swap",
                    "",
                ],
            )

        assert dropped == ["public.users__drt_swap", "public.orders__drt_swap"]
        assert set(failed) == {"no_schema", ""}
        assert mock_cur.execute.call_count == 2

    def test_drop_orphan_tables_rejects_non_swap_tables(self):
        """Should reject tables not ending with __drt_swap."""
        dest = PostgresDestination()
        config = _config()
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            dropped, failed = dest.drop_orphan_swap_tables(
                config,
                [
                    "public.users__drt_swap",
                    "public.regular_table",
                    "public.orders__drt_swap",
                ],
            )

        assert dropped == ["public.users__drt_swap", "public.orders__drt_swap"]
        assert failed == ["public.regular_table"]
        assert mock_cur.execute.call_count == 2

    def test_drop_orphan_tables_closes_connection_on_error(self):
        """Should close connection even if drop fails."""
        dest = PostgresDestination()
        config = _config()
        mock_conn = mock.Mock()
        mock_cur = mock.Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.execute.side_effect = Exception("DROP failed")

        with mock.patch.object(dest, "_connect", return_value=mock_conn):
            dest.drop_orphan_swap_tables(config, ["public.users__drt_swap"])

        mock_conn.close.assert_called_once()
