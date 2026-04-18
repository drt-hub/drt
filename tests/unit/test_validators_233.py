"""Tests for freshness, unique, and accepted_values test validators."""

from __future__ import annotations

from datetime import timedelta

import pytest

from drt.config.models import AcceptedValuesTest, FreshnessTest, SyncTest, UniqueTest
from drt.engine.test_runner import _parse_max_age, build_test_query


class TestParseMaxAge:
    """Test max_age string parsing."""

    def test_parse_days(self) -> None:
        """Parse '7 days' correctly."""
        result = _parse_max_age("7 days")
        assert result == timedelta(days=7)

    def test_parse_hours(self) -> None:
        """Parse '1 hour' correctly."""
        result = _parse_max_age("1 hour")
        assert result == timedelta(hours=1)

    def test_parse_minutes(self) -> None:
        """Parse '30 minutes' correctly."""
        result = _parse_max_age("30 minutes")
        assert result == timedelta(minutes=30)

    def test_parse_seconds(self) -> None:
        """Parse '3600 seconds' correctly."""
        result = _parse_max_age("3600 seconds")
        assert result == timedelta(seconds=3600)

    def test_parse_weeks(self) -> None:
        """Parse '2 weeks' correctly."""
        result = _parse_max_age("2 weeks")
        assert result == timedelta(weeks=2)

    def test_parse_singular_forms(self) -> None:
        """Parse singular forms like '1 day'."""
        assert _parse_max_age("1 day") == timedelta(days=1)
        assert _parse_max_age("1 hour") == timedelta(hours=1)
        assert _parse_max_age("1 minute") == timedelta(minutes=1)
        assert _parse_max_age("1 second") == timedelta(seconds=1)
        assert _parse_max_age("1 week") == timedelta(weeks=1)

    def test_parse_invalid_format(self) -> None:
        """Raise error for invalid format."""
        with pytest.raises(ValueError, match="Invalid max_age format"):
            _parse_max_age("7")
        with pytest.raises(ValueError, match="Invalid max_age format"):
            _parse_max_age("7 days 3 hours")

    def test_parse_invalid_value(self) -> None:
        """Raise error for non-integer value."""
        with pytest.raises(ValueError, match="Invalid max_age value"):
            _parse_max_age("abc days")

    def test_parse_unknown_unit(self) -> None:
        """Raise error for unknown time unit."""
        with pytest.raises(ValueError, match="Unknown time unit"):
            _parse_max_age("7 months")


class TestFreshnessTest:
    """Test freshness validator query generation."""

    def test_freshness_query_generation(self) -> None:
        """Freshness test should generate correct SQL."""
        test = SyncTest(freshness=FreshnessTest(column="updated_at", max_age="7 days"))
        query, check_func = build_test_query(test, "users")
        
        assert "SELECT COUNT(*)" in query
        assert "updated_at" in query
        assert "users" in query
        # Query should use < operator (older than)
        assert "<" in query

    def test_freshness_check_passes_on_zero_stale(self) -> None:
        """Freshness check passes when no stale rows."""
        test = SyncTest(freshness=FreshnessTest(column="updated_at", max_age="7 days"))
        _, check_func = build_test_query(test, "users")
        
        # 0 stale rows = all data is fresh
        assert check_func(0) is True

    def test_freshness_check_fails_on_stale_data(self) -> None:
        """Freshness check fails when stale rows exist."""
        test = SyncTest(freshness=FreshnessTest(column="updated_at", max_age="7 days"))
        _, check_func = build_test_query(test, "users")
        
        # Any stale rows means test fails
        assert check_func(1) is False
        assert check_func(100) is False

    def test_freshness_with_hours(self) -> None:
        """Freshness test with hour-based max_age."""
        test = SyncTest(freshness=FreshnessTest(column="created_at", max_age="24 hours"))
        query, _ = build_test_query(test, "events")
        
        assert "created_at" in query
        assert "events" in query


class TestUniqueTest:
    """Test unique validator query generation."""

    def test_unique_single_column_query(self) -> None:
        """Unique test with single column."""
        test = SyncTest(unique=UniqueTest(columns=["id"]))
        query, _ = build_test_query(test, "products")
        
        assert "SELECT" in query
        assert "COUNT(*) - COUNT(DISTINCT id)" in query
        assert "products" in query

    def test_unique_multiple_columns_query(self) -> None:
        """Unique test with multiple columns."""
        test = SyncTest(unique=UniqueTest(columns=["tenant_id", "user_id"]))
        query, _ = build_test_query(test, "subscriptions")
        
        assert "DISTINCT tenant_id, user_id" in query
        assert "subscriptions" in query

    def test_unique_check_passes_on_no_duplicates(self) -> None:
        """Unique check passes when duplicate count is 0."""
        test = SyncTest(unique=UniqueTest(columns=["id"]))
        _, check_func = build_test_query(test, "users")
        
        assert check_func(0) is True

    def test_unique_check_fails_on_duplicates(self) -> None:
        """Unique check fails when duplicates exist."""
        test = SyncTest(unique=UniqueTest(columns=["id"]))
        _, check_func = build_test_query(test, "users")
        
        assert check_func(1) is False
        assert check_func(10) is False


class TestAcceptedValuesTest:
    """Test accepted_values validator query generation."""

    def test_accepted_values_query_generation(self) -> None:
        """Accepted values test should generate correct SQL."""
        test = SyncTest(
            accepted_values=AcceptedValuesTest(
                column="status",
                values=["active", "inactive", "pending"]
            )
        )
        query, _ = build_test_query(test, "users")
        
        assert "SELECT COUNT(*)" in query
        assert "status NOT IN" in query
        assert "'active'" in query
        assert "'inactive'" in query
        assert "'pending'" in query
        assert "users" in query

    def test_accepted_values_check_passes_on_no_invalid(self) -> None:
        """Accepted values check passes when no invalid values."""
        test = SyncTest(
            accepted_values=AcceptedValuesTest(
                column="status",
                values=["active", "inactive"]
            )
        )
        _, check_func = build_test_query(test, "users")
        
        # 0 invalid rows = all values are accepted
        assert check_func(0) is True

    def test_accepted_values_check_fails_on_invalid(self) -> None:
        """Accepted values check fails when invalid values exist."""
        test = SyncTest(
            accepted_values=AcceptedValuesTest(
                column="status",
                values=["active", "inactive"]
            )
        )
        _, check_func = build_test_query(test, "users")
        
        # Any invalid rows means test fails
        assert check_func(1) is False
        assert check_func(5) is False

    def test_accepted_values_with_single_value(self) -> None:
        """Accepted values test with single allowed value."""
        test = SyncTest(
            accepted_values=AcceptedValuesTest(column="type", values=["premium"])
        )
        query, _ = build_test_query(test, "subscriptions")
        
        assert "'premium'" in query
        assert "NOT IN" in query


class TestMultipleTestTypes:
    """Test that only one test type should be used at a time."""

    def test_row_count_takes_precedence(self) -> None:
        """If row_count is set, it takes precedence."""
        test = SyncTest(
            row_count={"min": 1, "max": 100},
            unique=UniqueTest(columns=["id"])
        )
        query, _ = build_test_query(test, "users")
        
        # Should be row count query, not unique query
        assert "COUNT(*)" in query
        assert "DISTINCT" not in query

    def test_not_null_takes_second_precedence(self) -> None:
        """If row_count not set, not_null takes precedence."""
        test = SyncTest(
            not_null={"columns": ["email"]},
            freshness=FreshnessTest(column="updated_at", max_age="1 day")
        )
        query, _ = build_test_query(test, "users")
        
        # Should be not_null query
        assert "IS NULL" in query
        assert "updated_at" not in query

    def test_freshness_takes_third_precedence(self) -> None:
        """If row_count/not_null not set, freshness takes precedence."""
        test = SyncTest(
            freshness=FreshnessTest(column="updated_at", max_age="1 day"),
            unique=UniqueTest(columns=["id"])
        )
        query, _ = build_test_query(test, "users")
        
        # Should be freshness query
        assert "updated_at" in query
        assert "DISTINCT" not in query


class TestInvalidTableNames:
    """Test SQL injection prevention."""

    def test_invalid_table_names_rejected(self) -> None:
        """Invalid characters in table names should raise error."""
        test = SyncTest(row_count={"min": 1})
        
        with pytest.raises(ValueError, match="Invalid character"):
            build_test_query(test, "users; DROP TABLE--")

    def test_invalid_column_names_rejected(self) -> None:
        """Invalid characters in column names should raise error."""
        test = SyncTest(freshness=FreshnessTest(column="col; DROP--", max_age="1 day"))
        
        with pytest.raises(ValueError, match="Invalid character"):
            build_test_query(test, "users")
