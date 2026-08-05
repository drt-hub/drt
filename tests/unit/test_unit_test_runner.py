"""Unit tests for the offline transform-pipeline test runner (#780)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from drt.config.models import SyncConfig, UnitTest
from drt.engine.unit_test_runner import (
    CaptureDestination,
    UnitTestLookupsUnsupportedError,
    run_unit_test,
)


def _sync(**sync_overrides: Any) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "users_to_hubspot",
            "model": "ref('users')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "full", **sync_overrides},
        }
    )


def _postgres_sync(**destination_overrides: Any) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "postgres_sync",
            "model": "ref('t')",
            "destination": {
                "type": "postgres",
                "host_env": "H",
                "dbname_env": "D",
                "user_env": "U",
                "password_env": "P",
                "table": "t",
                "upsert_key": ["id"],
                **destination_overrides,
            },
        }
    )


class TestUnitTestConfig:
    def test_rejects_empty_given(self) -> None:
        """An empty `given` would make the test vacuously pass forever —
        catch it at config load, not by writing a fixture no one reviews."""
        with pytest.raises(ValidationError, match="given"):
            UnitTest(name="x", given=[], expect=[{"id": 1}])

    def test_rejects_empty_expect(self) -> None:
        with pytest.raises(ValidationError, match="expect"):
            UnitTest(name="x", given=[{"id": 1}], expect=[])

    def test_unit_tests_default_to_empty_on_sync_config(self) -> None:
        sync = _sync()
        assert sync.unit_tests == []

    def test_sync_config_accepts_a_unit_tests_block(self) -> None:
        sync = SyncConfig.model_validate(
            {
                "name": "s",
                "model": "ref('t')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "unit_tests": [
                    {"name": "t1", "given": [{"id": 1}], "expect": [{"id": 1}]},
                ],
            }
        )
        assert len(sync.unit_tests) == 1
        assert sync.unit_tests[0].name == "t1"


class TestGivenExpect:
    def test_a_passing_row_through_the_real_pipeline(self) -> None:
        """field_mappings + mask both run — this is the real production
        pipeline, not a reimplementation of it."""
        sync = _sync(field_mappings={"first": "given_name"}, mask={"email": "hash"})
        test = UnitTest(
            name="masks_and_renames",
            given=[{"id": 1, "email": "alice@example.com", "first": "Alice", "last": "Doe"}],
            expect=[{"id": 1, "given_name": "Alice", "last": "Doe"}],
        )

        result = run_unit_test(sync, test)

        assert result.passed
        assert result.mismatches == []
        assert result.actual[0]["given_name"] == "Alice"
        assert result.actual[0]["email"] != "alice@example.com"  # masked, not the raw value

    def test_expect_is_a_subset_match_not_exact(self) -> None:
        """An unrelated source column growing later must not break existing
        unit tests — only the keys `expect` declares are checked."""
        sync = _sync()
        test = UnitTest(
            name="only_checks_id",
            given=[{"id": 1, "unrelated_new_column": "whatever"}],
            expect=[{"id": 1}],
        )

        assert run_unit_test(sync, test).passed

    def test_wrong_value_is_a_mismatch(self) -> None:
        sync = _sync()
        test = UnitTest(name="wrong", given=[{"id": 1}], expect=[{"id": 2}])

        result = run_unit_test(sync, test)

        assert not result.passed
        assert len(result.mismatches) == 1
        assert "row 0" in result.mismatches[0]

    def test_row_count_mismatch_is_reported_not_silently_zipped(self) -> None:
        """zip() would silently truncate to the shorter list — a dropped row
        must surface as a failure, not vanish from the comparison."""
        sync = _sync(on_error="skip")
        test = UnitTest(
            name="drops_a_row",
            given=[{"id": 1}, {"id": 2}],
            expect=[{"id": 1}, {"id": 2}, {"id": 3}],
        )

        result = run_unit_test(sync, test)

        assert not result.passed
        assert "expected 3 row(s), got 2" in result.mismatches[0]

    def test_row_order_is_preserved_across_batches(self) -> None:
        """batch_size=1 forces three separate load() calls — CaptureDestination
        must still hand back rows in the order they were given."""
        sync = _sync(batch_size=1)
        test = UnitTest(
            name="order",
            given=[{"id": 1}, {"id": 2}, {"id": 3}],
            expect=[{"id": 1}, {"id": 2}, {"id": 3}],
        )

        result = run_unit_test(sync, test)

        assert result.passed
        assert [r["id"] for r in result.actual] == [1, 2, 3]


class TestLookupsRejected:
    def test_a_sync_with_lookups_is_rejected(self) -> None:
        sync = _postgres_sync(
            lookups={
                "account_id": {
                    "table": "accounts",
                    "match": {"email": "email"},
                    "select": "id",
                }
            }
        )
        test = UnitTest(name="x", given=[{"id": 1}], expect=[{"id": 1}])

        with pytest.raises(UnitTestLookupsUnsupportedError, match="lookups"):
            run_unit_test(sync, test)

    def test_a_sync_without_lookups_is_unaffected(self) -> None:
        sync = _postgres_sync()  # no lookups key at all
        test = UnitTest(name="x", given=[{"id": 1}], expect=[{"id": 1}])

        assert run_unit_test(sync, test).passed


class TestAlertsStripped:
    """run_sync() fires real HTTP alerts from its own finally block on any
    reported failure — not observer-mediated, so this can't rely on
    dry_run or a passed-in observer to suppress it."""

    def _sync_with_alerts(self) -> SyncConfig:
        return SyncConfig.model_validate(
            {
                "name": "z",
                "model": "ref('t')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"on_error": "skip"},
                "alerts": {
                    "on_failure": [{"type": "webhook", "url": "https://hooks.example/x"}]
                },
            }
        )

    def test_dispatch_alerts_is_never_called(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_dispatch = MagicMock()
        monkeypatch.setattr("drt.alerts.dispatch_alerts", mock_dispatch)
        sync = self._sync_with_alerts()

        run_unit_test(sync, UnitTest(name="ok", given=[{"id": 1}], expect=[{"id": 1}]))

        mock_dispatch.assert_not_called()

    def test_the_callers_sync_config_is_not_mutated(self) -> None:
        """model_copy(update=...) must not touch the caller's own object —
        a shared SyncConfig loaded once for `drt test --unit` still needs
        its real alerts config for whatever else uses it."""
        sync = self._sync_with_alerts()

        run_unit_test(sync, UnitTest(name="ok", given=[{"id": 1}], expect=[{"id": 1}]))

        assert sync.alerts is not None
        assert sync.alerts.on_failure[0].url == "https://hooks.example/x"


class TestExceptionsBecomeFailures:
    """The point of a unit test is to report a broken config safely — one
    bad fixture must not crash `drt test --unit` before later tests run."""

    def test_an_unsupported_match_policy_is_reported_not_raised(self) -> None:
        """CaptureDestination doesn't implement MatchPolicyCapable, so
        `match_policy: update_only` fails run_sync()'s fail-fast guard —
        exactly the kind of config error a unit test should catch safely."""
        sync = _sync(match_policy="update_only")
        test = UnitTest(name="unsupported_match_policy", given=[{"id": 1}], expect=[{"id": 1}])

        result = run_unit_test(sync, test)

        assert not result.passed
        assert len(result.mismatches) == 1
        assert "match_policy" in result.mismatches[0]


class TestCaptureDestination:
    def test_accumulates_across_multiple_load_calls(self) -> None:
        capture = CaptureDestination()
        result1 = capture.load([{"id": 1}], MagicMock(), MagicMock())
        result2 = capture.load([{"id": 2}, {"id": 3}], MagicMock(), MagicMock())

        assert capture.records == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert result1.success == 1
        assert result2.success == 2
