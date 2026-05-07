"""Unit tests for the diff engine (#413).

Covers compute_diff() across:
- Queryable destinations (true add/update/delete diff)
- Non-queryable destinations (sample-only fallback)
- Mode-specific behavior (deleted only relevant for replace)
- Limit application (truncation)
- Field-level change detection in updated rows
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from drt.config.models import (
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SlackDestinationConfig,
    SyncOptions,
)
from drt.engine.diff import DiffResult, compute_diff


def _pg_config(
    table: str = "users", upsert_key: list[str] | None = None
) -> PostgresDestinationConfig:
    return PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        user="test",
        password="test",
        table=table,
        upsert_key=upsert_key or ["id"],
    )


def _options(mode: str = "full") -> SyncOptions:
    return SyncOptions(mode=mode)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Non-queryable destinations: sample-mode fallback
# ---------------------------------------------------------------------------


class TestComputeDiffSampleMode:
    def test_rest_api_returns_sample(self) -> None:
        config = RestApiDestinationConfig(type="rest_api", url="https://x", method="POST")
        records = [{"id": i, "name": f"u{i}"} for i in range(50)]

        result = compute_diff(records, config, _options(), limit=10)

        assert not result.supported
        assert result.fallback_reason
        reason = result.fallback_reason.lower()
        assert "rest_api" in reason or "comparison" in reason
        assert len(result.sample) == 10
        assert result.sample[0] == {"id": 0, "name": "u0"}
        assert result.total_source_rows == 50
        assert result.added == [] and result.updated == [] and result.deleted == []

    def test_slack_returns_sample(self) -> None:
        config = SlackDestinationConfig(
            type="slack", webhook_url="https://hook.slack.com/x", message_template="{{ row.msg }}"
        )
        records = [{"msg": f"alert {i}"} for i in range(5)]

        result = compute_diff(records, config, _options(), limit=20)

        assert not result.supported
        assert len(result.sample) == 5
        assert result.total_source_rows == 5

    def test_sample_truncated_when_records_exceed_limit(self) -> None:
        config = RestApiDestinationConfig(type="rest_api", url="https://x", method="POST")
        records = [{"id": i} for i in range(100)]

        result = compute_diff(records, config, _options(), limit=20)

        assert len(result.sample) == 20
        assert result.truncated is True
        assert result.total_source_rows == 100


# ---------------------------------------------------------------------------
# Queryable destinations: true diff
# ---------------------------------------------------------------------------


class TestComputeDiffQueryable:
    @patch("drt.engine.diff.fetch_rows")
    def test_added_only(self, mock_fetch: Any) -> None:
        """Source has rows that destination doesn't — all added."""
        mock_fetch.return_value = []  # destination empty
        records = [{"id": 1, "score": 0.9}, {"id": 2, "score": 0.8}]

        result = compute_diff(records, _pg_config(), _options("replace"), limit=20)

        assert result.supported
        assert len(result.added) == 2
        assert result.added[0] == {"id": 1, "score": 0.9}
        assert result.updated == []
        assert result.deleted == []
        assert result.total_destination_rows == 0

    @patch("drt.engine.diff.fetch_rows")
    def test_updated_with_field_level_diff(self, mock_fetch: Any) -> None:
        """Same key, different values — captured as updated with old + new."""
        mock_fetch.return_value = [
            {"id": 1, "score": 0.5, "name": "Alice"},
            {"id": 2, "score": 0.9, "name": "Bob"},
        ]
        records = [
            {"id": 1, "score": 0.95, "name": "Alice"},  # score changed
            {"id": 2, "score": 0.9, "name": "Bob"},  # unchanged → not updated
        ]

        result = compute_diff(records, _pg_config(), _options("replace"), limit=20)

        assert len(result.updated) == 1
        old, new = result.updated[0]
        assert old["score"] == 0.5
        assert new["score"] == 0.95
        assert result.added == []
        assert result.deleted == []

    @patch("drt.engine.diff.fetch_rows")
    def test_deleted_when_mode_is_replace(self, mock_fetch: Any) -> None:
        """In replace mode, destination rows missing from source are deleted."""
        mock_fetch.return_value = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
            {"id": 3, "score": 0.7},  # not in source
        ]
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.9}]

        result = compute_diff(records, _pg_config(), _options("replace"), limit=20)

        assert len(result.deleted) == 1
        assert result.deleted[0]["id"] == 3
        assert len(result.updated) == 1  # id=1 score changed

    @patch("drt.engine.diff.fetch_rows")
    def test_deleted_hidden_when_mode_is_full(self, mock_fetch: Any) -> None:
        """In full (upsert) mode, 'deleted' has no semantic — must be empty."""
        mock_fetch.return_value = [
            {"id": 1, "score": 0.5},
            {"id": 99, "score": 0.7},  # not in source
        ]
        records = [{"id": 1, "score": 0.95}]

        result = compute_diff(records, _pg_config(), _options("full"), limit=20)

        # Deleted is suppressed for non-replace mode
        assert result.deleted == []

    @patch("drt.engine.diff.fetch_rows")
    def test_composite_key(self, mock_fetch: Any) -> None:
        """Composite upsert_key: tuple matching across columns."""
        mock_fetch.return_value = [
            {"company_id": "c1", "user_id": "u1", "score": 0.5},
        ]
        records = [
            {"company_id": "c1", "user_id": "u1", "score": 0.99},  # update
            {"company_id": "c2", "user_id": "u2", "score": 0.7},  # add
        ]

        result = compute_diff(
            records,
            _pg_config(upsert_key=["company_id", "user_id"]),
            _options("replace"),
            limit=20,
        )

        assert len(result.added) == 1
        assert result.added[0]["company_id"] == "c2"
        assert len(result.updated) == 1

    @patch("drt.engine.diff.fetch_rows")
    def test_truncation_with_added_exceeding_limit(self, mock_fetch: Any) -> None:
        mock_fetch.return_value = []
        records = [{"id": i, "score": 0.5} for i in range(30)]

        result = compute_diff(records, _pg_config(), _options("replace"), limit=10)

        assert len(result.added) == 10
        assert result.truncated is True

    @patch("drt.engine.diff.fetch_rows")
    def test_no_changes(self, mock_fetch: Any) -> None:
        mock_fetch.return_value = [{"id": 1, "score": 0.5}]
        records = [{"id": 1, "score": 0.5}]

        result = compute_diff(records, _pg_config(), _options("replace"), limit=20)

        assert result.added == []
        assert result.updated == []
        assert result.deleted == []
        assert result.total_source_rows == 1
        assert result.total_destination_rows == 1


# ---------------------------------------------------------------------------
# DiffResult helpers
# ---------------------------------------------------------------------------


class TestDiffResult:
    def test_changed_fields_helper(self) -> None:
        """DiffResult.changed_fields returns dict of {col: (old, new)} per updated row."""
        old = {"id": 1, "score": 0.5, "name": "Alice"}
        new = {"id": 1, "score": 0.95, "name": "Alice"}

        changed = DiffResult.changed_fields(old, new)

        assert changed == {"score": (0.5, 0.95)}

    def test_changed_fields_multiple(self) -> None:
        old = {"id": 1, "score": 0.5, "name": "Alice"}
        new = {"id": 1, "score": 0.95, "name": "Allison"}

        changed = DiffResult.changed_fields(old, new)

        assert changed == {"score": (0.5, 0.95), "name": ("Alice", "Allison")}

    def test_changed_fields_with_dict_value(self) -> None:
        """dict / list values compare by equality (order-insensitive for dict)."""
        old = {"id": 1, "metadata": {"a": 1}}
        new = {"id": 1, "metadata": {"a": 1}}

        changed = DiffResult.changed_fields(old, new)

        assert changed == {}
