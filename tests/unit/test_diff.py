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
from unittest.mock import MagicMock, patch

from drt.config.models import (
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SlackDestinationConfig,
    SyncOptions,
)
from drt.destinations._mirror_state import key_hash, key_json
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
        # The rows disappear because the table is rebuilt (#693, Task B2)
        assert result.delete_reason == "replace"

    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_deleted_hidden_when_mode_is_full(self, mock_fetch_keys: Any) -> None:
        """In full (upsert) mode, 'deleted' has no semantic — must be empty.

        Full mode (#470) uses the keyed fetch, which by design only returns
        dest rows whose key is in the source set — so the id=99 dest-only row
        is never seen. ``deleted`` stays empty regardless.
        """
        mock_fetch_keys.return_value = [
            {"id": 1, "score": 0.5},
        ]
        records = [{"id": 1, "score": 0.95}]

        result = compute_diff(records, _pg_config(), _options("full"), limit=20)

        # Deleted is suppressed for non-replace mode
        assert result.deleted == []
        # Nothing is dropped, so there is no delete story to tell (#693 B2)
        assert result.delete_reason is None

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
# Keyed fetch (#470): compute_diff batches destination lookup by source PKs
# ---------------------------------------------------------------------------


class TestComputeDiffKeyedFetch:
    @patch("drt.engine.diff.fetch_rows")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_compute_diff_uses_keyed_fetch_for_upsert_mode(
        self, mock_fetch_keys: Any, mock_fetch: Any
    ) -> None:
        """Upsert (non-replace) mode + upsert_key -> keyed fetch, not SELECT *.

        added/updated must match the full-scan behaviour for the same rows.
        """
        mock_fetch_keys.return_value = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        records = [
            {"id": 1, "score": 0.95},  # update
            {"id": 2, "score": 0.9},  # unchanged
            {"id": 3, "score": 0.7},  # add
        ]

        result = compute_diff(records, _pg_config(), _options("full"), limit=20)

        # keyed fetch used; full-table SELECT * never issued
        assert mock_fetch_keys.called
        assert not mock_fetch.called

        # fetch bounded to the source key set (order-insensitive)
        call = mock_fetch_keys.call_args
        assert call.args[1] == ["id"]  # key_cols
        assert set(call.args[2]) == {(1,), (2,), (3,)}  # source key tuples
        assert call.kwargs["columns"] == ["id", "score"]

        assert len(result.added) == 1
        assert result.added[0]["id"] == 3
        assert len(result.updated) == 1
        old, new = result.updated[0]
        assert old["score"] == 0.5 and new["score"] == 0.95
        assert result.deleted == []

    @patch("drt.engine.diff.fetch_rows")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_compute_diff_replace_mode_still_full_scans(
        self, mock_fetch_keys: Any, mock_fetch: Any
    ) -> None:
        """Replace mode must NOT use keyed fetch — deleted set must survive."""
        mock_fetch.return_value = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
            {"id": 3, "score": 0.7},  # not in source -> deleted
        ]
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.9}]

        result = compute_diff(records, _pg_config(), _options("replace"), limit=20)

        # full scan used; keyed fetch never issued in replace mode
        assert mock_fetch.called
        assert not mock_fetch_keys.called

        assert len(result.deleted) == 1
        assert result.deleted[0]["id"] == 3
        assert len(result.updated) == 1

    @patch("drt.engine.diff.fetch_rows")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_compute_diff_clickhouse_falls_back_to_full_scan(
        self, mock_fetch_keys: Any, mock_fetch: Any
    ) -> None:
        """NotImplementedError (ClickHouse) -> fall back to full SELECT * scan."""
        mock_fetch_keys.side_effect = NotImplementedError("ClickHouse unsupported")
        mock_fetch.return_value = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        records = [
            {"id": 1, "score": 0.95},  # update
            {"id": 3, "score": 0.7},  # add
        ]

        result = compute_diff(records, _pg_config(), _options("full"), limit=20)

        # keyed fetch attempted, then full scan used as the fallback
        assert mock_fetch_keys.called
        assert mock_fetch.called

        assert len(result.added) == 1
        assert result.added[0]["id"] == 3
        assert len(result.updated) == 1
        assert result.deleted == []

    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_compute_diff_keyed_fetch_query_failure_falls_back_to_sample(
        self, mock_fetch_keys: Any
    ) -> None:
        """A non-NotImplementedError from the keyed fetch → sample fallback.

        The query-failure fallback (shared with the full-scan path) must still
        fire when the keyed fetch itself errors.
        """
        mock_fetch_keys.side_effect = RuntimeError("connection refused")
        records = [{"id": 1, "score": 0.95}]

        result = compute_diff(records, _pg_config(), _options("full"), limit=20)

        assert not result.supported
        assert result.fallback_reason is not None
        assert "Could not query destination" in result.fallback_reason
        assert result.sample == records


# ---------------------------------------------------------------------------
# Mirror tracked-strategy delete preview (#693, Task B1)
# ---------------------------------------------------------------------------


def _mirror_tracked_options(sync_name: str | None = "users_sync") -> SyncOptions:
    options = SyncOptions(mode="mirror", mirror={"strategy": "tracked"})  # type: ignore[arg-type]
    if sync_name is not None:
        options._sync_name = sync_name
    return options


class TestComputeDiffMirrorTracked:
    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_previews_tracked_mirror_deletes(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """previous={a,b,c}, source={a,b} → deleted previews {c}, read-only.

        Mirror mode takes the keyed-fetch path (#470), which structurally can
        never see dest-only rows — so the delete preview must come from the
        tracked state table, not from ``dest_rows``.
        """
        mock_fetch_keys.return_value = [{"id": "a", "score": 0.5}]
        mock_state.return_value = {
            key_hash(("a",)): key_json(("a",)),
            key_hash(("b",)): key_json(("b",)),
            key_hash(("c",)): key_json(("c",)),
        }
        records = [{"id": "a", "score": 0.9}, {"id": "b", "score": 0.8}]

        result = compute_diff(
            records, _pg_config(), _mirror_tracked_options(), limit=20
        )

        assert result.supported
        assert result.deleted == [{"id": "c"}]
        # These rows are DELETEd by statement, not lost to a rebuild (#693 B2)
        assert result.delete_reason == "mirror"
        # sync_name derivation: SyncOptions._sync_name, falling back to table
        assert mock_state.call_args.args[1] == "users_sync"

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_tracked_preview_is_read_only(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """The preview must never write: only SELECTs reach the cursor.

        ``fetch_tracked_state`` is exercised for real against a fake cursor so
        the read-only guarantee is proved end-to-end, not just at the seam.
        """
        from drt.destinations.query import fetch_tracked_state as real_fetch

        cursor = MagicMock()
        cursor.fetchone.return_value = ("public._drt_synced_keys",)
        cursor.fetchall.return_value = [(key_hash(("c",)), key_json(("c",)))]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        mock_fetch_keys.return_value = []
        mock_state.side_effect = real_fetch
        records = [{"id": "a"}]

        with patch(
            "drt.destinations.postgres.PostgresDestination._connect",
            return_value=conn,
        ):
            result = compute_diff(
                records,
                _pg_config(table="public.users"),
                _mirror_tracked_options(),
                limit=20,
            )

        assert result.deleted == [{"id": "c"}]
        assert cursor.execute.call_count == 2  # existence probe + state SELECT
        for call in cursor.execute.call_args_list:
            stmt = call[0][0]
            text = str(stmt.seq) if hasattr(stmt, "seq") else str(stmt)
            upper = text.upper()
            for kw in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TRUNCATE"):
                assert kw not in upper, f"write keyword {kw} in {text}"
        cursor.executemany.assert_not_called()
        conn.commit.assert_not_called()

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_tracked_preview_empty_when_state_absent(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """No state table (first run) → baseline semantics, nothing deleted."""
        mock_fetch_keys.return_value = []
        mock_state.return_value = {}
        records = [{"id": "a"}]

        result = compute_diff(
            records, _pg_config(), _mirror_tracked_options(), limit=20
        )

        assert result.deleted == []
        assert result.supported

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_tracked_preview_swallows_state_read_failure(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """A failing state read must not break the whole diff — deleted = []."""
        mock_fetch_keys.return_value = [{"id": "a", "score": 0.5}]
        mock_state.side_effect = RuntimeError("permission denied for _drt_synced_keys")
        records = [{"id": "a", "score": 0.9}]

        result = compute_diff(
            records, _pg_config(), _mirror_tracked_options(), limit=20
        )

        assert result.supported  # diff itself still usable
        assert result.deleted == []
        assert len(result.updated) == 1

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_tracked_preview_sync_name_falls_back_to_table(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """Same derivation as ``_finalize_mirror_tracked``: ``_sync_name or table``."""
        mock_fetch_keys.return_value = []
        mock_state.return_value = {}

        compute_diff(
            [{"id": "a"}],
            _pg_config(table="public.users"),
            _mirror_tracked_options(sync_name=None),
            limit=20,
        )

        assert mock_state.call_args.args[1] == "public.users"

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_destination_strategy_mirror_not_previewed(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """Only the tracked strategy is previewed in B1 (destination = B4)."""
        mock_fetch_keys.return_value = []
        options = SyncOptions(mode="mirror")  # type: ignore[arg-type]

        result = compute_diff([{"id": "a"}], _pg_config(), options, limit=20)

        assert result.deleted == []
        mock_state.assert_not_called()

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_tracked_preview_composite_key_and_truncation(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """Composite keys map back to columns; over-limit sets ``truncated``."""
        mock_fetch_keys.return_value = []
        previous = {}
        for i in range(3):
            key = ("c1", f"u{i}")
            previous[key_hash(key)] = key_json(key)
        mock_state.return_value = previous

        result = compute_diff(
            [{"company_id": "c1", "user_id": "u0"}],
            _pg_config(upsert_key=["company_id", "user_id"]),
            _mirror_tracked_options(),
            limit=1,
        )

        assert result.truncated is True
        assert len(result.deleted) == 1
        assert set(result.deleted[0]) == {"company_id", "user_id"}
        assert result.deleted[0]["company_id"] == "c1"

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows")
    def test_tracked_preview_previews_full_wipe_on_empty_source(
        self, mock_fetch: Any, mock_state: Any
    ) -> None:
        """Empty source → every tracked key is a delete candidate.

        With no records the engine takes the full-scan path (``use_keyed_fetch``
        needs ``records``), but the tracked preview is independent of that read.
        """
        mock_fetch.return_value = []
        mock_state.return_value = {key_hash(("a",)): key_json(("a",))}

        result = compute_diff([], _pg_config(), _mirror_tracked_options(), limit=20)

        assert result.deleted == [{"id": "a"}]


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
