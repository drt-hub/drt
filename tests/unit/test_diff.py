"""Unit tests for the diff engine (#413).

Covers compute_diff() across:
- Queryable destinations (true add/update/delete diff)
- Non-queryable destinations (sample-only fallback)
- Mode-specific behavior (deleted only relevant for replace)
- Limit application (truncation)
- Field-level change detection in updated rows
"""

from __future__ import annotations

import importlib.util
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import (
    ClickHouseDestinationConfig,
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SlackDestinationConfig,
    SyncOptions,
)
from drt.destinations._mirror_state import key_hash, key_json
from drt.engine.diff import DiffResult, compute_diff


def _has_psycopg2() -> bool:
    try:
        return importlib.util.find_spec("psycopg2") is not None
    except (ImportError, ValueError):
        return False


# The two read-only proofs below deliberately run the *real* query helper
# (rather than a mock) to show no write statement is issued — which means they
# need the [postgres] extra. Every other case here mocks the fetch, so only
# these two are marked.
needs_psycopg2 = pytest.mark.skipif(
    not _has_psycopg2(), reason="requires drt-core[postgres]"
)



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


def _clickhouse_config(
    table: str = "users", upsert_key: list[str] | None = None
) -> ClickHouseDestinationConfig:
    return ClickHouseDestinationConfig(
        type="clickhouse",
        host="localhost",
        database="test",
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
    @needs_psycopg2
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
        assert result.delete_preview_unavailable_reason is None

    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_tracked_preview_surfaces_state_read_failure(
        self, mock_fetch_keys: Any, mock_state: Any
    ) -> None:
        """A failed state read keeps the diff but marks deletes as unknown."""
        mock_fetch_keys.return_value = [{"id": "a", "score": 0.5}]
        mock_state.side_effect = RuntimeError("permission denied for _drt_synced_keys")
        records = [{"id": "a", "score": 0.9}]

        result = compute_diff(
            records, _pg_config(), _mirror_tracked_options(), limit=20
        )

        assert result.supported  # diff itself still usable
        assert result.deleted == []
        assert len(result.updated) == 1
        assert result.delete_preview_unavailable_reason == (
            "RuntimeError: permission denied for _drt_synced_keys"
        )

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

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_tracked_state")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_destination_strategy_does_not_read_tracked_state(
        self, mock_fetch_keys: Any, mock_state: Any, mock_all_keys: Any
    ) -> None:
        """The destination strategy has no state table — don't go looking.

        Was ``test_destination_strategy_mirror_not_previewed`` in #833 (B1), when
        the destination strategy was deliberately left unpreviewed. B4 previews
        it from the destination's own key set, so what remains to guard is that
        the two strategies use *disjoint* reads.
        """
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = []
        options = SyncOptions(mode="mirror")  # type: ignore[arg-type]

        result = compute_diff([{"id": "a"}], _pg_config(), options, limit=20)

        assert result.deleted == []
        mock_state.assert_not_called()
        assert mock_all_keys.called

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
    def test_tracked_preview_deletes_nothing_on_empty_source(
        self, mock_fetch: Any, mock_state: Any
    ) -> None:
        """Empty source → no deletes previewed, matching what the run would do.

        ``BaseSqlDestination._finalize_mirror`` bails out with
        ``if not self._mirror_keys: return None`` *before* dispatching to the
        tracked strategy, so a transient empty source keeps the destination (and
        the tracked baseline) intact. Previewing a full wipe here would tell the
        operator the opposite of what actually happens.
        """
        mock_fetch.return_value = []
        mock_state.return_value = {key_hash(("a",)): key_json(("a",))}

        result = compute_diff([], _pg_config(), _mirror_tracked_options(), limit=20)

        assert result.deleted == []
        assert result.delete_reason is None


# ---------------------------------------------------------------------------
# Mirror destination-strategy + scoped delete preview (#693, Tasks B3/B4)
#
# The destination strategy DELETEs ``dest_keys - source_keys`` (the ``NOT IN``
# form of ``_build_mirror_delete``), so previewing it needs the destination's own
# key set — a read #470's keyed fetch structurally cannot supply. ``mirror.scope``
# (#687) is not a separate strategy but a narrowing of this same path.
# ---------------------------------------------------------------------------


def _mirror_destination_options(scope: list[str] | None = None) -> SyncOptions:
    mirror: dict[str, Any] = {"strategy": "destination"}
    if scope is not None:
        mirror["scope"] = scope
    return SyncOptions(mode="mirror", mirror=mirror)  # type: ignore[arg-type]


class TestComputeDiffMirrorDestination:
    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_previews_destination_mirror_deletes(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """dest={a,b,c}, source={a,b} → deleted previews {c}."""
        mock_fetch_keys.return_value = [{"id": "a", "score": 0.5}]
        mock_all_keys.return_value = [("a",), ("b",), ("c",)]
        records = [{"id": "a", "score": 0.9}, {"id": "b", "score": 0.8}]

        result = compute_diff(
            records, _pg_config(), _mirror_destination_options(), limit=20
        )

        assert result.supported
        assert result.deleted == [{"id": "c"}]
        # Distinct from tracked "mirror": this preview cost a destination read.
        assert result.delete_reason == "mirror_scan"

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_clickhouse_dest_and_source_keys_compare_by_string_form(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """Caught in review, #1060: ClickHouse's key SELECT wraps columns in
        toString() (matching its real DELETE's own comparison), so
        fetch_all_keys returns strings even when the source produced a
        different Python type (e.g. int) for the same logical value. The
        comparison must not misreport a live row as a preview deletion just
        because the two sides' native types differ, as long as their string
        forms match. ClickHouse-only -- see the sibling test below proving
        other dialects are NOT coerced this way.
        """
        mock_fetch_keys.return_value = []
        # Destination key "1" (string) is the same row as source key 1 (int).
        mock_all_keys.return_value = [("1",), ("2",)]
        records = [{"id": 1}]  # source has row 1; destination also has "1" and "2"

        result = compute_diff(
            records, _clickhouse_config(), _mirror_destination_options(), limit=20
        )

        # Only "2" (unseen by the source, in either type) previews as deleted.
        assert result.deleted == [{"id": "2"}]

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_non_clickhouse_keys_are_not_stringified(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """A second review pass caught the first version of the ClickHouse
        fix comparing every dialect by string form -- wrong, not just
        unnecessary: coercing to strings can turn two natively-equal values
        into a false mismatch (e.g. numeric types whose string forms differ)
        just as easily as it can fix one. This proves non-ClickHouse
        dialects still compare keys with plain native equality, unchanged
        from before #1060 -- Postgres' own driver already returns the same
        Python type the source produced, so no coercion was ever needed
        there."""
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = [(1,)]  # destination key: int 1
        records = [{"id": "1"}]  # source key: str "1" (mismatched type)

        result = compute_diff(
            records, _pg_config(), _mirror_destination_options(), limit=20
        )

        # If this were stringified (as the first version of the fix did for
        # every dialect), str(1) == "1" and this would wrongly show as
        # nothing to delete. Native comparison correctly treats int 1 and
        # str "1" as different keys, so 1 previews as deleted.
        assert result.deleted == [{"id": 1}]

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_omitted_strategy_defaults_to_destination(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """``mirror:`` absent entirely is the destination strategy (#340)."""
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = [("a",), ("b",)]

        result = compute_diff(
            [{"id": "a"}],
            _pg_config(),
            SyncOptions(mode="mirror"),  # type: ignore[arg-type]
            limit=20,
        )

        assert result.deleted == [{"id": "b"}]
        assert result.delete_reason == "mirror_scan"
        # No scope configured → no scope narrowing passed to the read.
        assert mock_all_keys.call_args.args[2] is None
        assert mock_all_keys.call_args.args[3] is None

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_key_read_is_bounded_to_upsert_key(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """The extra read pulls key columns only, for the configured key."""
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = []

        compute_diff(
            [{"company_id": "c1", "user_id": "u1"}],
            _pg_config(upsert_key=["company_id", "user_id"]),
            _mirror_destination_options(),
            limit=20,
        )

        assert mock_all_keys.call_args.args[1] == ["company_id", "user_id"]

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_composite_key_deletes_map_back_to_columns(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = [("c1", "u1"), ("c1", "u2")]

        result = compute_diff(
            [{"company_id": "c1", "user_id": "u1"}],
            _pg_config(upsert_key=["company_id", "user_id"]),
            _mirror_destination_options(),
            limit=20,
        )

        assert result.deleted == [{"company_id": "c1", "user_id": "u2"}]

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_scope_narrows_the_read_to_observed_scope_values(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """Scope values are re-derived from the source records, not from state.

        ``_mirror_scopes`` is accumulated during ``load()``, which a dry run never
        calls — so the preview must recompute the observed scope tuples the same
        way ``_accumulate_mirror_state`` does, and hand them to the read so the
        server-side filter matches ``_build_mirror_delete``'s scope clause.
        """
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = [("a",), ("b",)]
        records = [
            {"id": "a", "region": "eu"},
            {"id": "x", "region": "eu"},
        ]

        result = compute_diff(
            records, _pg_config(), _mirror_destination_options(scope=["region"]), limit=20
        )

        scope_cols = mock_all_keys.call_args.args[2]
        scopes = mock_all_keys.call_args.args[3]
        assert scope_cols == ["region"]
        assert scopes == [("eu",)]  # deduped, only observed values
        # 'b' is inside the scope the read returned, and unobserved → deleted.
        assert result.deleted == [{"id": "b"}]
        assert result.delete_reason == "mirror_scan"

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_composite_scope_tuples_are_deduped(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = []
        records = [
            {"id": "a", "region": "eu", "tier": "gold"},
            {"id": "b", "region": "eu", "tier": "gold"},
            {"id": "c", "region": "us", "tier": "silver"},
        ]

        compute_diff(
            records,
            _pg_config(),
            _mirror_destination_options(scope=["region", "tier"]),
            limit=20,
        )

        scopes = mock_all_keys.call_args.args[3]
        assert scopes is not None
        assert sorted(scopes) == [("eu", "gold"), ("us", "silver")]

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_missing_scope_column_yields_none_scope_value(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """A record without the scope column contributes ``None``.

        Same ``record.get(c)`` derivation as ``_accumulate_mirror_state`` — the
        real run fails fast on a missing scope column via
        ``_validate_mirror_scope``, so the preview only has to not crash.
        """
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = []

        compute_diff(
            [{"id": "a"}],
            _pg_config(),
            _mirror_destination_options(scope=["region"]),
            limit=20,
        )

        assert mock_all_keys.call_args.args[3] == [(None,)]

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows")
    def test_empty_source_previews_no_deletes(
        self, mock_fetch: Any, mock_all_keys: Any
    ) -> None:
        """Empty source → no preview, because the real run skips the DELETE.

        ``_finalize_mirror`` returns early on an empty ``_mirror_keys`` so a
        transient empty source cannot wipe the destination. Previewing a full
        wipe here would contradict what the run would actually do.
        """
        mock_fetch.return_value = []

        result = compute_diff([], _pg_config(), _mirror_destination_options(), limit=20)

        assert result.deleted == []
        assert result.delete_reason is None
        mock_all_keys.assert_not_called()

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_unsupported_config_marks_preview_unavailable(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """An unsupported reader is distinct from a successful zero-delete read."""
        mock_fetch_keys.return_value = [{"id": "a"}]
        mock_all_keys.side_effect = NotImplementedError("clickhouse")

        result = compute_diff(
            [{"id": "a"}], _pg_config(), _mirror_destination_options(), limit=20
        )

        assert result.supported
        assert result.deleted == []
        assert result.delete_reason is None
        assert result.delete_preview_unavailable_reason == (
            "NotImplementedError: clickhouse"
        )

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_key_read_failure_does_not_break_the_diff(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """Same degradation as the tracked preview: lose the deletes, keep the diff."""
        mock_fetch_keys.return_value = [{"id": "a", "score": 0.5}]
        mock_all_keys.side_effect = RuntimeError("permission denied for users")

        result = compute_diff(
            [{"id": "a", "score": 0.9}],
            _pg_config(),
            _mirror_destination_options(),
            limit=20,
        )

        assert result.supported
        assert result.deleted == []
        assert len(result.updated) == 1
        assert result.delete_preview_unavailable_reason == (
            "RuntimeError: permission denied for users"
        )

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    @needs_psycopg2
    def test_destination_preview_is_read_only(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        """The key read must never write: only SELECTs reach the cursor.

        ``fetch_all_keys`` runs for real against a fake cursor so the read-only
        guarantee is proved end-to-end, not just at the seam.
        """
        from drt.destinations.query import fetch_all_keys as real_fetch

        cursor = MagicMock()
        cursor.fetchall.return_value = [("a",), ("b",)]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        mock_fetch_keys.return_value = []
        mock_all_keys.side_effect = real_fetch

        with patch(
            "drt.destinations.postgres.PostgresDestination._connect",
            return_value=conn,
        ):
            result = compute_diff(
                [{"id": "a", "region": "eu"}],
                _pg_config(table="public.users"),
                _mirror_destination_options(scope=["region"]),
                limit=20,
            )

        assert result.deleted == [{"id": "b"}]
        assert cursor.execute.call_count == 1
        for call in cursor.execute.call_args_list:
            stmt = call[0][0]
            text = str(stmt.seq) if hasattr(stmt, "seq") else str(stmt)
            upper = text.upper()
            assert "SELECT" in upper
            for kw in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TRUNCATE"):
                assert kw not in upper, f"write keyword {kw} in {text}"
        cursor.executemany.assert_not_called()
        conn.commit.assert_not_called()

    @patch("drt.engine.diff.fetch_all_keys")
    @patch("drt.engine.diff.fetch_rows_by_keys")
    def test_truncation_applies_to_destination_preview(
        self, mock_fetch_keys: Any, mock_all_keys: Any
    ) -> None:
        mock_fetch_keys.return_value = []
        mock_all_keys.return_value = [("a",), ("b",), ("c",)]

        result = compute_diff(
            [{"id": "a"}], _pg_config(), _mirror_destination_options(), limit=1
        )

        assert result.truncated is True
        assert len(result.deleted) == 1


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
