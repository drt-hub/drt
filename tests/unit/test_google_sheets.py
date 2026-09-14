"""Unit tests for Google Sheets destination.

Mocks the Google Sheets API client since there is no local server equivalent.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import GoogleSheetsDestinationConfig, SyncConfig, SyncOptions


def _options() -> SyncOptions:
    return SyncOptions()


class TestGoogleSheetsDestination:
    def test_overwrite_clears_and_writes(self) -> None:
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            sheet="Sheet1",
            mode="overwrite",
        )
        records = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.clear.return_value.execute.return_value = {}
        mock_values.update.return_value.execute.return_value = {"updatedRows": 3}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result = dest.load(records, config, _options())

        assert result.success == 2
        assert result.failed == 0
        mock_values.clear.assert_called_once()
        mock_values.update.assert_called_once()

    def test_first_batch_unions_columns_instead_of_failing_on_a_heterogeneous_batch(
        self,
    ) -> None:
        """Codex review on #1144: no positional constraint applies yet for
        the very first batch of a sync -- nothing has been written. A field
        appearing only in a later record of that same first batch should
        be unioned into the header set and blank-filled, not treated as a
        column mismatch (the strict raise only makes sense from the
        SECOND batch onward, once earlier rows are already committed under
        a fixed header set)."""
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            sheet="Sheet1",
            mode="overwrite",
        )
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob", "note": "flagged"},
        ]

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.clear.return_value.execute.return_value = {}
        mock_values.update.return_value.execute.return_value = {"updatedRows": 3}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result = dest.load(records, config, _options())

        assert result.success == 2
        assert result.failed == 0
        written = mock_values.update.call_args.kwargs["body"]["values"]
        assert written == [
            ["id", "name", "note"],
            ["1", "Alice", ""],
            ["2", "Bob", "flagged"],
        ]

    def test_overwrite_second_batch_does_not_reclear_and_clobber_first(self) -> None:
        """The engine calls load() once per sync.batch_size chunk of the
        source, all on the SAME destination instance (drt/engine/sync.py's
        ``for record_batch in batch(records_iter, sync.sync.batch_size)``
        loop) -- with the default batch_size of 100, any sync producing
        more than 100 records silently loses everything but the last
        batch under mode: overwrite today: every batch calls clear() with
        a bare sheet name (the whole sheet, in A1 notation) then writes
        only its own rows. This must clear only once per sync (on the
        first batch this instance sees) and append subsequent batches,
        matching the precedent already set for FileDestination's
        analogous first-batch-truncates/later-batches-append fix (#1002/
        #1006)."""
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            sheet="Sheet1",
            mode="overwrite",
        )

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.clear.return_value.execute.return_value = {}
        mock_values.update.return_value.execute.return_value = {"updatedRows": 3}
        mock_values.append.return_value.execute.return_value = {"updates": {"updatedRows": 1}}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result1 = dest.load([{"id": 1, "name": "Alice"}], config, _options())
            result2 = dest.load([{"id": 2, "name": "Bob"}], config, _options())

        assert result1.success == 1
        assert result2.success == 1
        # Only the FIRST batch may clear the sheet -- a second clear() would
        # wipe the first batch's just-written row.
        mock_values.clear.assert_called_once()
        # The second batch must land via append (not another destructive
        # clear + update of just its own rows).
        mock_values.append.assert_called_once()
        appended_rows = mock_values.append.call_args.kwargs["body"]["values"]
        assert appended_rows == [["2", "Bob"]]

    def test_append_does_not_clear(self) -> None:
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            sheet="Sheet1",
            mode="append",
        )
        records = [{"id": 1, "name": "Alice"}]

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.append.return_value.execute.return_value = {"updates": {"updatedRows": 1}}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result = dest.load(records, config, _options())

        assert result.success == 1
        mock_values.clear.assert_not_called()
        mock_values.append.assert_called_once()

    def test_later_batch_with_different_columns_raises_instead_of_dropping_or_misaligning(
        self,
    ) -> None:
        """#1134: once headers are remembered from the first batch (#1143),
        a later batch introducing a field the first batch's header set
        didn't have can't be silently handled -- the sheet is positional
        and rows already written under the narrower header set can't be
        retroactively widened. Fail loudly instead, matching
        FileDestination._write_csv's identical raise (#1002/#1006) for the
        same cross-batch column-mismatch shape."""
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            sheet="Sheet1",
            mode="append",
        )

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.append.return_value.execute.return_value = {"updates": {"updatedRows": 1}}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result1 = dest.load([{"id": 1, "name": "Alice"}], config, _options())
            result2 = dest.load([{"id": 2, "name": "Bob", "note": "flagged"}], config, _options())

        assert result1.success == 1
        assert result2.failed == 1
        assert "column mismatch" in result2.errors[0]
        assert "note" in result2.errors[0]
        # Only batch 1's append should have gone through.
        assert mock_values.append.call_count == 1

    def test_later_batch_missing_an_optional_column_is_blank_filled_not_rejected(
        self,
    ) -> None:
        """P1, Codex review round 2 on #1144: a later-batch record simply
        missing a column the header set has creates no positional
        ambiguity (row.get(h, "") blank-fills it either way), and the
        first batch's own union step already tolerates exactly this shape
        for a record within IT. Rejecting it only from the second batch
        onward would be an inconsistency this fix itself introduces --
        only a genuinely UNEXPECTED (new) column needs the raise."""
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            sheet="Sheet1",
            mode="append",
        )

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.append.return_value.execute.return_value = {"updates": {"updatedRows": 1}}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result1 = dest.load([{"id": 1, "name": "Alice", "note": "vip"}], config, _options())
            result2 = dest.load([{"id": 2, "name": "Bob"}], config, _options())

        assert result1.success == 1
        assert result2.success == 1
        assert result2.failed == 0
        appended_rows = mock_values.append.call_args.kwargs["body"]["values"]
        assert appended_rows == [["2", "Bob", ""]]

    def test_empty_records(self) -> None:
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
        )
        dest = GoogleSheetsDestination()
        result = dest.load([], config, _options())
        assert result.success == 0
        assert result.failed == 0

    def test_api_error_reports_failure(self) -> None:
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            mode="overwrite",
        )
        records = [{"id": 1, "name": "Alice"}]

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.clear.return_value.execute.side_effect = Exception("API error")

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            result = dest.load(records, config, _options())

        assert result.failed == 1
        assert result.success == 0

    def test_api_error_on_first_batch_does_not_mark_headers_established(self) -> None:
        """A failed first batch must not leave self._headers set -- a retry
        (or the next real batch) needs to still be treated as this sync's
        first batch (clear + establish columns), not as a later batch that
        skips clearing because a previous attempt looked like it started."""
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            mode="overwrite",
        )
        records = [{"id": 1, "name": "Alice"}]

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.clear.return_value.execute.side_effect = Exception("API error")

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            dest.load(records, config, _options())
            assert dest._headers is None

    def test_reset_write_state_resets_header_state_for_reused_instance(self) -> None:
        """#1143: a library caller reusing one instance across multiple
        run_sync() calls must have the second run's first batch treated as
        a fresh first batch (clear + establish columns), not as a later
        batch of the first run. Mirrors FileDestination's identical
        reset_write_state() reset (#1002/#1006, renamed from
        finalize_sync() by #1145)."""
        from drt.destinations.google_sheets import GoogleSheetsDestination

        config = GoogleSheetsDestinationConfig(
            type="google_sheets",
            spreadsheet_id="test-id",
            mode="overwrite",
        )

        mock_service = MagicMock()
        mock_sheets = mock_service.spreadsheets.return_value
        mock_values = mock_sheets.values.return_value
        mock_values.clear.return_value.execute.return_value = {}
        mock_values.update.return_value.execute.return_value = {"updatedRows": 1}

        with patch(
            "drt.destinations.google_sheets._build_sheets_service",
            return_value=mock_service,
        ):
            dest = GoogleSheetsDestination()
            dest.load([{"id": 1, "name": "Alice"}], config, _options())
            assert dest._headers == ["id", "name"]

            dest.reset_write_state(config, _options())
            assert dest._headers is None

            dest.load([{"id": 2, "name": "Bob"}], config, _options())

        assert mock_values.clear.call_count == 2


# ---------------------------------------------------------------------------
# Engine-level: reset_write_state() on a source exception (#1145)
# ---------------------------------------------------------------------------


class _RaisingAfterNSource:
    """Yields the first ``n`` rows, then raises -- simulates a dropped
    source connection mid-extraction (#1145)."""

    def __init__(self, rows: list[dict[str, Any]], n: int) -> None:
        self._rows = rows
        self._n = n

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for i, row in enumerate(self._rows):
            if i >= self._n:
                raise RuntimeError("source connection dropped")
            yield row

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


class _RowsSource:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


def _profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _sheets_sync(*, batch_size: int = 100, name: str = "sheets_sync") -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": name,
            "model": "ref('rows')",
            "destination": {
                "type": "google_sheets",
                "spreadsheet_id": "test-id",
                "sheet": "Sheet1",
                "mode": "overwrite",
            },
            "sync": {"batch_size": batch_size},
        }
    )


def test_source_exception_after_first_batch_still_resets_state_for_reused_instance(
    tmp_path: Path,
) -> None:
    """#1145: a source-side exception mid-extraction must not leave
    GoogleSheetsDestination's header-state bookkeeping stale on a reused
    instance. The first run's source raises after yielding exactly one full
    batch (which gets loaded and written); the second run on the SAME
    instance must still start fresh (clear + establish columns) rather than
    treating its own first batch as a continuation of the run that
    raised."""
    from drt.destinations.google_sheets import GoogleSheetsDestination
    from drt.engine.sync import run_sync

    mock_service = MagicMock()
    mock_values = mock_service.spreadsheets.return_value.values.return_value
    mock_values.clear.return_value.execute.return_value = {}
    mock_values.update.return_value.execute.return_value = {}

    destination = GoogleSheetsDestination()
    # 4 rows total, but the source raises after yielding the first 3 (index
    # 3 never yields) -- one full batch loads successfully before the
    # exception propagates out of the batch loop.
    written_records = [{"id": i, "name": f"first-{i}"} for i in range(3)]
    unreached_record = [{"id": 3, "name": "first-3"}]

    with patch(
        "drt.destinations.google_sheets._build_sheets_service",
        return_value=mock_service,
    ):
        with pytest.raises(RuntimeError, match="source connection dropped"):
            run_sync(
                _sheets_sync(batch_size=3),
                _RaisingAfterNSource(written_records + unreached_record, n=3),
                destination,
                _profile(),
                tmp_path,
            )

        # reset_write_state() already cleared the header state fired by the
        # exception path -- the assertion that matters is the SECOND run's
        # behavior below (a fresh first batch, not a continuation).
        assert destination._headers is None

        second_records = [{"id": i, "name": f"second-{i}"} for i in range(2)]
        second_result = run_sync(
            _sheets_sync(batch_size=100, name="second_run"),
            _RowsSource(second_records),
            destination,
            _profile(),
            tmp_path,
        )

    assert second_result.success == 2
    # reset_write_state() unconditionally resets at the end of EVERY
    # run_sync() call, success or failure -- state is only meaningful within
    # one run's own batch loop.
    assert destination._headers is None
    # clear() ran again on the second run -- proof the second run's first
    # batch was treated as fresh (re-cleared), not appended to the first
    # run's incomplete data.
    assert mock_values.clear.call_count == 2
    # The second update() call wrote only second_records' own header +
    # rows -- not merged with anything left over from the first run.
    second_update_call = mock_values.update.call_args_list[-1]
    assert second_update_call.kwargs["body"]["values"] == [
        ["id", "name"],
        ["0", "second-0"],
        ["1", "second-1"],
    ]
