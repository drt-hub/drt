"""Unit tests for Google Sheets destination.

Mocks the Google Sheets API client since there is no local server equivalent.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from drt.config.models import GoogleSheetsDestinationConfig, SyncOptions


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

    def test_finalize_sync_resets_header_state_for_reused_instance(self) -> None:
        """#1143: a library caller reusing one instance across multiple
        run_sync() calls must have the second run's first batch treated as
        a fresh first batch (clear + establish columns), not as a later
        batch of the first run. Mirrors FileDestination's identical
        finalize_sync() reset (#1002/#1006)."""
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

            dest.finalize_sync(config, _options())
            assert dest._headers is None

            dest.load([{"id": 2, "name": "Bob"}], config, _options())

        assert mock_values.clear.call_count == 2
