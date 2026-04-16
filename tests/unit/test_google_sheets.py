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
