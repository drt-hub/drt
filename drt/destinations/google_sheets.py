"""Google Sheets destination — write rows to a spreadsheet.

Requires: pip install drt-core[sheets]

Example sync YAML:

    destination:
      type: google_sheets
      spreadsheet_id: "1BxiMVs..."
      sheet: "Sheet1"
      mode: overwrite
"""

from __future__ import annotations

import os
from typing import Any

from drt.config.models import GoogleSheetsDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import DetailedSyncResult


def _build_sheets_service(config: GoogleSheetsDestinationConfig) -> Any:
    """Build Google Sheets API v4 service client."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    keyfile = config.credentials_path or (
        os.environ.get(config.credentials_env) if config.credentials_env else None
    )

    if keyfile:
        creds = service_account.Credentials.from_service_account_file(
            keyfile, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        import google.auth

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )

    return build("sheets", "v4", credentials=creds)


class GoogleSheetsDestination:
    """Write records to a Google Sheets spreadsheet."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: GoogleSheetsDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        if not records:
            return SyncResult()

        result = DetailedSyncResult()

        try:
            service = _build_sheets_service(config)
            sheets = service.spreadsheets()
            range_name = config.sheet

            headers = list(records[0].keys())
            rows = [headers] + [
                [str(row.get(h, "")) for h in headers] for row in records
            ]
            body = {"values": rows}

            if config.mode == "overwrite":
                sheets.values().clear(
                    spreadsheetId=config.spreadsheet_id,
                    range=range_name,
                    body={},
                ).execute()
                sheets.values().update(
                    spreadsheetId=config.spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body=body,
                ).execute()
            else:
                sheets.values().append(
                    spreadsheetId=config.spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body={"values": rows[1:]},
                ).execute()

            result.success = len(records)

        except Exception as e:
            result.failed = len(records)
            result.errors.append(str(e))

        return result  # type: ignore[return-value]