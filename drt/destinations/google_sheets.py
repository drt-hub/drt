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

from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, GoogleSheetsDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult


def _build_sheets_service(config: GoogleSheetsDestinationConfig) -> Any:
    """Build Google Sheets API v4 service client."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    keyfile = resolve_env(config.credentials_path, config.credentials_env)

    if keyfile:
        creds = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
            keyfile, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        import google.auth

        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])

    return build("sheets", "v4", credentials=creds)


class GoogleSheetsDestination:
    """Write records to a Google Sheets spreadsheet."""

    def __init__(self) -> None:
        # The engine constructs one destination per sync and calls load() on
        # that same instance once per sync.batch_size-sized chunk of the
        # source (#1143) -- keep the header order instance-local so only the
        # FIRST batch clears the sheet / establishes columns, and every
        # later batch appends instead of re-clearing. range=config.sheet is
        # a bare sheet name, which in A1 notation means the WHOLE sheet --
        # re-clearing on a later batch would wipe every earlier batch's
        # just-written rows (the default sync.batch_size of 100 meant any
        # sync over 100 records lost all but its last batch).
        self._headers: list[str] | None = None

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, GoogleSheetsDestinationConfig)
        if not records:
            return SyncResult()

        result = SyncResult()

        try:
            service = _build_sheets_service(config)
            sheets = service.spreadsheets()
            range_name = config.sheet

            first_batch = self._headers is None
            headers = self._headers if self._headers is not None else list(records[0].keys())

            # A field a later batch introduces that this sync's remembered
            # header set doesn't have can't be silently handled the way
            # #1091/#1134 widen a SQL destination's write columns: a sheet
            # is positional, and rows already written under the narrower
            # header set can't be retroactively backfilled with a new
            # column. Fail loudly instead, matching FileDestination
            # (#1002/#1006)'s identical raise for the same cross-batch
            # column-mismatch shape.
            expected = set(headers)
            for index, record in enumerate(records):
                actual = set(record)
                if actual != expected:
                    missing = sorted(expected - actual)
                    unexpected = sorted(actual - expected)
                    raise ValueError(
                        f"Google Sheets column mismatch at batch record {index}: "
                        f"expected {headers!r}; missing {missing!r}; "
                        f"unexpected {unexpected!r}"
                    )

            rows = [[str(row.get(h, "")) for h in headers] for row in records]

            if first_batch and config.mode == "overwrite":
                sheets.values().clear(
                    spreadsheetId=config.spreadsheet_id,
                    range=range_name,
                    body={},
                ).execute()
                sheets.values().update(
                    spreadsheetId=config.spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body={"values": [headers, *rows]},
                ).execute()
            else:
                # mode: append (always), or mode: overwrite's later batches
                # (the sheet already holds this sync's header + earlier
                # rows from the first batch -- append after them rather
                # than re-clearing).
                sheets.values().append(
                    spreadsheetId=config.spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body={"values": rows},
                ).execute()

            result.success = len(records)
            if first_batch:
                self._headers = headers

        except Exception as e:
            result.failed = len(records)
            result.errors.append(str(e))

        return result

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """End-of-sync hook (duck-typed, see ``drt/engine/sync.py``): drop
        this run's remembered header state.

        Mirrors ``FileDestination``'s identical reset (#1002/#1006). A
        CLI/engine-driven sync gets a fresh ``GoogleSheetsDestination`` per
        run (via ``get_destination()``), so this never fires mid-run in
        that path. It matters for a library caller that reuses one
        instance across multiple ``run_sync()`` calls -- without this
        reset, that second run's own first batch would be treated as a
        later batch of the first run (appended, sheet never re-cleared)
        instead of getting its own fresh first-batch treatment.
        """
        del config, sync_options
        self._headers = None
