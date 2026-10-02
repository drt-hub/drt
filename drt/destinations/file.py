"""File destination — write records to CSV, JSON, or JSONL files.

No extra dependencies required (uses stdlib csv/json + built-in I/O).

Example sync YAML:

    destination:
      type: file
      path: output/users.csv
      format: csv

    destination:
      type: file
      path: output/users.json
      format: json

    destination:
      type: file
      path: output/users.jsonl
      format: jsonl
"""

from __future__ import annotations

import csv
import json
import os
from typing import Any

from drt.config.models import DestinationConfig, FileDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult


class FileDestination:
    """Write records to a CSV, JSON, or JSONL file."""

    def __init__(self) -> None:
        # The engine constructs one destination per sync and calls load() on that
        # same instance for every batch. Keep write state instance-local so the
        # first batch truncates a previous run's file while later batches append.
        self._csv_columns: dict[str, tuple[str, ...]] = {}
        self._json_records: dict[str, list[dict[str, Any]]] = {}
        self._jsonl_started_paths: set[str] = set()

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, FileDestinationConfig)
        if not records:
            return SyncResult()

        result = SyncResult()

        try:
            os.makedirs(os.path.dirname(config.path) or ".", exist_ok=True)

            if config.format == "csv":
                self._write_csv(config.path, records)
            elif config.format == "json":
                self._write_json(config.path, records)
            elif config.format == "jsonl":
                self._write_jsonl(config.path, records)

            result.success = len(records)
        except Exception as e:
            result.failed = len(records)
            result.errors.append(str(e))

        return result

    def reset_write_state(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """Guaranteed end-of-sync hook (duck-typed, see ``drt/engine/sync.py``
        ``run_sync()``'s outer ``finally``): drop this run's write-state
        bookkeeping.

        A CLI/engine-driven sync gets a fresh ``FileDestination`` per run (via
        ``get_destination()``), so this never fires mid-run in that path. It
        matters for a library caller that reuses one instance across multiple
        ``run_sync()`` calls (e.g. a loop, or a long-lived process) — without
        this reset, the second run would append to or fold in the first run's
        records instead of truncating, contradicting the "first batch of this
        run replaces the file" contract ``load()`` documents (caught in Codex
        review on PR #1006).

        Named separately from ``finalize_sync()`` (#1145): unlike that
        duck-typed hook (used by SQL destinations to promote a staged swap,
        real non-idempotent work that must not run if the batch loop never
        completed), this one is called unconditionally by the engine on
        every exit path — success, interruption, or a raised exception from
        the source — so it must stay pure state-reset, safe to call more
        than once and on an already-empty state.
        """
        del config, sync_options
        self._csv_columns.clear()
        self._json_records.clear()
        self._jsonl_started_paths.clear()

    def _write_csv(self, path: str, records: list[dict[str, Any]]) -> None:
        columns = self._csv_columns.get(path)
        first_batch = columns is None
        batch_columns = tuple(dict.fromkeys(key for record in records for key in record))
        if columns is None:
            # The header has not been emitted yet, so every field in this
            # batch can safely contribute a column. ``dict.fromkeys`` above
            # preserves first-seen order, keeping homogeneous output
            # byte-identical while allowing omitted fields to use restval.
            columns = batch_columns

        expected_columns = set(columns)
        unexpected = sorted(set(batch_columns) - expected_columns)
        if unexpected:
            # A previous batch's rows are already positioned under ``columns``.
            # Appending a wider row would either be rejected by DictWriter or
            # silently misalign/drop data. Fail before opening the file instead
            # of rewriting a potentially large streaming output.
            raise ValueError(
                f"CSV columns cannot change after the header was written for "
                f"'{path}': expected {list(columns)!r}; unexpected {unexpected!r}"
            )

        mode = "w" if first_batch else "a"
        with open(path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=columns,
                restval="",
                extrasaction="raise",
            )
            if first_batch:
                writer.writeheader()
            writer.writerows(records)
        if first_batch:
            self._csv_columns[path] = columns

    def _write_json(self, path: str, records: list[dict[str, Any]]) -> None:
        # A single top-level JSON array cannot be extended with a plain append.
        # Buffer this format's full sync in memory and rewrite the valid array on
        # each batch. This deliberate memory tradeoff is specific to array JSON;
        # CSV and JSONL remain streaming and retain only small bookkeeping state.
        accumulated = [*self._json_records.get(path, []), *records]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(accumulated, f, indent=2, default=str)
        self._json_records[path] = accumulated

    def _write_jsonl(self, path: str, records: list[dict[str, Any]]) -> None:
        mode = "a" if path in self._jsonl_started_paths else "w"
        with open(path, mode, encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
        self._jsonl_started_paths.add(path)
