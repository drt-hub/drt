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

    def _write_csv(self, path: str, records: list[dict[str, Any]]) -> None:
        columns = self._csv_columns.get(path)
        first_batch = columns is None
        if columns is None:
            columns = tuple(records[0].keys())

        expected_columns = set(columns)
        for index, record in enumerate(records):
            actual_columns = set(record)
            if actual_columns != expected_columns:
                missing = sorted(expected_columns - actual_columns)
                unexpected = sorted(actual_columns - expected_columns)
                raise ValueError(
                    f"CSV column mismatch for '{path}' at batch record {index}: "
                    f"expected {list(columns)!r}; missing {missing!r}; "
                    f"unexpected {unexpected!r}"
                )

        mode = "w" if first_batch else "a"
        with open(path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="raise")
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
