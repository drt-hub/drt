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

    @staticmethod
    def _write_csv(path: str, records: list[dict[str, Any]]) -> None:
        columns = list(records[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(records)

    @staticmethod
    def _write_json(path: str, records: list[dict[str, Any]]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, default=str)

    @staticmethod
    def _write_jsonl(path: str, records: list[dict[str, Any]]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
