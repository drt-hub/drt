"""Parquet file destination — write records to a local Parquet file.

Uses pandas + pyarrow for Parquet serialisation. Supports partitioning
and compression (snappy, gzip, zstd, none).

Requires: pip install drt-core[parquet]

Example sync YAML:

    destination:
      type: parquet
      path: output/scores.parquet
      compression: snappy
      partition_by: [region]
"""

from __future__ import annotations

import os
from typing import Any

from drt.config.models import DestinationConfig, ParquetDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult


class ParquetDestination:
    """Write records to a Parquet file."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, ParquetDestinationConfig)
        if not records:
            return SyncResult()

        try:
            import pandas as pd  # type: ignore[import-untyped]
            import pyarrow  # type: ignore[import-untyped]  # noqa: F401
        except ImportError as e:
            raise ImportError("Parquet destination requires: pip install drt-core[parquet]") from e

        result = SyncResult()

        try:
            df = pd.DataFrame(records)
            os.makedirs(os.path.dirname(config.path) or ".", exist_ok=True)

            compression = config.compression if config.compression != "none" else None

            df.to_parquet(
                config.path,
                engine="pyarrow",
                compression=compression,
                index=False,
                partition_cols=config.partition_by,
            )
            result.success = len(records)
        except Exception as e:
            result.failed = len(records)
            result.errors.append(str(e))
            return result

        return result
