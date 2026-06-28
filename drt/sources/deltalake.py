"""Delta Lake source — read Delta tables from local / S3 / GCS via delta-rs.

Loads a Delta table with the ``deltalake`` (delta-rs) bindings into Arrow,
registers it in an in-memory DuckDB, then runs your model SQL against it — so
column selection and incremental filters work like any other source.

Requires: pip install drt-core[deltalake]

Example ~/.drt/profiles.yml:
    lakehouse:
      type: deltalake
      location: s3://my-bucket/delta/users     # or ./data/delta/users, gs://...
      table: users                             # SQL name (default: last path segment)
      storage_options:                         # cloud auth; *_ENV reads the env var
        AWS_ACCESS_KEY_ID_ENV: AWS_KEY
        AWS_SECRET_ACCESS_KEY_ENV: AWS_SECRET

Then in a model: ``SELECT id, email FROM users WHERE updated_at > '{{ cursor }}'``
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import DeltaLakeProfile, ProfileConfig, resolve_env_dict


def _table_name(profile: DeltaLakeProfile) -> str:
    """SQL name the Delta table is registered under — explicit ``table`` or the
    last path segment of ``location`` (``s3://b/delta/users`` → ``users``)."""
    if profile.table:
        return profile.table
    return profile.location.rstrip("/").rsplit("/", 1)[-1] or "delta_table"


class DeltaLakeSource:
    """Extract records from a Delta Lake table."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, DeltaLakeProfile)
        try:
            from deltalake import DeltaTable
        except ImportError as e:
            raise ImportError("Delta Lake support requires: pip install drt-core[deltalake]") from e
        import duckdb  # bundled with drt-core

        options = resolve_env_dict(config.storage_options) or None
        arrow = DeltaTable(config.location, storage_options=options).to_pyarrow_table()

        conn = duckdb.connect()
        try:
            conn.register(_table_name(config), arrow)
            result = conn.execute(query)
            columns = [desc[0] for desc in result.description]
            for row in result.fetchall():
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, DeltaLakeProfile)
        try:
            from deltalake import DeltaTable

            options = resolve_env_dict(config.storage_options) or None
            DeltaTable(config.location, storage_options=options).version()
            return True
        except Exception:
            return False
