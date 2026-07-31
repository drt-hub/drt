"""Delta Lake source — read Delta tables from local / S3 / GCS via delta-rs.

Opens a Delta table with the ``deltalake`` (delta-rs) bindings as a lazy Arrow
*dataset*, registers it in an in-memory DuckDB, then runs your model SQL against
it — so column selection and incremental filters work like any other source, and
are pushed into the Parquet scan rather than applied after a full read (#679).

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
from drt.config.profiles import DEFAULT_FETCH_SIZE


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

        # Predicate/projection pushdown (#679). ``to_pyarrow_table()`` read the
        # entire table into memory before DuckDB ever saw the query, so a
        # model's incremental ``WHERE`` reduced nothing and a large table risked
        # an OOM regardless of how selective the query was. A *dataset* is lazy:
        # DuckDB pushes the filter and the column list into the Parquet scan, so
        # only matching row groups and referenced columns are read.
        #
        # Measured with 2 of 3 columns and ~1/9th of the rows selected; figures
        # in docs/research/extraction-memory.md, which is the single source.
        #
        # DuckDB's ``delta_scan()`` is faster still (+19 MB measured, kept here
        # because it is the argument for rejecting a dependency rather than a
        # benchmark) but needs the ``delta`` extension, which DuckDB fetches
        # from its repository on first use — an implicit network call at sync
        # time that would break air-gapped and offline installs. Not worth it
        # for an extract path.
        dataset = DeltaTable(config.location, storage_options=options).to_pyarrow_dataset()

        conn = duckdb.connect()
        try:
            conn.register(_table_name(config), dataset)
            result = conn.execute(query)
            columns = [desc[0] for desc in result.description]
            # Batched rather than fetchall() (#765): the result set no longer
            # needs to be materialised either.
            while True:
                batch = result.fetchmany(DEFAULT_FETCH_SIZE)
                if not batch:
                    break
                for row in batch:
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
