"""Live Databricks ``write_policy: fill_empty`` coverage (#1238).

Mock cursors can assert the generated MERGE text, but cannot prove Databricks'
CAST/TRIM semantics or its composition with Delta staging presence flags. This
suite runs only with the existing ``DRT_SMOKE_DATABRICKS_*`` credentials.
"""

from __future__ import annotations

from typing import Any

import pytest

from drt.config.models import DatabricksDestinationConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

dbsql = pytest.importorskip("databricks.sql")

HOST_ENV = "DRT_SMOKE_DATABRICKS_HOST"
HTTP_PATH_ENV = "DRT_SMOKE_DATABRICKS_HTTP_PATH"
TOKEN_ENV = "DRT_SMOKE_DATABRICKS_TOKEN"


def _connect(creds: dict[str, str]) -> Any:
    return dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )


def test_databricks_write_policy_fill_empty_merge_semantics() -> None:
    """Fill blank values, keep populated values/zero, override, and preserve omission."""
    creds = require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )
    catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
    schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
    table = unique_table("drt_write_policy")
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    staging_fqn = f"`{catalog}`.`{schema}`.`__drt_staging_{table}`"
    config = DatabricksDestinationConfig(
        **{
            "type": "databricks",
            "host_env": HOST_ENV,
            "http_path_env": HTTP_PATH_ENV,
            "token_env": TOKEN_ENV,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "mode": "merge",
            "upsert_key": ["id"],
        }
    )
    options = SyncOptions(
        mode="upsert",
        write_policy="fill_empty",
        write_policy_overrides={"overwrite_value": "overwrite"},
    )
    records = [
        {
            "id": i,
            "fill_value": f"source-{i}",
            "overwrite_value": f"new-{i}",
            "numeric_value": 99,
        }
        for i in range(1, 6)
    ]
    # The fill column is absent, not explicitly NULL. Its presence flag must
    # preserve the target value even though other columns on the row are sent.
    records.append(
        {
            "id": 6,
            "overwrite_value": "new-6",
            "numeric_value": 99,
        }
    )

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {fqn} ("
                "id INT, fill_value STRING, overwrite_value STRING, numeric_value INT) "
                "USING DELTA"
            )
            cur.execute(
                f"INSERT INTO {fqn} VALUES "
                "(1, NULL, 'old-1', NULL), "
                "(2, '', 'old-2', 2), "
                "(3, '   ', 'old-3', 3), "
                "(4, 'existing', 'old-4', 4), "
                "(5, 'zero-row', 'old-5', 0), "
                "(6, 'sparse-existing', 'old-6', 6)"
            )
    finally:
        conn.close()

    try:
        result = DatabricksDestination().load(records, config, options)
        assert result.success == 6 and result.failed == 0

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, fill_value, overwrite_value, numeric_value FROM {fqn} ORDER BY id"
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        assert rows == [
            (1, "source-1", "new-1", 99),  # NULL text and number are filled
            (2, "source-2", "new-2", 2),  # empty string is filled
            (3, "source-3", "new-3", 3),  # spaces-only text is filled
            (4, "existing", "new-4", 4),  # existing values are kept
            (5, "zero-row", "new-5", 0),  # numeric zero is a value, not empty
            (6, "sparse-existing", "new-6", 6),  # omitted fill column stays untouched
        ]
    finally:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_fqn}")
                cur.execute(f"DROP TABLE IF EXISTS {fqn}")
        finally:
            conn.close()
