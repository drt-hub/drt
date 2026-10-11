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
    # drt cannot write Python bytes into a Databricks BINARY column (the driver
    # binds them as an array), so the payload carries NULL for binary_value. That
    # still pins the rule: an empty or space-only BINARY target must be KEPT (a
    # wrongly text-trimmed binary would be overwritten with this NULL).
    records = [
        {
            "id": i,
            "string_value": f"source-{i}",
            "varchar_value": f"source-{i}",
            "char_value": f"source-{i}",
            "binary_value": None,
            "struct_value": {"value": f"source-{i}"},
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
            "varchar_value": "source-6",
            "char_value": "source-6",
            "binary_value": None,
            "struct_value": {"value": "source-6"},
            "overwrite_value": "new-6",
            "numeric_value": 99,
        }
    )

    try:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {fqn} ("
                    "id INT, string_value STRING, varchar_value VARCHAR(20), "
                    "char_value CHAR(20), binary_value BINARY, "
                    "struct_value STRUCT<value: STRING>, overwrite_value STRING, "
                    "numeric_value INT) USING DELTA"
                )
                cur.execute(
                    f"INSERT INTO {fqn} VALUES "
                    "(1, NULL, NULL, NULL, CAST('' AS BINARY), NULL, 'old-1', NULL), "
                    "(2, '', '', '', CAST(' ' AS BINARY), named_struct('value', ''), "
                    "'old-2', 2), "
                    "(3, '   ', '   ', '   ', NULL, named_struct('value', '   '), "
                    "'old-3', 3), "
                    "(4, 'existing', 'existing', 'existing', CAST('old-4' AS BINARY), "
                    "named_struct('value', 'existing'), 'old-4', 4), "
                    "(5, 'zero-row', 'existing-5', 'existing-5', "
                    "CAST('old-5' AS BINARY), named_struct('value', 'existing-5'), "
                    "'old-5', 0), "
                    "(6, 'sparse-existing', 'existing-6', 'existing-6', "
                    "CAST('old-6' AS BINARY), named_struct('value', 'existing-6'), "
                    "'old-6', 6)"
                )
                cur.execute(
                    f"SELECT typeof(string_value), typeof(varchar_value), "
                    f"typeof(char_value) FROM {fqn} LIMIT 1"
                )
                type_names = tuple(str(value).lower() for value in cur.fetchone())
        finally:
            conn.close()

        # Pin the typeof() spellings that the MERGE predicate recognizes.
        assert type_names[0].startswith("string")
        assert type_names[1].startswith(("string", "varchar"))
        assert type_names[2].startswith(("string", "char"))

        result = DatabricksDestination().load(records, config, options)
        assert result.success == 6 and result.failed == 0

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, string_value, varchar_value, rtrim(char_value), "
                    f"hex(binary_value), struct_value.value, overwrite_value, "
                    f"numeric_value FROM {fqn} ORDER BY id"
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        assert rows == [
            # NULL text, struct, and number fill; empty binary remains a value.
            (1, "source-1", "source-1", "source-1", "", "source-1", "new-1", 99),
            # Empty/space-only text fills; space byte and STRUCT empty string stay.
            (2, "source-2", "source-2", "source-2", "20", "", "new-2", 2),
            # NULL binary fills (with the NULL payload), STRUCT spaces remain a value.
            (
                3,
                "source-3",
                "source-3",
                "source-3",
                None,
                "   ",
                "new-3",
                3,
            ),
            (
                4,
                "existing",
                "existing",
                "existing",
                b"old-4".hex().upper(),
                "existing",
                "new-4",
                4,
            ),
            (
                5,
                "zero-row",
                "existing-5",
                "existing-5",
                b"old-5".hex().upper(),
                "existing-5",
                "new-5",
                0,
            ),
            (
                6,
                "sparse-existing",
                "existing-6",
                "existing-6",
                b"old-6".hex().upper(),
                "existing-6",
                "new-6",
                6,
            ),
        ]
    finally:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_fqn}")
                cur.execute(f"DROP TABLE IF EXISTS {fqn}")
        finally:
            conn.close()
