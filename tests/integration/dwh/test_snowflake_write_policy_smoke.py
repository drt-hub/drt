"""Live Snowflake ``write_policy: fill_empty`` coverage (#1238).

Mock cursors can assert the generated MERGE text, but cannot prove Snowflake's
cast/TRIM semantics. This suite runs only with the existing
``DRT_SMOKE_SNOWFLAKE_*`` credentials.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

snowflake_connector = pytest.importorskip("snowflake.connector")

ACCOUNT_ENV = "DRT_SMOKE_SNOWFLAKE_ACCOUNT"
USER_ENV = "DRT_SMOKE_SNOWFLAKE_USER"
PASSWORD_ENV = "DRT_SMOKE_SNOWFLAKE_PASSWORD"
KEY_ENV = "DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY"


def _require_creds() -> dict[str, str]:
    if not os.environ.get(KEY_ENV) and not os.environ.get(PASSWORD_ENV):
        pytest.skip(
            "Snowflake smoke auth not set: need DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY "
            "(preferred) or DRT_SMOKE_SNOWFLAKE_PASSWORD."
        )
    return require_env(
        ACCOUNT_ENV,
        USER_ENV,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )


def _auth_config_kwargs() -> dict[str, str]:
    if os.environ.get(KEY_ENV):
        return {"private_key_env": KEY_ENV}
    return {"password_env": PASSWORD_ENV}


def _connect(creds: dict[str, str]) -> Any:
    auth: dict[str, object] = {}
    pem = os.environ.get(KEY_ENV)
    if pem:
        from drt.config.credentials import load_snowflake_private_key

        auth["private_key"] = load_snowflake_private_key(pem)
    else:
        auth["password"] = os.environ[PASSWORD_ENV]
    return snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        **auth,
    )


def test_snowflake_write_policy_fill_empty_merge_semantics() -> None:
    """Fill NULL/blank text, keep values/zero, and overwrite one override."""
    creds = _require_creds()
    table = unique_table("DRT_WRITE_POLICY")
    table_fq = (
        f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{table}"
    )
    config = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            **_auth_config_kwargs(),
            "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "table": table,
            "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
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

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table_fq} ("
                "id INTEGER, fill_value VARCHAR, overwrite_value VARCHAR, numeric_value INTEGER)"
            )
            cur.execute(
                f"INSERT INTO {table_fq} VALUES "
                "(1, NULL, 'old-1', NULL), "
                "(2, '', 'old-2', 2), "
                "(3, '   ', 'old-3', 3), "
                "(4, 'existing', 'old-4', 4), "
                "(5, 'zero-row', 'old-5', 0)"
            )

        result = SnowflakeDestination().load(records, config, options)
        assert result.success == 5 and result.failed == 0

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, fill_value, overwrite_value, numeric_value FROM {table_fq} ORDER BY id"
            )
            rows = cur.fetchall()

        assert rows == [
            (1, "source-1", "new-1", 99),  # NULL text and number are filled
            (2, "source-2", "new-2", 2),  # empty string is filled
            (3, "source-3", "new-3", 3),  # spaces-only text is filled
            (4, "existing", "new-4", 4),  # existing values are kept
            (5, "zero-row", "new-5", 0),  # numeric zero is a value, not empty
        ]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_fq}")
        conn.close()
