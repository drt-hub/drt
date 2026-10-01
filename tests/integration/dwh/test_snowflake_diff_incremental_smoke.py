"""Live Snowflake snapshot-diff incremental coverage (#1112/#755).

Mock cursors cannot validate Snowflake's CTAS/JOIN/HASH/SWAP syntax or prove
that native ``HASH`` distinguishes SQL NULL from an empty string. This suite
runs only with the existing ``DRT_SMOKE_SNOWFLAKE_*`` credentials.
"""

from __future__ import annotations

import os
import uuid

import pytest

from drt.config.credentials import SnowflakeProfile
from drt.sources.snowflake import SnowflakeSource

from .conftest import require_env

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


def _profile(creds: dict[str, str]) -> SnowflakeProfile:
    kwargs: dict[str, object] = {
        "type": "snowflake",
        "account": creds[ACCOUNT_ENV],
        "user": creds[USER_ENV],
        "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        "managed_schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
    }
    if os.environ.get(KEY_ENV):
        kwargs["private_key_env"] = KEY_ENV
    else:
        kwargs["password_env"] = PASSWORD_ENV
    return SnowflakeProfile(**kwargs)  # type: ignore[arg-type]


def _connect(creds: dict[str, str]):
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


def test_snowflake_diff_incremental_round_trip_and_crash_recovery() -> None:
    """Classify two generations, then prove an uncommitted extract retries.

    The quoted lowercase output aliases exercise identifier preservation; the
    hyphenated sync name exercises quoting of drt-managed table names.
    """
    creds = _require_creds()
    profile = _profile(creds)
    source = SnowflakeSource()
    suffix = uuid.uuid4().hex[:10]
    source_table = f"DRT_DIFF_SOURCE_{suffix}"
    source_fq = (
        f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}."
        f"{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{source_table}"
    )
    sync_name = f"snowflake-diff-{suffix}"
    query = f'SELECT id AS "id", note AS "note", plan AS "plan" FROM {source_fq}'
    baseline, scratch = source._snapshot_table_names(sync_name)

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {source_fq} (id INTEGER, note VARCHAR, plan VARCHAR)")
            cur.execute(
                f"INSERT INTO {source_fq} VALUES "
                "(1, NULL, 'free'), (2, 'b', 'free'), (3, 'c', 'pro')"
            )

        first = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["id"],
            hash_columns=["note", "plan"],
        )
        assert first.is_first_run is True
        assert {row["id"] for row in first.added} == {1, 2, 3}
        assert list(first.changed) == []
        assert list(first.removed_keys) == []
        source.commit_snapshot_diff(profile, sync_name)

        with conn.cursor() as cur:
            cur.execute(f"UPDATE {source_fq} SET note = '' WHERE id = 1")
            cur.execute(f"UPDATE {source_fq} SET plan = 'pro' WHERE id = 2")
            cur.execute(f"DELETE FROM {source_fq} WHERE id = 3")
            cur.execute(f"INSERT INTO {source_fq} VALUES (4, 'd', 'free')")

        second = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["id"],
            hash_columns=["note", "plan"],
        )
        assert second.is_first_run is False
        assert {row["id"] for row in second.added} == {4}
        changed = list(second.changed)
        assert {row["id"] for row in changed} == {1, 2}
        assert next(row for row in changed if row["id"] == 1)["note"] == ""
        assert list(second.removed_keys) == [{"id": 3}]
        # Simulate a delivery failure/crash: deliberately skip commit.

        retry = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["id"],
            hash_columns=["note", "plan"],
        )
        assert {row["id"] for row in retry.added} == {4}
        assert {row["id"] for row in retry.changed} == {1, 2}
        assert list(retry.removed_keys) == [{"id": 3}]
        source.commit_snapshot_diff(profile, sync_name)

        unchanged = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["id"],
            hash_columns=["note", "plan"],
        )
        assert list(unchanged.added) == []
        assert list(unchanged.changed) == []
        assert list(unchanged.removed_keys) == []
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {source_fq}")
        conn.close()
        source.drop_managed_table(profile, scratch)
        source.drop_managed_table(profile, baseline)


def test_snowflake_diff_incremental_detects_concurrent_scratch_replacement() -> None:
    """A second run cannot be streamed or promoted by the first run."""
    creds = _require_creds()
    profile = _profile(creds)
    first_source = SnowflakeSource()
    second_source = SnowflakeSource()
    suffix = uuid.uuid4().hex[:10]
    source_table = f"DRT_DIFF_RACE_SOURCE_{suffix}"
    source_fq = (
        f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}."
        f"{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{source_table}"
    )
    sync_name = f"snowflake-diff-race-{suffix}"
    query = f'SELECT id AS "id", note AS "note" FROM {source_fq}'
    baseline, scratch = first_source._snapshot_table_names(sync_name)

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {source_fq} (id INTEGER, note VARCHAR)")
            cur.execute(f"INSERT INTO {source_fq} VALUES (1, 'first')")

        first = first_source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["id"],
            hash_columns="all",
        )
        # The second run uses the same fixed scratch name and replaces it with
        # a different per-run token before the first run starts streaming.
        second_source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["id"],
            hash_columns="all",
        )

        yielded: list[dict[str, object]] = []
        with pytest.raises(RuntimeError, match=r"must not run concurrently"):
            for row in first.added:
                yielded.append(row)
        assert yielded == []

        with pytest.raises(RuntimeError, match=r"must not run concurrently"):
            first_source.commit_snapshot_diff(profile, sync_name)
        assert first_source.managed_table_exists(profile, baseline) is False
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {source_fq}")
        conn.close()
        first_source.drop_managed_table(profile, scratch)
        first_source.drop_managed_table(profile, baseline)
