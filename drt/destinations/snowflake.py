"""Snowflake destination — write data back to Snowflake tables.

Supports:
- INSERT (append)
- MERGE (upsert using key columns)

Install: snowflake-connector-python.
"""

from __future__ import annotations

from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, SnowflakeDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class SnowflakeDestination:
    """Write records into Snowflake tables."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SnowflakeDestinationConfig)
        if not records:
            return SyncResult()

        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "Snowflake destination requires: pip install drt-core[snowflake]"
            ) from e

        account = resolve_env(None, config.account_env)
        user = resolve_env(None, config.user_env)
        password = resolve_env(None, config.password_env)

        if not account or not user or not password:
            raise ValueError(
                "Missing Snowflake credentials. Check environment variables or secrets.toml."
            )

        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema_,
        )

        result = SyncResult()

        try:
            with conn.cursor() as cur:
                columns = list(records[0].keys())
                col_list = ", ".join(columns)

                placeholders = ", ".join(["%s"] * len(columns))

                table_fq = f"{config.database}.{config.schema_}.{config.table}"

                if config.mode == "insert":
                    sql = f"""
                        INSERT INTO {table_fq} ({col_list})
                        VALUES ({placeholders})
                    """

                    for i, row in enumerate(records):
                        try:
                            cur.execute(sql, list(row.values()))
                            result.success += 1
                        except Exception as e:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=i,
                                    record_preview=str(row)[:200],
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )
                            if sync_options.on_error == "fail":
                                raise

                elif config.mode == "merge":
                    if not config.upsert_key:
                        raise ValueError("upsert_key is required for merge mode")

                    key_clause = " AND ".join(
                        [f"target.{k} = source.{k}" for k in config.upsert_key]
                    )

                    update_cols = [c for c in columns if c not in config.upsert_key]
                    update_clause = ", ".join(
                        [f"{c} = source.{c}" for c in update_cols]
                    )

                    insert_cols = col_list
                    insert_vals = ", ".join([f"source.{c}" for c in columns])

                    staging_table = f"TMP_{config.table.upper()}"

                    cur.execute(f"CREATE TEMP TABLE {staging_table} LIKE {table_fq}")

                    for i, row in enumerate(records):
                        try:
                            cur.execute(
                                f"""
                                INSERT INTO {staging_table} ({col_list})
                                VALUES ({placeholders})
                                """,
                                list(row.values()),
                            )
                        except Exception as e:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=i,
                                    record_preview=str(row)[:200],
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )
                            if sync_options.on_error == "fail":
                                raise

                    matched_clause = (
                        f"WHEN MATCHED THEN UPDATE SET {update_clause}"
                        if update_cols
                        else ""
                    )

                    merge_sql = f"""
                        MERGE INTO {table_fq} target
                        USING {staging_table} source
                        ON {key_clause}
                        {matched_clause}
                        WHEN NOT MATCHED THEN INSERT ({insert_cols})
                        VALUES ({insert_vals})
                    """

                    cur.execute(merge_sql)
                    result.success += len(records) - result.failed

                else:
                    raise ValueError(f"Unsupported mode: {config.mode}")

        finally:
            conn.close()

        return result

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, SnowflakeDestinationConfig)
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "Snowflake destination requires: pip install drt-core[snowflake]"
            ) from e

        account = resolve_env(None, config.account_env)
        user = resolve_env(None, config.user_env)
        password = resolve_env(None, config.password_env)

        if not account or not user or not password:
            raise ValueError(
                "Missing Snowflake credentials. Check environment variables or secrets.toml."
            )

        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema_,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            conn.close()