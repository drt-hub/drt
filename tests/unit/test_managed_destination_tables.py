"""First-release managed destination tables (#760): Postgres and MySQL."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from drt.config.models import (
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.destinations.query import fetch_rows_by_keys
from drt.destinations.sql_base import _sql_type_family
from drt.engine.diff import compute_diff


def _postgres_config(**overrides: Any) -> PostgresDestinationConfig:
    values: dict[str, Any] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "app",
        "table": "public.users",
        "upsert_key": ["id"],
        "manage_table": True,
    }
    values.update(overrides)
    return PostgresDestinationConfig(**values)


def _mysql_config(**overrides: Any) -> MySQLDestinationConfig:
    values: dict[str, Any] = {
        "type": "mysql",
        "host": "localhost",
        "dbname": "app",
        "table": "app.users",
        "upsert_key": ["id"],
        "manage_table": True,
    }
    values.update(overrides)
    return MySQLDestinationConfig(**values)


def _connection(rows: list[tuple[Any, ...]]) -> tuple[MagicMock, MagicMock]:
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = rows
    return connection, cursor


def _prepare(
    destination: Any,
    config: Any,
    rows: list[tuple[Any, ...]],
    records: list[dict[str, Any]],
    *,
    dry_run: bool = True,
    options: SyncOptions | None = None,
) -> tuple[Any, MagicMock, MagicMock]:
    connection, cursor = _connection(rows)
    with patch.object(destination, "_dialect_connect", return_value=connection):
        result = destination.prepare_table(
            records,
            config,
            options or SyncOptions(),
            dry_run=dry_run,
        )
    return result, connection, cursor


def test_config_is_strict_and_defaults_are_safe() -> None:
    config = _postgres_config(manage_table=False)
    assert config.manage_table is False
    assert config.on_schema_change == "fail"
    with pytest.raises(ValidationError):
        _postgres_config(manage_table="false")
    with pytest.raises(ValidationError):
        _mysql_config(on_schema_change="drop_and_recreate")


@pytest.mark.parametrize(
    ("type_name", "family"),
    [
        ("NUMERIC(12, 2)", "number"),
        ("BOOLEAN", "boolean"),
        ("TIMESTAMP WITH TIME ZONE", "timestamp"),
        ("DATE", "date"),
        ("JSONB", "json"),
        ("BYTEA", "binary"),
        ("INTEGER[]", "array"),
        ("INTERVAL", "interval"),
    ],
)
def test_source_type_families_are_conservative(type_name: str, family: str) -> None:
    assert _sql_type_family(type_name) == family


@pytest.mark.parametrize("key", ["manage_table", "on_schema_change"])
def test_unsupported_destination_rejects_reserved_keys(key: str) -> None:
    destination = {"type": "slack", "webhook_url_env": "SLACK_WEBHOOK", key: True}
    if key == "on_schema_change":
        destination[key] = "ignore"
    with pytest.raises(ValidationError, match="supported only for postgres and mysql"):
        SyncConfig.model_validate(
            {"name": "users", "model": "select 1", "destination": destination}
        )


def test_reserved_key_validator_ignores_non_mapping_inputs() -> None:
    marker = object()
    assert SyncConfig._reject_unsupported_managed_table_keys(marker) is marker
    value = {"name": "x", "destination": marker}
    assert SyncConfig._reject_unsupported_managed_table_keys(value) is value


def test_absent_keys_do_not_add_an_introspection_call() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="app",
        table="users",
        upsert_key=["id"],
    )
    destination = PostgresDestination()
    with patch.object(destination, "_dialect_connect") as connect:
        prepared = destination.prepare_table([{"id": 1}], config, SyncOptions(), dry_run=False)
    assert prepared.records == [{"id": 1}]
    connect.assert_not_called()


def test_postgres_missing_table_create_is_safe_exact_and_read_only() -> None:
    hostile = 'profile.name"; DROP TABLE audit; --'
    result, connection, cursor = _prepare(
        PostgresDestination(),
        _postgres_config(table="reporting.users", upsert_key=["id"]),
        [],
        [{"id": "1", hostile: "Alice"}],
    )
    assert result.ddl == [
        'CREATE TABLE IF NOT EXISTS "reporting"."users" '
        '("id" TEXT NOT NULL, "profile.name""; DROP TABLE audit; --" TEXT, PRIMARY KEY ("id"))'
    ]
    assert cursor.execute.call_count == 1  # INFORMATION_SCHEMA only; no DDL on dry-run
    connection.commit.assert_not_called()


def test_mysql_missing_table_uses_indexable_fallback_key_and_safe_quotes() -> None:
    hostile = "profile.name`; DROP TABLE audit; --"
    result, _, cursor = _prepare(
        MySQLDestination(),
        _mysql_config(),
        [],
        [{"id": "1", hostile: "Alice"}],
    )
    assert result.ddl == [
        "CREATE TABLE IF NOT EXISTS `app`.`users` "
        "(`id` VARCHAR(255) NOT NULL, `profile.name``; DROP TABLE audit; --` TEXT NULL, "
        "PRIMARY KEY (`id`))"
    ]
    assert cursor.execute.call_count == 1


def test_replace_create_does_not_require_or_constrain_an_upsert_key() -> None:
    options = SyncOptions(mode="replace")
    result, _, _ = _prepare(
        PostgresDestination(),
        _postgres_config(upsert_key=["id"]),
        [],
        [{"name": "Ada"}],
        options=options,
    )

    assert result.ddl == ['CREATE TABLE IF NOT EXISTS "public"."users" ("name" TEXT)']


def test_unqualified_tables_use_the_current_database_schema() -> None:
    _, _, postgres_cursor = _prepare(
        PostgresDestination(),
        _postgres_config(table="users"),
        [],
        [{"id": "1"}],
    )
    assert "table_schema = current_schema()" in postgres_cursor.execute.call_args.args[0]

    _, _, mysql_cursor = _prepare(
        MySQLDestination(),
        _mysql_config(table="users"),
        [],
        [{"id": "1"}],
    )
    assert "table_schema = DATABASE()" in mysql_cursor.execute.call_args.args[0]


def test_create_executes_and_records_only_after_commit() -> None:
    options = SyncOptions()
    result, connection, cursor = _prepare(
        PostgresDestination(),
        _postgres_config(),
        [],
        [{"id": 1, "name": "Ada"}],
        dry_run=False,
        options=options,
    )
    assert cursor.execute.call_args_list[-1].args[0] == result.ddl[0]
    connection.commit.assert_called_once_with()
    assert options._executed_ddl == result.ddl


def test_missing_table_without_management_is_clear_and_plan_is_unavailable() -> None:
    config = _postgres_config(manage_table=False)
    with pytest.raises(ValueError, match="Destination table 'public.users' does not exist"):
        _prepare(PostgresDestination(), config, [], [{"id": 1}])

    options = SyncOptions()
    options._planning = True
    result, _, _ = _prepare(PostgresDestination(), config, [], [{"id": 1}], options=options)
    assert result.ddl == []
    assert "does not exist" in (options._managed_table_unavailable_reason or "")


def test_new_columns_fail_ignore_or_append_before_write() -> None:
    columns = [("id", "bigint", "NO", None, "")]
    records = [{"id": 1, "new_field": "x"}]

    with pytest.raises(ValueError, match=r"new column\(s\).*new_field"):
        _prepare(
            PostgresDestination(),
            _postgres_config(manage_table=False, on_schema_change="fail"),
            columns,
            records,
        )

    ignored, _, _ = _prepare(
        PostgresDestination(),
        _postgres_config(manage_table=False, on_schema_change="ignore"),
        columns,
        records,
    )
    assert ignored.records == [{"id": 1}]
    assert "new_field" in ignored.warnings[0]

    appended, connection, cursor = _prepare(
        PostgresDestination(),
        _postgres_config(manage_table=False, on_schema_change="append_new_columns"),
        columns,
        records,
        dry_run=False,
    )
    assert appended.ddl == ['ALTER TABLE "public"."users" ADD COLUMN "new_field" TEXT NULL']
    assert cursor.execute.call_args_list[-1].args[0] == appended.ddl[0]
    connection.commit.assert_called_once_with()


def test_ignore_policy_filters_later_batches_from_cached_schema() -> None:
    destination = MySQLDestination()
    config = _mysql_config(manage_table=False, on_schema_change="ignore")
    options = SyncOptions()
    first, _, _ = _prepare(
        destination,
        config,
        [("id", "bigint", "NO", None, "")],
        [{"id": 1, "ignored": "x"}],
        options=options,
    )
    assert first.records == [{"id": 1}]
    cached = destination.prepare_table([{"id": 2, "ignored": "y"}], config, options, dry_run=True)
    assert cached.records == [{"id": 2}]


def test_missing_source_or_destination_upsert_key_fails_before_ddl() -> None:
    with pytest.raises(ValueError, match="absent from the first source batch"):
        _prepare(PostgresDestination(), _postgres_config(), [], [{"name": "Ada"}])

    with pytest.raises(ValueError, match=r"missing upsert_key column\(s\).+id"):
        _prepare(
            PostgresDestination(),
            _postgres_config(manage_table=False),
            [("name", "text", "YES", None, "NO")],
            [{"id": "1", "name": "Ada"}],
        )


def test_mysql_append_ddl_is_nullable_and_safely_quoted() -> None:
    result, _, _ = _prepare(
        MySQLDestination(),
        _mysql_config(manage_table=False, on_schema_change="append_new_columns"),
        [("id", "varchar(255)", "NO", None, "")],
        [{"id": "1", "new`field": "x"}],
    )
    assert result.ddl == ["ALTER TABLE `app`.`users` ADD COLUMN `new``field` TEXT NULL"]


def test_required_destination_column_missing_from_source_errors() -> None:
    columns = [
        ("id", "bigint", "NO", None, ""),
        ("tenant", "text", "NO", None, ""),
    ]
    with pytest.raises(ValueError, match=r"required column\(s\).*tenant"):
        _prepare(
            MySQLDestination(),
            _mysql_config(manage_table=False, on_schema_change="ignore"),
            columns,
            [{"id": 1}],
        )


def test_nullable_or_defaulted_destination_columns_may_be_omitted() -> None:
    columns = [
        ("id", "bigint", "NO", None, ""),
        ("optional", "text", "YES", None, ""),
        ("created_at", "datetime", "NO", "CURRENT_TIMESTAMP", ""),
    ]
    result, _, _ = _prepare(
        MySQLDestination(),
        _mysql_config(manage_table=False),
        columns,
        [{"id": 1}],
    )
    assert result.records == [{"id": 1}]


def test_metadata_type_mismatch_fails_except_under_ignore() -> None:
    columns = [("id", "bigint", "NO", None, "")]
    options = SyncOptions()
    options._source_column_types = {"id": "TEXT"}
    with pytest.raises(ValueError, match="TEXT -> bigint"):
        _prepare(
            PostgresDestination(),
            _postgres_config(manage_table=False),
            columns,
            [{"id": "not-an-int"}],
            options=options,
        )

    ignored, _, _ = _prepare(
        PostgresDestination(),
        _postgres_config(manage_table=False, on_schema_change="ignore"),
        columns,
        [{"id": "not-an-int"}],
        options=options,
    )
    assert ignored.records == [{"id": "not-an-int"}]


def test_ddl_failure_names_required_privilege_and_records_nothing() -> None:
    options = SyncOptions()
    connection, cursor = _connection([])
    cursor.execute.side_effect = [None, PermissionError("denied")]
    destination = PostgresDestination()
    with (
        patch.object(destination, "_dialect_connect", return_value=connection),
        pytest.raises(RuntimeError, match="Grant CREATE"),
    ):
        destination.prepare_table([{"id": 1}], _postgres_config(), options, dry_run=False)
    assert options._executed_ddl == []
    connection.rollback.assert_called_once_with()


def test_ddl_failure_preserves_original_error_when_rollback_also_fails() -> None:
    connection, cursor = _connection([])
    cursor.execute.side_effect = [None, PermissionError("denied")]
    connection.rollback.side_effect = RuntimeError("rollback failed")
    destination = PostgresDestination()
    with (
        patch.object(destination, "_dialect_connect", return_value=connection),
        pytest.raises(RuntimeError, match="Grant CREATE") as caught,
    ):
        destination.prepare_table([{"id": 1}], _postgres_config(), SyncOptions(), dry_run=False)
    assert isinstance(caught.value.__cause__, PermissionError)


def test_reset_clears_cached_managed_table_state() -> None:
    destination = MySQLDestination()
    destination._managed_table_checked = True
    destination._managed_allowed_columns = {"id"}
    destination._schema_cache["users"] = {"id": "scalar"}
    destination.reset_write_state(_mysql_config(), SyncOptions())
    assert destination._managed_table_checked is False
    assert destination._managed_allowed_columns is None
    assert destination._schema_cache == {}


def test_plan_diff_treats_a_managed_missing_table_as_empty() -> None:
    options = SyncOptions()
    options._managed_table_missing = True
    diff = compute_diff(
        [{"id": 1, "name": "Ada"}],
        _postgres_config(),
        options,
        limit=20,
    )
    assert diff.supported is True
    assert diff.total_destination_rows == 0
    assert diff.added == [{"id": 1, "name": "Ada"}]


def test_plan_diff_keeps_missing_unmanaged_table_unavailable() -> None:
    options = SyncOptions()
    options._managed_table_unavailable_reason = "Destination table 'users' does not exist"
    diff = compute_diff(
        [{"id": 1}],
        _postgres_config(manage_table=False),
        options,
        limit=20,
    )
    assert diff.supported is False
    assert diff.fallback_reason == "Destination table 'users' does not exist"


def test_plan_diff_does_not_query_columns_that_only_would_be_added() -> None:
    options = SyncOptions()
    options._managed_new_columns = {"new_field"}
    with patch(
        "drt.engine.diff.fetch_rows_by_keys",
        return_value=[{"id": 1}],
    ) as fetch:
        diff = compute_diff(
            [{"id": 1, "new_field": "x"}],
            _postgres_config(manage_table=False, on_schema_change="append_new_columns"),
            options,
            limit=20,
        )
    assert fetch.call_args.kwargs["columns"] == ["id"]
    assert diff.updated == [({"id": 1}, {"id": 1, "new_field": "x"})]


def test_mysql_keyed_plan_read_quotes_hostile_columns() -> None:
    connection, cursor = _connection([])
    cursor.fetchall.return_value = [("1", "Ada")]
    hostile_key = "user.id` OR 1=1 --"
    hostile_value = "profile.name`; DROP TABLE audit; --"
    with patch.object(MySQLDestination, "_connect", return_value=connection):
        rows = fetch_rows_by_keys(
            _mysql_config(upsert_key=[hostile_key]),
            [hostile_key],
            [("1",)],
            [hostile_key, hostile_value],
        )
    statement = cursor.execute.call_args.args[0]
    assert "`user.id`` OR 1=1 --`" in statement
    assert "`profile.name``; DROP TABLE audit; --`" in statement
    assert rows == [{hostile_key: "1", hostile_value: "Ada"}]
