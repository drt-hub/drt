"""First-release managed destination tables (#760): Postgres and MySQL."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from drt.config.credentials import BigQueryProfile
from drt.config.models import (
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.destinations.query import fetch_all_keys, fetch_rows_by_keys
from drt.destinations.sql_base import BaseSqlDestination, DestinationTableColumn, _sql_type_family
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
    if isinstance(destination, MySQLDestination) and rows:
        key_rows = [
            ("PRIMARY", column, position)
            for position, column in enumerate(config.upsert_key, start=1)
        ]
        cursor.fetchall.side_effect = [rows, key_rows]
    elif not dry_run and not rows:
        described = []
        for column in {key for record in records for key in record}:
            if isinstance(destination, MySQLDestination):
                described.append((column, "text", "NO", None, ""))
            else:
                described.append((column, "text", "NO", None, "NO"))
        if isinstance(destination, MySQLDestination):
            key_rows = [
                ("PRIMARY", column, position)
                for position, column in enumerate(config.upsert_key, start=1)
            ]
            cursor.fetchall.side_effect = [[], described, key_rows]
        else:
            cursor.fetchall.side_effect = [[], described]
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


def test_declared_portable_types_drive_create_and_append_ddl() -> None:
    options = SyncOptions()
    options._source_column_types = {
        "id": "integer",
        "score": "number",
        "active": "boolean",
        "seen_at": "timestamp",
        "day": "date",
        "payload": "json",
    }
    records = [{name: 1 for name in options._source_column_types}]

    postgres, _, _ = _prepare(
        PostgresDestination(), _postgres_config(), [], records, options=options
    )
    assert postgres.ddl == [
        'CREATE TABLE IF NOT EXISTS "public"."users" '
        '("id" BIGINT NOT NULL, "score" NUMERIC, "active" BOOLEAN, '
        '"seen_at" TIMESTAMPTZ, "day" DATE, "payload" JSONB, PRIMARY KEY ("id"))'
    ]

    mysql, _, _ = _prepare(MySQLDestination(), _mysql_config(), [], records, options=options)
    assert mysql.ddl == [
        "CREATE TABLE IF NOT EXISTS `app`.`users` "
        "(`id` BIGINT NOT NULL, `score` DECIMAL(65, 30) NULL, `active` BOOLEAN NULL, "
        "`seen_at` DATETIME(6) NULL, `day` DATE NULL, `payload` JSON NULL, "
        "PRIMARY KEY (`id`))"
    ]

    append_options = SyncOptions()
    append_options._source_column_types = {"id": "integer", "payload": "json"}
    appended, _, _ = _prepare(
        PostgresDestination(),
        _postgres_config(manage_table=False, on_schema_change="append_new_columns"),
        [("id", "bigint", "NO", None, "")],
        [{"id": 1, "payload": {"ok": True}}],
        options=append_options,
    )
    assert appended.ddl == [
        'ALTER TABLE "public"."users" ADD COLUMN IF NOT EXISTS "payload" JSONB NULL'
    ]


def test_engine_maps_declared_types_to_destination_names_and_supplements_metadata(
    tmp_path: Path,
) -> None:
    from collections.abc import Iterator

    from drt.destinations.base import SyncResult, TablePreparation
    from drt.engine.sync import run_sync

    class TypedSource:
        def extract(
            self, query: str, config: Any, *, query_tags: dict[str, str] | None = None
        ) -> Iterator[dict[str, Any]]:
            del query, config, query_tags
            yield {"raw_id": 1, "extra": {"ok": True}}

        def source_column_types(self) -> dict[str, str]:
            return {"raw_id": "TEXT", "extra": "JSON"}

        def test_connection(self, config: Any) -> bool:
            return True

    class CapturingDestination:
        seen_types: dict[str, str] = {}

        def prepare_table(
            self,
            records: list[dict[str, Any]],
            config: Any,
            sync_options: SyncOptions,
            *,
            dry_run: bool,
        ) -> TablePreparation:
            del config, dry_run
            self.seen_types = dict(sync_options._source_column_types)
            return TablePreparation(records)

        def reset_write_state(self, config: Any, sync_options: SyncOptions) -> None:
            del config, sync_options

        def load(
            self, records: list[dict[str, Any]], config: Any, sync_options: SyncOptions
        ) -> SyncResult:
            del config, sync_options
            return SyncResult(success=len(records))

    sync = SyncConfig.model_validate(
        {
            "name": "typed",
            "model": "ref('rows')",
            "destination": {"type": "rest_api", "url": "https://example.test"},
            "columns": [{"name": "raw_id", "type": "integer"}],
            "sync": {"field_mappings": {"raw_id": "id"}},
        }
    )
    destination = CapturingDestination()
    run_sync(
        sync,
        TypedSource(),
        destination,
        BigQueryProfile(type="bigquery", project="p", dataset="d"),
        tmp_path,
    )
    assert destination.seen_types == {"raw_id": "TEXT", "extra": "JSON", "id": "integer"}


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
    assert any(call.args[0] == result.ddl[0] for call in cursor.execute.call_args_list)
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
    assert appended.ddl == [
        'ALTER TABLE "public"."users" ADD COLUMN IF NOT EXISTS "new_field" TEXT NULL'
    ]
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


@pytest.mark.parametrize(
    ("policy", "expected_second", "expected_schema", "raises"),
    [
        ("fail", None, {"id"}, True),
        ("ignore", {"id": 2}, {"id"}, False),
        ("append_new_columns", {"id": 2, "email": "a@b.test"}, {"id", "email"}, False),
    ],
)
def test_batch_size_one_reconciles_a_column_first_seen_in_the_second_batch(
    tmp_path: Path,
    policy: str,
    expected_second: dict[str, Any] | None,
    expected_schema: set[str],
    raises: bool,
) -> None:
    from collections.abc import Iterator

    from drt.destinations.base import SyncResult
    from drt.engine.sync import run_sync

    class TwoRowSource:
        def extract(
            self, query: str, config: Any, *, query_tags: dict[str, str] | None = None
        ) -> Iterator[dict[str, Any]]:
            del query, config, query_tags
            yield {"id": 1}
            yield {"id": 2, "email": "a@b.test"}

        def test_connection(self, config: Any) -> bool:
            return True

    class RecordingManagedDestination(BaseSqlDestination):
        def __init__(self) -> None:
            super().__init__()
            self.schema = {"id"}
            self.writes: list[dict[str, Any]] = []

        def _dialect_connect(self, config: Any, query_tags: Any = None) -> MagicMock:
            del config, query_tags
            return MagicMock()

        def _managed_describe_table(self, cur: Any, config: Any) -> list[DestinationTableColumn]:
            del cur, config
            return [
                DestinationTableColumn(name, "text", name != "id", False)
                for name in sorted(self.schema)
            ]

        def _managed_build_create_table(self, *args: Any, **kwargs: Any) -> str:
            raise AssertionError("the test table already exists")

        def _managed_build_add_column(
            self, config: Any, column: str, source_type: str | None
        ) -> str:
            del config, source_type
            return f"ADD {column}"

        def _execute_managed_ddl(
            self,
            conn: Any,
            cur: Any,
            statement: str,
            privilege: str,
            table: str,
            *,
            column: str | None = None,
            config: Any = None,
        ) -> bool:
            del conn, cur, statement, privilege, table, config
            assert column is not None
            self.schema.add(column)
            return True

        def load(
            self, records: list[dict[str, Any]], config: Any, sync_options: SyncOptions
        ) -> SyncResult:
            prepared = self.prepare_table(records, config, sync_options, dry_run=False)
            self.writes.extend(prepared.records)
            return SyncResult(success=len(prepared.records), ddl=prepared.ddl)

    sync = SyncConfig.model_validate(
        {
            "name": "late_column",
            "model": "ref('rows')",
            "destination": {
                "type": "postgres",
                "host": "localhost",
                "dbname": "app",
                "table": "users",
                "upsert_key": ["id"],
                "manage_table": False,
                "on_schema_change": policy,
            },
            "sync": {"batch_size": 1},
        }
    )
    destination = RecordingManagedDestination()
    call = lambda: run_sync(  # noqa: E731 - compact shared assertion setup
        sync,
        TwoRowSource(),
        destination,
        BigQueryProfile(type="bigquery", project="p", dataset="d"),
        tmp_path,
    )
    if raises:
        with pytest.raises(ValueError, match="new column.*email"):
            call()
    else:
        result = call()
        assert result.success == 2
        assert destination.writes[1] == expected_second
    assert destination.schema == expected_schema
    assert destination.writes[:1] == [{"id": 1}]


@pytest.mark.parametrize(
    ("policy", "expected_records", "expected_ddl"),
    [
        ("ignore", {"id": 2}, []),
        (
            "append_new_columns",
            {"id": 2, "email": "a@b.test"},
            ['ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "email" TEXT NULL'],
        ),
    ],
)
def test_missing_table_dry_run_reconciles_late_batch_shape(
    policy: str, expected_records: dict[str, Any], expected_ddl: list[str]
) -> None:
    destination = PostgresDestination()
    config = _postgres_config(table="users", on_schema_change=policy)
    options = SyncOptions()
    first, _, _ = _prepare(destination, config, [], [{"id": 1}], options=options)
    assert first.ddl == [
        'CREATE TABLE IF NOT EXISTS "users" ("id" TEXT NOT NULL, PRIMARY KEY ("id"))'
    ]
    second, _, _ = _prepare(
        destination,
        config,
        [],
        [{"id": 2, "email": "a@b.test"}],
        options=options,
    )
    assert second.records == [expected_records]
    assert second.ddl == expected_ddl


def test_missing_table_dry_run_fail_policy_rejects_late_batch_column() -> None:
    destination = PostgresDestination()
    config = _postgres_config(table="users", on_schema_change="fail")
    options = SyncOptions()
    _prepare(destination, config, [], [{"id": 1}], options=options)
    with pytest.raises(ValueError, match="email.*planned destination table"):
        _prepare(
            destination,
            config,
            [],
            [{"id": 2, "email": "a@b.test"}],
            options=options,
        )


def test_missing_source_or_destination_upsert_key_fails_before_ddl() -> None:
    with pytest.raises(ValueError, match="absent from the source batch"):
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


def test_mysql_existing_table_requires_unique_upsert_key_before_write() -> None:
    connection, cursor = _connection([])
    cursor.fetchall.side_effect = [
        [("id", "bigint", "NO", None, "")],
        [],
    ]
    destination = MySQLDestination()
    with (
        patch.object(destination, "_dialect_connect", return_value=connection),
        pytest.raises(ValueError, match=r"PRIMARY or UNIQUE index.*\['id'\]"),
    ):
        destination.prepare_table(
            [{"id": 1}], _mysql_config(manage_table=False), SyncOptions(), dry_run=False
        )


def test_mysql_rejects_case_insensitive_source_column_collision_before_connect() -> None:
    destination = MySQLDestination()
    with (
        patch.object(destination, "_dialect_connect") as connect,
        pytest.raises(ValueError, match=r"Name.*name.*case-insensitive"),
    ):
        destination.prepare_table(
            [{"id": 1, "Name": "A", "name": "B"}],
            _mysql_config(),
            SyncOptions(),
            dry_run=False,
        )
    connect.assert_not_called()


def test_mysql_duplicate_column_race_reintrospects_and_continues() -> None:
    connection, cursor = _connection([])
    columns = [("id", "bigint", "NO", None, "")]
    cursor.fetchall.side_effect = [
        columns,
        [("PRIMARY", "id", 1)],
        columns + [("email", "text", "YES", None, "")],
    ]
    cursor.execute.side_effect = [
        None,
        None,
        RuntimeError(1060, "Duplicate column name 'email'"),
        None,
    ]
    options = SyncOptions()
    destination = MySQLDestination()
    with patch.object(destination, "_dialect_connect", return_value=connection):
        result = destination.prepare_table(
            [{"id": 1, "email": "a@b.test"}],
            _mysql_config(manage_table=False, on_schema_change="append_new_columns"),
            options,
            dry_run=False,
        )
    assert result.ddl == []
    assert options._executed_ddl == []


def test_non_permission_ddl_failure_keeps_redacted_database_error() -> None:
    connection, cursor = _connection([])
    cursor.execute.side_effect = [
        None,
        RuntimeError("database syntax error; dsn=postgres://me:secret@db/bad"),
    ]
    destination = PostgresDestination()
    with (
        patch.object(destination, "_dialect_connect", return_value=connection),
        pytest.raises(RuntimeError, match="database syntax error") as caught,
    ):
        destination.prepare_table([{"id": 1}], _postgres_config(), SyncOptions(), dry_run=False)
    assert "postgres://" not in str(caught.value)
    assert "Grant CREATE" not in str(caught.value)


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
    destination._managed_known_columns = {"id": "id"}
    destination._managed_allowed_columns = {"id"}
    destination._managed_dry_run_created = True
    destination._schema_cache["users"] = {"id": "scalar"}
    destination.reset_write_state(_mysql_config(), SyncOptions())
    assert destination._managed_table_checked is False
    assert destination._managed_known_columns == {}
    assert destination._managed_allowed_columns is None
    assert destination._managed_dry_run_created is False
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


def test_mysql_mirror_plan_scan_quotes_hostile_key_and_scope_columns() -> None:
    connection, cursor = _connection([])
    hostile_key = "user`id"
    hostile_scope = "tenant`id"
    with patch.object(MySQLDestination, "_connect", return_value=connection):
        fetch_all_keys(
            _mysql_config(upsert_key=[hostile_key]),
            [hostile_key],
            [hostile_scope],
            [("acme",)],
        )
    statement = cursor.execute.call_args.args[0]
    assert statement == "SELECT `user``id` FROM `app`.`users` WHERE `tenant``id` IN (%s)"
