"""Bounded extraction tests for ADR 0021 slice 2."""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import (
    BigQueryProfile,
    ClickHouseProfile,
    DatabricksProfile,
    DeltaLakeProfile,
    DuckDBProfile,
    IcebergProfile,
    MySQLProfile,
    PostgresProfile,
    RedshiftProfile,
    RestApiProfile,
    SnowflakeProfile,
    SQLiteProfile,
    SQLServerProfile,
)
from drt.config.profiles import ProfileConfigLike
from drt.engine.backfill import WindowCursorRangeError, validate_window_row
from drt.engine.backfill_extraction import snapshot_numeric_ceiling
from drt.engine.resolver import (
    build_numeric_ceiling_query,
    resolve_model_ref,
    resolve_windowed_model_ref,
    wrap_windowed_query,
)
from drt.sources.base import WindowedIncrementalSource, is_window_capable
from drt.sources.rest_api import RestApiSource

SQL_PROFILES: list[tuple[ProfileConfigLike, str]] = [
    (BigQueryProfile(type="bigquery", project="p", dataset="d"), "`updated_at`"),
    (ClickHouseProfile(type="clickhouse"), "`updated_at`"),
    (DatabricksProfile(type="databricks"), "`updated_at`"),
    (DuckDBProfile(type="duckdb"), '"updated_at"'),
    (MySQLProfile(type="mysql"), "`updated_at`"),
    (PostgresProfile(type="postgres"), '"updated_at"'),
    (RedshiftProfile(type="redshift"), '"updated_at"'),
    (SnowflakeProfile(type="snowflake"), '"UPDATED_AT"'),
    (SQLiteProfile(type="sqlite"), '"updated_at"'),
    (SQLServerProfile(type="sqlserver"), "[updated_at]"),
    (DeltaLakeProfile(type="deltalake"), '"updated_at"'),
    (IcebergProfile(type="iceberg"), '"updated_at"'),
]

TIMESTAMP_WHERE = {
    "bigquery": (
        "`updated_at` >= TIMESTAMP '2025-01-01 00:00:00+00' "
        "AND `updated_at` < TIMESTAMP '2025-01-02 00:00:00+00'"
    ),
    "clickhouse": (
        "`updated_at` >= toDateTime64('2025-01-01 00:00:00', 6, 'UTC') "
        "AND `updated_at` < toDateTime64('2025-01-02 00:00:00', 6, 'UTC')"
    ),
    "databricks": (
        "`updated_at` >= TIMESTAMP '2025-01-01 00:00:00+00:00' "
        "AND `updated_at` < TIMESTAMP '2025-01-02 00:00:00+00:00'"
    ),
    "duckdb": (
        "epoch(\"updated_at\") >= epoch(TIMESTAMPTZ '2025-01-01 00:00:00+00') "
        "AND epoch(\"updated_at\") < epoch(TIMESTAMPTZ '2025-01-02 00:00:00+00')"
    ),
    "mysql": "`updated_at` >= '2025-01-01 00:00:00' AND `updated_at` < '2025-01-02 00:00:00'",
    "postgres": (
        "\"updated_at\" >= TIMESTAMPTZ '2025-01-01 00:00:00+00' "
        "AND \"updated_at\" < TIMESTAMPTZ '2025-01-02 00:00:00+00'"
    ),
    "redshift": (
        "\"updated_at\" >= TIMESTAMPTZ '2025-01-01 00:00:00+00' "
        "AND \"updated_at\" < TIMESTAMPTZ '2025-01-02 00:00:00+00'"
    ),
    "snowflake": (
        "\"UPDATED_AT\" >= TO_TIMESTAMP_TZ('2025-01-01 00:00:00+00:00') "
        "AND \"UPDATED_AT\" < TO_TIMESTAMP_TZ('2025-01-02 00:00:00+00:00')"
    ),
    "sqlite": "\"updated_at\" >= '2025-01-01 00:00:00' AND \"updated_at\" < '2025-01-02 00:00:00'",
    "sqlserver": (
        "[updated_at] >= CAST('2025-01-01 00:00:00' AS datetime2(7)) "
        "AND [updated_at] < CAST('2025-01-02 00:00:00' AS datetime2(7))"
    ),
    "deltalake": (
        "epoch(\"updated_at\") >= epoch(TIMESTAMPTZ '2025-01-01 00:00:00+00') "
        "AND epoch(\"updated_at\") < epoch(TIMESTAMPTZ '2025-01-02 00:00:00+00')"
    ),
    "iceberg": (
        "epoch(\"updated_at\") >= epoch(TIMESTAMPTZ '2025-01-01 00:00:00+00') "
        "AND epoch(\"updated_at\") < epoch(TIMESTAMPTZ '2025-01-02 00:00:00+00')"
    ),
}


@pytest.mark.parametrize(
    ("profile", "quoted"), SQL_PROFILES, ids=[profile.type for profile, _ in SQL_PROFILES]
)
def test_every_sql_source_family_gets_golden_timestamp_wrapper(
    profile: ProfileConfigLike, quoted: str
) -> None:
    sql = wrap_windowed_query(
        "SELECT id, updated_at FROM events",
        profile,
        "updated_at",
        "2025-01-01T09:00:00+09:00",
        "2025-01-02 00:00:00",
    )

    assert sql == (
        "SELECT /* drt:windowed-backfill */ * FROM (\n"
        "SELECT id, updated_at FROM events\n"
        ") AS _drt_base\n"
        f"WHERE {TIMESTAMP_WHERE[profile.type]}"
    )


@pytest.mark.parametrize(
    ("profile", "quoted"), SQL_PROFILES, ids=[profile.type for profile, _ in SQL_PROFILES]
)
def test_every_sql_source_family_gets_golden_integer_wrapper(
    profile: ProfileConfigLike, quoted: str
) -> None:
    sql = wrap_windowed_query("SELECT * FROM events", profile, "updated_at", 0, 100)

    assert sql == (
        "SELECT /* drt:windowed-backfill */ * FROM (\n"
        "SELECT * FROM events\n"
        ") AS _drt_base\n"
        f"WHERE {quoted} >= 0 AND {quoted} < 100"
    )


def test_window_wrapper_quotes_injection_shaped_column_as_one_identifier() -> None:
    profile = PostgresProfile(type="postgres")

    sql = wrap_windowed_query(
        "SELECT * FROM events", profile, 'updated_at"; DROP TABLE users; --', 0, 10
    )

    assert sql == (
        "SELECT /* drt:windowed-backfill */ * FROM (\nSELECT * FROM events\n"
        ") AS _drt_base\nWHERE "
        '"updated_at""; drop table users; --" >= 0 '
        'AND "updated_at""; drop table users; --" < 10'
    )


def test_bigquery_window_identifier_rejects_embedded_backtick() -> None:
    profile = BigQueryProfile(type="bigquery", project="p", dataset="d")

    with pytest.raises(ValueError, match="BigQuery cursor column identifier"):
        wrap_windowed_query("SELECT * FROM events", profile, "updated`at", 0, 10)


@pytest.mark.parametrize(
    ("profile", "expected"),
    [
        (PostgresProfile(type="postgres"), '"mixedcase"'),
        (SnowflakeProfile(type="snowflake"), '"MIXEDCASE"'),
    ],
)
def test_window_identifier_mixed_case_matches_legacy_unquoted_folding(
    profile: ProfileConfigLike, expected: str
) -> None:
    query = wrap_windowed_query("SELECT 1 AS MixedCase", profile, "MixedCase", 0, 2)

    assert f"WHERE {expected} >= 0 AND {expected} < 2" in query


def test_timestamp_wrapper_renders_aware_and_naive_bounds_identically() -> None:
    profile = DuckDBProfile(type="duckdb")

    assert wrap_windowed_query(
        "SELECT * FROM events",
        profile,
        "ts",
        "2025-01-01T09:00:00+09:00",
        "2025-01-01T10:00:00+09:00",
    ) == wrap_windowed_query(
        "SELECT * FROM events",
        profile,
        "ts",
        "2025-01-01 00:00:00",
        "2025-01-01 01:00:00",
    )


@pytest.mark.parametrize("timezone_name", ["Asia/Tokyo", "America/Los_Angeles"])
@pytest.mark.parametrize("cursor_type", ["TIMESTAMPTZ", "TIMESTAMP"])
def test_duckdb_timestamp_windows_are_session_timezone_independent(
    timezone_name: str, cursor_type: str
) -> None:
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    connection.execute(f"SET TimeZone='{timezone_name}'")
    value = "2025-01-01 00:30:00+00" if cursor_type == "TIMESTAMPTZ" else "2025-01-01 00:30:00"
    query = wrap_windowed_query(
        f"SELECT {cursor_type} '{value}' AS ts",
        DuckDBProfile(type="duckdb"),
        "ts",
        "2025-01-01T00:00:00Z",
        "2025-01-01T01:00:00Z",
    )

    expected = connection.execute(f"SELECT {cursor_type} '{value}'").fetchone()[0]
    assert connection.execute(query).fetchall() == [(expected,)]


@pytest.mark.parametrize("builder", [wrap_windowed_query, build_numeric_ceiling_query])
def test_resolver_rejects_nested_cursor_paths(builder: Any) -> None:
    args = ("SELECT 15 AS cursor", DuckDBProfile(type="duckdb"), "payload.updated_at")
    with pytest.raises(ValueError, match="nested cursor paths are not supported"):
        if builder is wrap_windowed_query:
            builder(*args, 10, 20)
        else:
            builder(*args)


@pytest.mark.parametrize(
    "model",
    ["SELECT 1 AS cursor;", "SELECT 1 AS cursor -- trailing comment"],
)
def test_sqlite_window_wrapper_handles_terminal_sql_syntax(model: str) -> None:
    query = wrap_windowed_query(model, SQLiteProfile(type="sqlite"), "cursor", 0, 2)

    assert sqlite3.connect(":memory:").execute(query).fetchall() == [(1,)]


def test_window_wrapper_leaves_internal_semicolons_to_database_syntax_check() -> None:
    query = wrap_windowed_query(
        "SELECT 1 AS cursor; SELECT 2 AS cursor;",
        SQLiteProfile(type="sqlite"),
        "cursor",
        0,
        2,
    )

    with pytest.raises(sqlite3.OperationalError):
        sqlite3.connect(":memory:").execute(query)


@pytest.mark.parametrize("model", ["", "  -- comment", "/* comment */\n-- another"])
def test_window_wrapper_rejects_empty_or_comment_only_model(model: str) -> None:
    with pytest.raises(ValueError, match="empty or contain only comments"):
        wrap_windowed_query(model, SQLiteProfile(type="sqlite"), "cursor", 0, 2)


@pytest.mark.parametrize(
    "model",
    [
        "SELECT 1 AS id, 15 AS cursor;",
        "SELECT 1 AS id, 15 AS cursor -- trailing comment",
    ],
)
def test_sqlite_window_wrapper_executes_trailing_terminators_safely(model: str) -> None:
    query = wrap_windowed_query(
        model,
        SQLiteProfile(type="sqlite"),
        "cursor",
        10,
        20,
    )

    assert sqlite3.connect(":memory:").execute(query).fetchall() == [(1, 15)]


def test_sqlserver_window_wrapper_rejects_leading_cte_after_comments() -> None:
    with pytest.raises(ValueError, match=r"not supported.*SQL Server.*move the CTE"):
        wrap_windowed_query(
            "-- model\n/* hint */ WITH events AS (SELECT 1 AS id) SELECT * FROM events",
            SQLServerProfile(type="sqlserver"),
            "id",
            0,
            2,
        )


def test_sqlserver_window_wrapper_rejects_semicolon_prefixed_cte() -> None:
    with pytest.raises(ValueError, match=r"not supported.*SQL Server.*move the CTE"):
        wrap_windowed_query(
            ";WITH events AS (SELECT 1 AS id) SELECT * FROM events",
            SQLServerProfile(type="sqlserver"),
            "id",
            0,
            2,
        )


def test_windowed_resolver_convenience_resolves_then_wraps(tmp_path: Path) -> None:
    profile = BigQueryProfile(type="bigquery", project="p", dataset="analytics")

    assert resolve_windowed_model_ref(
        "ref('events')", tmp_path, profile, "event_id", 10, 20, sync_name="events"
    ) == (
        "SELECT /* drt:windowed-backfill */ * FROM (\n"
        "SELECT * FROM `analytics`.`events`\n"
        ") AS _drt_base\nWHERE `event_id` >= 10 AND `event_id` < 20"
    )


@pytest.mark.parametrize("template", ["{{ cursor_value }}", "{{ watermark }}"])
def test_windowed_resolver_rejects_legacy_lower_only_templates(
    tmp_path: Path, template: str
) -> None:
    profile = DuckDBProfile(type="duckdb")

    with pytest.raises(ValueError, match=r"sync\.backfill.*legacy lower-only.*template") as exc:
        resolve_model_ref(
            f"SELECT * FROM events WHERE updated_at > '{template}'",
            tmp_path,
            profile,
            cursor_field="updated_at",
            window_bounds=(
                "2025-01-01T00:00:00Z",
                "2025-01-02T00:00:00Z",
            ),
            sync_name="events_backfill",
        )

    assert "events_backfill" in str(exc.value)
    assert template in str(exc.value)


@pytest.mark.parametrize(
    "template",
    ["{{ cursor_value | string }}", "{% if watermark %}SELECT 1{% endif %}"],
)
def test_windowed_resolver_detects_legacy_jinja_ast_references(
    tmp_path: Path, template: str
) -> None:
    with pytest.raises(ValueError, match="legacy lower-only cursor template"):
        resolve_model_ref(
            template,
            tmp_path,
            DuckDBProfile(type="duckdb"),
            cursor_field="updated_at",
            windowed_backfill=True,
        )


def test_plain_cursor_words_in_sql_comments_and_literals_are_not_templates(tmp_path: Path) -> None:
    sql = "SELECT 'cursor_value' AS label -- watermark"

    assert (
        resolve_model_ref(sql, tmp_path, DuckDBProfile(type="duckdb"), windowed_backfill=True)
        == sql
    )


def test_invalid_jinja_falls_back_to_exact_token_detection(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="legacy lower-only cursor template"):
        resolve_model_ref(
            "SELECT '{{ cursor_value }}' {% invalid %}",
            tmp_path,
            DuckDBProfile(type="duckdb"),
            cursor_field="updated_at",
            windowed_backfill=True,
        )


@pytest.mark.parametrize(
    "model",
    [
        "SELECT '{{ cursor_value | }}'",
        "{% if watermark %} SELECT 1 {% invalid %}",
    ],
)
def test_invalid_jinja_fails_closed_for_cursor_names_anywhere_in_blocks(
    tmp_path: Path, model: str
) -> None:
    with pytest.raises(ValueError, match="legacy lower-only cursor template"):
        resolve_model_ref(
            model,
            tmp_path,
            DuckDBProfile(type="duckdb"),
            cursor_field="updated_at",
            windowed_backfill=True,
        )


def test_numeric_backfill_rejects_legacy_template_before_ceiling_exists(tmp_path: Path) -> None:
    profile = DuckDBProfile(type="duckdb")

    with pytest.raises(ValueError, match=r"sync\.backfill.*cursor_value"):
        resolve_model_ref(
            "SELECT * FROM events WHERE event_id > '{{ cursor_value }}'",
            tmp_path,
            profile,
            cursor_field="event_id",
            sync_name="events",
            windowed_backfill=True,
        )


@pytest.mark.parametrize(
    ("value", "expected_error"),
    [
        (10, None),
        (19, None),
        (None, "null"),
        (9, "outside"),
        (20, "outside"),
        (21, "outside"),
    ],
)
def test_numeric_row_range_validation(value: int | None, expected_error: str | None) -> None:
    row = {"event_id": value, "payload": "x"}
    if expected_error == "outside":
        with pytest.raises(WindowCursorRangeError, match="outside requested"):
            validate_window_row(row, "event_id", 10, 20)
        return

    error = validate_window_row(row, "event_id", 10, 20, batch_index=7)
    if expected_error == "null":
        assert error is not None
        assert error.batch_index == 7
        assert "null or missing" in error.error_message
    else:
        assert error is None


@pytest.mark.parametrize(
    ("value", "raises"),
    [
        ("2025-01-01T00:00:00Z", False),
        (datetime(2025, 1, 1, 12, tzinfo=timezone.utc), False),
        ("2025-01-01T23:59:59Z", False),
        ("2024-12-31T23:59:59Z", True),
        ("2025-01-02T00:00:00Z", True),
    ],
)
def test_timestamp_row_range_validation(value: object, raises: bool) -> None:
    if raises:
        with pytest.raises(WindowCursorRangeError):
            validate_window_row(
                {"updated_at": value},
                "updated_at",
                "2025-01-01T00:00:00Z",
                "2025-01-02T00:00:00Z",
            )
    else:
        assert (
            validate_window_row(
                {"updated_at": value},
                "updated_at",
                "2025-01-01T00:00:00Z",
                "2025-01-02T00:00:00Z",
            )
            is None
        )


@pytest.mark.parametrize(
    ("record", "low", "high", "message"),
    [
        ({"cursor": "10"}, 0, 20, "non-integral"),
        ({"cursor": 10}, "2025-01-01", "2025-01-02", "non-timestamp"),
        ({"cursor": "not-a-time"}, "2025-01-01", "2025-01-02", "ISO-8601"),
    ],
)
def test_row_range_validation_rejects_cursor_type_or_parse_errors(
    record: dict[str, object], low: str | int, high: str | int, message: str
) -> None:
    with pytest.raises(WindowCursorRangeError, match=message):
        validate_window_row(record, "cursor", low, high)


@pytest.mark.parametrize(
    ("low", "high", "message"),
    [
        (0, "2025-01-02", "same timestamp or integer type"),
        (10, 10, "low must be less"),
        ("2025-01-02", "2025-01-01", "low must be less"),
    ],
)
def test_row_range_validation_rejects_invalid_window_bounds(
    low: str | int, high: str | int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_window_row({"cursor": low}, "cursor", low, high)


class _CeilingSource:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.queries.append(query)
        yield from self.rows

    def test_connection(self, config: ProfileConfigLike) -> bool:
        return True


def test_numeric_ceiling_snapshots_max_once_and_adds_one() -> None:
    source = _CeilingSource([{"_drt_cursor_max": 41}])
    profile = PostgresProfile(type="postgres")

    ceiling = snapshot_numeric_ceiling(source, "SELECT event_id FROM events", profile, "event_id")

    assert ceiling == 42
    assert source.queries == [
        'SELECT MAX("event_id") AS "_drt_cursor_max" '
        "FROM (\nSELECT event_id FROM events\n) AS _drt_base"
    ]


def test_empty_numeric_source_has_no_ceiling_window() -> None:
    source = _CeilingSource([{"_drt_cursor_max": None}])
    profile = BigQueryProfile(type="bigquery", project="p", dataset="d")

    assert (
        snapshot_numeric_ceiling(source, "SELECT event_id FROM events", profile, "event_id") is None
    )


def test_numeric_ceiling_accepts_driver_case_folded_alias() -> None:
    source = _CeilingSource([{"_DRT_CURSOR_MAX": 9}])
    profile = SnowflakeProfile(type="snowflake")

    assert (
        snapshot_numeric_ceiling(source, "SELECT event_id FROM events", profile, "event_id") == 10
    )


def test_numeric_ceiling_rejects_missing_result_row() -> None:
    source = _CeilingSource([])
    profile = DuckDBProfile(type="duckdb")

    with pytest.raises(RuntimeError, match="MAX query returned no result row"):
        snapshot_numeric_ceiling(source, "SELECT event_id FROM events", profile, "event_id")


def test_numeric_ceiling_rejects_missing_result_column() -> None:
    source = _CeilingSource([{"wrong_alias": 9}])
    profile = DuckDBProfile(type="duckdb")

    with pytest.raises(RuntimeError, match="expected _drt_cursor_max result column"):
        snapshot_numeric_ceiling(source, "SELECT event_id FROM events", profile, "event_id")


def test_numeric_ceiling_rejects_non_integral_max() -> None:
    source = _CeilingSource([{"_drt_cursor_max": 1.5}])
    profile = DuckDBProfile(type="duckdb")

    with pytest.raises(TypeError, match="integral cursor_field"):
        snapshot_numeric_ceiling(source, "SELECT event_id FROM events", profile, "event_id")


def test_numeric_ceiling_rejects_signed_64_bit_limit() -> None:
    source = _CeilingSource([{"_drt_cursor_max": 9223372036854775807}])

    with pytest.raises(
        ValueError,
        match=r"cursor is at the 64-bit limit; set an explicit backfill\.end",
    ):
        snapshot_numeric_ceiling(
            source,
            "SELECT event_id FROM events",
            DuckDBProfile(type="duckdb"),
            "event_id",
        )


def test_numeric_ceiling_rejects_non_sql_source_without_explicit_end() -> None:
    source = _CeilingSource([])
    profile = RestApiProfile(type="rest_api", url="https://example.com")

    with pytest.raises(ValueError, match=r"non-SQL.*explicit sync\.backfill\.end"):
        snapshot_numeric_ceiling(source, "ignored", profile, "event_id")


def test_numeric_ceiling_query_quotes_alias_and_cursor() -> None:
    profile = SQLServerProfile(type="sqlserver")
    assert build_numeric_ceiling_query("SELECT event_id FROM events", profile, "event_id") == (
        "SELECT MAX([event_id]) AS [_drt_cursor_max] "
        "FROM (\nSELECT event_id FROM events\n) AS _drt_base"
    )


def test_rest_source_structurally_conforms_to_window_capability() -> None:
    assert isinstance(RestApiSource(), WindowedIncrementalSource)
    assert is_window_capable(RestApiSource())


def test_source_without_extract_window_does_not_conform() -> None:
    assert not isinstance(_CeilingSource([]), WindowedIncrementalSource)
    assert not is_window_capable(_CeilingSource([]))


def test_presence_only_protocol_does_not_accept_bad_window_signature() -> None:
    class Bad:
        def extract_window(
            self, query: str, profile: ProfileConfigLike
        ) -> Iterator[dict[str, Any]]:
            yield {}

    assert isinstance(Bad(), WindowedIncrementalSource)
    assert not is_window_capable(Bad())


def test_window_capability_rejects_async_callables() -> None:
    class CoroutineSource:
        async def extract_window(
            self, query: str, profile: ProfileConfigLike, low: str | int, high: str | int
        ) -> Iterator[dict[str, Any]]:
            return iter(())

    class AsyncGeneratorSource:
        async def extract_window(
            self, query: str, profile: ProfileConfigLike, low: str | int, high: str | int
        ) -> AsyncIterator[dict[str, Any]]:
            yield {}

    for source in (CoroutineSource(), AsyncGeneratorSource()):
        assert isinstance(source, WindowedIncrementalSource)
        assert not is_window_capable(source)
