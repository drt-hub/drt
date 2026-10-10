"""``sync.write_policy: fill_empty`` (#1238): config, SQL, engine guard, diff and plan."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SyncOptions
from drt.destinations.base import WritePolicyCapable
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.diff import DiffResult, compute_diff
from drt.engine.plan import build_plan, render_markdown, render_text
from drt.engine.sync import _check_write_policy_supported

# --- config ---------------------------------------------------------------


def test_defaults_overwrite_everything() -> None:
    options = SyncOptions()

    assert options.write_policy == "overwrite" and options.write_policy_overrides == {}
    assert options.uses_fill_empty is False
    assert options.fill_empty_columns(["a", "b"]) == set()


def test_default_fill_empty_with_an_overwrite_override() -> None:
    options = SyncOptions(
        write_policy="fill_empty", write_policy_overrides={"lifecycle_stage": "overwrite"}
    )

    assert options.uses_fill_empty is True
    assert options.fill_empty_columns(["industry", "lifecycle_stage"]) == {"industry"}


def test_default_overwrite_with_a_fill_empty_override() -> None:
    options = SyncOptions(write_policy_overrides={"industry": "fill_empty"})

    assert options.uses_fill_empty is True
    assert options.fill_empty_columns(["industry", "score"]) == {"industry"}


def test_an_overwrite_only_override_is_not_fill_empty() -> None:
    assert SyncOptions(write_policy_overrides={"score": "overwrite"}).uses_fill_empty is False


def test_replace_rejects_fill_empty_but_accepts_the_default() -> None:
    SyncOptions(mode="replace")
    with pytest.raises(ValueError, match="not compatible with mode: replace"):
        SyncOptions(mode="replace", write_policy="fill_empty")
    with pytest.raises(ValueError, match="not compatible with mode: replace"):
        SyncOptions(mode="replace", write_policy_overrides={"a": "fill_empty"})


def test_an_unknown_policy_value_is_rejected() -> None:
    with pytest.raises(ValueError):
        SyncOptions(write_policy="fill_blank")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        SyncOptions(write_policy_overrides={"a": "keep"})  # type: ignore[dict-item]


def test_unseen_overrides_are_reported() -> None:
    options = SyncOptions(write_policy_overrides={"industry": "fill_empty", "tpyo": "overwrite"})

    assert options.unseen_write_policy_overrides(["id", "industry"]) == ["tpyo"]
    assert options.unseen_write_policy_overrides(["industry", "tpyo"]) == []


# --- SQL ------------------------------------------------------------------


def test_postgres_upsert_keeps_a_non_empty_value_for_fill_columns_only() -> None:
    sql = str(
        PostgresDestination._build_upsert_sql(
            "contacts", ["id", "industry", "score"], ["id"], ["industry", "score"], {"industry"}
        )
    )

    assert "CASE WHEN" in sql and "btrim(" in sql and "::text" in sql and "EXCLUDED" in sql
    assert sql.count("CASE WHEN") == 1  # score stays a plain overwrite
    # In ON CONFLICT DO UPDATE an unqualified column is ambiguous with EXCLUDED, so the
    # target is aliased and every reference to the stored value is qualified.
    assert "_drt_target" in sql and sql.count("Identifier('_drt_target')") >= 4
    assert sql.count("EXCLUDED") == 2  # industry (inside the CASE) and score (plain)


def test_postgres_upsert_without_fill_columns_is_unchanged() -> None:
    sql = str(PostgresDestination._build_upsert_sql("t", ["id", "a"], ["id"], ["a"]))

    assert "CASE" not in sql and "EXCLUDED" in sql


def test_postgres_update_only_fill_column_binds_one_placeholder_per_column() -> None:
    sql = str(PostgresDestination._build_update_only_sql("t", ["a", "b"], ["id"], {"a"}))

    # One bound value per SET column plus the key: the fill expression does not duplicate it.
    assert sql.count("Placeholder()") == 3
    assert "CASE WHEN" in sql


def test_mysql_upsert_and_update_only_build_the_fill_expression() -> None:
    upsert = MySQLDestination._build_upsert_sql("t", ["id", "a", "b"], ["a", "b"], {"a"})
    update = MySQLDestination._build_update_only_sql("t", ["a", "b"], ["id"], {"a"})

    assert "`a` = IF(`a` IS NULL OR TRIM(CAST(`a` AS CHAR)) = '', VALUES(`a`), `a`)" in upsert
    assert "`b` = VALUES(`b`)" in upsert
    assert "`a` = IF(`a` IS NULL OR TRIM(CAST(`a` AS CHAR)) = '', %s, `a`)" in update
    assert "`b` = %s" in update
    assert update.count("%s") == 3  # one value per column plus the key, unchanged order


def test_mysql_default_upsert_sql_is_unchanged() -> None:
    sql = MySQLDestination._build_upsert_sql("t", ["id", "a"], ["a"])

    assert "IF(" not in sql and "`a` = VALUES(`a`)" in sql


# --- engine guard ----------------------------------------------------------


class _NoPolicy:
    pass


def test_postgres_and_mysql_declare_the_capability() -> None:
    for destination in (PostgresDestination(), MySQLDestination()):
        assert isinstance(destination, WritePolicyCapable)
        assert "fill_empty" in destination.supported_write_policies()


def test_the_default_policy_needs_no_capability() -> None:
    _check_write_policy_supported(SyncOptions(), _NoPolicy())
    _check_write_policy_supported(
        SyncOptions(write_policy_overrides={"a": "overwrite"}), _NoPolicy()
    )


@pytest.mark.parametrize(
    "options",
    [
        SyncOptions(write_policy="fill_empty"),
        SyncOptions(write_policy_overrides={"a": "fill_empty"}),
    ],
)
def test_a_destination_that_cannot_honour_fill_empty_fails_fast(options: SyncOptions) -> None:
    with pytest.raises(ValueError, match="not supported by _NoPolicy"):
        _check_write_policy_supported(options, _NoPolicy())


# --- diff / plan ------------------------------------------------------------


def _pg_config() -> Any:
    from drt.config.models import PostgresDestinationConfig

    return PostgresDestinationConfig(
        type="postgres",
        host="h",
        dbname="d",
        user="u",
        password="p",
        table="contacts",
        upsert_key=["id"],
    )


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_diff_keeps_non_empty_destination_values_and_counts_them(fetch: MagicMock) -> None:
    fetch.return_value = [
        {"id": 1, "industry": "Retail", "score": 1},  # industry set -> kept
        {"id": 2, "industry": "", "score": 1},  # empty string -> filled
        {"id": 3, "industry": None, "score": 1},  # NULL -> filled
        {"id": 4, "industry": "Retail", "score": 1},  # only a kept difference -> no update
    ]
    records = [
        {"id": 1, "industry": "Software", "score": 2},
        {"id": 2, "industry": "Software", "score": 1},
        {"id": 3, "industry": "Software", "score": 1},
        {"id": 4, "industry": "Software", "score": 1},
    ]
    options = SyncOptions(write_policy_overrides={"industry": "fill_empty"})

    result = compute_diff(records, _pg_config(), options, limit=20)

    assert result.kept_values == 2  # rows 1 and 4
    assert sorted(new["id"] for _old, new in result.updated) == [1, 2, 3]
    row1 = next(new for _old, new in result.updated if new["id"] == 1)
    assert row1["industry"] == "Retail" and row1["score"] == 2  # score still overwritten
    assert DiffResult.changed_fields(*next(p for p in result.updated if p[1]["id"] == 2)) == {
        "industry": ("", "Software")
    }


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_diff_without_a_policy_reports_the_overwrite(fetch: MagicMock) -> None:
    fetch.return_value = [{"id": 1, "industry": "Retail"}]

    result = compute_diff(
        [{"id": 1, "industry": "Software"}], _pg_config(), SyncOptions(), limit=20
    )

    assert result.kept_values == 0 and len(result.updated) == 1


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_a_non_text_column_is_empty_only_when_null(fetch: MagicMock) -> None:
    fetch.return_value = [{"id": 1, "score": 0}, {"id": 2, "score": None}]
    options = SyncOptions(write_policy="fill_empty")

    result = compute_diff(
        [{"id": 1, "score": 9}, {"id": 2, "score": 9}], _pg_config(), options, limit=20
    )

    assert result.kept_values == 1 and [new["id"] for _o, new in result.updated] == [2]


_BASE: dict[str, Any] = {
    "sync_name": "s",
    "sync_mode": "upsert",
    "match_policy": "upsert",
    "destination": "postgres",
    "config_fingerprint": "sha256:cfg",
    "environment_fingerprint": "sha256:env",
    "plan_key": b"k",
    "drt_version": "1.1.0",
    "key_columns": ["id"],
    "created_at": "2026-10-10T00:00:00+00:00",
}


def test_the_plan_carries_and_renders_the_kept_count() -> None:
    diff = DiffResult(
        updated=[({"id": 1, "a": "x"}, {"id": 1, "a": "x", "b": 2})],
        total_source_rows=1,
        total_destination_rows=1,
        kept_values=3,
    )

    plan = build_plan(diff, **_BASE)

    assert plan.to_dict()["summary"]["kept_values"] == 3
    assert "3 existing value(s) kept" in render_text(plan)
    assert "3 existing value(s) kept" in render_markdown(plan)
    assert build_plan(DiffResult(), **_BASE).to_dict()["summary"]["kept_values"] == 0


# --- destinations (mock cursors) ---------------------------------------------


def _pg_dest_config() -> Any:
    from drt.config.models import PostgresDestinationConfig

    return PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="d",
        user="u",
        password="p",
        table="public.contacts",
        upsert_key=["id"],
        introspect_schema=False,
    )


def _mysql_dest_config() -> Any:
    from drt.config.models import MySQLDestinationConfig

    return MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="d",
        user="u",
        password="p",
        table="contacts",
        upsert_key=["id"],
        introspect_schema=False,
    )


def _conn() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value.rowcount = 1
    return conn


def test_postgres_load_runs_the_fill_statement_and_commits() -> None:
    conn = _conn()
    options = SyncOptions(write_policy_overrides={"industry": "fill_empty"})

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = PostgresDestination().load(
            [{"id": 1, "industry": "Software", "score": 2}], _pg_dest_config(), options
        )

    query = str(conn.cursor.return_value.execute.call_args.args[0])
    assert "CASE WHEN" in query and "btrim(" in query and "_drt_target" in query
    assert result.success == 1 and conn.commit.called and not conn.rollback.called


def _with_schema(destination: Any, columns: dict[str, str] | None) -> Any:
    destination._resolve_schema = lambda _config: columns  # type: ignore[method-assign]
    return destination


def test_an_override_that_is_not_a_destination_column_is_refused_before_any_write() -> None:
    conn = _conn()
    options = SyncOptions(write_policy_overrides={"industy": "fill_empty"})  # typo
    destination = _with_schema(PostgresDestination(), {"id": "int", "industry": "text"})

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match=r"\['industy'\].*not columns of the destination"):
            destination.load([{"id": 1, "industry": "x"}], _pg_dest_config(), options)

    assert not conn.cursor.return_value.execute.called  # nothing was attempted


def test_the_check_ignores_case_and_needs_introspection() -> None:
    options = SyncOptions(write_policy_overrides={"Industry": "fill_empty"})

    with patch.object(PostgresDestination, "_connect", return_value=_conn()):
        _with_schema(PostgresDestination(), {"industry": "text"}).load(
            [{"id": 1, "industry": "x"}], _pg_dest_config(), options
        )
        # No introspection (introspect_schema: false / json_columns): nothing to check against,
        # and `drt plan` / `--dry-run --diff` still check the names against the source.
        _with_schema(PostgresDestination(), None).load(
            [{"id": 1, "industry": "x"}], _pg_dest_config(), options
        )


def test_mysql_load_runs_the_fill_statement_and_refuses_an_unknown_override() -> None:
    conn = _conn()
    fill = SyncOptions(write_policy="fill_empty")
    typo = SyncOptions(write_policy_overrides={"industy": "overwrite"})

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        ok = MySQLDestination().load([{"id": 1, "industry": "x"}], _mysql_dest_config(), fill)
        assert "IF(`industry` IS NULL" in conn.cursor.return_value.execute.call_args.args[0]
        assert ok.success == 1 and conn.commit.called
        conn.reset_mock()
        with pytest.raises(ValueError, match="not columns of the destination"):
            _with_schema(MySQLDestination(), {"industry": "text"}).load(
                [{"id": 1, "industry": "x"}], _mysql_dest_config(), typo
            )

    assert not conn.cursor.return_value.execute.called


def test_an_optional_override_column_missing_from_a_batch_is_fine() -> None:
    """The old per-batch check raised here; the destination column list makes it exact."""
    conn = _conn()
    options = SyncOptions(write_policy_overrides={"industry": "fill_empty", "score": "overwrite"})
    destination = _with_schema(
        PostgresDestination(), {"id": "int", "industry": "text", "score": "int"}
    )

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        first = destination.load([{"id": 1, "industry": "a"}], _pg_dest_config(), options)
        second = destination.load([{"id": 2, "score": 3}], _pg_dest_config(), options)

    assert first.success == 1 and second.success == 1 and conn.commit.call_count == 2


# --- emptiness, duplicate keys, typos in the diff -----------------------------


@pytest.mark.parametrize(
    ("value", "empty"),
    [
        (None, True),
        ("", True),
        ("   ", True),  # spaces only: a CHAR(n) column's padding, as the SQL btrim/TRIM
        ("\t", False),  # a tab is a value (btrim / TRIM strip spaces only)
        ("x", False),
        (0, False),
        (False, False),
        (b"", False),
    ],
)
def test_diff_emptiness_matches_the_sql(value: Any, empty: bool) -> None:
    from drt.engine.diff import _is_empty

    assert _is_empty(value) is empty


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_diff_models_first_writer_wins_for_a_repeated_source_key(fetch: MagicMock) -> None:
    fetch.return_value = [{"id": 1, "industry": None}]
    options = SyncOptions(write_policy="fill_empty")

    result = compute_diff(
        [{"id": 1, "industry": "A"}, {"id": 1, "industry": "B"}], _pg_config(), options, limit=20
    )

    # Live SQL stores "A" and then keeps it when "B" arrives.
    assert result.kept_values == 1
    assert [(new["industry"]) for _o, new in result.updated] == ["A", "A"][:1] or len(
        result.updated
    ) == 1
    assert result.updated[0][1]["industry"] == "A"


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_diff_models_a_repeated_key_for_a_new_row(fetch: MagicMock) -> None:
    fetch.return_value = []
    options = SyncOptions(write_policy="fill_empty")

    result = compute_diff(
        [{"id": 9, "industry": "A"}, {"id": 9, "industry": "B"}], _pg_config(), options, limit=20
    )

    assert len(result.added) == 1 and result.kept_values == 1  # the second row finds "A" taken
    assert result.updated == []


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_without_fill_empty_the_diff_is_unchanged_for_repeated_keys(fetch: MagicMock) -> None:
    fetch.return_value = []

    result = compute_diff(
        [{"id": 9, "industry": "A"}, {"id": 9, "industry": "B"}],
        _pg_config(),
        SyncOptions(),
        limit=20,
    )

    assert len(result.added) == 2  # existing behaviour, untouched


@patch("drt.engine.diff.fetch_rows_by_keys")
def test_an_override_naming_a_column_the_source_does_not_produce_makes_the_plan_unavailable(
    fetch: MagicMock,
) -> None:
    options = SyncOptions(write_policy_overrides={"industy": "fill_empty"})

    result = compute_diff([{"id": 1, "industry": "x"}], _pg_config(), options, limit=20)

    assert result.supported is False
    assert "industy" in (result.fallback_reason or "")
    plan = build_plan(result, **_BASE)
    assert plan.available is False and "industy" in (plan.unavailable_reason or "")
    assert not fetch.called


def test_the_plan_schema_keeps_kept_values_optional_so_v1_plans_stay_valid() -> None:
    from drt.engine.plan import PLAN_JSON_SCHEMA

    assert "kept_values" not in PLAN_JSON_SCHEMA["properties"]["summary"]["required"]
    assert "kept_values" in PLAN_JSON_SCHEMA["properties"]["summary"]["properties"]
