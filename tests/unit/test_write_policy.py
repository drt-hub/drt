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

    assert "CASE WHEN" in sql and "::text = ''" in sql and "EXCLUDED" in sql
    assert sql.count("CASE WHEN") == 1  # score stays a plain overwrite
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

    assert "`a` = IF(`a` IS NULL OR CAST(`a` AS CHAR) = '', VALUES(`a`), `a`)" in upsert
    assert "`b` = VALUES(`b`)" in upsert
    assert "`a` = IF(`a` IS NULL OR CAST(`a` AS CHAR) = '', %s, `a`)" in update
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
    assert "CASE WHEN" in query and "::text = ''" in query
    assert result.success == 1 and conn.commit.called and not conn.rollback.called


def test_postgres_typo_in_an_override_rolls_back_instead_of_overwriting() -> None:
    conn = _conn()
    options = SyncOptions(write_policy_overrides={"industy": "fill_empty"})  # typo

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match=r"\['industy'\]"):
            PostgresDestination().load(
                [{"id": 1, "industry": "Software"}], _pg_dest_config(), options
            )

    assert conn.rollback.called and not conn.commit.called


def test_mysql_load_runs_the_fill_statement_and_a_typo_rolls_back() -> None:
    conn = _conn()
    fill = SyncOptions(write_policy="fill_empty")
    typo = SyncOptions(write_policy_overrides={"industy": "overwrite"})

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        ok = MySQLDestination().load([{"id": 1, "industry": "x"}], _mysql_dest_config(), fill)
        assert "IF(`industry` IS NULL" in conn.cursor.return_value.execute.call_args.args[0]
        assert ok.success == 1 and conn.commit.called
        conn.reset_mock()
        with pytest.raises(ValueError, match=r"\['industy'\]"):
            MySQLDestination().load([{"id": 1, "industry": "x"}], _mysql_dest_config(), typo)

    assert conn.rollback.called and not conn.commit.called


def test_a_heterogeneous_batch_only_needs_each_override_somewhere() -> None:
    conn = _conn()
    options = SyncOptions(write_policy_overrides={"industry": "fill_empty", "score": "overwrite"})

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = PostgresDestination().load(
            [{"id": 1, "industry": "a"}, {"id": 2, "score": 3}], _pg_dest_config(), options
        )

    assert result.success == 2 and conn.commit.called
