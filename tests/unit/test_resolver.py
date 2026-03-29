"""Tests for the model reference resolver."""

from __future__ import annotations

from pathlib import Path

from drt.config.credentials import BigQueryProfile
from drt.engine.resolver import parse_ref, resolve_model_ref


def _profile(dataset: str = "my_dataset") -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="my_project", dataset=dataset)


# ---------------------------------------------------------------------------
# parse_ref
# ---------------------------------------------------------------------------

def test_parse_ref_single_quotes() -> None:
    assert parse_ref("ref('new_users')") == "new_users"


def test_parse_ref_double_quotes() -> None:
    assert parse_ref('ref("orders")') == "orders"


def test_parse_ref_with_spaces() -> None:
    assert parse_ref("ref( 'my_table' )") == "my_table"


def test_parse_ref_none_for_raw_sql() -> None:
    assert parse_ref("SELECT * FROM orders") is None


def test_parse_ref_none_for_table_name() -> None:
    assert parse_ref("my_dataset.my_table") is None


# ---------------------------------------------------------------------------
# resolve_model_ref
# ---------------------------------------------------------------------------

def test_resolve_ref_to_select(tmp_path: Path) -> None:
    sql = resolve_model_ref("ref('orders')", tmp_path, _profile("sales"))
    assert sql == "SELECT * FROM `sales`.`orders`"


def test_resolve_raw_sql_passthrough(tmp_path: Path) -> None:
    raw = "SELECT id FROM `sales`.`orders` WHERE active = true"
    assert resolve_model_ref(raw, tmp_path, _profile()) == raw


def test_resolve_sql_file_takes_priority(tmp_path: Path) -> None:
    models_dir = tmp_path / "syncs" / "models"
    models_dir.mkdir(parents=True)
    (models_dir / "orders.sql").write_text("SELECT id, name FROM `sales`.`orders`")

    sql = resolve_model_ref("ref('orders')", tmp_path, _profile("sales"))
    assert sql == "SELECT id, name FROM `sales`.`orders`"


def test_resolve_non_ref_string_passthrough(tmp_path: Path) -> None:
    table = "analytics.my_table"
    assert resolve_model_ref(table, tmp_path, _profile()) == table


# ---------------------------------------------------------------------------
# incremental cursor injection
# ---------------------------------------------------------------------------

def test_resolve_incremental_injects_where(tmp_path: Path) -> None:
    sql = resolve_model_ref(
        "ref('events')",
        tmp_path,
        _profile("ds"),
        cursor_field="updated_at",
        last_cursor_value="2024-01-01T00:00:00",
    )
    assert "WHERE updated_at > '2024-01-01T00:00:00'" in sql
    assert "SELECT * FROM `ds`.`events`" in sql


def test_resolve_no_cursor_returns_base_sql(tmp_path: Path) -> None:
    sql = resolve_model_ref(
        "ref('events')",
        tmp_path,
        _profile("ds"),
        cursor_field="updated_at",
        last_cursor_value=None,
    )
    assert sql == "SELECT * FROM `ds`.`events`"
    assert "WHERE" not in sql


def test_resolve_incremental_raw_sql(tmp_path: Path) -> None:
    raw = "SELECT * FROM events WHERE active = true"
    sql = resolve_model_ref(
        raw,
        tmp_path,
        _profile(),
        cursor_field="updated_at",
        last_cursor_value="2024-06-01",
    )
    assert "WHERE updated_at > '2024-06-01'" in sql
    assert raw in sql
