"""Schema-aware serialization for the Databricks destination (#317, Layer 3).

Shape-asserting tests for the write-path wiring: json-category columns bind via
``from_json(%s, '<ddl>')`` (STRUCT/ARRAY/MAP) or ``parse_json(%s)`` (VARIANT),
scalars pass through, and the no-json path stays byte-identical. A live
round-trip against a real warehouse is the DWH smoke harness's job (#674); here
we assert the generated SQL + binds, plus the JSON encoding round-trips.
"""

from __future__ import annotations

import json
from typing import Any

from drt.config.models import DatabricksDestinationConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination, _bind_row, _value_clause


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *a: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((" ".join(sql.split()), params))

    def fetchall(self) -> list[Any]:
        return []


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur

    def cursor(self) -> _FakeCursor:
        return self._cur

    def close(self) -> None:
        pass


def _cfg(**kw: Any) -> DatabricksDestinationConfig:
    base: dict[str, Any] = dict(
        type="databricks",
        host_env="H",
        http_path_env="P",
        token_env="T",
        catalog="main",
        schema="default",
        table="t",
        mode="insert",
    )
    base.update(kw)
    return DatabricksDestinationConfig(**base)


def _load(
    records: list[dict[str, Any]],
    category: dict[str, str] | None,
    ddls: dict[str, str] | None,
    *,
    mode: str = "insert",
    sync: SyncOptions | None = None,
    upsert_key: list[str] | None = None,
) -> list[tuple[str, Any]]:
    cfg = _cfg(mode=mode, upsert_key=upsert_key)
    dest = DatabricksDestination()
    cur = _FakeCursor()
    dest._connect = lambda c: _FakeConn(cur)  # type: ignore[method-assign]
    dest._schema_cache = {"t": category}
    dest._ddl_cache = {"t": ddls}
    dest.load(records, cfg, sync or SyncOptions(mode="full"))
    return cur.calls


# --- unit-level: _value_clause / _bind_row -------------------------------------


def test_value_clause_no_json_is_values_form() -> None:
    clause, json_cols = _value_clause(["id", "name"], {"id": "scalar", "name": "scalar"}, None)
    assert clause == "VALUES (%s, %s)"
    assert json_cols == []


def test_value_clause_no_map_is_values_form() -> None:
    # Introspection unavailable (None) → unchanged behaviour.
    clause, json_cols = _value_clause(["id", "name"], None, None)
    assert clause == "VALUES (%s, %s)"
    assert json_cols == []


def test_value_clause_struct_array_use_from_json_with_ddl() -> None:
    clause, json_cols = _value_clause(
        ["id", "profile", "tags"],
        {"id": "scalar", "profile": "json", "tags": "json"},
        {"profile": "struct<a: int>", "tags": "array<string>"},
    )
    assert clause.startswith("SELECT ")
    assert "from_json(%s, 'struct<a: int>')" in clause
    assert "from_json(%s, 'array<string>')" in clause
    assert json_cols == ["profile", "tags"]


def test_value_clause_variant_uses_parse_json() -> None:
    # json-category but absent from ddls → VARIANT → parse_json (no DDL).
    clause, json_cols = _value_clause(
        ["id", "doc"], {"id": "scalar", "doc": "json"}, {}
    )
    assert "parse_json(%s)" in clause
    assert "from_json" not in clause
    assert json_cols == ["doc"]


def test_value_clause_case_insensitive_column_match() -> None:
    # information_schema lower-cases; record key may differ in case.
    clause, json_cols = _value_clause(["Tags"], {"tags": "json"}, {"tags": "array<int>"})
    assert "from_json(%s, 'array<int>')" in clause
    assert json_cols == ["Tags"]


def test_bind_row_json_dumps_only_json_columns() -> None:
    row = {"id": 1, "profile": {"city": "London"}, "name": "Al"}
    bound = _bind_row(row, ["id", "profile", "name"], ["profile"])
    assert bound[0] == 1
    assert bound[1] == json.dumps({"city": "London"})
    assert bound[2] == "Al"


def test_bind_row_encoding_round_trips() -> None:
    value = {"nested": [1, 2, {"k": "v"}], "flag": True}
    bound = _bind_row({"c": value}, ["c"], ["c"])
    assert json.loads(bound[0]) == value  # faithful round-trip through JSON


# --- write-path integration (all four paths) -----------------------------------


def test_insert_path_wraps_and_binds() -> None:
    calls = _load(
        [{"id": 1, "profile": {"city": "London"}, "blob": {"k": 1}}],
        {"id": "scalar", "profile": "json", "blob": "json"},
        {"profile": "struct<city: string>"},  # blob absent → VARIANT
    )
    sql, params = calls[0]
    assert sql.startswith("INSERT INTO main.default.t (id, profile, blob) SELECT")
    assert "from_json(%s, 'struct<city: string>')" in sql
    assert "parse_json(%s)" in sql
    assert params[1] == json.dumps({"city": "London"})
    assert params[2] == json.dumps({"k": 1})


def test_insert_no_json_is_byte_identical_values() -> None:
    calls = _load([{"id": 1, "name": "Alice"}], {"id": "scalar", "name": "scalar"}, None)
    sql, params = calls[0]
    assert sql == "INSERT INTO main.default.t (id, name) VALUES (%s, %s)"
    assert params == [1, "Alice"]


def test_introspect_schema_off_skips_introspection() -> None:
    import drt.destinations.schema as schema_mod

    calls_made = {"n": 0}
    orig = schema_mod.describe_columns
    schema_mod.describe_columns = lambda c: (  # type: ignore[assignment]
        calls_made.__setitem__("n", calls_made["n"] + 1) or {}
    )
    try:
        cfg = _cfg(introspect_schema=False)
        dest = DatabricksDestination()
        cur = _FakeCursor()
        dest._connect = lambda c: _FakeConn(cur)  # type: ignore[method-assign]
        dest.load([{"id": 1, "doc": {"x": 1}}], cfg, SyncOptions(mode="full"))
    finally:
        schema_mod.describe_columns = orig  # type: ignore[assignment]
    sql, _ = cur.calls[0]
    assert "VALUES (%s, %s)" in sql and "from_json" not in sql
    assert calls_made["n"] == 0  # gate short-circuits before any introspection


def test_replace_truncate_path_wraps() -> None:
    calls = _load(
        [{"id": 1, "tags": ["x"]}],
        {"id": "scalar", "tags": "json"},
        {"tags": "array<string>"},
        sync=SyncOptions(mode="replace", replace_strategy="truncate"),
    )
    insert = next(s for s, _ in calls if s.startswith("INSERT"))
    assert "SELECT %s, from_json(%s, 'array<string>')" in insert


def test_replace_swap_path_wraps() -> None:
    calls = _load(
        [{"id": 1, "tags": ["x"]}],
        {"id": "scalar", "tags": "json"},
        {"tags": "array<string>"},
        sync=SyncOptions(mode="replace", replace_strategy="swap"),
    )
    insert = next(s for s, _ in calls if s.startswith("INSERT"))
    assert "from_json(%s, 'array<string>')" in insert


def test_merge_staging_path_wraps() -> None:
    calls = _load(
        [{"id": 1, "tags": ["x"]}],
        {"id": "scalar", "tags": "json"},
        {"tags": "array<string>"},
        mode="merge",
        upsert_key=["id"],
    )
    staging = next(
        s for s, _ in calls if s.startswith("INSERT INTO main.default.__drt_staging")
    )
    assert "from_json(%s, 'array<string>')" in staging
