# `drt test` Command Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `drt test` command that validates data correctness in destinations after sync, like dbt's `dbt test`.

**Architecture:** Tests are defined in sync YAML under a `tests` key. Each test type (row_count, not_null, freshness) generates a SQL query against the destination DB. Results are collected and displayed with ✓/✗ markers. Only DB destinations (PostgreSQL, MySQL, ClickHouse) support tests — API/file destinations are skipped with a warning.

**Tech Stack:** Pydantic models for test config, existing DB destination `_connect()` methods for querying, Typer CLI, Rich output.

---

## Scope Decision

### v0.5 scope (this plan):
- `row_count` test (min/max)
- `not_null` test (columns)
- CLI: `drt test` / `drt test --select <name>`
- DB destinations only (Postgres, MySQL, ClickHouse)
- Non-DB destinations skipped with warning

### Deferred (future):
- `freshness` test — needs timezone handling, more complex
- `unique` test
- `accepted_values` test
- `--output json` for `drt test` (covered by #230)

---

## Task 1: Add test config models

**Files:**
- Modify: `drt/config/models.py`
- Test: `tests/unit/test_config.py`

### Step 1: Write the failing test

Add to `tests/unit/test_config.py`:

```python
def test_sync_config_with_tests() -> None:
    data = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {"type": "rest_api", "url": "http://x", "method": "POST"},
        "tests": [
            {"row_count": {"min": 1}},
            {"not_null": {"columns": ["id", "name"]}},
        ],
    }
    sync = SyncConfig.model_validate(data)
    assert len(sync.tests) == 2


def test_sync_config_without_tests() -> None:
    data = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {"type": "rest_api", "url": "http://x", "method": "POST"},
    }
    sync = SyncConfig.model_validate(data)
    assert sync.tests == []
```

### Step 2: Run test to verify it fails

```bash
pytest tests/unit/test_config.py::test_sync_config_with_tests -v
```
Expected: FAIL — `tests` field not on SyncConfig

### Step 3: Write minimal implementation

In `drt/config/models.py`, add before `SyncConfig`:

```python
class RowCountTest(BaseModel):
    min: int | None = None
    max: int | None = None

class NotNullTest(BaseModel):
    columns: list[str]

class SyncTest(BaseModel):
    row_count: RowCountTest | None = None
    not_null: NotNullTest | None = None
```

Update `SyncConfig`:

```python
class SyncConfig(BaseModel):
    name: str
    description: str = ""
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
    tests: list[SyncTest] = Field(default_factory=list)
```

### Step 4: Run test to verify it passes

```bash
pytest tests/unit/test_config.py::test_sync_config_with_tests tests/unit/test_config.py::test_sync_config_without_tests -v
```

### Step 5: Commit

```bash
git add drt/config/models.py tests/unit/test_config.py
git commit -m "feat: add test config models (row_count, not_null) to SyncConfig (#141)"
```

---

## Task 2: Create test runner engine

**Files:**
- Create: `drt/engine/test_runner.py`
- Test: `tests/unit/test_test_runner.py`

### Step 1: Write the failing tests

Create `tests/unit/test_test_runner.py`:

```python
"""Tests for drt test runner."""

from __future__ import annotations

import pytest

from drt.config.models import NotNullTest, RowCountTest, SyncTest
from drt.engine.test_runner import TestResult, build_test_query


def test_build_row_count_min() -> None:
    t = SyncTest(row_count=RowCountTest(min=1))
    query, check = build_test_query(t, "public.users")
    assert "COUNT(*)" in query
    assert check(5) is True
    assert check(0) is False


def test_build_row_count_max() -> None:
    t = SyncTest(row_count=RowCountTest(max=100))
    _, check = build_test_query(t, "public.users")
    assert check(50) is True
    assert check(101) is False


def test_build_row_count_min_max() -> None:
    t = SyncTest(row_count=RowCountTest(min=10, max=100))
    _, check = build_test_query(t, "public.users")
    assert check(50) is True
    assert check(5) is False
    assert check(101) is False


def test_build_not_null() -> None:
    t = SyncTest(not_null=NotNullTest(columns=["id", "name"]))
    query, check = build_test_query(t, "public.users")
    assert "id" in query
    assert "name" in query
    assert "NULL" in query.upper()
    assert check(0) is True   # 0 nulls = pass
    assert check(3) is False  # 3 nulls = fail


def test_build_unknown_test_raises() -> None:
    t = SyncTest()  # no test type set
    with pytest.raises(ValueError, match="No test type"):
        build_test_query(t, "public.users")
```

### Step 2: Run test to verify it fails

```bash
pytest tests/unit/test_test_runner.py -v
```
Expected: FAIL — module not found

### Step 3: Write minimal implementation

Create `drt/engine/test_runner.py`:

```python
"""Test runner — executes validation queries against destinations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from drt.config.models import SyncTest


@dataclass
class TestResult:
    test_name: str
    passed: bool
    message: str


def _safe_table(table: str) -> str:
    """Basic table name validation."""
    for ch in table:
        if not (ch.isalnum() or ch in "._"):
            raise ValueError(f"Invalid character in table name: {ch!r}")
    return table


def build_test_query(
    test: SyncTest, table: str
) -> tuple[str, Callable[[int], bool]]:
    """Return (SQL query, check_function) for a test.

    The query should return a single integer value.
    The check function takes that integer and returns True if the test passes.
    """
    safe_table = _safe_table(table)

    if test.row_count is not None:
        rc = test.row_count
        query = f"SELECT COUNT(*) FROM {safe_table}"

        def check_row_count(val: int) -> bool:
            if rc.min is not None and val < rc.min:
                return False
            if rc.max is not None and val > rc.max:
                return False
            return True

        return query, check_row_count

    if test.not_null is not None:
        nn = test.not_null
        conditions = " OR ".join(f"{col} IS NULL" for col in nn.columns)
        query = f"SELECT COUNT(*) FROM {safe_table} WHERE {conditions}"

        def check_not_null(val: int) -> bool:
            return val == 0

        return query, check_not_null

    raise ValueError("No test type defined in SyncTest.")
```

### Step 4: Run test to verify it passes

```bash
pytest tests/unit/test_test_runner.py -v
```

### Step 5: Commit

```bash
git add drt/engine/test_runner.py tests/unit/test_test_runner.py
git commit -m "feat: add test runner with row_count and not_null queries (#141)"
```

---

## Task 3: Add destination query capability

**Files:**
- Create: `drt/destinations/query.py`
- Test: `tests/unit/test_destination_query.py`

### Step 1: Write the failing test

Create `tests/unit/test_destination_query.py`:

```python
"""Tests for destination query execution."""

from __future__ import annotations

import pytest

from drt.config.models import (
    PostgresDestinationConfig,
    RestApiDestinationConfig,
)
from drt.destinations.query import is_queryable, get_table_name


def test_postgres_is_queryable() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    assert is_queryable(config) is True


def test_rest_api_is_not_queryable() -> None:
    config = RestApiDestinationConfig(
        type="rest_api",
        url="http://example.com",
        method="POST",
    )
    assert is_queryable(config) is False


def test_get_table_name_postgres() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    assert get_table_name(config) == "public.users"
```

### Step 2: Run test to verify it fails

```bash
pytest tests/unit/test_destination_query.py -v
```

### Step 3: Write minimal implementation

Create `drt/destinations/query.py`:

```python
"""Query destination databases for test validation."""

from __future__ import annotations

from typing import Any

from drt.config.models import (
    ClickHouseDestinationConfig,
    DestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
)

_QUERYABLE_TYPES = (
    PostgresDestinationConfig,
    MySQLDestinationConfig,
    ClickHouseDestinationConfig,
)


def is_queryable(config: DestinationConfig) -> bool:
    """Return True if we can run validation queries against this destination."""
    return isinstance(config, _QUERYABLE_TYPES)


def get_table_name(config: DestinationConfig) -> str:
    """Extract the target table name from a DB destination config."""
    if isinstance(config, PostgresDestinationConfig):
        return config.table
    if isinstance(config, MySQLDestinationConfig):
        return config.table
    if isinstance(config, ClickHouseDestinationConfig):
        return config.table
    raise TypeError(f"Cannot get table name from {type(config).__name__}")


def execute_test_query(config: DestinationConfig, query: str) -> int:
    """Execute a query against a DB destination and return a single int."""
    if isinstance(config, PostgresDestinationConfig):
        return _query_postgres(config, query)
    if isinstance(config, MySQLDestinationConfig):
        return _query_mysql(config, query)
    if isinstance(config, ClickHouseDestinationConfig):
        return _query_clickhouse(config, query)
    raise TypeError(f"Cannot query {type(config).__name__}")


def _query_postgres(config: PostgresDestinationConfig, query: str) -> int:
    from drt.destinations.postgres import PostgresDestination

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        result: int = cur.fetchone()[0]
        return result
    finally:
        conn.close()


def _query_mysql(config: MySQLDestinationConfig, query: str) -> int:
    from drt.destinations.mysql import MySQLDestination

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        result: int = row[0] if isinstance(row, tuple) else list(row.values())[0]
        return result
    finally:
        conn.close()


def _query_clickhouse(
    config: ClickHouseDestinationConfig, query: str
) -> int:
    from drt.destinations.clickhouse import ClickHouseDestination

    client = ClickHouseDestination._connect(config)
    try:
        result = client.query(query)
        val: int = result.result_rows[0][0]
        return val
    finally:
        client.close()
```

### Step 4: Run test to verify it passes

```bash
pytest tests/unit/test_destination_query.py -v
```

### Step 5: Commit

```bash
git add drt/destinations/query.py tests/unit/test_destination_query.py
git commit -m "feat: add destination query capability for test validation (#141)"
```

---

## Task 4: Add CLI command and output

**Files:**
- Modify: `drt/cli/main.py`
- Modify: `drt/cli/output.py`
- Test: `tests/unit/test_cli_test_command.py`

### Step 1: Write the failing test

Create `tests/unit/test_cli_test_command.py`:

```python
"""Tests for drt test CLI command."""

from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def test_drt_test_no_syncs(
    tmp_path: Path, monkeypatch: object
) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)
    result = runner.invoke(app, ["test"])
    assert "No syncs found" in result.output
    mp.undo()


def test_drt_test_no_tests_defined(
    tmp_path: Path, monkeypatch: object
) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    sync_data = {
        "name": "no-tests",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "http://example.com",
            "method": "POST",
        },
    }
    with (syncs_dir / "sync.yml").open("w") as f:
        yaml.dump(sync_data, f)

    result = runner.invoke(app, ["test"])
    assert "No tests defined" in result.output
    mp.undo()


def test_drt_test_skips_non_queryable(
    tmp_path: Path, monkeypatch: object
) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    sync_data = {
        "name": "api-sync",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "http://example.com",
            "method": "POST",
        },
        "tests": [{"row_count": {"min": 1}}],
    }
    with (syncs_dir / "sync.yml").open("w") as f:
        yaml.dump(sync_data, f)

    result = runner.invoke(app, ["test"])
    assert "skipped" in result.output.lower() or "not supported" in result.output.lower()
    mp.undo()
```

### Step 2: Run test to verify it fails

```bash
pytest tests/unit/test_cli_test_command.py -v
```

### Step 3: Add output helpers

In `drt/cli/output.py`, add after the validate section:

```python
# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------

def print_test_result(
    sync_name: str, test_name: str, passed: bool, message: str
) -> None:
    mark = "[green]✓[/green]" if passed else "[red]✗[/red]"
    console.print(f"  {mark} {test_name}: {message}")


def print_test_header(sync_name: str) -> None:
    console.print(f"\n[bold]{sync_name}[/bold]")


def print_test_skip(sync_name: str, reason: str) -> None:
    console.print(f"  [dim]⏭ {sync_name}: {reason}[/dim]")
```

### Step 4: Add CLI command

In `drt/cli/main.py`, add the `test` command (between `status` and `mcp`):

```python
# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


@app.command(name="test")
def test_syncs(
    select: str = typer.Option(
        None, "--select", "-s", help="Test a specific sync by name."
    ),
) -> None:
    """Run post-sync validation tests."""
    from drt.config.parser import load_syncs
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import build_test_query

    syncs = load_syncs(Path("."))
    if not syncs:
        console.print("[dim]No syncs found.[/dim]")
        return

    if select:
        syncs = [s for s in syncs if s.name == select]
        if not syncs:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    # Filter to syncs that have tests
    syncs_with_tests = [s for s in syncs if s.tests]
    if not syncs_with_tests:
        console.print("[dim]No tests defined in any sync.[/dim]")
        return

    had_failures = False

    for sync in syncs_with_tests:
        print_test_header(sync.name)

        if not is_queryable(sync.destination):
            print_test_skip(
                sync.name,
                f"tests not supported for {sync.destination.type} destinations",
            )
            continue

        table = get_table_name(sync.destination)
        for test_def in sync.tests:
            try:
                query, check = build_test_query(test_def, table)
                result_val = execute_test_query(sync.destination, query)
                passed = check(result_val)
                test_name = _test_display_name(test_def)
                if passed:
                    print_test_result(sync.name, test_name, True, f"{result_val}")
                else:
                    print_test_result(sync.name, test_name, False, f"{result_val}")
                    had_failures = True
            except Exception as e:
                test_name = _test_display_name(test_def)
                print_test_result(sync.name, test_name, False, str(e))
                had_failures = True

    if had_failures:
        raise typer.Exit(1)


def _test_display_name(test_def: "SyncTest") -> str:  # noqa: F821
    """Human-readable name for a test definition."""
    if test_def.row_count is not None:
        parts = []
        if test_def.row_count.min is not None:
            parts.append(f"min={test_def.row_count.min}")
        if test_def.row_count.max is not None:
            parts.append(f"max={test_def.row_count.max}")
        return f"row_count({', '.join(parts)})"
    if test_def.not_null is not None:
        cols = ", ".join(test_def.not_null.columns)
        return f"not_null({cols})"
    return "unknown"
```

Update imports at the top of `main.py`:

```python
from drt.cli.output import (
    ...
    print_test_header,
    print_test_result,
    print_test_skip,
)
```

### Step 5: Run test to verify it passes

```bash
pytest tests/unit/test_cli_test_command.py -v
```

### Step 6: Run full test suite + lint

```bash
make test && ruff check drt tests && mypy drt
```

### Step 7: Commit

```bash
git add drt/cli/main.py drt/cli/output.py tests/unit/test_cli_test_command.py
git commit -m "feat: add drt test CLI command (#141)"
```

---

## Task 5: Update docs and skills

**Files:**
- Modify: `CLAUDE.md` — add `drt test` to CLI commands list
- Modify: `docs/llm/CONTEXT.md` — add test command docs
- Modify: `docs/llm/API_REFERENCE.md` — add test YAML examples

### Step 1: Update CLAUDE.md

Add `drt test` to the CLI commands line:
```
- CLI fully wired: `init`, `run`, `list`, `validate`, `status`, `test`, `mcp run`
```

### Step 2: Update API_REFERENCE.md

Add test YAML example in the sync config section.

### Step 3: Commit

```bash
git add CLAUDE.md docs/llm/CONTEXT.md docs/llm/API_REFERENCE.md
git commit -m "docs: add drt test command documentation (#141)"
```

---

## Task 6: Create PR

```bash
gh pr create --title "feat: add drt test command for post-sync validation" \
  --body "## Summary
- Add \`drt test\` command for post-sync data validation (#141)
- Support \`row_count\` (min/max) and \`not_null\` (columns) tests
- Tests defined in sync YAML under \`tests:\` key
- DB destinations (Postgres, MySQL, ClickHouse) supported
- Non-DB destinations skipped with warning

Closes #141."
```

---

## Summary

| Task | What | Files | Tests |
|------|------|-------|-------|
| 1 | Config models | models.py | 2 |
| 2 | Test runner engine | test_runner.py | 6 |
| 3 | Destination query | query.py | 3 |
| 4 | CLI command + output | main.py, output.py | 3 |
| 5 | Docs | CLAUDE.md, API_REFERENCE.md | — |
| 6 | PR | — | — |

Total: ~14 new tests, 3 new files, 3 modified files.
