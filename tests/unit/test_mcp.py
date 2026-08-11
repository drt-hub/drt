"""Unit tests for the drt MCP server tools.

Requires: pip install drt-core[mcp]
These tests are skipped automatically when fastmcp is not installed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("fastmcp", reason="requires drt-core[mcp]")

from typing import Any

from fastmcp import FastMCP  # noqa: E402

from drt.mcp.server import create_server  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def call(server: FastMCP, tool_name: str, **kwargs: Any) -> Any:
    """Call an MCP tool and return the structured result.

    FastMCP wraps non-dict returns in {"result": value};
    dict returns are passed through directly.
    """
    result = await server.call_tool(tool_name, kwargs)
    sc = result.structured_content
    # list / scalar returns are wrapped in {"result": ...}
    if isinstance(sc, dict) and list(sc.keys()) == ["result"]:
        return sc["result"]
    return sc


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    (tmp_path / "drt_project.yml").write_text("name: test-project\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
    )
    return tmp_path


@pytest.fixture()
def server(project_dir: Path) -> FastMCP:
    return create_server(project_dir)


# ---------------------------------------------------------------------------
# Server creation
# ---------------------------------------------------------------------------


def test_create_server_returns_fastmcp_instance() -> None:
    from fastmcp import FastMCP

    assert isinstance(create_server(), FastMCP)


@pytest.mark.asyncio
async def test_server_has_expected_tools() -> None:
    srv = create_server()
    tools = await srv._local_provider._list_tools()
    tool_names = {t.name for t in tools}
    expected = {
        "drt_list_syncs",
        "drt_run_sync",
        "drt_run_test",
        "drt_get_status",
        "drt_validate",
        "drt_get_schema",
    }
    assert expected <= tool_names


# ---------------------------------------------------------------------------
# drt_list_syncs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_syncs_returns_sync(server: FastMCP) -> None:
    result = await call(server, "drt_list_syncs")
    assert len(result) == 1
    assert result[0]["name"] == "notify"
    assert result[0]["destination_type"] == "rest_api"


@pytest.mark.asyncio
async def test_list_syncs_empty_project(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: empty\nprofile: default\n")
    (tmp_path / "syncs").mkdir()
    srv = create_server(tmp_path)
    result = await call(srv, "drt_list_syncs")
    assert result == []


# ---------------------------------------------------------------------------
# drt_validate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_returns_valid_syncs(server: FastMCP) -> None:
    result = await call(server, "drt_validate")
    assert "notify" in result["valid"]
    assert result["errors"] == {}


@pytest.mark.asyncio
async def test_validate_warns_on_hardcoded_secret(tmp_path: Path) -> None:
    """Matches ``drt validate``'s non-strict behaviour: a hardcoded secret is
    a warning, not an error — the sync stays valid (#870)."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "secret.yml").write_text(
        "name: secret\n"
        "model: SELECT 1\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/api\n"
        "  method: POST\n"
        "  auth:\n"
        "    type: bearer\n"
        f"    token: sk-{'a' * 32}\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_validate")
    assert "secret" in result["valid"]
    assert result["errors"] == {}
    assert "hardcoded secret" in result["warnings"]["secret"][0]


@pytest.mark.asyncio
async def test_validate_strict_promotes_secret_warning_to_error(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "secret.yml").write_text(
        "name: secret\n"
        "model: SELECT 1\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/api\n"
        "  method: POST\n"
        "  auth:\n"
        "    type: bearer\n"
        f"    token: sk-{'b' * 32}\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_validate", strict=True)
    assert "secret" not in result["valid"]
    assert "hardcoded secret" in result["errors"]["secret"][0]


@pytest.mark.asyncio
async def test_validate_check_connection_reports_per_sync(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``check_connection=True`` adds a ``connection_tests`` entry per sync
    (mirrors ``drt validate --check-connection``, #870). ``notify`` is a
    rest_api destination, which ``_run_connection_test`` skips (SQL-only)."""
    srv = create_server(project_dir)
    result = await call(srv, "drt_validate", check_connection=True)
    assert result["connection_tests"]["notify"]["skipped"] is True


# ---------------------------------------------------------------------------
# drt_run_test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_test_no_syncs(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: empty\nprofile: default\n")
    (tmp_path / "syncs").mkdir()
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test")
    assert result == {"status": "no_syncs", "results": []}


@pytest.mark.asyncio
async def test_run_test_sync_not_found(server: FastMCP) -> None:
    result = await call(server, "drt_run_test", sync_name="nonexistent")
    assert "error" in result


@pytest.mark.asyncio
async def test_run_test_no_tests_defined(server: FastMCP) -> None:
    # The default fixture sync has no `tests:` block
    result = await call(server, "drt_run_test")
    assert result == {"status": "no_tests", "results": []}


@pytest.mark.asyncio
async def test_run_test_dry_run_previews_without_executing(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """``dry_run=True`` (#870) lists the test plan without connecting to the
    destination or running queries (mirrors ``drt test --dry-run``)."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - not_null: { columns: [email] }\n"
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)

    def fail_if_called(*_a: Any, **_k: Any) -> int:
        raise AssertionError("dry_run must not execute a query")

    monkeypatch.setattr(query_module, "execute_test_query", fail_if_called)

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", dry_run=True)

    assert result["dry_run"] is True
    assert result["results"][0]["tests"][0]["dry_run"] is True


@pytest.mark.asyncio
async def test_run_test_fail_fast_skips_remaining_syncs(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """``fail_fast=True`` (#870) stops after the first sync with a failing
    test; remaining syncs are reported skipped, not run (mirrors ``drt test
    --fail-fast``)."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    for name in ("first", "second"):
        (syncs_dir / f"{name}.yml").write_text(
            f"name: {name}\n"
            "model: ref('orders')\n"
            "destination:\n"
            "  type: postgres\n"
            "  host: localhost\n"
            "  dbname: test\n"
            "  table: orders\n"
            "  upsert_key: [id]\n"
            "tests:\n"
            "  - not_null: { columns: [email] }\n"
        )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 3)  # fails not_null

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", fail_fast=True)

    assert result["status"] == "failed"
    assert len(result["results"]) == 2
    assert result["results"][1]["skipped"] is True
    assert result["results"][1]["reason"] == "fail_fast"


@pytest.mark.asyncio
async def test_run_test_skips_non_queryable_destination(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    # rest_api is not queryable — tests should report skipped
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
        "tests:\n"
        "  - row_count: { min: 1 }\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", sync_name="notify")
    assert result["status"] == "passed"
    assert len(result["results"]) == 1
    assert result["results"][0]["skipped"] is True
    assert "rest_api" in result["results"][0]["reason"]


@pytest.mark.asyncio
async def test_run_test_severity_warn_failure_does_not_flip_status(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """#836: severity: warn parity between drt_run_test and drt test /
    drt build was enforced only by a comment. A warn-severity failure must
    still be reported, but must not flip the top-level status to "failed" —
    the exact rule #400 was a drift bug about."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - name: warn_test\n"
        "    severity: warn\n"
        "    not_null: { columns: [email] }\n"
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 3)  # 3 nulls -> fails

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test")

    test_entry = result["results"][0]["tests"][0]
    assert test_entry["passed"] is False
    assert test_entry["severity"] == "warn"
    # The failure is visible, but doesn't fail the run — same rule drt test
    # and drt build apply.
    assert result["status"] == "passed"


@pytest.mark.asyncio
async def test_run_test_severity_warn_exception_does_not_flip_status(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The exception path needs the same guard as the pass/fail path — a
    severity: warn test that errors (destination unreachable, bad query)
    must not flip status either."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - name: warn_test\n"
        "    severity: warn\n"
        "    not_null: { columns: [email] }\n"
    )
    from drt.destinations import query as query_module

    def _raise(dest: object, q: str) -> int:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", _raise)

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test")

    test_entry = result["results"][0]["tests"][0]
    assert test_entry["passed"] is False
    assert test_entry["severity"] == "warn"
    assert "connection refused" in test_entry["error"]
    assert result["status"] == "passed"


@pytest.mark.asyncio
async def test_run_test_error_severity_failure_flips_status(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The other half of the severity rule: a default-severity failure MUST
    flip the top-level status. Without this, `status: "passed"` could be
    hardcoded and the two warn tests above would still pass."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - not_null: { columns: [email] }\n"
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 3)  # 3 nulls -> fails

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test")

    test_entry = result["results"][0]["tests"][0]
    assert test_entry == {
        "name": "not_null(email)",
        "passed": False,
        "value": "3",
        "severity": "error",
    }
    assert result["status"] == "failed"


@pytest.mark.asyncio
async def test_run_test_writes_nothing_to_the_console(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    """#851: the tool now runs the same execution path as `drt test`, which
    prints a header + a line per test. MCP speaks over stdio — a stray print
    corrupts the transport, so the tool must stay silent on both the pass
    and the skip path."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - not_null: { columns: [email] }\n"
    )
    # A second, non-queryable sync exercises the skip branch's printing too.
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
        "tests:\n"
        "  - row_count: { min: 1 }\n"
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 0)

    srv = create_server(tmp_path)
    capsys.readouterr()  # drop anything the fixtures above emitted
    result = await call(srv, "drt_run_test")

    captured = capsys.readouterr()
    # stdout only: `drt.cli.output.console` is a plain `Console()`, so that is
    # where the CLI's test header and per-test lines would land, and stdout is
    # the channel an MCP stdio server may not put non-protocol bytes on.
    # stderr is the sanctioned place for a server to log — asserting it empty
    # would turn any future warning from fastmcp / pydantic / a DB driver into
    # a failure of a test about console leakage.
    assert captured.out == ""
    assert result["status"] == "passed"


@pytest.mark.asyncio
async def test_run_test_does_not_store_failure_samples_by_default(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """`--store-failures` (#779) writes .drt/test_failures/… to disk.

    `drt_run_test` defaults `store_failures=False` (#870) — same default as
    `drt test` — so a failing test through MCP leaves the filesystem
    untouched unless the caller opts in. Opting in is covered by
    ``test_run_test_store_failures_writes_sample_and_reports_path`` below;
    this test only pins the unchanged default."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - not_null: { columns: [email] }\n"
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 3)
    # Samples land under `project_dir` — which the tool resolves from the
    # context, but which `execute_tests_for_sync` defaults to `Path(".")`.
    # Pointing the cwd at tmp_path too means a regression on *either* — the
    # store_failures flag or the project_dir argument — writes inside the tmp
    # dir where the assertion below can see it, rather than silently into the
    # repo checkout, where it would pass and leave a stray file behind.
    monkeypatch.chdir(tmp_path)

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test")

    assert result["status"] == "failed"
    assert not (tmp_path / ".drt" / "test_failures").exists()
    assert "failures_stored" not in result["results"][0]["tests"][0]


@pytest.mark.asyncio
async def test_run_test_store_failures_writes_sample_and_reports_path(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """``store_failures=True`` (#870) is the opt-in twin of the default-off
    test above — the sample now writes under ``project_dir``, masked, and
    the entry's ``failures_stored`` carries the path an MCP caller with its
    own filesystem access (Read/Bash on the same checkout) can open."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "tests:\n"
        "  - not_null: { columns: [email] }\n"
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "orders")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 3)
    monkeypatch.setattr(
        query_module,
        "fetch_failing_rows",
        lambda dest, query, limit: [{"id": 1, "email": None}],
    )
    monkeypatch.chdir(tmp_path)

    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", store_failures=True, store_failures_limit=5)

    assert result["status"] == "failed"
    stored = result["results"][0]["tests"][0]["failures_stored"]
    assert stored["count"] == 1
    assert Path(stored["path"]).exists()


# ---------------------------------------------------------------------------
# drt_run_test — unit=True (#780)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_test_unit_no_unit_tests_defined(server: FastMCP) -> None:
    """The default fixture sync has no unit_tests: block. sync.tests: alone
    (a different sync than this fixture, checked below) must not count."""
    result = await call(server, "drt_run_test", unit=True)
    assert result == {"status": "no_tests", "results": []}


@pytest.mark.asyncio
async def test_run_test_unit_ignores_syncs_with_only_quality_tests(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
        "tests:\n"
        "  - row_count: { min: 1 }\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", unit=True)
    assert result == {"status": "no_tests", "results": []}


@pytest.mark.asyncio
async def test_run_test_unit_pass_and_fail(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "notify.yml").write_text(
        "name: notify\n"
        "model: ref('users')\n"
        "destination:\n"
        "  type: rest_api\n"
        "  url: https://example.com/hook\n"
        "sync:\n"
        "  field_mappings: { first: given_name }\n"
        "unit_tests:\n"
        "  - name: renames\n"
        "    given: [{ id: 1, first: Alice }]\n"
        "    expect: [{ id: 1, given_name: Alice }]\n"
        "  - name: wrong\n"
        "    given: [{ id: 1 }]\n"
        "    expect: [{ id: 999 }]\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", unit=True)

    assert result["status"] == "failed"
    [sync_result] = result["results"]
    by_name = {t["name"]: t for t in sync_result["tests"]}
    assert by_name["renames"]["passed"] is True
    assert by_name["renames"]["mismatches"] == []
    assert by_name["wrong"]["passed"] is False
    assert by_name["wrong"]["mismatches"]


@pytest.mark.asyncio
async def test_run_test_unit_lookups_reported_as_failure(tmp_path: Path) -> None:
    """No fake lookup table yet (#780's own stated scope) — a sync with
    destination.lookups fails the unit test rather than silently running
    without the lookup step."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "orders.yml").write_text(
        "name: orders\n"
        "model: ref('orders')\n"
        "destination:\n"
        "  type: postgres\n"
        "  host_env: H\n"
        "  dbname_env: D\n"
        "  user_env: U\n"
        "  password_env: P\n"
        "  table: orders\n"
        "  upsert_key: [id]\n"
        "  lookups:\n"
        "    account_id: { table: accounts, match: { email: email }, select: id }\n"
        "unit_tests:\n"
        "  - name: cannot_run\n"
        "    given: [{ id: 1 }]\n"
        "    expect: [{ id: 1 }]\n"
    )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", unit=True)

    assert result["status"] == "failed"
    test_entry = result["results"][0]["tests"][0]
    assert test_entry["passed"] is False
    assert "lookups" in test_entry["error"]


@pytest.mark.asyncio
async def test_run_test_unit_sync_name_filters_first(tmp_path: Path) -> None:
    """unit=True composes with sync_name the same way the tests: path does."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    for name in ("a", "b"):
        (syncs_dir / f"{name}.yml").write_text(
            f"name: {name}\n"
            "model: ref('t')\n"
            "destination:\n"
            "  type: rest_api\n"
            "  url: https://example.com\n"
            "unit_tests:\n"
            "  - name: t\n"
            "    given: [{ id: 1 }]\n"
            "    expect: [{ id: 1 }]\n"
        )
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_test", sync_name="a", unit=True)
    assert [r["sync"] for r in result["results"]] == ["a"]


# ---------------------------------------------------------------------------
# drt_get_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_no_history(server: FastMCP) -> None:
    result = await call(server, "drt_get_status")
    assert result == {}


@pytest.mark.asyncio
async def test_get_status_specific_not_found(server: FastMCP) -> None:
    result = await call(server, "drt_get_status", sync_name="nonexistent")
    assert "error" in result


@pytest.mark.asyncio
async def test_get_status_after_state_saved(server, project_dir: Path) -> None:
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="notify",
            last_run_at="2026-03-30T12:00:00",
            records_synced=42,
            status="success",
        )
    )
    result = await call(server, "drt_get_status", sync_name="notify")
    assert result["notify"]["records_synced"] == 42
    assert result["notify"]["status"] == "success"


# ---------------------------------------------------------------------------
# drt_get_schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_schema_sync(server: FastMCP) -> None:
    schema = await call(server, "drt_get_schema", schema_type="sync")
    assert isinstance(schema, dict)
    assert "$defs" in schema or "properties" in schema


@pytest.mark.asyncio
async def test_get_schema_project(server: FastMCP) -> None:
    schema = await call(server, "drt_get_schema", schema_type="project")
    assert isinstance(schema, dict)
    assert "$defs" in schema or "properties" in schema


# ---------------------------------------------------------------------------
# drt_run_sync — compute_diff parameter (#413 parity)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_sync_returns_error_for_unknown_sync(project_dir: Path, monkeypatch: Any) -> None:
    """Unknown ``sync_name`` returns a structured error (no engine call).

    Bypasses ``load_profile`` (which would otherwise try to read the
    real ``~/.drt/profiles.yml`` on the developer's machine) — the
    sync-name match happens after profile loading in the flow.
    """
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="nonexistent")
    assert "error" in result
    assert "nonexistent" in result["error"]


@pytest.mark.asyncio
async def test_run_sync_compute_diff_requires_dry_run(server: FastMCP) -> None:
    """``compute_diff=True`` without ``dry_run=True`` is a contract
    violation — matches the CLI ``drt run --diff`` requiring
    ``--dry-run``. Returns a structured error rather than executing
    the sync against a live destination.
    """
    result = await call(
        server, "drt_run_sync", sync_name="notify", compute_diff=True, dry_run=False
    )
    assert "error" in result
    assert "dry_run" in result["error"]


@pytest.mark.asyncio
async def test_run_sync_compute_diff_threads_diff_into_response(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``compute_diff=True`` + ``dry_run=True`` → response carries a
    ``diff`` field built from ``diff_to_dict``. This is the success
    path that exercises the load_project / run_sync / response-with-diff
    branch — which the error-path tests can't reach.

    Patches the engine + source/destination factory functions at
    their source modules so the inside-function imports resolve to
    the test doubles, avoiding a real warehouse / HTTP destination.
    """
    from drt.engine.sync import SyncResult

    fake_diff = object()  # diff_to_dict tolerates None / unknown shapes

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        result = SyncResult()
        result.success = 1
        result.failed = 0
        result.diff = fake_diff  # type: ignore[attr-defined]
        return result

    def fake_diff_to_dict(_diff: object) -> dict[str, Any]:
        return {"added": [{"id": 1}], "updated": [], "deleted": [], "unchanged": []}

    # Patch the engine + factory layers at their source modules so the
    # inside-function imports inside `drt_run_sync` pick up the doubles.
    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    monkeypatch.setattr("drt.cli.output.diff_to_dict", fake_diff_to_dict)

    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="notify", dry_run=True, compute_diff=True)

    assert "diff" in result
    assert result["diff"] == {
        "added": [{"id": 1}],
        "updated": [],
        "deleted": [],
        "unchanged": [],
    }
    assert result["dry_run"] is True
    assert result["success"] == 1


@pytest.mark.asyncio
async def test_run_sync_dry_run_without_compute_diff_omits_diff_field(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``compute_diff=False`` → response has no ``diff`` field even
    when ``dry_run=True``. Exercises the response-building path
    without the diff serialisation branch."""
    from drt.engine.sync import SyncResult

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        result = SyncResult()
        result.success = 1
        return result

    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())

    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="notify", dry_run=True)

    assert "diff" not in result
    assert result["dry_run"] is True
    assert result["success"] == 1


# ---------------------------------------------------------------------------
# drt_run_sync — limit / vars parameters (#870)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_sync_limit_and_vars_reach_the_engine(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``limit``/``vars`` (#870) must actually reach ``engine.sync.run_sync``
    as ``extract_limit``/``vars`` — not just be accepted and dropped."""
    from drt.engine.sync import SyncResult

    captured: dict[str, Any] = {}

    def fake_run_sync(*_args: Any, **kwargs: Any) -> SyncResult:
        captured.update(kwargs)
        result = SyncResult()
        result.success = 1
        result.limit_applied = kwargs.get("extract_limit")
        return result

    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())

    srv = create_server(project_dir)
    result = await call(
        srv,
        "drt_run_sync",
        sync_name="notify",
        limit=5,
        vars={"lookback_days": 1},
    )

    assert captured["extract_limit"] == 5
    assert captured["vars"]["lookback_days"] == 1
    assert result["limit_applied"] == 5


@pytest.mark.asyncio
async def test_run_sync_limit_rejects_non_positive(project_dir: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    srv = create_server(project_dir)
    result = await call(srv, "drt_run_sync", sync_name="notify", limit=0)
    assert "error" in result
    assert "positive" in result["error"]


@pytest.mark.asyncio
async def test_run_sync_limit_rejected_for_mirror_mode(tmp_path: Path, monkeypatch: Any) -> None:
    """Matches the CLI guard (#774): a sampled mirror would DELETE the
    destination rows the sample skipped."""
    (tmp_path / "drt_project.yml").write_text("name: test\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "mirror_sync.yml").write_text(
        "name: mirror_sync\n"
        "model: ref('users')\n"
        "sync:\n"
        "  mode: mirror\n"
        "  upsert_key: [id]\n"
        "destination:\n"
        "  type: postgres\n"
        "  host: localhost\n"
        "  dbname: test\n"
        "  table: users\n"
        "  upsert_key: [id]\n"
    )
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    srv = create_server(tmp_path)
    result = await call(srv, "drt_run_sync", sync_name="mirror_sync", limit=5)
    assert "error" in result
    assert "mirror" in result["error"]


# ---------------------------------------------------------------------------
# drt_doctor — environment diagnostics (mirrors `drt doctor` CLI)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_doctor_returns_structured_report(project_dir: Path, monkeypatch: Any) -> None:
    """``drt_doctor`` returns ``{passed, checks}`` with at minimum
    Python version + drt version + project file rows.
    """
    # `_check_*` helpers in drt/cli/doctor.py read from CWD; cd into
    # the temp project so project_file and profile checks see real
    # files instead of whatever the test runner's CWD is.
    monkeypatch.chdir(project_dir)
    srv = create_server(project_dir)

    result = await call(srv, "drt_doctor")
    assert "passed" in result
    assert "checks" in result
    assert isinstance(result["checks"], list)
    # At minimum: Python version, drt version, project file
    names = {c["name"] for c in result["checks"]}
    assert "Python version" in names
    assert "drt version" in names
    assert "Project file" in names
    # Each check has the documented shape
    for check in result["checks"]:
        assert set(check.keys()) >= {"category", "name", "ok", "message"}


@pytest.mark.asyncio
async def test_doctor_passes_on_well_formed_project(project_dir: Path, monkeypatch: Any) -> None:
    """On a well-formed project (project file + profile file + syncs/),
    ``passed`` is True. The fixture creates exactly this shape, so any
    regression that breaks the happy path surfaces here."""
    monkeypatch.chdir(project_dir)

    # Profile fixture: ~/.drt/profiles.yml gets read by _check_profile.
    # The fixture project references profile "default"; provide a
    # minimal profiles.yml under a fake HOME to keep the test
    # self-contained and avoid touching the developer's real
    # ~/.drt/profiles.yml.
    fake_home = project_dir / "fake_home"
    (fake_home / ".drt").mkdir(parents=True)
    (fake_home / ".drt" / "profiles.yml").write_text("default: { type: duckdb }\n")
    monkeypatch.setenv("HOME", str(fake_home))

    srv = create_server(project_dir)
    result = await call(srv, "drt_doctor")
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_doctor_fails_without_project_file(tmp_path: Path, monkeypatch: Any) -> None:
    """Outside a drt project, ``passed`` is False — the project-file
    check is required for a green report."""
    srv = create_server(tmp_path)
    monkeypatch.chdir(tmp_path)  # empty dir, no drt_project.yml
    result = await call(srv, "drt_doctor")
    assert result["passed"] is False
    project_file_row = next(c for c in result["checks"] if c["name"] == "Project file")
    assert project_file_row["ok"] is False


@pytest.mark.asyncio
async def test_server_lists_drt_doctor_tool() -> None:
    """The newly added `drt_doctor` is registered alongside the
    existing tools."""
    srv = create_server()
    tools = await srv._local_provider._list_tools()
    tool_names = {t.name for t in tools}
    assert "drt_doctor" in tool_names


# ---------------------------------------------------------------------------
# drt_list_connectors — inventory / registry parity (#718)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_connectors_matches_connector_ssot(server: FastMCP) -> None:
    """The MCP inventory must list *exactly* the registered connector types.

    This is the test-time guard against the drift that let `salesforce_bulk`
    silently fall out of the inventory: comparing against the `drt.config.
    connectors` SSoT (kept in lockstep with the registry by
    `test_cli_list_connectors`) catches both missing and extra entries.
    """
    from drt.config.connectors import DESTINATIONS, SOURCES

    result = await call(server, "drt_list_connectors")
    assert {c["type"] for c in result["destinations"]} == {t for t, _ in DESTINATIONS}
    assert {c["type"] for c in result["sources"]} == {t for t, _ in SOURCES}


# ---------------------------------------------------------------------------
# drt_dlq — Dead Letter Queue inspection (#718, v0.7.9 parity)
# ---------------------------------------------------------------------------


def _seed_dlq(project_dir: Path, ids: list[int]) -> None:
    from drt.state.dlq import DeadLetter, DlqStore

    DlqStore(project_dir).append(
        "notify",
        [DeadLetter(record={"id": i}, error_message="boom") for i in ids],
    )


@pytest.mark.asyncio
async def test_dlq_empty_project_reports_no_depths(server: FastMCP) -> None:
    result = await call(server, "drt_dlq")
    assert result == {"depths": {}}


@pytest.mark.asyncio
async def test_dlq_reports_depth_and_records(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3])
    result = await call(server, "drt_dlq", sync_name="notify")
    assert result["depth"] == 3
    assert [r["record"]["id"] for r in result["records"]] == [1, 2, 3]
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_dlq_truncates_records_to_limit(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3, 4, 5])
    result = await call(server, "drt_dlq", sync_name="notify", limit=2)
    assert result["depth"] == 5
    assert len(result["records"]) == 2
    assert result["truncated"] is True


@pytest.mark.asyncio
async def test_dlq_all_depths(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2])
    result = await call(server, "drt_dlq")
    assert result["depths"] == {"notify": 2}


# ---------------------------------------------------------------------------
# drt_retry — Dead Letter Queue replay (#718, mirrors `drt retry`)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_empty_queue(server: FastMCP) -> None:
    result = await call(server, "drt_retry", sync_name="notify")
    assert result["status"] == "empty"


@pytest.mark.asyncio
async def test_retry_unknown_sync(server: FastMCP) -> None:
    result = await call(server, "drt_retry", sync_name="nope")
    assert "error" in result


@pytest.mark.asyncio
async def test_retry_negative_limit(server: FastMCP) -> None:
    result = await call(server, "drt_retry", sync_name="notify", limit=-1)
    assert "error" in result


@pytest.mark.asyncio
async def test_retry_dry_run_sends_nothing(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3])
    result = await call(server, "drt_retry", sync_name="notify", dry_run=True)
    assert result["status"] == "dry_run"
    assert result["would_retry"] == 3
    from drt.state.dlq import DlqStore

    assert DlqStore(project_dir).depth("notify") == 3  # untouched


@pytest.mark.asyncio
async def test_retry_clear(server: FastMCP, project_dir: Path) -> None:
    _seed_dlq(project_dir, [1, 2, 3])
    result = await call(server, "drt_retry", sync_name="notify", clear=True)
    assert result["status"] == "cleared"
    assert result["cleared"] == 3
    from drt.state.dlq import DlqStore

    assert DlqStore(project_dir).depth("notify") == 0


# ---------------------------------------------------------------------------
# drt_get_manifest — sync catalog + lineage (#718, `drt docs` JSON)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_manifest_returns_catalog(server: FastMCP) -> None:
    result = await call(server, "drt_get_manifest")
    assert isinstance(result, dict)
    assert "schema_version" in result
    assert [s["name"] for s in result["syncs"]] == ["notify"]


@pytest.mark.asyncio
async def test_get_manifest_labels_are_docs_safe_by_default(server: FastMCP) -> None:
    """The manifest is the same artifact `drt docs generate` ships (#696), so
    the MCP tool defaults to the same safe labels as the CLI."""
    import json

    result = await call(server, "drt_get_manifest")
    assert result["destinations"][0]["label"] == "rest_api"
    assert "example.com" not in json.dumps(result)


@pytest.mark.asyncio
async def test_get_manifest_full_labels_opts_in(server: FastMCP) -> None:
    """`full_labels=True` mirrors `drt docs generate --full-labels`."""
    result = await call(server, "drt_get_manifest", full_labels=True)
    assert result["destinations"][0]["label"] == "rest_api (https://example.com/hook)"


def _seed_history(project_dir: Path, n: int) -> None:
    import json

    hist_dir = project_dir / ".drt" / "history"
    hist_dir.mkdir(parents=True, exist_ok=True)
    (hist_dir / "notify.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "sync_name": "notify",
                    "started_at": f"2026-05-14T0{i}:00:00+00:00",
                    "completed_at": f"2026-05-14T0{i}:00:05+00:00",
                    "duration_seconds": 5.0,
                    "status": "success",
                    "records_synced": 10,
                    "records_failed": 0,
                    "errors": [],
                    "cursor_value_used": None,
                    "dry_run": False,
                }
            )
            + "\n"
            for i in range(n)
        )
    )


@pytest.mark.asyncio
async def test_get_manifest_history_depth_mirrors_cli(server: FastMCP, project_dir: Path) -> None:
    """`history_depth` mirrors `drt docs generate --history-depth` (schema v2, #698):
    runs ride along with `include_state`, newest first, capped at the depth."""
    _seed_history(project_dir, 3)

    result = await call(server, "drt_get_manifest", include_state=True, history_depth=2)
    runs = result["syncs"][0]["runs"]
    assert len(runs) == 2
    assert runs[0]["started_at"] == "2026-05-14T02:00:00+00:00"
    assert result["syncs"][0]["dlq_depth"] == 0

    # Without include_state the manifest stays catalog-only, like the CLI.
    result = await call(server, "drt_get_manifest", history_depth=2)
    assert "runs" not in result["syncs"][0]


# ---------------------------------------------------------------------------
# drt_list_profiles / drt_test_profile — credential diagnostics (#718)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_profiles(server: FastMCP, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "drt.config.credentials.load_raw_profiles",
        lambda: {"default": {"type": "duckdb"}, "prod": {"type": "bigquery"}},
    )
    result = await call(server, "drt_list_profiles")
    assert result["profiles"] == [
        {"name": "default", "type": "duckdb"},
        {"name": "prod", "type": "bigquery"},
    ]


@pytest.mark.asyncio
async def test_test_profile_not_found(server: FastMCP, monkeypatch: Any) -> None:
    def _raise(_name: str) -> Any:
        raise KeyError("Profile 'nope' not found.")

    monkeypatch.setattr("drt.config.credentials.load_profile", _raise)
    result = await call(server, "drt_test_profile", name="nope")
    assert result["ok"] is False
    assert "error" in result


@pytest.mark.asyncio
async def test_test_profile_ok(server: FastMCP, monkeypatch: Any) -> None:
    from types import SimpleNamespace

    monkeypatch.setattr(
        "drt.config.credentials.load_profile",
        lambda _name: SimpleNamespace(type="duckdb"),
    )
    monkeypatch.setattr(
        "drt.connectors.registry.get_source",
        lambda _profile: SimpleNamespace(test_connection=lambda _p: True),
    )
    result = await call(server, "drt_test_profile", name="default")
    assert result == {"name": "default", "type": "duckdb", "ok": True}


@pytest.mark.asyncio
async def test_test_profile_connection_error(server: FastMCP, monkeypatch: Any) -> None:
    """A source whose test_connection raises → ok=False with the error message
    (the profile loaded fine, so `type` is still reported)."""
    from types import SimpleNamespace

    def _boom(_profile: object) -> bool:
        raise ConnectionError("connection refused")

    monkeypatch.setattr(
        "drt.config.credentials.load_profile",
        lambda _name: SimpleNamespace(type="postgres"),
    )
    monkeypatch.setattr(
        "drt.connectors.registry.get_source",
        lambda _profile: SimpleNamespace(test_connection=_boom),
    )
    result = await call(server, "drt_test_profile", name="prod")
    assert result["ok"] is False
    assert result["type"] == "postgres"
    assert "connection refused" in result["error"]


# ---------------------------------------------------------------------------
# drt_run_sync — cursor_value / profile_name overrides (#718)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_sync_threads_profile_override_and_full_mode_cursor_guard(
    project_dir: Path, monkeypatch: Any
) -> None:
    """`profile_name` is resolved via `resolve_profile_name(override, default)`,
    and `cursor_value` is suppressed for a non-incremental sync (the fixture
    `notify` is full-mode), mirroring the `drt run` guard."""
    from drt.engine.sync import SyncResult

    resolve_args: dict[str, Any] = {}
    captured: dict[str, Any] = {}

    def fake_resolve(override: str | None, default: str) -> str:
        resolve_args["value"] = (override, default)
        return "default"

    def fake_run_sync(*_args: Any, **kwargs: Any) -> SyncResult:
        captured.update(kwargs)
        return SyncResult()

    monkeypatch.setattr("drt.cli._helpers.resolve_profile_name", fake_resolve)
    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())

    srv = create_server(project_dir)
    await call(srv, "drt_run_sync", sync_name="notify", cursor_value="100", profile_name="prod")

    assert resolve_args["value"] == ("prod", "default")
    assert captured["cursor_value_override"] is None  # full-mode sync → guarded off


@pytest.mark.asyncio
async def test_server_lists_new_parity_tools() -> None:
    srv = create_server()
    tools = await srv._local_provider._list_tools()
    names = {t.name for t in tools}
    assert {
        "drt_dlq",
        "drt_retry",
        "drt_get_manifest",
        "drt_list_profiles",
        "drt_test_profile",
    } <= names


# ---------------------------------------------------------------------------
# drt_state_show / drt_state_reset (#776)
# ---------------------------------------------------------------------------
#
# Added with the feature rather than retrofitted. The #870 audit found the
# v0.8.0 flag wave (--limit / --fail-fast / --failed / --vars) never reached
# MCP at all, so an agent driving drt could not do the safe thing. Shipping
# the CLI and the tool together is the fix for that pattern, not just for
# this feature.


@pytest.mark.asyncio
async def test_state_show_reports_no_state(server: FastMCP) -> None:
    result = await call(server, "drt_state_show", sync_name="never-run")
    assert result.get("state") is None


@pytest.mark.asyncio
async def test_state_show_returns_the_stored_watermark(
    server: FastMCP, project_dir: Path
) -> None:
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=7,
            status="success",
            last_cursor_value="2026-06-01",
        )
    )

    result = await call(server, "drt_state_show", sync_name="users")

    assert result["state"]["last_cursor_value"] == "2026-06-01"


@pytest.mark.asyncio
async def test_state_reset_requires_a_level(server: FastMCP, project_dir: Path) -> None:
    """Same safety property as the CLI: never treat "no level" as "all of it".

    An agent is *more* likely to call this with defaults than a human is, so
    the refusal matters more here, not less.
    """
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=7,
            status="success",
        )
    )

    result = await call(server, "drt_state_reset", sync_name="users")

    assert "error" in result
    assert StateManager(project_dir).get_last_sync("users") is not None


@pytest.mark.asyncio
async def test_state_reset_runs_clears_state(server: FastMCP, project_dir: Path) -> None:
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=7,
            status="success",
        )
    )

    result = await call(server, "drt_state_reset", sync_name="users", runs=True)

    assert result.get("reset") == ["runs"]
    assert StateManager(project_dir).get_last_sync("users") is None


@pytest.mark.asyncio
async def test_state_reset_dry_run_changes_nothing(
    server: FastMCP, project_dir: Path
) -> None:
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=7,
            status="success",
        )
    )

    await call(server, "drt_state_reset", sync_name="users", runs=True, dry_run=True)

    assert StateManager(project_dir).get_last_sync("users") is not None


@pytest.mark.asyncio
async def test_state_show_all_syncs(server: FastMCP, project_dir: Path) -> None:
    from drt.state.manager import StateManager, SyncState

    StateManager(project_dir).save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=1,
            status="success",
        )
    )

    result = await call(server, "drt_state_show")

    assert "users" in result["states"]


@pytest.mark.asyncio
async def test_state_reset_watermark_level(server: FastMCP, project_dir: Path) -> None:
    """The watermark branch — no backend configured, so this exercises the
    loop without a storage delete and must still report the level."""
    result = await call(server, "drt_state_reset", sync_name="users", watermark=True)

    assert result["reset"] == ["watermark"]


@pytest.mark.asyncio
async def test_state_reset_tracked_mirror_warns(
    server: FastMCP, project_dir: Path
) -> None:
    """An agent has no help text, so the re-baseline consequence has to be in
    the response itself (#686)."""
    result = await call(
        server, "drt_state_reset", sync_name="no-such-sync", tracked_mirror=True
    )

    # Unknown sync: no destination to touch, but the level is still reported
    # rather than silently dropped.
    assert result["reset"] == ["tracked-mirror"]


@pytest.mark.asyncio
async def test_run_sync_rejects_full_refresh_with_cursor_value(
    server: FastMCP,
) -> None:
    """Mutually exclusive, same as the CLI: one says "start from nothing",
    the other "start from here"."""
    result = await call(
        server,
        "drt_run_sync",
        sync_name="anything",
        full_refresh=True,
        cursor_value="2026-01-01",
    )

    assert "mutually exclusive" in result["error"]


@pytest.mark.asyncio
async def test_run_sync_full_refresh_dry_run_does_not_clear_watermark(
    project_dir: Path, monkeypatch: Any
) -> None:
    """``dry_run=True`` is documented as a preview — ``full_refresh=True``
    combined with it must not actually delete the stored watermark (#876:
    it did, silently, mirroring the same bug fixed in the CLI's
    ``drt run --full-refresh --dry-run``)."""
    from unittest.mock import MagicMock

    from drt.engine.sync import SyncResult

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        return SyncResult()

    storage = MagicMock()
    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    monkeypatch.setattr("drt.cli._helpers.get_watermark_storage", lambda *_a, **_k: storage)

    srv = create_server(project_dir)
    await call(srv, "drt_run_sync", sync_name="notify", dry_run=True, full_refresh=True)

    storage.delete.assert_not_called()


@pytest.mark.asyncio
async def test_run_sync_full_refresh_real_run_clears_watermark(
    project_dir: Path, monkeypatch: Any
) -> None:
    """The other half of the #876 fix: a real (non-dry-run) run with
    ``full_refresh=True`` must still actually clear the watermark — the
    ``not dry_run`` guard must not have swallowed the real path too."""
    from unittest.mock import MagicMock

    from drt.engine.sync import SyncResult

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        return SyncResult()

    storage = MagicMock()
    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_source", lambda _profile: object())
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _sync: object())
    monkeypatch.setattr("drt.config.credentials.load_profile", lambda _name: object())
    monkeypatch.setattr("drt.cli._helpers.get_watermark_storage", lambda *_a, **_k: storage)

    srv = create_server(project_dir)
    await call(srv, "drt_run_sync", sync_name="notify", dry_run=False, full_refresh=True)

    storage.delete.assert_called_once_with("notify")
