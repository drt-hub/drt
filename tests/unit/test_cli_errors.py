"""Tests for drt.cli.errors — ErrorFormatter."""

from __future__ import annotations

import os

import pytest

from drt.cli.errors import (
    FormattedError,
    Stage,
    classify_filename,
    format_error,
    infer_stage,
    suggest,
)

# ---------------------------------------------------------------------------
# classify_filename (pure)
# ---------------------------------------------------------------------------


def _p(*parts: str) -> str:
    """Build a filename with the OS-native separator."""
    return os.sep + os.sep.join(parts)


@pytest.mark.parametrize(
    "filename, expected",
    [
        (_p("repo", "drt", "sources", "postgres.py"), Stage.SOURCE),
        (_p("repo", "drt", "destinations", "rest_api.py"), Stage.DESTINATION),
        (_p("repo", "drt", "engine", "sync.py"), Stage.ENGINE),
        (_p("repo", "drt", "state", "manager.py"), Stage.STATE),
        (_p("repo", "drt", "cli", "main.py"), Stage.UNKNOWN),
        (_p("site-packages", "httpx", "_client.py"), Stage.UNKNOWN),
        ("", Stage.UNKNOWN),
    ],
)
def test_classify_filename(filename: str, expected: Stage) -> None:
    assert classify_filename(filename) == expected


# ---------------------------------------------------------------------------
# infer_stage (traceback walk via real raises from drt modules)
# ---------------------------------------------------------------------------


def test_infer_stage_returns_source_when_raised_from_source_module() -> None:
    """A real exception raised inside drt.sources.duckdb is classified SOURCE.

    Uses the existing AssertionError path in DuckDBSource.extract() when the
    config type doesn't match — no extra setup needed.
    """
    pytest.importorskip("duckdb")
    from drt.sources.duckdb import DuckDBSource

    class FakeConfig:
        database = ":memory:"

    try:
        list(DuckDBSource().extract("SELECT 1", FakeConfig()))  # type: ignore[arg-type]
    except AssertionError as e:
        assert infer_stage(e) is Stage.SOURCE
    else:
        pytest.fail("Expected AssertionError from DuckDBSource.extract on bad config")


def test_infer_stage_falls_back_to_unknown_for_plain_exception() -> None:
    """An exception raised in a test module (no drt path) lands on UNKNOWN."""
    try:
        raise ValueError("plain test failure")
    except ValueError as e:
        assert infer_stage(e) is Stage.UNKNOWN


def test_infer_stage_picks_deepest_drt_frame() -> None:
    """When the traceback crosses multiple drt modules, the deepest wins.

    Simulates source code raising → engine catching/re-raising. The
    re-raise from the engine frame should NOT override the original
    source attribution.
    """
    pytest.importorskip("duckdb")
    from drt.engine import sync as engine_sync  # noqa: F401 — frame visibility
    from drt.sources.duckdb import DuckDBSource

    class FakeConfig:
        database = ":memory:"

    def engine_wrapper() -> None:
        # Frame inside drt/engine/* re-raising; the inner frame is in drt/sources/*
        try:
            list(DuckDBSource().extract("x", FakeConfig()))  # type: ignore[arg-type]
        except Exception:
            raise

    # Patch this function into the engine module so its frame's co_filename
    # claims drt/engine path.
    engine_wrapper.__code__ = engine_wrapper.__code__.replace(
        co_filename=engine_sync.__file__
    )

    try:
        engine_wrapper()
    except AssertionError as e:
        # Deepest drt frame is the source raise — even though the outer
        # call chain went through engine — so SOURCE wins.
        assert infer_stage(e) is Stage.SOURCE


# ---------------------------------------------------------------------------
# suggest
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stage, message, hint_substring",
    [
        (Stage.SOURCE, "connection refused", "drt validate --check-connection"),
        (Stage.SOURCE, "Could not connect to host", "drt validate --check-connection"),
        (Stage.SOURCE, "401 Unauthorized", "auth env vars"),
        (Stage.SOURCE, "permission denied for table users", "auth env vars"),
        (Stage.DESTINATION, "401 Unauthorized", "token / api key"),
        (Stage.DESTINATION, "HTTP 429 Too Many Requests", "rate_limit"),
        (Stage.DESTINATION, "Read timeout", "network reachability"),
        (Stage.STATE, "anything", ".drt/state.json"),
    ],
)
def test_suggest_returns_actionable_hint(
    stage: Stage, message: str, hint_substring: str
) -> None:
    hint = suggest(stage, RuntimeError(message))
    assert hint is not None
    assert hint_substring in hint


def test_suggest_returns_none_for_engine_stage() -> None:
    """Engine errors are usually programming bugs — let the traceback speak."""
    assert suggest(Stage.ENGINE, RuntimeError("internal engine assertion")) is None


def test_suggest_returns_none_for_unknown_stage() -> None:
    assert suggest(Stage.UNKNOWN, RuntimeError("mystery")) is None


def test_suggest_falls_through_to_generic_when_no_keyword_matches() -> None:
    """SOURCE / DESTINATION return a generic hint even without keyword match."""
    hint = suggest(Stage.SOURCE, RuntimeError("table 'orders' has no column 'price'"))
    assert hint is not None
    assert "source profile" in hint or "query/table" in hint


# ---------------------------------------------------------------------------
# format_error (integration of the pieces)
# ---------------------------------------------------------------------------


def test_format_error_populates_all_fields() -> None:
    exc = RuntimeError("connection refused")
    # Manually set stage via classify_filename behaviour — see that fields land.
    fe = format_error("my_sync", exc)

    assert isinstance(fe, FormattedError)
    assert fe.sync_name == "my_sync"
    assert fe.error_type == "RuntimeError"
    assert fe.message == "connection refused"
    # Stage is UNKNOWN here (raised from test file), suggestion is therefore None
    assert fe.stage is Stage.UNKNOWN
    assert fe.suggestion is None


def test_formatted_error_to_dict_round_trip() -> None:
    fe = FormattedError(
        sync_name="s",
        stage=Stage.SOURCE,
        error_type="ConnectionError",
        message="refused",
        suggestion="check it",
    )
    d = fe.to_dict()
    assert d == {
        "sync_name": "s",
        "stage": "source",
        "error_type": "ConnectionError",
        "message": "refused",
        "suggestion": "check it",
    }


# ---------------------------------------------------------------------------
# Integration with cli._run_one: enriched entry fields land on the failed path
# ---------------------------------------------------------------------------


def test_run_one_failed_entry_includes_error_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """The JSON entry dict on a failed run carries the new structured fields.

    Locks in the contract for ``drt run --output json`` consumers:
    ``error`` (str, preserved for back-compat), plus new siblings
    ``error_type``, ``error_stage``, ``error_suggestion``.
    """
    from drt.cli.main import _run_one
    from drt.engine.sync import SyncResult  # noqa: F401  — type used by patched fn

    def boom(*_a: object, **_k: object) -> SyncResult:
        raise RuntimeError("connection refused")

    monkeypatch.setattr("drt.engine.sync.run_sync", boom)
    monkeypatch.setattr("drt.cli.main.run_sync", boom, raising=False)

    # Minimal sync + ctx + profile — reuse the project's existing helpers
    # if available, otherwise stub the bare minimum.
    from tests.unit.test_cli_run_telemetry import _ctx, _fake_profile, _fake_sync

    name, entry, had_err = _run_one(_fake_sync(), _ctx(json_mode=True), _fake_profile())

    assert had_err is True
    assert entry["status"] == "failed"
    assert entry["error"] == "connection refused"
    assert entry["error_type"] == "RuntimeError"
    # Stage is UNKNOWN here because the exception was raised inside the test
    # module, not inside drt/<area>/.
    assert entry["error_stage"] == "unknown"
    # Suggestion None for UNKNOWN stage
    assert entry["error_suggestion"] is None


def test_render_to_console_does_not_crash() -> None:
    """Smoke test: the Rich panel renderer is callable end-to-end."""
    from drt.cli.errors import render_to_console

    fe = FormattedError(
        sync_name="my_sync",
        stage=Stage.SOURCE,
        error_type="ConnectionError",
        message="refused",
        suggestion="Verify with: drt validate --check-connection",
    )
    render_to_console(fe)  # writes to project console; no return value


def test_render_to_console_omits_suggestion_when_none() -> None:
    """Smoke test: no suggestion line for engine / unknown stages."""
    from drt.cli.errors import render_to_console

    fe = FormattedError(
        sync_name="my_sync",
        stage=Stage.ENGINE,
        error_type="AssertionError",
        message="internal invariant",
        suggestion=None,
    )
    render_to_console(fe)
