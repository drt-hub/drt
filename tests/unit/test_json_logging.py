"""Tests for --log-format json structured logging (GitHub Issue #275)."""

from __future__ import annotations

import io
import json
import logging

from drt.cli.main import _configure_json_logging, _JsonFormatter

# ---------------------------------------------------------------------------
# Test 1: _configure_json_logging() produces valid JSON with required fields
# ---------------------------------------------------------------------------


def test_json_formatter_basic_fields() -> None:
    """_JsonFormatter emits valid JSON with ts, level, and msg fields."""
    original_handlers = logging.root.handlers[:]
    original_level = logging.root.level
    try:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonFormatter())

        logger = logging.getLogger("test_json_logging.basic")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.DEBUG)

        logger.info("hello world")

        output = stream.getvalue().strip()
        assert output, "Expected log output but got nothing"

        data = json.loads(output)
        assert "ts" in data, "Missing 'ts' field"
        assert "level" in data, "Missing 'level' field"
        assert "msg" in data, "Missing 'msg' field"
        assert data["level"] == "INFO"
        assert data["msg"] == "hello world"
        # ts must be ISO 8601 ending in Z
        assert data["ts"].endswith("Z"), f"Expected ISO 8601 UTC timestamp, got: {data['ts']}"
    finally:
        logging.root.handlers = original_handlers
        logging.root.level = original_level


# ---------------------------------------------------------------------------
# Test 2: sync_complete log line includes rows, duration_ms, and status
# ---------------------------------------------------------------------------


def test_json_formatter_sync_complete_fields() -> None:
    """sync_complete log line includes rows, duration_ms, and status fields."""
    original_handlers = logging.root.handlers[:]
    original_level = logging.root.level
    try:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonFormatter())

        logger = logging.getLogger("test_json_logging.sync_complete")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.DEBUG)

        logger.info(
            "sync_complete",
            extra={
                "sync": "my_sync",
                "rows": 42,
                "duration_ms": 1500,
                "status": "success",
            },
        )

        output = stream.getvalue().strip()
        data = json.loads(output)

        assert data["msg"] == "sync_complete"
        assert data["sync"] == "my_sync"
        assert data["rows"] == 42
        assert data["duration_ms"] == 1500
        assert data["status"] == "success"
        assert data["level"] == "INFO"
    finally:
        logging.root.handlers = original_handlers
        logging.root.level = original_level


# ---------------------------------------------------------------------------
# Test 3: each log line is a single valid JSON object (JSON Lines format)
# ---------------------------------------------------------------------------


def test_json_formatter_each_line_is_valid_json() -> None:
    """Multiple log calls each produce a separate valid JSON line."""
    original_handlers = logging.root.handlers[:]
    original_level = logging.root.level
    try:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonFormatter())

        logger = logging.getLogger("test_json_logging.multiline")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.DEBUG)

        logger.info("sync_started", extra={"sync": "s1"})
        logger.info(
            "sync_complete",
            extra={"sync": "s1", "rows": 10, "duration_ms": 200, "status": "success"},
        )

        lines = [line for line in stream.getvalue().splitlines() if line.strip()]
        assert len(lines) == 2, f"Expected 2 log lines, got {len(lines)}"
        for line in lines:
            obj = json.loads(line)  # must not raise
            assert "ts" in obj
            assert "level" in obj
            assert "msg" in obj
    finally:
        logging.root.handlers = original_handlers
        logging.root.level = original_level


# ---------------------------------------------------------------------------
# Test 4: _configure_json_logging replaces root handlers cleanly
# ---------------------------------------------------------------------------


def test_configure_json_logging_replaces_handlers() -> None:
    """_configure_json_logging() sets exactly one handler on the root logger."""
    old_handlers = logging.root.handlers[:]
    old_level = logging.root.level
    # Add a dummy handler first to verify replacement
    dummy = logging.StreamHandler(io.StringIO())
    logging.root.addHandler(dummy)
    try:
        _configure_json_logging()

        assert len(logging.root.handlers) == 1
        assert isinstance(logging.root.handlers[0].formatter, _JsonFormatter)
    finally:
        logging.root.handlers = old_handlers
        logging.root.setLevel(old_level)
