"""Tests for ``drt.cli.output.print_error`` — Rich-markup safety."""

from __future__ import annotations

from io import StringIO

import pytest


@pytest.mark.parametrize(
    "message, must_appear",
    [
        # The bug that motivated this guard: ``[mcp]`` got eaten as a Rich
        # style tag, turning "pip install drt-core[mcp]" into "pip install
        # drt-core" in the rendered output.
        ("MCP server requires: pip install drt-core[mcp]", "drt-core[mcp]"),
        # Bracketed log-level markers similarly.
        ("[INFO] something happened", "[INFO]"),
        # Multiple bracket groups in one message.
        ("install drt-core[postgres,duckdb] for full DB support", "drt-core[postgres,duckdb]"),
        # Plain messages still work — sanity check that the escape didn't
        # break the happy path.
        ("simple error message", "simple error message"),
    ],
)
def test_print_error_preserves_square_brackets(
    message: str, must_appear: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rich must not consume square-bracket fragments as style tags."""
    from rich.console import Console

    from drt.cli import output as out

    buf = StringIO()
    monkeypatch.setattr(out, "console", Console(file=buf, force_terminal=False, no_color=True))

    out.print_error(message)
    rendered = buf.getvalue()

    assert must_appear in rendered, (
        f"Expected '{must_appear}' in rendered output. Got: {rendered!r}"
    )
