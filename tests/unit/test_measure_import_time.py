"""Tests for the reproducible #1017 startup benchmark."""

from __future__ import annotations

from scripts.measure_import_time import _parse_import_trace, _percentile


def test_parse_import_trace_converts_microseconds_to_milliseconds() -> None:
    trace = """\
import time: self [us] | cumulative | imported package
import time:       125 |        125 |   drt.child
import time:       250 |        375 | drt
unrelated stderr
"""

    assert _parse_import_trace(trace) == {
        "drt.child": (0.125, 0.125),
        "drt": (0.25, 0.375),
    }


def test_percentile_uses_nearest_rank_within_sample() -> None:
    assert _percentile([5.0, 1.0, 4.0, 2.0, 3.0], 0.95) == 5.0
