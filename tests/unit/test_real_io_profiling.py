"""Non-network invariants for the manual real-I/O profiler (#1008)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from benchmarks.profile_real_io import (
    _percentages,
    _ProfileStats,
    _socket_io_seconds,
    _synthetic_records,
)


def test_percentages_sum_to_one_hundred_after_rounding() -> None:
    percentages = _percentages(3.0, (1.0, 1.0, 1.0))

    assert percentages == (33.33, 33.33, 33.34)
    assert sum(percentages) == 100.0


def test_socket_io_sums_self_time_without_inclusive_double_counting() -> None:
    recv_key = ("~", 0, "<method 'recv' of '_socket.socket' objects>")
    send_key = ("~", 0, "<method 'send' of '_socket.socket' objects>")
    wrapper_key = ("/httpcore/_backends/sync.py", 1, "read")
    stats = cast(
        _ProfileStats,
        SimpleNamespace(
            stats={
                recv_key: (2, 2, 0.7, 0.7, {wrapper_key: (2, 2, 0.7, 0.7)}),
                send_key: (2, 2, 0.1, 0.1, {wrapper_key: (2, 2, 0.1, 0.1)}),
                wrapper_key: (2, 2, 0.2, 1.0, {}),
            },
            total_tt=1.0,
            total_calls=6,
            prim_calls=6,
        ),
    )

    assert _socket_io_seconds(stats) == pytest.approx(0.8)


def test_synthetic_records_match_existing_benchmark_shape() -> None:
    assert _synthetic_records(2) == [
        {
            "id": 1,
            "name": "user-00000001",
            "email": "user-00000001@example.com",
            "score": 1,
        },
        {
            "id": 2,
            "name": "user-00000002",
            "email": "user-00000002@example.com",
            "score": 2,
        },
    ]
