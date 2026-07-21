"""Unit tests for ``sync.match_policy`` — update_only / create_only (#757).

Three layers are covered:

1. **Config** — the ``match_policy`` field, its default, and the mode
   compatibility validator (rejected for ``replace`` / ``mirror``).
2. **Engine fail-fast** — ``_check_match_policy_supported`` raises for a
   non-default policy on a destination that doesn't declare support, so the
   policy is never silently ignored.
3. **Postgres reference leg** — ``create_only`` emits ``ON CONFLICT DO
   NOTHING`` and ``update_only`` emits ``UPDATE ... WHERE``, both counting
   ``cur.rowcount == 0`` as a "skipped, no match" via ``SyncResult.skipped``.

Postgres tests mock psycopg2 — no real database needed.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SyncOptions
from drt.destinations.base import MatchPolicyCapable
from drt.engine.sync import _check_match_policy_supported

# ---------------------------------------------------------------------------
# Config: field + mode compatibility validator
# ---------------------------------------------------------------------------


def test_match_policy_defaults_to_upsert() -> None:
    assert SyncOptions().match_policy == "upsert"


@pytest.mark.parametrize("policy", ["update_only", "create_only"])
@pytest.mark.parametrize("mode", ["full", "upsert", "incremental"])
def test_match_policy_valid_on_upsert_family_modes(mode: str, policy: str) -> None:
    kwargs: dict[str, Any] = {"mode": mode, "match_policy": policy}
    if mode == "incremental":
        kwargs["cursor_field"] = "updated_at"
    opts = SyncOptions(**kwargs)
    assert opts.match_policy == policy


@pytest.mark.parametrize("mode", ["replace", "mirror"])
def test_match_policy_rejected_on_replace_and_mirror(mode: str) -> None:
    with pytest.raises(ValueError, match="not compatible with"):
        SyncOptions(mode=mode, match_policy="update_only")


def test_default_upsert_policy_allowed_on_every_mode() -> None:
    # The default must never be rejected — including on replace / mirror.
    for mode in ("full", "upsert", "replace", "mirror"):
        SyncOptions(mode=mode)  # match_policy defaults to "upsert"


# ---------------------------------------------------------------------------
# Engine fail-fast: _check_match_policy_supported
# ---------------------------------------------------------------------------


class _Incapable:
    """A destination with no match_policy support."""

    def load(self, *a: Any, **k: Any) -> None: ...


class _Capable:
    def load(self, *a: Any, **k: Any) -> None: ...

    def supported_match_policies(self) -> frozenset[str]:
        return frozenset({"update_only"})


def test_upsert_policy_is_always_a_noop() -> None:
    # Never raises regardless of destination capability.
    _check_match_policy_supported("upsert", _Incapable())  # type: ignore[arg-type]


def test_unsupported_destination_raises() -> None:
    with pytest.raises(ValueError, match="not supported by _Incapable"):
        _check_match_policy_supported("update_only", _Incapable())  # type: ignore[arg-type]


def test_capable_destination_with_matching_policy_passes() -> None:
    _check_match_policy_supported("update_only", _Capable())  # type: ignore[arg-type]


def test_capable_destination_rejects_unsupported_value() -> None:
    # _Capable declares update_only but not create_only.
    with pytest.raises(ValueError, match="not supported by _Capable"):
        _check_match_policy_supported("create_only", _Capable())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Postgres reference leg
# ---------------------------------------------------------------------------

pytest.importorskip("psycopg2.sql")

from drt.config.models import PostgresDestinationConfig  # noqa: E402
from drt.destinations.postgres import PostgresDestination  # noqa: E402


def _pg_config(**overrides: Any) -> PostgresDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "public.scores",
        "upsert_key": ["id"],
        "introspect_schema": False,
    }
    defaults.update(overrides)
    return PostgresDestinationConfig(**defaults)


def _fake_connection(rowcount: int = 1) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.rowcount = rowcount
    conn.cursor.return_value = cur
    return conn


def test_postgres_declares_match_policy_capability() -> None:
    dest = PostgresDestination()
    assert isinstance(dest, MatchPolicyCapable)
    assert dest.supported_match_policies() == frozenset(
        {"upsert", "update_only", "create_only"}
    )


def test_create_only_emits_do_nothing_and_counts_existing_as_skipped() -> None:
    dest = PostgresDestination()
    conn = _fake_connection(rowcount=0)  # every row already exists -> conflict
    opts = SyncOptions(mode="upsert", match_policy="create_only")

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load(
            [{"id": 1, "score": 5}, {"id": 2, "score": 6}], _pg_config(), opts
        )

    query = str(conn.cursor.return_value.execute.call_args.args[0])
    assert "ON CONFLICT" in query and "DO NOTHING" in query
    assert result.skipped == 2
    assert result.success == 0


def test_create_only_counts_inserted_rows_as_success() -> None:
    dest = PostgresDestination()
    conn = _fake_connection(rowcount=1)  # every row is new -> inserted
    opts = SyncOptions(mode="upsert", match_policy="create_only")

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load([{"id": 1, "score": 5}], _pg_config(), opts)

    assert result.success == 1
    assert result.skipped == 0


def test_update_only_emits_update_where_with_set_then_key_params() -> None:
    dest = PostgresDestination()
    conn = _fake_connection(rowcount=1)  # row exists -> updated
    opts = SyncOptions(mode="upsert", match_policy="update_only")

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load(
            [{"id": 1, "score": 5, "name": "a"}], _pg_config(), opts
        )

    call = conn.cursor.return_value.execute.call_args
    query = str(call.args[0])  # psycopg2 Composed repr
    assert "UPDATE " in query
    assert " SET " in query and " WHERE " in query
    assert "INSERT" not in query
    # columns = [id, score, name]; SET binds the non-key cols (score, name),
    # then WHERE binds the key (id): [5, "a", 1].
    assert call.args[1] == [5, "a", 1]
    assert result.success == 1


def test_update_only_no_match_is_counted_as_skipped() -> None:
    dest = PostgresDestination()
    conn = _fake_connection(rowcount=0)  # no such row
    opts = SyncOptions(mode="upsert", match_policy="update_only")

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load([{"id": 99, "score": 5}], _pg_config(), opts)

    assert result.skipped == 1
    assert result.success == 0


def test_update_only_requires_a_non_key_column() -> None:
    dest = PostgresDestination()
    conn = _fake_connection()
    opts = SyncOptions(mode="upsert", match_policy="update_only")

    # Only the key column present -> nothing to SET -> clear error.
    with patch.object(PostgresDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match="at least one non-key column"):
            dest.load([{"id": 1}], _pg_config(), opts)


def test_default_upsert_policy_still_upserts() -> None:
    dest = PostgresDestination()
    conn = _fake_connection()

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load([{"id": 1, "score": 5}], _pg_config(), SyncOptions())

    query = str(conn.cursor.return_value.execute.call_args.args[0])
    assert "ON CONFLICT" in query and "DO UPDATE" in query
    assert result.success == 1
    assert result.skipped == 0
