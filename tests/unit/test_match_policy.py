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
    assert dest.supported_match_policies() == frozenset({"upsert", "update_only", "create_only"})


def test_create_only_emits_do_nothing_and_counts_existing_as_skipped() -> None:
    dest = PostgresDestination()
    conn = _fake_connection(rowcount=0)  # every row already exists -> conflict
    opts = SyncOptions(mode="upsert", match_policy="create_only")

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load([{"id": 1, "score": 5}, {"id": 2, "score": 6}], _pg_config(), opts)

    # on_error defaults to "fail" -- no per-row SAVEPOINT overhead there
    # (#1139).
    query = str(conn.cursor.return_value.execute.call_args.args[0])
    assert "ON CONFLICT" in query and "DO NOTHING" in query
    assert result.skipped == 2
    assert result.skipped_no_match == 2  # #757 — named subset of skipped
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
        result = dest.load([{"id": 1, "score": 5, "name": "a"}], _pg_config(), opts)

    # on_error defaults to "fail" -- no per-row SAVEPOINT overhead there
    # (#1139).
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
    assert result.skipped_no_match == 1
    assert result.success == 0


def test_savepoint_recovery_failure_excludes_no_match_skips_from_batch_abort() -> None:
    """#1139 round 4 (Codex): a match_policy row already resolved to "no
    create/update target" wrote nothing either way -- if a LATER row's
    failure forces a full-batch abort (the row-level SAVEPOINT recovery
    itself fails, e.g. a deadlock), that already-skipped row must stay
    counted only as skipped, not ALSO recorded as a batch-abort failure
    and wrongly routed to the DLQ as if it were a real error.
    """
    dest = PostgresDestination()
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    def execute_side_effect(sql: Any, *args: Any) -> None:
        text = str(sql)
        if args and args[0] == [1, 99]:  # update_only: SET score=1 WHERE id=99
            cur.rowcount = 0  # no such row -> skipped, no match
            return
        if args and args[0] == [42, 7]:  # update_only: SET score=42 WHERE id=7
            raise Exception("deadlock detected")
        if text.startswith("ROLLBACK TO SAVEPOINT"):
            raise Exception("current transaction is aborted")
        cur.rowcount = 1

    cur.execute.side_effect = execute_side_effect
    opts = SyncOptions(mode="upsert", match_policy="update_only", on_error="skip")
    records = [{"id": 99, "score": 1}, {"id": 7, "score": 42}]

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load(records, _pg_config(), opts)

    assert result.skipped == 1
    assert result.skipped_no_match == 1
    assert result.failed == 1  # only the deadlocked row, not the skipped one too
    assert result.success == 0
    assert {e.batch_index for e in result.row_errors} == {1}


def test_savepoint_rowcount_captured_before_release_overwrites_it() -> None:
    """#1139 round 6 (Codex): cur.rowcount reflects the LAST executed
    statement on most DB-API cursors, so match_policy's "no match" check
    must read it right after the UPDATE/INSERT, before RELEASE SAVEPOINT
    (itself a separate statement) runs and could overwrite it. This test's
    fake cursor deliberately sets a different, wrong rowcount when RELEASE
    SAVEPOINT executes -- if the accounting read cur.rowcount afterward,
    the row would be misclassified as skipped even though it matched.
    """
    dest = PostgresDestination()
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    def execute_side_effect(sql: Any, *args: Any) -> None:
        text = str(sql)
        if args and args[0] == [1, 99]:  # update_only: SET score=1 WHERE id=99
            cur.rowcount = 1  # matched -> should count as success
            return
        if text.startswith("RELEASE SAVEPOINT"):
            # Simulate a driver where a later statement's own rowcount
            # (here, a no-op RELEASE) clobbers the cursor's rowcount.
            cur.rowcount = 0
            return
        cur.rowcount = 1

    cur.execute.side_effect = execute_side_effect
    opts = SyncOptions(mode="upsert", match_policy="update_only", on_error="skip")
    records = [{"id": 99, "score": 1}]

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load(records, _pg_config(), opts)

    assert result.success == 1
    assert result.skipped == 0
    assert result.skipped_no_match == 0


def test_update_only_requires_a_non_key_column() -> None:
    dest = PostgresDestination()
    conn = _fake_connection()
    opts = SyncOptions(mode="upsert", match_policy="update_only")

    # Only the key column present -> nothing to SET -> clear error.
    with patch.object(PostgresDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match="at least one non-key column"):
            dest.load([{"id": 1}], _pg_config(), opts)


def test_skipped_no_match_defaults_to_zero_and_is_subset_of_skipped() -> None:
    from drt.destinations.base import SyncResult

    r = SyncResult()
    assert r.skipped_no_match == 0
    # It names a reason *within* skipped, so total ignores it (no double count).
    r = SyncResult(success=8, failed=0, skipped=2, skipped_no_match=2)
    assert r.total == 10  # success + failed + skipped only


def test_cli_output_shows_no_match_breakdown() -> None:
    """`drt run` prints '… N skipped (M no match)' when match_policy skipped rows."""
    from drt.cli.output import print_sync_result
    from drt.destinations.base import SyncResult

    with patch("drt.cli.output.console") as console:
        print_sync_result(
            "enrich_contacts",
            SyncResult(rows_extracted=10, success=8, skipped=2, skipped_no_match=2),
            elapsed=1.0,
        )
    printed = " ".join(str(c.args[0]) for c in console.print.call_args_list)
    assert "2 skipped (2 no match)" in printed


def test_default_upsert_policy_still_upserts() -> None:
    dest = PostgresDestination()
    conn = _fake_connection()

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        result = dest.load([{"id": 1, "score": 5}], _pg_config(), SyncOptions())

    # on_error defaults to "fail" -- no per-row SAVEPOINT overhead there
    # (#1139).
    query = str(conn.cursor.return_value.execute.call_args.args[0])
    assert "ON CONFLICT" in query and "DO UPDATE" in query
    assert result.success == 1
    assert result.skipped == 0
