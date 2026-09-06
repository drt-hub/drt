"""SnapshotDiffSource against a real Postgres (#755, ADR 0005 step 5).

A mock cursor accepts any SQL string — it cannot prove the JOIN/anti-join
structure is correct, nor catch the documented ``ROW(...)::text`` NULL-vs-
empty-string ambiguity this module's hash expression was deliberately built
to avoid (see ``_diff_hash_expr`` in ``drt/sources/postgres.py``). Both need
a live engine, same discipline as #960's ``test_managed_table_primitive_smoke.py``.
"""

from __future__ import annotations

import pytest

from drt.config.credentials import PostgresProfile
from drt.sources.postgres import PostgresSource

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def _seed(admin: object, sql: str) -> None:
    with admin.cursor() as cur:  # type: ignore[attr-defined]
        cur.execute(sql)
    admin.commit()  # type: ignore[attr-defined]


def test_diff_incremental_round_trip() -> None:
    """First run sends everything as added; a second run after edits
    classifies added / changed / removed correctly; a third run with no
    edits sees nothing."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT, plan TEXT); "
                "INSERT INTO users VALUES (1, 'a@x.com', 'free'), "
                "(2, 'b@x.com', 'free'), (3, 'c@x.com', 'pro')",
            )
            query = "SELECT id, email, plan FROM users"

            # --- Run 1: first run, nothing to diff against yet. ---
            result = source.extract_snapshot_diff(
                query,
                config,
                sync_name="users_sync",
                key_columns=["id"],
                hash_columns="all",
            )
            added = list(result.added)
            assert result.is_first_run is True
            assert list(result.changed) == []
            assert list(result.removed_keys) == []
            assert {r["id"] for r in added} == {1, 2, 3}
            source.commit_snapshot_diff(config, "users_sync")

            # --- Edit: row 1 changes plan, row 2 is deleted, row 4 is added. ---
            _seed(
                admin,
                "UPDATE users SET plan = 'pro' WHERE id = 1; "
                "DELETE FROM users WHERE id = 2; "
                "INSERT INTO users VALUES (4, 'd@x.com', 'free')",
            )

            # --- Run 2: classify against run 1's committed baseline. ---
            result = source.extract_snapshot_diff(
                query,
                config,
                sync_name="users_sync",
                key_columns=["id"],
                hash_columns="all",
            )
            assert result.is_first_run is False
            added = list(result.added)
            changed = list(result.changed)
            removed = list(result.removed_keys)
            assert [r["id"] for r in added] == [4]
            assert [r["id"] for r in changed] == [1]
            assert changed[0]["plan"] == "pro"
            assert removed == [{"id": 2}]
            source.commit_snapshot_diff(config, "users_sync")

            # --- Run 3: no edits since the last commit — nothing to report. ---
            result = source.extract_snapshot_diff(
                query,
                config,
                sync_name="users_sync",
                key_columns=["id"],
                hash_columns="all",
            )
            assert list(result.added) == []
            assert list(result.changed) == []
            assert list(result.removed_keys) == []
        finally:
            admin.close()


def test_uncommitted_run_rediffs_against_the_same_stale_baseline() -> None:
    """The engine only commits after a run with zero row failures (#755's
    conservative reconcile-on-next-run posture, mirroring #920/#955). Prove
    the source side actually supports that: calling extract twice with no
    commit between must produce the identical classification both times."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT); "
                "INSERT INTO users VALUES (1, 'a@x.com')",
            )
            query = "SELECT id, email FROM users"
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            list(result.added)
            source.commit_snapshot_diff(config, "s")

            _seed(admin, "UPDATE users SET email = 'a2@x.com' WHERE id = 1")

            first = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            first_changed = [r["id"] for r in first.changed]
            list(first.added)
            list(first.removed_keys)
            # Simulate a failed run: no commit.

            second = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            second_changed = [r["id"] for r in second.changed]
            assert first_changed == second_changed == [1]
        finally:
            admin.close()


def test_null_to_empty_string_transition_is_detected_as_changed() -> None:
    """Postgres's ``ROW(...)::text`` cast renders a NULL column and an empty
    string identically for some column types — a naive row-hash built that
    way would silently miss this transition as 'unchanged'. The hash
    expression this module actually uses (coalesce+sentinel+separator, not
    ROW()::text) must not have that blind spot."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, note TEXT); INSERT INTO users VALUES (1, NULL)",
            )
            query = "SELECT id, note FROM users"
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            list(result.added)
            source.commit_snapshot_diff(config, "s")

            _seed(admin, "UPDATE users SET note = '' WHERE id = 1")

            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            changed = list(result.changed)
            assert [r["id"] for r in changed] == [1]
            assert changed[0]["note"] == ""
        finally:
            admin.close()


def test_explicit_hash_columns_ignores_columns_outside_the_list() -> None:
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT, last_seen TEXT); "
                "INSERT INTO users VALUES (1, 'a@x.com', 'monday')",
            )
            query = "SELECT id, email, last_seen FROM users"
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns=["email"]
            )
            list(result.added)
            source.commit_snapshot_diff(config, "s")

            # last_seen changes every run but is not a hash column — must
            # NOT register as changed.
            _seed(admin, "UPDATE users SET last_seen = 'tuesday' WHERE id = 1")
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns=["email"]
            )
            assert list(result.changed) == []
            source.commit_snapshot_diff(config, "s")

            # email IS a hash column — must register.
            _seed(admin, "UPDATE users SET email = 'new@x.com' WHERE id = 1")
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns=["email"]
            )
            assert [r["id"] for r in result.changed] == [1]
        finally:
            admin.close()


def test_hash_columns_typo_raises_loudly() -> None:
    """A typo'd hash_columns entry must fail loudly, not silently narrow the
    hash and hide real changes as unchanged rows (the worst failure class:
    silent data staleness)."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(admin, "CREATE TABLE users (id INTEGER, email TEXT)")
            with pytest.raises(ValueError, match="check for a typo"):
                source.extract_snapshot_diff(
                    "SELECT id, email FROM users",
                    config,
                    sync_name="s",
                    key_columns=["id"],
                    hash_columns=["emial"],
                )
        finally:
            admin.close()


def test_missing_key_column_raises_loudly() -> None:
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(admin, "CREATE TABLE users (id INTEGER, email TEXT)")
            with pytest.raises(ValueError, match="upsert_key column"):
                source.extract_snapshot_diff(
                    "SELECT id, email FROM users",
                    config,
                    sync_name="s",
                    key_columns=["user_id"],
                    hash_columns="all",
                )
        finally:
            admin.close()


def test_commit_without_extract_is_a_noop() -> None:
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        # Never called extract_snapshot_diff for this sync_name — must not raise.
        source.commit_snapshot_diff(config, "never_run")


def test_concurrent_runs_of_the_same_sync_never_corrupt_the_snapshot() -> None:
    """Two (or more) concurrent extract_snapshot_diff/commit_snapshot_diff
    calls for the *same* sync_name race Postgres's own catalog on the
    scratch-table rebuild and the current-table swap — confirmed live, at
    multiple points (the initial CREATE, a later SELECT if a concurrent
    rebuild drops the table mid-read, and commit's RENAME swap). This is
    NOT a supported usage pattern (running the same sync twice concurrently
    isn't safe anywhere else in drt either — see #854's drt serve
    coalescing, built specifically to prevent this).

    ``drt/sources/postgres.py`` reclassifies the races it can identify by
    SQLSTATE into a clear ``RuntimeError`` — but this is deliberately
    best-effort, not a guarantee: a full guarantee would need a lock held
    across the entire extract-through-commit lifecycle (spanning separate
    connections, with a destination write physically in between), and a
    session-scoped Postgres advisory lock held that long would leak
    indefinitely in a long-running process (drt serve, dagster-drt) if
    commit is skipped after a row failure — worse than an occasional
    unclassified error. So the one guarantee this test actually enforces is
    the one that matters: no run, however it fails, ever leaves a partial or
    mixed snapshot behind. Every attempt either raises (cleanly reclassified
    or not) with zero effect on the persisted snapshot, or completes with a
    fully correct one. Reproduced by running many concurrent attempts
    repeatedly, since a single run isn't guaranteed to hit the race at all.
    """
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        # Autocommit: a lingering open transaction from a verification SELECT
        # below would hold a lock the concurrent worker threads' own ALTER
        # TABLE (in commit_snapshot_diff's swap) can block on indefinitely --
        # confirmed live, this is exactly what made an early version of this
        # test hang rather than fail.
        admin.autocommit = True
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT); "
                "INSERT INTO users SELECT g, 'u' || g || '@x.com' "
                "FROM generate_series(1, 50) g",
            )
            query = "SELECT id, email FROM users"

            import threading

            for _attempt in range(4):
                source = PostgresSource()
                errors: list[BaseException] = []
                added_counts: list[int] = []
                lock = threading.Lock()

                def worker() -> None:
                    try:
                        result = source.extract_snapshot_diff(
                            query,
                            config,
                            sync_name="dup_sync",
                            key_columns=["id"],
                            hash_columns="all",
                        )
                        added = list(result.added)
                        list(result.changed)
                        list(result.removed_keys)
                        with lock:
                            added_counts.append(len(added))
                        source.commit_snapshot_diff(config, "dup_sync")
                    except Exception as e:  # noqa: BLE001 - collecting for assertion
                        with lock:
                            errors.append(e)

                threads = [threading.Thread(target=worker) for _ in range(6)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                # Best-effort reclassification (see the docstring above):
                # most races surface as the clear RuntimeError, but a known
                # residual window can still surface Postgres's own
                # concurrent-DDL error class directly. Either is acceptable
                # here; anything else would be a genuinely new failure mode.
                _KNOWN_RACE_PGCODES = {"23505", "42P07", "42710", "42P01"}
                for e in errors:
                    if isinstance(e, RuntimeError) and "another run of sync" in str(e):
                        continue
                    pgcode = getattr(e.__cause__, "pgcode", None) or getattr(e, "pgcode", None)
                    assert pgcode in _KNOWN_RACE_PGCODES, (
                        f"unexpected failure mode: {type(e).__name__}: {e}"
                    )

                # No matter how many attempts succeeded, the final snapshot
                # must reflect exactly one complete generation (50 rows) —
                # never a partial or double-counted one. (Not asserting on
                # added_counts itself: only the first attempt is a genuine
                # first-run/full-send case — the source table is never
                # mutated between attempts here, so attempts 2+ correctly
                # diff against an unchanged baseline and report 0 added.)
                with admin.cursor() as cur:
                    cur.execute("SELECT count(*) FROM _drt._drt_snapshot_dup_sync")
                    assert cur.fetchone() == (50,)
        finally:
            admin.close()
