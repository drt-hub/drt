"""PostgreSQL source implementation.

Requires: pip install drt-core[postgres]

Example ~/.drt/profiles.yml:
    pg:
      type: postgres
      host: localhost
      port: 5432
      dbname: analytics
      user: analyst
      password_env: PG_PASSWORD   # export PG_PASSWORD=secret
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal

from drt.config.credentials import PostgresProfile, ProfileConfigLike, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry
from drt.sources.base import SnapshotDiffResult

# Sentinels for the #755 diff-hash concatenation (see extract_snapshot_diff).
# Control characters, not printable text, so no real column value collides
# with them by accident.
_DIFF_NULL_SENTINEL = "\x01"
_DIFF_FIELD_SEP = "\x02"

# SQLSTATEs observed live when two concurrent extract_snapshot_diff calls for
# the *same* sync_name race the scratch-table rebuild -- confirmed by running
# 6 concurrent calls repeatedly: unique_violation (pg_type catalog),
# duplicate_table, duplicate_object, and undefined_table (a second run's
# rebuild dropping the table a first run's later SELECT is still reading).
# Not a closed set -- different timing/Postgres versions may raise others in
# the same family -- so this narrows "was this a concurrent-rebuild race" to
# a best-effort classification, not a guarantee. What IS guaranteed (live
# reproduced across repeated runs): every attempt either raises loudly with
# no destination write, or completes with a correct, uncorrupted result --
# never a silently mixed/wrong one. Full mutual exclusion (a session-scoped
# advisory lock spanning extract_snapshot_diff through commit_snapshot_diff)
# was considered and rejected -- the engine deliberately skips
# commit_snapshot_diff on any row failure, which would leak a held lock
# indefinitely rather than self-heal on the next run the way this
# reclassify-and-fail-loudly approach does.
_DIFF_CONCURRENT_RACE_PGCODES = frozenset({"23505", "42P07", "42710", "42P01"})


def _reraise_diff_concurrency_race(e: BaseException, sync_name: str) -> None:
    """Reclassify a concurrent scratch-table race (see
    _DIFF_CONCURRENT_RACE_PGCODES) into a clear, actionable error. Returns
    normally (does NOT raise) for anything else, so callers always follow
    this with their own bare ``raise`` to re-propagate the original.
    """
    if getattr(e, "pgcode", None) in _DIFF_CONCURRENT_RACE_PGCODES:
        raise RuntimeError(
            f"sync.incremental_strategy: diff — another run of sync "
            f"{sync_name!r} appears to be building the same snapshot table "
            f"concurrently. diff-strategy syncs must not run concurrently "
            f"for the same sync — see drt serve's request coalescing (#854) "
            f"or your scheduler's own overlap protection."
        ) from e


def _key_join_condition(key_columns: list[str], left: str, right: str) -> Any:
    """Compose ``left.k1 = right.k1 AND left.k2 = right.k2 ...`` for a multi-column key."""
    from psycopg2 import sql as _pgsql

    parts = [
        _pgsql.SQL("{} = {}").format(_pgsql.Identifier(left, k), _pgsql.Identifier(right, k))
        for k in key_columns
    ]
    return _pgsql.SQL(" AND ").join(parts)


def _diff_hash_expr(columns: list[str], alias: str) -> Any:
    """Compose a NULL-safe row hash over ``columns``, immune to ``ROW(...)::text``'s
    documented NULL-vs-empty-string ambiguity (verified live — see
    ``test_diff_changed_detects_null_to_empty_string_transition``): each
    column is cast to text, NULLs replaced with a sentinel byte no real
    value collides with, then joined with a separator byte before hashing —
    so ``NULL`` and ``''`` in the same column produce different hashes.
    """
    from psycopg2 import sql as _pgsql

    parts: list[Any] = []
    for i, c in enumerate(columns):
        if i > 0:
            parts.append(_pgsql.SQL(" || {} || ").format(_pgsql.Literal(_DIFF_FIELD_SEP)))
        parts.append(
            _pgsql.SQL("coalesce({}::text, {})").format(
                _pgsql.Identifier(alias, c), _pgsql.Literal(_DIFF_NULL_SENTINEL)
            )
        )
    return _pgsql.SQL("md5({})").format(_pgsql.SQL("").join(parts))


class PostgresSource:
    """Extract records from a PostgreSQL database."""

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Transient — the server or the link to it was momentarily unavailable,
        and the identical query may well succeed on a second attempt:

        - ``OperationalError`` — connection refused, ``server closed the
          connection unexpectedly``, ``terminating connection due to
          administrator command`` (failover, restart, idle-timeout reaper).
        - ``InterfaceError`` — the driver's own connection object went bad.

        Permanent — ``ProgrammingError`` (bad SQL, missing relation, denied
        privilege), ``DataError``, ``IntegrityError``. Retrying these only
        delays an error the user has to fix anyway.

        Matched against ``OperationalError`` / ``InterfaceError`` specifically,
        not the shared base: psycopg2 makes ``OperationalError``,
        ``ProgrammingError``, ``DataError`` and ``IntegrityError`` all siblings
        under ``DatabaseError`` (PEP 249's hierarchy), so testing against the
        base would happily retry a typo in the user's SQL three times.

        ``OperationalError`` *does* have subclasses, and not all of them are
        transient — ``psycopg2.errors.InvalidPassword`` and
        ``InvalidAuthorizationSpecification`` live under it. Retrying a wrong
        password is not just wasted work: three rapid attempts can trip an
        account lockout policy and turn a config typo into an outage. They are
        excluded both by class and by SQLSTATE class ``28``, because neither
        signal is always present — ``pgcode`` is only populated on errors the
        server actually raised.

        ``psycopg2`` is imported inside the method: it is an optional extra,
        and this class is imported unconditionally by the connector registry.
        """
        try:
            import psycopg2
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        if not isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            return False
        # Exclude authentication failures. psycopg2 files these *under*
        # OperationalError, so the isinstance above lets them through.
        # Matched two ways because either can be absent: by class (works for
        # any exception the driver constructs) and by SQLSTATE class 28,
        # invalid_authorization_specification (set only on server-raised
        # errors, but authoritative when present).
        auth_errors = (
            psycopg2.errors.InvalidAuthorizationSpecification,
            psycopg2.errors.InvalidPassword,
        )
        if isinstance(exc, auth_errors):
            return False
        pgcode = getattr(exc, "pgcode", None)
        return not (pgcode and str(pgcode).startswith("28"))

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** Opening
        the connection, executing the query and fetching the result set are
        wrapped in exponential backoff, so a Postgres restart or a dropped
        connection on the way in no longer fails the whole sync.

        A failure **after the first row has been yielded is not retried** and
        propagates. By then the engine has already handed those rows to the
        destination; re-running the query would re-emit them, and skipping
        them would need a stable ordering the query does not promise. That is
        a checkpointing problem, not a retry problem — see #766.

        **Streaming (#765).** Rows arrive through a *named* (server-side)
        cursor in ``fetch_size`` batches rather than a single ``fetchall()``,
        so peak memory tracks the batch instead of the result set — an order of
        magnitude lower on a 300k-row extract. Measured figures live in
        ``docs/research/extraction-memory.md``, which is the single source for
        them. A plain ``cursor()`` would not do — psycopg2 buffers the whole
        result set client-side unless the cursor is named.

        The lifecycle is the subtle part. A server-side cursor lives only as
        long as its connection, so #766's ``finally: conn.close()`` *inside*
        the retried unit is no longer possible — closing there would invalidate
        the cursor before the first row is read. Instead:

        - the retried unit connects and executes, and closes its own
          connection on failure, so a retried attempt leaks nothing;
        - the yield loop owns the connection afterwards and closes it in a
          ``finally``, which also runs on ``GeneratorExit`` when a consumer
          abandons the iterator (``--limit`` / ``--fail-fast``, #775/#774).

        That last case is a real leak, not a theoretical one: verified against
        a live Postgres that dropping the cursor reference and forcing a GC
        leaves the server-side cursor open in ``pg_cursors``.

        ``query_tags`` is unused — Postgres has no session/job-level tagging
        primitive drt can reach, so the SQL comment the engine already
        prepended to ``query`` is this connector's only attribution (#768).
        """
        assert isinstance(config, PostgresProfile)

        def _connect_and_execute() -> tuple[Any, Any]:
            conn = self._connect(config)
            try:
                # Named cursor => server-side. The name only has to be unique
                # within the session, and each extract() gets its own
                # connection, so a fixed name is safe.
                cur = conn.cursor(name="drt_extract")
                cur.itersize = config.fetch_size
                cur.execute(query)
                return conn, cur
            except BaseException:
                # The failed attempt cleans up after itself — with the close
                # moved out of `finally`, nothing else would.
                conn.close()
                raise

        conn, cur = with_retry(_connect_and_execute, RetryConfig(), retry_on=self._is_transient)

        # Iteration sits outside the retry: once a row is yielded it cannot be
        # un-sent, so re-running is not safe. The finally also fires on
        # GeneratorExit, so an abandoned generator still closes the connection.
        try:
            # ``cur.description`` is None until the first batch actually
            # arrives — a named cursor has not touched the server at DECLARE
            # time, unlike a plain one where execute() populates it
            # immediately. So columns are read inside the loop, on the first
            # iteration, rather than up front.
            columns: list[str] = []
            for row in cur:
                if not columns:
                    columns = [desc[0] for desc in cur.description]
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, PostgresProfile)
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def _connect(self, config: PostgresProfile) -> Any:
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError("PostgreSQL support requires: pip install drt-core[postgres]") from e

        password = resolve_env(config.password, config.password_env)
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=password,
        )

    # --- ManagedTableCapable (#960, ADR 0005 step 3) ------------------------
    #
    # Uses its own plain cursor + explicit commit() — deliberately not the
    # named (server-side) cursor extract() uses, since a named cursor cannot
    # run DDL (see extract()'s docstring), and nothing on this connection
    # sets autocommit.

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        """Create ``config.managed_schema`` if it does not already exist.

        Two race conditions this specifically guards, both confirmed live
        (self-review after a Codex review pass on #920, the first real
        consumer of this method, hit its usage limit mid-review):

        1. **The escape hatch** (#695 discipline): probe first — a
           locked-down user with no ``CREATE`` privilege, but an
           admin-pre-provisioned schema, must never have the ``CREATE``
           statement issued at all.
        2. **Concurrent first use.** ``CREATE SCHEMA IF NOT EXISTS`` is NOT
           atomic across sessions in Postgres: two sessions can both pass
           the probe above (schema doesn't exist yet) and both attempt the
           ``CREATE`` — the loser gets a catalog ``UniqueViolation``
           (``pg_namespace_nspname_index``), not a graceful no-op.
           Reproduced live with 8 concurrent first writes through #920's
           warehouse backend, all racing this method for the very first
           time. If the schema exists after the error, the other session
           won the race; anything else re-raises.
        """
        assert isinstance(config, PostgresProfile)
        from psycopg2 import sql as _pgsql

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (config.managed_schema,),
            )
            if cur.fetchone() is not None:
                return
            try:
                cur.execute(
                    _pgsql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        _pgsql.Identifier(config.managed_schema)
                    )
                )
                conn.commit()
            except Exception:
                conn.rollback()
                cur = conn.cursor()
                cur.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                    (config.managed_schema,),
                )
                if cur.fetchone() is None:
                    raise
        finally:
            conn.close()

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        assert isinstance(config, PostgresProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Not to_regclass(%s) with a formatted "schema.table" string: it
            # parses its argument as an identifier, so an unquoted mixed-case
            # managed_schema (e.g. "DrtManaged") gets folded to lowercase and
            # never matches the case-sensitive schema ensure_managed_schema()
            # actually created via Identifier(). Binding schema and table as
            # separate parameters here does no identifier parsing at all.
            # table_type = 'BASE TABLE' excludes views and foreign tables,
            # which information_schema.tables otherwise also lists here —
            # same distinction the destination catalog query already makes
            # (drt/destinations/postgres.py). A same-named view would
            # otherwise read back as "exists" and later fail on DROP TABLE
            # (a view needs DROP VIEW). Partitioned tables still report as
            # 'BASE TABLE' here, so this doesn't exclude them.
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'",
                (config.managed_schema, table_name),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        assert isinstance(config, PostgresProfile)
        from psycopg2 import sql as _pgsql

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                _pgsql.SQL("DROP TABLE IF EXISTS {}").format(
                    _pgsql.Identifier(config.managed_schema, table_name)
                )
            )
            conn.commit()
        finally:
            conn.close()

    # --- SnapshotDiffSource (#755, ADR 0005 step 5) --------------------------
    #
    # Builds on ManagedTableCapable above for the schema; owns its own
    # snapshot table naming and DDL (same scope split #960's docstring
    # describes). Table names: "_drt_snapshot_<sync_name>" (the persisted
    # baseline) and "..._scratch" (this run's fresh extract, promoted to the
    # baseline by commit_snapshot_diff — never read directly by a caller).

    def _snapshot_table_names(self, sync_name: str) -> tuple[str, str]:
        return f"_drt_snapshot_{sync_name}", f"_drt_snapshot_{sync_name}_scratch"

    def extract_snapshot_diff(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        sync_name: str,
        key_columns: list[str],
        hash_columns: Literal["all"] | list[str],
        query_tags: dict[str, str] | None = None,
    ) -> SnapshotDiffResult:
        """See ``SnapshotDiffSource.extract_snapshot_diff``.

        ``query_tags`` is unused for the same reason as ``extract()`` —
        Postgres has no session/job-level tagging primitive; the SQL comment
        is already baked into ``query``.
        """
        assert isinstance(config, PostgresProfile)
        from psycopg2 import sql as _pgsql

        self.ensure_managed_schema(config)
        current_table, scratch_table = self._snapshot_table_names(sync_name)

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Fresh scratch every call. DROP first (not CREATE OR REPLACE —
            # Postgres has no such thing for tables) so a crashed prior run's
            # never-promoted scratch can't leak stale columns into this
            # run's introspection below.
            try:
                cur.execute(
                    _pgsql.SQL("DROP TABLE IF EXISTS {}").format(
                        _pgsql.Identifier(config.managed_schema, scratch_table)
                    )
                )
                cur.execute(
                    _pgsql.SQL("CREATE TABLE {} AS {}").format(
                        _pgsql.Identifier(config.managed_schema, scratch_table),
                        _pgsql.SQL(query),
                    )
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                # Confirmed live: two concurrent extract_snapshot_diff calls
                # for the same sync_name race this DROP+CREATE — Postgres's
                # own catalog doesn't serialize them into a safe no-op the
                # way CREATE SCHEMA/TABLE IF NOT EXISTS's existence-probe
                # pattern lets #960/#920 recover from. Unlike that case, a
                # concurrent scratch table's *content* isn't fungible with
                # this run's — silently reusing whichever writer won would
                # silently mix two different model results into one diff.
                # So this fails loudly with an actionable message instead of
                # attempting recovery.
                _reraise_diff_concurrency_race(e, sync_name)
                raise

            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (config.managed_schema, scratch_table),
            )
            all_columns = [row[0] for row in cur.fetchall()]

            missing_keys = [k for k in key_columns if k not in all_columns]
            if missing_keys:
                raise ValueError(
                    f"sync.incremental_strategy: diff — destination "
                    f"upsert_key column(s) {missing_keys} not found in the "
                    f"model's output columns {all_columns}."
                )
            if hash_columns == "all":
                diff_columns = [c for c in all_columns if c not in key_columns]
            else:
                missing_hash = [c for c in hash_columns if c not in all_columns]
                if missing_hash:
                    raise ValueError(
                        f"sync.diff.hash_columns: column(s) {missing_hash} "
                        f"not found in the model's output columns "
                        f"{all_columns} — check for a typo."
                    )
                diff_columns = list(hash_columns)

            is_first_run = not self.managed_table_exists(config, current_table)
        finally:
            conn.close()

        if is_first_run:
            added = self._stream_query(
                config,
                _pgsql.SQL("SELECT * FROM {}").format(
                    _pgsql.Identifier(config.managed_schema, scratch_table)
                ),
                sync_name=sync_name,
            )
            changed: Iterator[dict[str, Any]] = iter(())
            removed_keys: Iterator[dict[str, Any]] = iter(())
        else:
            schema = config.managed_schema
            join_cond = _key_join_condition(key_columns, "s", "c")
            anti_join_probe = _pgsql.Identifier("c", key_columns[0])
            reverse_anti_join_probe = _pgsql.Identifier("s", key_columns[0])

            added = self._stream_query(
                config,
                _pgsql.SQL(
                    "SELECT s.* FROM {scratch} s LEFT JOIN {current} c ON {join_cond} "
                    "WHERE {probe} IS NULL"
                ).format(
                    scratch=_pgsql.Identifier(schema, scratch_table),
                    current=_pgsql.Identifier(schema, current_table),
                    join_cond=join_cond,
                    probe=anti_join_probe,
                ),
                sync_name=sync_name,
            )
            removed_keys = self._stream_query(
                config,
                _pgsql.SQL(
                    "SELECT {key_list} FROM {current} c LEFT JOIN {scratch} s ON {join_cond} "
                    "WHERE {probe} IS NULL"
                ).format(
                    key_list=_pgsql.SQL(", ").join(_pgsql.Identifier("c", k) for k in key_columns),
                    current=_pgsql.Identifier(schema, current_table),
                    scratch=_pgsql.Identifier(schema, scratch_table),
                    join_cond=_key_join_condition(key_columns, "c", "s"),
                    probe=reverse_anti_join_probe,
                ),
                sync_name=sync_name,
            )
            if diff_columns:
                changed = self._stream_query(
                    config,
                    _pgsql.SQL(
                        "SELECT s.* FROM {scratch} s JOIN {current} c ON {join_cond} "
                        "WHERE {hash_s} IS DISTINCT FROM {hash_c}"
                    ).format(
                        scratch=_pgsql.Identifier(schema, scratch_table),
                        current=_pgsql.Identifier(schema, current_table),
                        join_cond=join_cond,
                        hash_s=_diff_hash_expr(diff_columns, "s"),
                        hash_c=_diff_hash_expr(diff_columns, "c"),
                    ),
                    sync_name=sync_name,
                )
            else:
                # No non-key columns to compare (key_columns covers every
                # returned column, or an explicit hash_columns list somehow
                # resolved empty — rejected earlier by DiffConfig for the
                # config-level case, but "all" minus every key column can
                # still legitimately land here). A row that matches on key
                # can never be "changed" with nothing left to differ on.
                changed = iter(())

        return SnapshotDiffResult(
            added=added,
            changed=changed,
            removed_keys=removed_keys,
            is_first_run=is_first_run,
        )

    def _stream_query(
        self, config: PostgresProfile, composed: Any, *, sync_name: str
    ) -> Iterator[dict[str, Any]]:
        """Run a composed query on its own connection, streaming rows as dicts.

        Own server-side cursor + own connection per call (same discipline as
        ``extract()``) — callers may hold several of these open at once
        (e.g. ``added`` and ``removed_keys`` from the same
        ``extract_snapshot_diff`` call), each against its own connection.

        ``sync_name`` is only used to reclassify a relation-not-found error
        into the same concurrent-run message ``extract_snapshot_diff``'s own
        scratch-table create already raises: this method's queries read the
        scratch/current tables built there, on a fresh connection opened
        *after* that step returns, so another concurrent run of the same
        sync racing its own scratch-table rebuild in between can drop the
        table out from under an in-flight read here too. Confirmed live, and
        wrapped around BOTH the initial execute (a named/server-side cursor's
        ``DECLARE`` happens at execute time) and the iteration loop below (its
        actual ``FETCH`` batches happen lazily, per iteration, so the same
        race can just as easily land mid-iteration as it can at execute).
        """

        def _connect_and_execute() -> tuple[Any, Any]:
            conn = self._connect(config)
            try:
                cur = conn.cursor(name="drt_snapshot_diff")
                cur.itersize = config.fetch_size
                cur.execute(composed)
                return conn, cur
            except BaseException as e:
                conn.close()
                _reraise_diff_concurrency_race(e, sync_name)
                raise

        conn, cur = with_retry(_connect_and_execute, RetryConfig(), retry_on=self._is_transient)
        try:
            columns: list[str] = []
            try:
                for row in cur:
                    if not columns:
                        columns = [desc[0] for desc in cur.description]
                    yield dict(zip(columns, row))
            except BaseException as e:
                _reraise_diff_concurrency_race(e, sync_name)
                raise
        finally:
            conn.close()

    def commit_snapshot_diff(self, config: ProfileConfigLike, sync_name: str) -> None:
        """See ``SnapshotDiffSource.commit_snapshot_diff``.

        Three-step swap — rename current -> current_old, scratch -> current
        (one transaction/commit), then drop current_old in a separate
        transaction — matching the destination-side ``replace_strategy:
        swap`` idiom already established in ``drt/destinations/postgres.py``
        (``_complete_swap``): a failure dropping the old table doesn't
        unwind an already-completed, already-committed swap.
        """
        assert isinstance(config, PostgresProfile)
        from psycopg2 import sql as _pgsql

        current_table, scratch_table = self._snapshot_table_names(sync_name)
        if not self.managed_table_exists(config, scratch_table):
            return  # extract_snapshot_diff was never called this run
        old_table = f"{current_table}_old"

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # A prior run that crashed between the rename commit and the
            # drop commit below leaves old_table behind — drop it first so
            # this run's rename doesn't fail with duplicate_table (same
            # crash-recovery idiom as extract_snapshot_diff's scratch-table
            # DROP TABLE IF EXISTS before CREATE).
            cur.execute(
                _pgsql.SQL("DROP TABLE IF EXISTS {}").format(
                    _pgsql.Identifier(config.managed_schema, old_table)
                )
            )
            if self.managed_table_exists(config, current_table):
                cur.execute(
                    _pgsql.SQL("ALTER TABLE {} RENAME TO {}").format(
                        _pgsql.Identifier(config.managed_schema, current_table),
                        _pgsql.Identifier(old_table),
                    )
                )
            cur.execute(
                _pgsql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    _pgsql.Identifier(config.managed_schema, scratch_table),
                    _pgsql.Identifier(current_table),
                )
            )
            conn.commit()
            cur.execute(
                _pgsql.SQL("DROP TABLE IF EXISTS {}").format(
                    _pgsql.Identifier(config.managed_schema, old_table)
                )
            )
            conn.commit()
        finally:
            conn.close()
