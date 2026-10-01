"""Snowflake source implementation.

Requires: pip install drt-core[snowflake]

Example ~/.drt/profiles.yml:
    snowflake_prod:
      type: snowflake
      account: xy12345.us-east-1
      user: analyst
      password_env: SNOWFLAKE_PASSWORD
      database: ANALYTICS
      schema: PUBLIC
      warehouse: COMPUTE_WH
      role: ANALYST_ROLE   # optional
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from typing import Any, Literal

from drt.config.credentials import ProfileConfigLike, SnowflakeProfile, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry
from drt.sources.base import SnapshotDiffResult

# Snowflake error code for an expired authentication token — the session has
# to be re-established, which is exactly what a retry does. Observed during
# the #654 smoke programme on long-running extracts.
_SNOWFLAKE_TOKEN_EXPIRED = 390114


def _raise_diff_concurrency_race(sync_name: str) -> None:
    """Fail loudly when another run replaced this run's scratch snapshot."""
    raise RuntimeError(
        f"sync.incremental_strategy: diff — another run of sync "
        f"{sync_name!r} appears to be building the same snapshot table "
        f"concurrently. diff-strategy syncs must not run concurrently "
        f"for the same sync — see drt serve's request coalescing (#854) "
        f"or your scheduler's own overlap protection."
    )


def _quote_identifier(identifier: str) -> str:
    """Quote one Snowflake identifier without changing its case."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _managed_identifier(identifier: str) -> str:
    """Quote an identifier using this connector's existing unquoted semantics.

    Managed-schema configuration historically used unquoted Snowflake names,
    which fold to uppercase. Normalizing before quoting preserves that public
    behaviour while removing caller-controlled text from raw SQL.
    """
    return _quote_identifier(identifier.upper())


def _managed_table_identifier(config: SnowflakeProfile, table_name: str) -> str:
    return ".".join(
        (
            _managed_identifier(config.database),
            _managed_identifier(config.managed_schema),
            _managed_identifier(table_name),
        )
    )


def _column_ref(alias: str, column: str) -> str:
    return f"{alias}.{_quote_identifier(column)}"


def _resolve_output_column(name: str, actual_columns: list[str]) -> str | None:
    """Match a configured column to the CTAS output: exact first, else Snowflake's
    unquoted-name fold (``id`` -> ``ID``)."""
    if name in actual_columns:
        return name
    folded = name.upper()
    return folded if folded in actual_columns else None


def _key_join_condition(key_columns: list[str], left: str, right: str) -> str:
    return " AND ".join(
        f"{_column_ref(left, column)} = {_column_ref(right, column)}" for column in key_columns
    )


def _diff_hash_expr(columns: list[str], alias: str) -> str:
    """Build Snowflake's native, type-aware row hash expression.

    ``HASH`` accepts multiple typed expressions directly and never returns
    NULL, including when one or every input is NULL. It therefore preserves
    the distinction between SQL NULL and an empty string without converting
    values through session-sensitive text formatting.
    """
    return f"HASH({', '.join(_column_ref(alias, column) for column in columns)})"


class SnowflakeSource:
    """Extract records from a Snowflake data warehouse."""

    def __init__(self) -> None:
        # One source instance is used for both extraction and commit by the
        # engine. A source is freshly constructed for each CLI invocation, so
        # this state identifies the scratch snapshot built by this run without
        # introducing a durable lock that could leak when commit is skipped.
        self._snapshot_diff_tokens: dict[tuple[str, str], str] = {}

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Two transient cases:

        - ``OperationalError`` — the connector's own class for network and
          service-availability trouble (including ``RevocationCheckError``,
          a CRL/OCSP endpoint being unreachable, which is its subclass).
        - ``DatabaseError`` carrying errno **390114**, ``Authentication token
          has expired``. Seen in #654: a long extract outstays its token, and
          re-connecting is precisely the fix.

        Order matters. ``ProgrammingError`` (SQL compilation errors, a missing
        table, insufficient privileges) is *also* a ``DatabaseError`` subclass
        in this driver, so the 390114 check is gated on the exact
        ``DatabaseError`` class rather than ``isinstance`` against the base —
        otherwise every SQL typo would be retried. This attribute-not-class
        distinction is why ``with_retry`` takes a predicate: 390114 and a
        permanent error can be the very same class.
        """
        try:
            from snowflake.connector import errors as sf_errors
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        if isinstance(exc, sf_errors.OperationalError):
            return True
        # Exact class only: ProgrammingError et al. also inherit DatabaseError.
        if type(exc) is sf_errors.DatabaseError:
            return getattr(exc, "errno", None) == _SNOWFLAKE_TOKEN_EXPIRED
        return False

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** An
        expired session token or an unavailable warehouse on the way in is
        retried with exponential backoff. A failure after the first row has
        been yielded propagates — those rows are already loaded downstream and
        cannot be un-sent. See the Postgres source for the full rationale.

        **Streaming (#765).** Rows arrive by iterating the cursor in
        ``fetch_size`` batches rather than through a single ``fetchall()``, so
        peak memory tracks the batch instead of the result set. The Snowflake
        cursor is iterable and honours ``arraysize``, so this needs no explicit
        ``fetchmany`` loop — the same shape as the Postgres leg.

        ``description`` is available right after ``execute()`` here (unlike a
        psycopg2 named cursor), so columns are read up front.

        The connection is held open for the whole load, since the result set
        lives server-side until it is consumed. As on every streaming source,
        the cursor is closed before the connection, and both happen in a
        ``finally`` that also fires on ``GeneratorExit`` — so an abandoned
        iterator (``--limit`` / ``--fail-fast``, #775/#774) still tears down.

        ``query_tags`` (#768) sets the session's ``QUERY_TAG`` at connect
        time — Snowflake's native cost-attribution mechanism, surfaced in
        ``QUERY_HISTORY.QUERY_TAG`` for every query the session runs, not
        just this one. That's more than the SQL-comment fallback offers
        (queryable structured metadata vs. text a human has to grep), which
        is why this connector gets its own path.
        """
        assert isinstance(config, SnowflakeProfile)

        def _connect_and_execute() -> tuple[Any, Any, list[str]]:
            conn = self._connect(config, query_tags=query_tags)
            try:
                cur = conn.cursor()
                cur.arraysize = config.fetch_size
                cur.execute(query)
                return conn, cur, [desc[0] for desc in cur.description]
            except BaseException:
                # The failed attempt cleans up after itself — with the close
                # moved out of `finally`, nothing else would.
                conn.close()
                raise

        conn, cur, columns = with_retry(
            _connect_and_execute, RetryConfig(), retry_on=self._is_transient
        )

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        try:
            for row in cur:
                yield dict(zip(columns, row))
        finally:
            cur.close()
            conn.close()

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, SnowflakeProfile)
        conn = None
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                return True
            finally:
                cur.close()
        except Exception:
            return False
        finally:
            if conn:
                conn.close()

    def _connect(
        self, config: SnowflakeProfile, *, query_tags: dict[str, str] | None = None
    ) -> Any:
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError("Snowflake support requires: pip install drt-core[snowflake]") from e

        connect_args: dict[str, Any] = {
            "account": config.account,
            "user": config.user,
            "database": config.database,
            "schema": config.schema,
        }
        if query_tags:
            # JSON so QUERY_HISTORY.QUERY_TAG carries structured attribution
            # rather than an opaque string — same convention dbt uses.
            import json

            connect_args["session_parameters"] = {
                "QUERY_TAG": json.dumps(query_tags, sort_keys=True)
            }
        # Key-pair auth (#737) wins over password — the SERVICE-user path for
        # accounts that enforce MFA on password sign-ins.
        private_key_pem = resolve_env(None, config.private_key_env)
        if private_key_pem:
            from drt.config.credentials import load_snowflake_private_key

            connect_args["private_key"] = load_snowflake_private_key(
                private_key_pem,
                resolve_env(None, config.private_key_passphrase_env),
            )
        else:
            connect_args["password"] = resolve_env(config.password, config.password_env) or ""
        if config.warehouse:
            connect_args["warehouse"] = config.warehouse
        if config.role:
            connect_args["role"] = config.role

        return snowflake.connector.connect(**connect_args)

    # --- ManagedTableCapable (#960/#1106, ADR 0005 step 3) ------------------
    #
    # Managed names retain the connector's historical unquoted/uppercase
    # semantics, but are now rendered as quoted identifiers. This preserves
    # compatibility with admin-created unquoted schemas while ensuring no
    # caller-controlled database/schema/table text is interpolated raw.

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        """Create ``config.managed_schema`` inside ``config.database`` if it
        does not already exist.

        Same two-part discipline as the Postgres implementation
        (``drt/sources/postgres.py``):

        1. **The escape hatch**: probe first — a locked-down role with no
           ``CREATE SCHEMA`` privilege, but an admin-pre-provisioned schema,
           must never have the ``CREATE`` statement issued at all.
        2. **Concurrent first use**: on any exception from the ``CREATE``,
           re-probe rather than assuming failure — if the schema exists now,
           another session won a first-use race; anything else re-raises.
           Unlike Postgres, there is no ``conn.rollback()`` here: Snowflake
           DDL autocommits and has no savepoints (see the three existing
           comments to this effect in ``destinations/snowflake.py``), so a
           failed ``CREATE`` leaves nothing open to roll back. Whether
           Snowflake's ``CREATE SCHEMA IF NOT EXISTS`` actually races the
           same ungraceful way Postgres's does is unverified by prior art —
           this guard is defensive either way, confirmed empirically by a
           live concurrent-first-caller smoke test.
        """
        assert isinstance(config, SnowflakeProfile)
        schemata = f"{_managed_identifier(config.database)}.INFORMATION_SCHEMA.SCHEMATA"
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT 1 FROM {schemata} WHERE UPPER(schema_name) = UPPER(%s)",
                (config.managed_schema,),
            )
            if cur.fetchone() is not None:
                return
            try:
                cur.execute(
                    "CREATE SCHEMA IF NOT EXISTS "
                    f"{_managed_identifier(config.database)}."
                    f"{_managed_identifier(config.managed_schema)}"
                )
            except Exception:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT 1 FROM {schemata} WHERE UPPER(schema_name) = UPPER(%s)",
                    (config.managed_schema,),
                )
                if cur.fetchone() is None:
                    raise
        finally:
            conn.close()

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        assert isinstance(config, SnowflakeProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            return self._managed_table_exists(cur, config, table_name)
        finally:
            conn.close()

    @staticmethod
    def _managed_table_exists(cur: Any, config: SnowflakeProfile, table_name: str) -> bool:
        # table_type = 'BASE TABLE' excludes views, same distinction the
        # Postgres implementation makes — a same-named view would otherwise
        # read back as "exists" and later fail on DROP TABLE.
        tables = f"{_managed_identifier(config.database)}.INFORMATION_SCHEMA.TABLES"
        cur.execute(
            f"SELECT 1 FROM {tables} "
            "WHERE UPPER(table_schema) = UPPER(%s) AND UPPER(table_name) = UPPER(%s) "
            "AND table_type = 'BASE TABLE'",
            (config.managed_schema, table_name),
        )
        return cur.fetchone() is not None

    @staticmethod
    def _managed_table_columns(cur: Any, config: SnowflakeProfile, table_name: str) -> list[str]:
        columns_table = f"{_managed_identifier(config.database)}.INFORMATION_SCHEMA.COLUMNS"
        cur.execute(
            f"SELECT column_name FROM {columns_table} "
            "WHERE UPPER(table_schema) = UPPER(%s) "
            "AND UPPER(table_name) = UPPER(%s) ORDER BY ordinal_position",
            (config.managed_schema, table_name),
        )
        return [row[0] for row in cur.fetchall()]

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        assert isinstance(config, SnowflakeProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {_managed_table_identifier(config, table_name)}")
        finally:
            conn.close()

    # --- SnapshotDiffSource (#755/#1112, ADR 0005 step 5) -------------------

    def _snapshot_table_names(self, sync_name: str) -> tuple[str, str]:
        # Managed identifiers fold to uppercase ("users"/"Users", "straße"/
        # "strasse"), so the exact name goes in as a digest to stay injective.
        digest = hashlib.sha1(sync_name.encode()).hexdigest()[:8]
        base = f"_drt_snapshot_{sync_name}_{digest}"
        return base, f"{base}_scratch"

    @staticmethod
    def _snapshot_token_key(config: SnowflakeProfile, sync_name: str) -> tuple[str, str]:
        return config.managed_schema.upper(), sync_name

    @staticmethod
    def _managed_table_comment(
        cur: Any, config: SnowflakeProfile, table_name: str
    ) -> tuple[bool, str | None]:
        """Return table existence and its comment from INFORMATION_SCHEMA.

        ``INFORMATION_SCHEMA.TABLES`` exposes ``COMMENT`` directly and can be
        filtered with bound values, unlike ``SHOW TABLES`` whose result shape
        must be parsed. The fully-qualified view also avoids depending on the
        connection's current schema.
        """
        tables = f"{_managed_identifier(config.database)}.INFORMATION_SCHEMA.TABLES"
        cur.execute(
            f"SELECT comment FROM {tables} "
            "WHERE UPPER(table_schema) = UPPER(%s) "
            "AND UPPER(table_name) = UPPER(%s) AND table_type = 'BASE TABLE'",
            (config.managed_schema, table_name),
        )
        row = cur.fetchone()
        return (False, None) if row is None else (True, row[0])

    def _assert_snapshot_token(
        self,
        cur: Any,
        config: SnowflakeProfile,
        table_name: str,
        expected_token: str,
        sync_name: str,
    ) -> None:
        exists, actual_token = self._managed_table_comment(cur, config, table_name)
        if not exists or actual_token != expected_token:
            _raise_diff_concurrency_race(sync_name)

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
        """See ``SnapshotDiffSource.extract_snapshot_diff``."""
        assert isinstance(config, SnowflakeProfile)

        self.ensure_managed_schema(config)
        current_table, scratch_table = self._snapshot_table_names(sync_name)
        current_ident = _managed_table_identifier(config, current_table)
        scratch_ident = _managed_table_identifier(config, scratch_table)
        run_token = str(uuid.uuid4())

        conn = self._connect(config, query_tags=query_tags)
        try:
            cur = conn.cursor()
            # CTAS is atomic in Snowflake. CREATE OR REPLACE also makes a
            # scratch table left by an interrupted extract self-healing. The
            # per-run comment lets every later connection detect when another
            # run of this sync replaced the fixed-name scratch in the meantime.
            # The token is a locally generated UUID, safe to inline. Binding it
            # would make the connector %-format the whole statement and break
            # model SQL containing a literal ``%`` (e.g. ``LIKE 'a%'``).
            cur.execute(
                f"CREATE OR REPLACE TABLE {scratch_ident} COMMENT = '{run_token}' AS {query}"
            )
            self._snapshot_diff_tokens[self._snapshot_token_key(config, sync_name)] = run_token
            all_columns = self._managed_table_columns(cur, config, scratch_table)

            resolved_keys = [_resolve_output_column(c, all_columns) for c in key_columns]
            missing_keys = [c for c, r in zip(key_columns, resolved_keys) if r is None]
            if missing_keys:
                raise ValueError(
                    "sync.incremental_strategy: diff — destination "
                    f"upsert_key column(s) {missing_keys} not found in the "
                    f"model's output columns {all_columns}."
                )
            sql_keys = [r for r in resolved_keys if r is not None]
            # Records must carry the key names the YAML configured (destinations
            # and masking look them up exactly), not Snowflake's folded casing.
            key_rename = {r: c for r, c in zip(sql_keys, key_columns) if r != c}
            if hash_columns == "all":
                diff_columns = [column for column in all_columns if column not in sql_keys]
            else:
                resolved_hash = [_resolve_output_column(c, all_columns) for c in hash_columns]
                missing_hash = [c for c, r in zip(hash_columns, resolved_hash) if r is None]
                if missing_hash:
                    raise ValueError(
                        f"sync.diff.hash_columns: column(s) {missing_hash} "
                        "not found in the model's output columns "
                        f"{all_columns} — check for a typo."
                    )
                diff_columns = [r for r in resolved_hash if r is not None]

            is_first_run = not self._managed_table_exists(cur, config, current_table)
            reclassify_all_existing = False
            if not is_first_run:
                baseline_columns = self._managed_table_columns(cur, config, current_table)
                if any(column not in baseline_columns for column in sql_keys):
                    # The key set changed since the baseline was written, so the
                    # old snapshot cannot be joined; start over from this one.
                    is_first_run = True
                else:
                    # A column added to the model is absent from the baseline,
                    # so every existing row must be re-sent to populate it.
                    reclassify_all_existing = any(
                        column not in baseline_columns for column in diff_columns
                    )
                    diff_columns = [c for c in diff_columns if c in baseline_columns]
        finally:
            conn.close()

        if is_first_run:
            added = self._stream_query(
                config,
                f"SELECT * FROM {scratch_ident}",
                rename=key_rename,
                scratch_table=scratch_table,
                expected_token=run_token,
                sync_name=sync_name,
                query_tags=query_tags,
            )
            changed: Iterator[dict[str, Any]] = iter(())
            removed_keys: Iterator[dict[str, Any]] = iter(())
        else:
            join_condition = _key_join_condition(sql_keys, "s", "c")
            added = self._stream_query(
                config,
                f"SELECT s.* FROM {scratch_ident} AS s "
                f"LEFT JOIN {current_ident} AS c ON {join_condition} "
                f"WHERE {_column_ref('c', sql_keys[0])} IS NULL",
                rename=key_rename,
                scratch_table=scratch_table,
                expected_token=run_token,
                sync_name=sync_name,
                query_tags=query_tags,
            )
            removed_keys = self._stream_query(
                config,
                "SELECT "
                + ", ".join(
                    _column_ref("c", resolved)
                    + ("" if resolved == configured else f" AS {_quote_identifier(configured)}")
                    for resolved, configured in zip(sql_keys, key_columns)
                )
                + f" FROM {current_ident} AS c LEFT JOIN {scratch_ident} AS s ON "
                + _key_join_condition(sql_keys, "c", "s")
                + f" WHERE {_column_ref('s', sql_keys[0])} IS NULL",
                scratch_table=scratch_table,
                expected_token=run_token,
                sync_name=sync_name,
                query_tags=query_tags,
            )
            if diff_columns or reclassify_all_existing:
                changed_filter = (
                    ""
                    if reclassify_all_existing
                    else f" WHERE {_diff_hash_expr(diff_columns, 's')} "
                    f"<> {_diff_hash_expr(diff_columns, 'c')}"
                )
                changed = self._stream_query(
                    config,
                    f"SELECT s.* FROM {scratch_ident} AS s "
                    f"JOIN {current_ident} AS c ON {join_condition}" + changed_filter,
                    rename=key_rename,
                    scratch_table=scratch_table,
                    expected_token=run_token,
                    sync_name=sync_name,
                    query_tags=query_tags,
                )
            else:
                changed = iter(())

        return SnapshotDiffResult(
            added=added,
            changed=changed,
            removed_keys=removed_keys,
            is_first_run=is_first_run,
        )

    def _stream_query(
        self,
        config: SnowflakeProfile,
        query: str,
        *,
        scratch_table: str,
        expected_token: str,
        sync_name: str,
        query_tags: dict[str, str] | None,
        rename: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream one snapshot classification query on its own connection.

        The token is checked immediately before the query starts and after it
        drains. There is necessarily a small TOCTOU window between a check and
        the following statement because Snowflake DDL autocommits and these
        iterators use independent connections. A later stream or commit checks
        again, so replacement is detected on a best-effort basis without a
        cross-lifecycle lock that could leak when the engine skips commit.
        """

        def _connect_and_execute() -> tuple[Any, Any, list[str]]:
            conn = self._connect(config, query_tags=query_tags)
            try:
                cur = conn.cursor()
                cur.arraysize = config.fetch_size
                self._assert_snapshot_token(cur, config, scratch_table, expected_token, sync_name)
                cur.execute(query)
                return conn, cur, [desc[0] for desc in cur.description]
            except BaseException:
                conn.close()
                raise

        conn, cur, columns = with_retry(
            _connect_and_execute, RetryConfig(), retry_on=self._is_transient
        )
        if rename:
            columns = [rename.get(c, c) for c in columns]
        try:
            for row in cur:
                yield dict(zip(columns, row))
            self._assert_snapshot_token(cur, config, scratch_table, expected_token, sync_name)
        finally:
            cur.close()
            conn.close()

    def commit_snapshot_diff(self, config: ProfileConfigLike, sync_name: str) -> None:
        """Atomically promote scratch to the next Snowflake baseline.

        Existing baselines use ``SWAP WITH``: the exchange is one atomic DDL
        statement, so readers can only observe the complete old or complete
        new snapshot. If a crash leaves the old baseline in scratch before
        its cleanup DROP, the next extract's CREATE OR REPLACE heals it.
        """
        assert isinstance(config, SnowflakeProfile)
        token_key = self._snapshot_token_key(config, sync_name)
        expected_token = self._snapshot_diff_tokens.get(token_key)
        if expected_token is None:
            # extract_snapshot_diff was never called on this run's source
            # instance. Never adopt a fixed-name scratch left by another run.
            return

        current_table, scratch_table = self._snapshot_table_names(sync_name)
        current_ident = _managed_table_identifier(config, current_table)
        scratch_ident = _managed_table_identifier(config, scratch_table)

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            if not self._managed_table_exists(cur, config, scratch_table):
                # A remembered token means this run built a scratch table, so
                # its absence is only benign if our snapshot is already the
                # baseline. Otherwise an overlapping run consumed it.
                promoted, actual_token = self._managed_table_comment(cur, config, current_table)
                if promoted and actual_token == expected_token:
                    self._snapshot_diff_tokens.pop(token_key, None)
                    return
                _raise_diff_concurrency_race(sync_name)
            current_exists = self._managed_table_exists(cur, config, current_table)
            self._assert_snapshot_token(cur, config, scratch_table, expected_token, sync_name)
            # Snowflake offers no conditional SWAP/RENAME based on COMMENT,
            # and DDL autocommits, so a replacement can still land in the tiny
            # TOCTOU window after this check. Verify the promoted table below
            # as well so that race fails loudly rather than returning success.
            # This is deliberately best-effort detection: holding a lock across
            # source extraction, destination writes, and this commit could leak
            # whenever commit is skipped.
            if current_exists:
                promote_sql = f"ALTER TABLE {scratch_ident} SWAP WITH {current_ident}"
                undo_sql = promote_sql  # SWAP is its own inverse
            else:
                promote_sql = f"ALTER TABLE {scratch_ident} RENAME TO {current_ident}"
                undo_sql = f"ALTER TABLE {current_ident} RENAME TO {scratch_ident}"
            cur.execute(promote_sql)
            try:
                self._assert_snapshot_token(cur, config, current_table, expected_token, sync_name)
            except RuntimeError:
                # A replacement landed between the check and the promotion, so
                # the baseline just advanced to another run's undelivered
                # snapshot. Put it back before surfacing the race.
                try:
                    cur.execute(undo_sql)
                except Exception:
                    pass
                raise
            if current_exists:
                cur.execute(f"DROP TABLE IF EXISTS {scratch_ident}")
            self._snapshot_diff_tokens.pop(token_key, None)
        finally:
            conn.close()
