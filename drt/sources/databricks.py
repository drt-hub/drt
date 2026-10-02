"""Databricks SQL Warehouse source.

Requires: pip install drt-core[databricks]

Example ~/.drt/profiles.yml:
    databricks_prod:
      type: databricks
      server_hostname: dbc-abc123.cloud.databricks.com
      http_path: /sql/1.0/warehouses/abc123xyz
      access_token_env: DATABRICKS_TOKEN
      catalog: main           # optional (Unity Catalog)
      schema: analytics
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Iterator
from typing import Any, Literal

from drt.config.credentials import DatabricksProfile, ProfileConfigLike, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry
from drt.sources.base import SnapshotDiffResult

_SNAPSHOT_TOKEN_PROPERTY = "drt.snapshot_run_token"


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
    """Quote one Databricks identifier without changing its case."""
    return f"`{identifier.replace('`', '``')}`"


def _managed_table_identifier(config: DatabricksProfile, table_name: str) -> str:
    assert config.catalog is not None
    return ".".join(
        _quote_identifier(part) for part in (config.catalog, config.managed_schema, table_name)
    )


def _column_ref(alias: str, column: str) -> str:
    return f"{alias}.{_quote_identifier(column)}"


def _resolve_output_column(name: str, actual_columns: list[str]) -> str | None:
    """Resolve a configured name using Delta's case-insensitive identifiers."""
    if name in actual_columns:
        return name
    folded = name.casefold()
    return next((column for column in actual_columns if column.casefold() == folded), None)


def _key_join_condition(key_columns: list[str], left: str, right: str) -> str:
    return " AND ".join(
        f"{_column_ref(left, column)} = {_column_ref(right, column)}" for column in key_columns
    )


def _diff_hash_expr(columns: list[str], alias: str) -> str:
    """Build a typed Databricks row hash that distinguishes NULL from values.

    Spark's xxhash64 accepts typed expressions directly. Pairing every value
    with an explicit nullness boolean prevents its normal NULL-skipping
    behaviour from making SQL NULL indistinguishable from an empty string.
    """
    arguments = [
        expression
        for column in columns
        for expression in (f"isnull({_column_ref(alias, column)})", _column_ref(alias, column))
    ]
    return f"xxhash64({', '.join(arguments)})"


class DatabricksSource:
    """Extract records from a Databricks SQL Warehouse."""

    def __init__(self) -> None:
        # One source instance performs both extraction and commit. The token
        # identifies the fixed-name scratch table built by this invocation.
        self._snapshot_diff_tokens: dict[tuple[str, str, str], str] = {}

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Transient, from ``databricks.sql.exc``:

        - ``OperationalError`` — the connector's class for connection and
          service-availability trouble.
        - ``RequestError`` — a failed request to the SQL endpoint. This is the
          **warehouse cold start**: a stopped SQL warehouse takes minutes to
          resume, and the first requests fail while it does. Observed in the
          #654 smoke programme, and the single most valuable retry here — the
          warehouse is coming up and the very same query will succeed shortly.

        Permanent: ``ProgrammingError`` (bad SQL, missing table),
        ``DatabaseError``, and ``NotSupportedError``.

        The driver derives its exception classes from PEP 249, so
        ``OperationalError`` and ``ProgrammingError`` are siblings under
        ``DatabaseError``; matching the specific classes rather than the base
        keeps a SQL typo from being retried. Note ``RequestError`` sits
        outside that tree (it subclasses the driver's own ``Error``).

        Imported inside the method — ``databricks-sql-connector`` is an
        optional extra, and this class is imported unconditionally by the
        connector registry.
        """
        try:
            from databricks.sql import exc as dbsql_exc  # type: ignore[import-untyped]
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        transient: tuple[type[BaseException], ...] = tuple(
            cls
            for cls in (
                getattr(dbsql_exc, "OperationalError", None),
                getattr(dbsql_exc, "RequestError", None),
            )
            if isinstance(cls, type)
        )
        if not transient:  # pragma: no cover - driver without the documented classes
            return False
        if not isinstance(exc, transient):
            return False
        # Exclude authentication failures. The driver's own retry policy
        # already treats these as hopeless — auth/retry.py answers 401 with
        # "Confirm your authentication credentials" and 403 with "403 codes
        # are not retried" — but it then surfaces them as ``RequestError``,
        # which subclasses ``OperationalError``, so the isinstance above lets
        # them straight back in and drt retried what the driver had just given
        # up on. Three rapid attempts with a bad token is exactly the shape
        # that trips a workspace lockout or an SSO alert.
        #
        # Keyed on the HTTP status in ``context`` rather than the message,
        # which is free text. Anything without a status — the warehouse cold
        # start this retry exists for (#654) — is unaffected.
        http_code = (getattr(exc, "context", None) or {}).get("http-code")
        return http_code not in (401, 403)

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A SQL
        warehouse resuming from a cold start no longer fails the sync. A
        failure after the first row has been yielded propagates — those rows
        are already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.

        **Streaming (#765).** Rows arrive by iterating the cursor rather than
        through ``fetchall()``. The cursor's ``__iter__`` delegates to the
        result set, whose own ``__iter__`` is a ``fetchone()`` loop, so this
        streams rather than materialising.

        No ``fetch_size`` knob here, unlike Postgres/Redshift/Snowflake: this
        cursor exposes no ``arraysize``, and ``fetchmany(size)`` takes a
        required argument rather than reading a configured one, so there is
        nothing for a profile field to set.

        The connection is held for the whole load. The cursor is closed before
        the connection, both in a ``finally`` that also fires on
        ``GeneratorExit`` — so an abandoned iterator (``--limit`` /
        ``--fail-fast``, #775/#774) still tears down.

        ``query_tags`` (#768) passes straight through to the driver's own
        ``query_tags`` connect kwarg, which the driver serializes into a
        ``QUERY_TAGS`` session config — Databricks' native attribution
        mechanism, applied to every query the session runs. More than the
        SQL-comment fallback offers, which is why this connector gets its
        own path.
        """
        assert isinstance(config, DatabricksProfile)

        def _connect_and_execute() -> tuple[Any, Any, list[str]]:
            conn = self._connect(config, query_tags=query_tags)
            try:
                cur = conn.cursor()
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
        # The finally also fires on GeneratorExit, so an abandoned iterator
        # still tears down in the right order.
        try:
            for row in cur:
                yield dict(zip(columns, row))
        finally:
            cur.close()
            conn.close()

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, DatabricksProfile)
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
        self, config: DatabricksProfile, *, query_tags: dict[str, str] | None = None
    ) -> Any:
        token = resolve_env(config.access_token, config.access_token_env) or ""
        if not token:
            raise ValueError(
                "Databricks profile: provide 'access_token' or set "
                "the env var named in 'access_token_env'."
            )

        try:
            from databricks import sql
        except ImportError as e:
            raise ImportError(
                "Databricks support requires: pip install drt-core[databricks]"
            ) from e

        connect_args: dict[str, Any] = {
            "server_hostname": config.server_hostname,
            "http_path": config.http_path,
            "access_token": token,
        }
        if config.catalog:
            connect_args["catalog"] = config.catalog
        if config.schema:
            connect_args["schema"] = config.schema
        if query_tags:
            connect_args["query_tags"] = query_tags

        return sql.connect(**connect_args)

    # --- ManagedTableCapable (#960/#1108, ADR 0005 step 3) ------------------
    #
    # Identifiers here are deliberately unquoted, matching this connector's
    # existing tracked-mirror bookkeeping table convention
    # (destinations/databricks.py's `_target_exists`/`_create_state_table`,
    # which builds fully-qualified names via plain f-string interpolation and
    # probes existence with `SHOW TABLES ... LIKE`, not `information_schema`).
    # Unlike the Snowflake leg (#1106), there is no UPPER()-normalization
    # here: Unity Catalog is case-preserving, not case-folding, for unquoted
    # identifiers, so a plain probe already agrees with what an unquoted
    # CREATE produced, regardless of the case actually typed in config.
    #
    # `catalog` is required for every method here even though the profile
    # field itself is optional (plain extraction queries can omit it) —
    # Unity Catalog has no reliable implicit "current catalog" to create a
    # schema or table against, so a missing value must raise loudly here
    # rather than resolve to some ambient default.

    def _require_catalog(self, config: DatabricksProfile) -> str:
        if not config.catalog:
            raise ValueError(
                "ManagedTableCapable requires DatabricksProfile.catalog to be set "
                "(the Unity Catalog namespace for drt's managed schema) — add "
                "'catalog: <name>' to this profile in profiles.yml."
            )
        return config.catalog

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        """Create ``config.managed_schema`` inside ``config.catalog`` if it
        does not already exist.

        Same two-part discipline as the Postgres/Snowflake implementations:

        1. **The escape hatch**: probe first — a locked-down principal with
           no ``CREATE SCHEMA`` privilege, but an admin-pre-provisioned
           schema, must never have the ``CREATE`` statement issued at all.
        2. **Concurrent first use**: on any exception from the ``CREATE``,
           re-probe rather than assuming failure — if the schema exists now,
           another session won a first-use race; anything else re-raises.
           No ``conn.rollback()``: Delta Lake has no multi-statement
           transactions at all (stronger than Snowflake's autocommit-by-
           default — there is no session state here to roll back), so a
           failed ``CREATE`` leaves nothing open to undo.
        """
        assert isinstance(config, DatabricksProfile)
        catalog = self._require_catalog(config)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(f"SHOW SCHEMAS IN {catalog} LIKE '{config.managed_schema}'")
            if cur.fetchall():
                return
            try:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config.managed_schema}")
            except Exception:
                cur = conn.cursor()
                cur.execute(f"SHOW SCHEMAS IN {catalog} LIKE '{config.managed_schema}'")
                if not cur.fetchall():
                    raise
        finally:
            conn.close()

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        assert isinstance(config, DatabricksProfile)
        catalog = self._require_catalog(config)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Schema existence is probed first: SHOW TABLES IN a schema that
            # does not exist yet raises (SCHEMA_NOT_FOUND) rather than
            # returning no rows, unlike a Postgres/Snowflake information_schema
            # query, which is always queryable regardless of whether the
            # referenced schema exists. Without this guard, every read-path
            # caller (managed table not yet created — the normal first-use
            # state) would raise instead of getting a clean "doesn't exist"
            # (caught in review — #1108's warehouse-state backend calls this
            # on every read before ever calling ensure_managed_schema()).
            cur.execute(f"SHOW SCHEMAS IN {catalog} LIKE '{config.managed_schema}'")
            if not cur.fetchall():
                return False
            # SHOW TABLES ... LIKE, matching destinations/databricks.py's
            # _target_exists exactly — not information_schema. Unlike the
            # Postgres/Snowflake legs, this does not exclude views (no
            # table_type predicate is available on this probe shape); the
            # existing tracked-mirror table uses the identical probe without
            # one, so this stays consistent rather than introducing a new
            # existence-check shape for #960 alone.
            cur.execute(f"SHOW TABLES IN {catalog}.{config.managed_schema} LIKE '{table_name}'")
            return bool(cur.fetchall())
        finally:
            conn.close()

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        assert isinstance(config, DatabricksProfile)
        catalog = self._require_catalog(config)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {catalog}.{config.managed_schema}.{table_name}")
        finally:
            conn.close()

    # --- SnapshotDiffSource (#755/#1114, ADR 0005 step 5) -------------------

    def _snapshot_table_names(self, sync_name: str) -> tuple[str, str]:
        # Unity Catalog identifiers are case-insensitive, while sync names are
        # not. The digest keeps names that differ only by case distinct.
        digest = hashlib.sha1(sync_name.encode()).hexdigest()[:8]
        # Unity Catalog rejects '.' and '/' in table names and caps their length
        # even when quoted; the digest of the raw name keeps sanitised/truncated
        # names distinct.
        readable = re.sub(r"[^A-Za-z0-9_-]", "_", sync_name)[:100]
        base = f"_drt_snapshot_{readable}_{digest}"
        return base, f"{base}_scratch"

    def _snapshot_token_key(
        self, config: DatabricksProfile, sync_name: str
    ) -> tuple[str, str, str]:
        return self._require_catalog(config).casefold(), config.managed_schema.casefold(), sync_name

    @staticmethod
    def _managed_table_exists(cur: Any, config: DatabricksProfile, table_name: str) -> bool:
        assert config.catalog is not None
        cur.execute(
            f"SELECT 1 FROM {_quote_identifier(config.catalog)}.information_schema.tables "
            "WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?) LIMIT 1",
            [config.managed_schema, table_name],
        )
        return cur.fetchone() is not None

    @staticmethod
    def _managed_table_columns(cur: Any, config: DatabricksProfile, table_name: str) -> list[str]:
        assert config.catalog is not None
        columns_table = f"{_quote_identifier(config.catalog)}.information_schema.columns"
        cur.execute(
            f"SELECT column_name FROM {columns_table} "
            "WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?) "
            "ORDER BY ordinal_position",
            [config.managed_schema, table_name],
        )
        return [str(row[0]) for row in cur.fetchall()]

    def _managed_table_token(
        self, cur: Any, config: DatabricksProfile, table_name: str
    ) -> tuple[bool, str | None]:
        if not self._managed_table_exists(cur, config, table_name):
            return False, None
        cur.execute(
            f"SHOW TBLPROPERTIES {_managed_table_identifier(config, table_name)} "
            f"('{_SNAPSHOT_TOKEN_PROPERTY}')"
        )
        row = cur.fetchone()
        return True, None if row is None else str(row[-1])

    def _assert_snapshot_token(
        self,
        cur: Any,
        config: DatabricksProfile,
        table_name: str,
        expected_token: str,
        sync_name: str,
    ) -> None:
        exists, actual_token = self._managed_table_token(cur, config, table_name)
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
        assert isinstance(config, DatabricksProfile)
        self._require_catalog(config)

        current_table, scratch_table = self._snapshot_table_names(sync_name)
        current_ident = _managed_table_identifier(config, current_table)
        scratch_ident = _managed_table_identifier(config, scratch_table)
        run_token = str(uuid.uuid4())

        def _setup() -> tuple[bool, bool, list[str], dict[str, str], list[str]]:
            self.ensure_managed_schema(config)
            conn = self._connect(config, query_tags=query_tags)
            try:
                cur = conn.cursor()
                # One Delta CTAS commit replaces an abandoned scratch atomically.
                # The locally-generated UUID is safe to inline; keeping model SQL
                # out of a parameterized statement also preserves native `?`
                # markers the model itself may contain.
                cur.execute(
                    f"CREATE OR REPLACE TABLE {scratch_ident} USING DELTA "
                    f"TBLPROPERTIES ('{_SNAPSHOT_TOKEN_PROPERTY}' = '{run_token}') AS {query}"
                )
                self._snapshot_diff_tokens[self._snapshot_token_key(config, sync_name)] = run_token
                all_columns = self._managed_table_columns(cur, config, scratch_table)

                resolved_keys = [_resolve_output_column(c, all_columns) for c in key_columns]
                missing_keys = [
                    c for c, resolved in zip(key_columns, resolved_keys) if resolved is None
                ]
                if missing_keys:
                    raise ValueError(
                        "sync.incremental_strategy: diff — destination "
                        f"upsert_key column(s) {missing_keys} not found in the "
                        f"model's output columns {all_columns}."
                    )
                sql_keys = [resolved for resolved in resolved_keys if resolved is not None]
                key_rename = {
                    resolved: configured
                    for resolved, configured in zip(sql_keys, key_columns)
                    if resolved != configured
                }

                if hash_columns == "all":
                    key_folds = {column.casefold() for column in sql_keys}
                    diff_columns = [
                        column for column in all_columns if column.casefold() not in key_folds
                    ]
                else:
                    resolved_hash = [
                        _resolve_output_column(column, all_columns) for column in hash_columns
                    ]
                    missing_hash = [
                        column
                        for column, resolved in zip(hash_columns, resolved_hash)
                        if resolved is None
                    ]
                    if missing_hash:
                        raise ValueError(
                            f"sync.diff.hash_columns: column(s) {missing_hash} "
                            "not found in the model's output columns "
                            f"{all_columns} — check for a typo."
                        )
                    diff_columns = [resolved for resolved in resolved_hash if resolved is not None]

                is_first_run = not self._managed_table_exists(cur, config, current_table)
                reclassify_all_existing = False
                if not is_first_run:
                    baseline_columns = self._managed_table_columns(cur, config, current_table)
                    if any(
                        _resolve_output_column(column, baseline_columns) is None
                        for column in sql_keys
                    ):
                        is_first_run = True
                    else:
                        reclassify_all_existing = any(
                            _resolve_output_column(column, baseline_columns) is None
                            for column in diff_columns
                        )
                        diff_columns = [
                            column
                            for column in diff_columns
                            if _resolve_output_column(column, baseline_columns) is not None
                        ]
            finally:
                conn.close()
            return is_first_run, reclassify_all_existing, sql_keys, key_rename, diff_columns

        is_first_run, reclassify_all_existing, sql_keys, key_rename, diff_columns = with_retry(
            _setup, RetryConfig(), retry_on=self._is_transient
        )

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
        config: DatabricksProfile,
        query: str,
        *,
        scratch_table: str,
        expected_token: str,
        sync_name: str,
        query_tags: dict[str, str] | None,
        rename: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream one classification query with best-effort overlap checks."""

        def _connect_and_execute() -> tuple[Any, Any, list[str]]:
            conn = self._connect(config, query_tags=query_tags)
            try:
                cur = conn.cursor()
                self._assert_snapshot_token(cur, config, scratch_table, expected_token, sync_name)
                cur.execute(query)
                return conn, cur, [str(desc[0]) for desc in cur.description]
            except BaseException:
                conn.close()
                raise

        conn, cur, columns = with_retry(
            _connect_and_execute, RetryConfig(), retry_on=self._is_transient
        )
        if rename:
            columns = [rename.get(column, column) for column in columns]
        try:
            for row in cur:
                yield dict(zip(columns, row))
            self._assert_snapshot_token(cur, config, scratch_table, expected_token, sync_name)
        finally:
            cur.close()
            conn.close()

    def commit_snapshot_diff(self, config: ProfileConfigLike, sync_name: str) -> None:
        """Promote scratch through one atomic Delta table replacement.

        Databricks has no transaction spanning the token check and CTAS. A
        replacement in that gap is detected by checking scratch again after
        CTAS; the prior Delta version is then restored (or a first-run table
        dropped). A still narrower same-sync overlap can race that recovery,
        so concurrent runs remain unsupported rather than lock-safe.
        """
        assert isinstance(config, DatabricksProfile)
        token_key = self._snapshot_token_key(config, sync_name)
        expected_token = self._snapshot_diff_tokens.get(token_key)
        if expected_token is None:
            return

        current_table, scratch_table = self._snapshot_table_names(sync_name)
        current_ident = _managed_table_identifier(config, current_table)
        scratch_ident = _managed_table_identifier(config, scratch_table)

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            scratch_exists, scratch_token = self._managed_table_token(cur, config, scratch_table)
            if not scratch_exists:
                promoted, current_token = self._managed_table_token(cur, config, current_table)
                if promoted and current_token == expected_token:
                    self._snapshot_diff_tokens.pop(token_key, None)
                    return
                _raise_diff_concurrency_race(sync_name)
            if scratch_token != expected_token:
                _raise_diff_concurrency_race(sync_name)

            current_exists = self._managed_table_exists(cur, config, current_table)
            previous_version: int | None = None
            if current_exists:
                cur.execute(f"DESCRIBE HISTORY {current_ident} LIMIT 1")
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        "sync.incremental_strategy: diff — could not read the "
                        f"current Delta version for {current_table!r}; baseline "
                        "promotion was not attempted."
                    )
                previous_version = int(row[0])

            # CREATE OR REPLACE is a single atomic Delta commit and preserves
            # table history and grants. It is the closest Databricks analogue
            # to Snowflake's SWAP; DROP + RENAME would expose an absent table.
            cur.execute(
                f"CREATE OR REPLACE TABLE {current_ident} USING DELTA "
                f"TBLPROPERTIES ('{_SNAPSHOT_TOKEN_PROPERTY}' = '{expected_token}') "
                f"AS SELECT * FROM {scratch_ident}"
            )
            try:
                self._assert_snapshot_token(cur, config, scratch_table, expected_token, sync_name)
                self._assert_snapshot_token(cur, config, current_table, expected_token, sync_name)
            except RuntimeError:
                try:
                    if previous_version is None:
                        cur.execute(f"DROP TABLE IF EXISTS {current_ident}")
                    else:
                        cur.execute(
                            f"RESTORE TABLE {current_ident} TO VERSION AS OF {previous_version}"
                        )
                except Exception:
                    pass
                raise
            # Leave scratch in place. The next CREATE OR REPLACE heals it,
            # while a post-check DROP would introduce a new window in which
            # this run could delete a concurrent run's freshly-built scratch.
            self._snapshot_diff_tokens.pop(token_key, None)
        finally:
            conn.close()
