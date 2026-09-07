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

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfigLike, SnowflakeProfile, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry

# Snowflake error code for an expired authentication token — the session has
# to be re-established, which is exactly what a retry does. Observed during
# the #654 smoke programme on long-running extracts.
_SNOWFLAKE_TOKEN_EXPIRED = 390114


class SnowflakeSource:
    """Extract records from a Snowflake data warehouse."""

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
    # Identifiers here are deliberately unquoted, unlike the Postgres
    # implementation's psycopg2.sql.Identifier() (which preserves exact
    # case). This connector has no quoting helper of its own — the existing
    # tracked-mirror bookkeeping table (destinations/snowflake.py's
    # _create_state_table/_mirror_table_ident) already builds fully-
    # qualified names via plain f-string interpolation and compares them
    # back with .upper() (_state_scope_columns_exist) — so unquoted DDL +
    # UPPER()-normalized probes matches that existing convention instead of
    # introducing a second, quoted-identifier one. Snowflake folds an
    # unquoted CREATE to uppercase; probing with UPPER() on both sides keeps
    # the create path, the probe path, and an admin's escape-hatch
    # CREATE SCHEMA (following the provisioning docs, also unquoted) all in
    # agreement regardless of the case actually typed in config.

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
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT 1 FROM {config.database}.information_schema.schemata "
                "WHERE UPPER(schema_name) = UPPER(%s)",
                (config.managed_schema,),
            )
            if cur.fetchone() is not None:
                return
            try:
                cur.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {config.database}.{config.managed_schema}"
                )
            except Exception:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT 1 FROM {config.database}.information_schema.schemata "
                    "WHERE UPPER(schema_name) = UPPER(%s)",
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
            # table_type = 'BASE TABLE' excludes views, same distinction the
            # Postgres implementation makes — a same-named view would
            # otherwise read back as "exists" and later fail on DROP TABLE.
            cur.execute(
                f"SELECT 1 FROM {config.database}.information_schema.tables "
                "WHERE UPPER(table_schema) = UPPER(%s) AND UPPER(table_name) = UPPER(%s) "
                "AND table_type = 'BASE TABLE'",
                (config.managed_schema, table_name),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        assert isinstance(config, SnowflakeProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                f"DROP TABLE IF EXISTS {config.database}.{config.managed_schema}.{table_name}"
            )
        finally:
            conn.close()
