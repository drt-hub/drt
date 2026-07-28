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

from drt.config.credentials import ProfileConfig, SnowflakeProfile, resolve_env
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

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** An
        expired session token or an unavailable warehouse on the way in is
        retried with exponential backoff. A failure after the first row has
        been yielded propagates — those rows are already loaded downstream and
        cannot be un-sent. See the Postgres source for the full rationale.
        """
        assert isinstance(config, SnowflakeProfile)

        def _connect_and_fetch() -> tuple[list[str], list[Any]]:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                try:
                    cur.execute(query)
                    columns = [desc[0] for desc in cur.description]
                    return columns, cur.fetchall()
                finally:
                    cur.close()
            finally:
                conn.close()

        columns, rows = with_retry(_connect_and_fetch, RetryConfig(), retry_on=self._is_transient)

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        for row in rows:
            yield dict(zip(columns, row))

    def test_connection(self, config: ProfileConfig) -> bool:
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

    def _connect(self, config: SnowflakeProfile) -> Any:
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
