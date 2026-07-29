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

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import DatabricksProfile, ProfileConfig, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry


class DatabricksSource:
    """Extract records from a Databricks SQL Warehouse."""

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

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A SQL
        warehouse resuming from a cold start no longer fails the sync. A
        failure after the first row has been yielded propagates — those rows
        are already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.
        """
        assert isinstance(config, DatabricksProfile)

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

    def _connect(self, config: DatabricksProfile) -> Any:
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

        return sql.connect(**connect_args)
