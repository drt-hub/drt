"""Amazon Redshift source implementation.

Requires: pip install drt-core[redshift]

Redshift is PostgreSQL-compatible (based on PartiQL/Postgres 8.x),
so this connector reuses psycopg2 with Redshift-specific defaults.

Example ~/.drt/profiles.yml:
    redshift_prod:
      type: redshift
      host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
      port: 5439
      dbname: analytics
      user: analyst
      password_env: REDSHIFT_PASSWORD
      schema: public
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfig, RedshiftProfile
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry


class RedshiftSource:
    """Extract records from an Amazon Redshift cluster.

    Redshift uses PostgreSQL wire protocol, so we connect via psycopg2.
    The main differences from vanilla Postgres:
      - Default port is 5439
      - Schema support is important for Redshift's multi-schema warehouses
      - Connection string uses same parameters
    """

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        Same psycopg2 classification as the Postgres source — Redshift speaks
        the Postgres wire protocol through the same driver. Transient:
        ``OperationalError`` (connection refused, cluster failover, a paused
        serverless workgroup resuming, the WLM dropping an idle session) and
        ``InterfaceError``. Permanent: ``ProgrammingError`` / ``DataError`` /
        ``IntegrityError``.

        Matched by exact class, not the shared ``DatabaseError`` base — under
        PEP 249 the permanent classes are siblings of ``OperationalError``,
        so a base-class check would retry bad SQL.
        """
        try:
            import psycopg2
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Execute query and yield records as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.
        """
        assert isinstance(config, RedshiftProfile)

        def _connect_and_fetch() -> tuple[list[str], list[Any]]:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                # Set search_path to the configured schema
                if config.schema:
                    cur.execute("SET search_path TO %s", (config.schema,))
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                return columns, cur.fetchall()
            finally:
                conn.close()

        columns, rows = with_retry(_connect_and_fetch, RetryConfig(), retry_on=self._is_transient)

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        for row in rows:
            yield dict(zip(columns, row))

    def test_connection(self, config: ProfileConfig) -> bool:
        """Test if the Redshift cluster is reachable."""
        assert isinstance(config, RedshiftProfile)
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def _connect(self, config: RedshiftProfile) -> Any:
        """Create a connection to Redshift using psycopg2."""
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError("Redshift support requires: pip install drt-core[redshift]") from e

        password = config.password or (
            os.environ.get(config.password_env) if config.password_env else None
        )
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=password,
        )
