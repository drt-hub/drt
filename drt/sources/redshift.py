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

from psycopg2 import sql

from drt.config.credentials import ProfileConfig, RedshiftProfile


class RedshiftSource:
    """Extract records from an Amazon Redshift cluster.

    Redshift uses PostgreSQL wire protocol, so we connect via psycopg2.
    The main differences from vanilla Postgres:
      - Default port is 5439
      - Schema support is important for Redshift's multi-schema warehouses
      - Connection string uses same parameters
    """

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Execute query and yield records as dicts."""
        assert isinstance(config, RedshiftProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Set search_path to the configured schema
            if config.schema:
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(config.schema)))
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                yield dict(zip(columns, row))
        finally:
            conn.close()

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
            raise ImportError(
                "Redshift support requires: pip install drt-core[redshift]"
            ) from e

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
