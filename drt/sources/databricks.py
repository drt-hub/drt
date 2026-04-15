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


class DatabricksSource:
    """Extract records from a Databricks SQL Warehouse."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, DatabricksProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            try:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                for row in cur.fetchall():
                    yield dict(zip(columns, row))
            finally:
                cur.close()
        finally:
            conn.close()

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
