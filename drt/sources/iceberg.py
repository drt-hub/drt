"""Apache Iceberg source — read Iceberg tables via pyiceberg.

Loads a table through a pyiceberg catalog (REST / SQL / Hive / local) into
Arrow, registers it in an in-memory DuckDB, then runs your model SQL against it.

Requires: pip install drt-core[iceberg]

Example ~/.drt/profiles.yml:
    iceberg_prod:
      type: iceberg
      table: analytics.users                   # namespace.table
      catalog_uri: https://my-catalog/api      # REST catalog (or use properties for sql/local)
      warehouse: s3://my-bucket/warehouse
      catalog_name: prod                        # optional; default "default"
      properties:                              # extra catalog props; *_ENV reads the env var
        s3.access-key-id_ENV: AWS_KEY

Then in a model: ``SELECT id, email FROM users WHERE updated_at > '{{ cursor }}'``
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import IcebergProfile, ProfileConfig, resolve_env_dict


def _catalog_properties(config: IcebergProfile) -> dict[str, str]:
    """Catalog properties for ``load_catalog`` — resolved ``properties`` plus the
    convenience ``catalog_uri`` → ``uri`` and ``warehouse`` mappings."""
    props = resolve_env_dict(config.properties)
    if config.catalog_uri:
        props["uri"] = config.catalog_uri
    if config.warehouse:
        props["warehouse"] = config.warehouse
    return props


class IcebergSource:
    """Extract records from an Apache Iceberg table."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, IcebergProfile)
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as e:
            raise ImportError("Iceberg support requires: pip install drt-core[iceberg]") from e
        import duckdb  # bundled with drt-core

        catalog = load_catalog(config.catalog_name, **_catalog_properties(config))
        arrow = catalog.load_table(config.table).scan().to_arrow()

        conn = duckdb.connect()
        try:
            conn.register(config.table.split(".")[-1], arrow)
            result = conn.execute(query)
            columns = [desc[0] for desc in result.description]
            for row in result.fetchall():
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, IcebergProfile)
        try:
            from pyiceberg.catalog import load_catalog

            catalog = load_catalog(config.catalog_name, **_catalog_properties(config))
            catalog.load_table(config.table)
            return True
        except Exception:
            return False
