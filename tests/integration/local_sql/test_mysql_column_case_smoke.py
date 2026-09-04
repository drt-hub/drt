"""Live MySQL column-name case-folding verification (#1064).

#1062/#1063 fixed a real collapse bug on Snowflake: unquoted identifiers are
uppercase-folded by the driver, so an unreconciled ``fetch_rows(columns=[])``
call returned metadata column names that never matched a lowercase
``upsert_key``, collapsing every row into one keyless diff entry.

#1064 asks whether MySQL has the same problem. Static reading of PyMySQL /
MySQL server docs can't answer this reliably — case-folding depends on the
server's ``lower_case_table_names`` setting and (for *column* names
specifically, as opposed to table names) MySQL's own documented behavior:
column names are not folded at all, only table/database names are subject to
``lower_case_table_names``. This test confirms that empirically against a
real server rather than trusting the docs reading.
"""

from __future__ import annotations

import pytest

from drt.config.models import MySQLDestinationConfig
from drt.destinations.query import fetch_rows

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

pymysql = pytest.importorskip("pymysql")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")


def test_mysql_column_names_round_trip_case_unquoted() -> None:
    """A mixed-case, unquoted column name must come back exactly as declared.

    If this fails, MySQL needs the same `_reconcile_column_case()` treatment
    #1063 gave Snowflake; if it passes, `fetch_rows`'s current unreconciled
    `[d[0] for d in cur.description]` is already correct for MySQL and #1064's
    MySQL leg can be closed without a code change.
    """
    require_docker()
    mysql_container = testcontainers_mysql.MySqlContainer

    mysql = mysql_container(
        "mysql:8.4",
        username="root",
        password="rootpass",
        dbname="testdb",
        dialect="pymysql",
    ).with_env("MYSQL_ROOT_HOST", "%")
    with mysql:
        host = mysql.get_container_host_ip()
        port = int(mysql.get_exposed_port(3306))
        admin = pymysql.connect(
            host=host,
            port=port,
            user="root",
            password="rootpass",
            database="testdb",
            autocommit=True,
        )
        try:
            with admin.cursor() as cur:
                # Unquoted, mixed-case identifiers on both the primary key
                # and a non-key column — MySQL requires backticks to *create*
                # a reserved-adjacent or case-sensitive-looking name, but the
                # identifiers themselves are stored/returned verbatim absent
                # `lower_case_table_names` (which affects table/db names,
                # not column names, as configured here).
                cur.execute(
                    "CREATE TABLE Scores (`ID` INT PRIMARY KEY, `PlayerScore` VARCHAR(255))"
                )
                cur.execute("INSERT INTO Scores (`ID`, `PlayerScore`) VALUES (1, 'one')")

            config = MySQLDestinationConfig(
                type="mysql",
                host=host,
                port=port,
                dbname="testdb",
                user="root",
                password="rootpass",
                table="Scores",
                upsert_key=["ID"],
                introspect_schema=False,
            )

            rows = fetch_rows(config, "SELECT * FROM Scores", columns=[], field_hint=["ID"])

            assert rows == [{"ID": 1, "PlayerScore": "one"}]
        finally:
            admin.close()
