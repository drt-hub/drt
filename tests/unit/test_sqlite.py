"""Tests for SQLite source connector."""

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from drt.config.credentials import SQLiteProfile
from drt.sources.sqlite import SQLiteSource


class TestSQLiteSource(unittest.TestCase):
    def setUp(self):
        # Create a temporary SQLite database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()

        # Initialize database and table
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    age INTEGER
                )
                """
            )
            cursor.executemany(
                "INSERT INTO users (name, age) VALUES (?, ?)",
                [
                    ("Alice", 30),
                    ("Bob", 25),
                    ("Charlie", 35),
                ],
            )
            conn.commit()

        self.source = SQLiteSource()
        self.config = SQLiteProfile(type="sqlite", database=self.db_path)

        assert isinstance(self.config, SQLiteProfile), "Config is not SQLiteProfile!"

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

    def test_extract_returns_all_rows(self):
        query = "SELECT id, name, age FROM users ORDER BY id"
        results = list(self.source.extract(query, self.config))

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["name"], "Alice")
        self.assertEqual(results[1]["name"], "Bob")
        self.assertEqual(results[2]["name"], "Charlie")

    def test_extract_empty_result(self):
        query = "SELECT * FROM users WHERE age > 100"
        results = list(self.source.extract(query, self.config))

        self.assertEqual(results, [])

    def test_extract_column_names(self):
        query = "SELECT id, name FROM users LIMIT 1"
        result = next(self.source.extract(query, self.config))

        self.assertIn("id", result)
        self.assertIn("name", result)

    def test_test_connection_success(self):
        self.assertTrue(self.source.test_connection(self.config))

    def test_test_connection_failure(self):
        bad_config = SQLiteProfile(type="sqlite", database="/invalid/path/to/db.sqlite")
        self.assertFalse(self.source.test_connection(bad_config))

    def test_invalid_config_type(self):
        class FakeConfig:
            database = self.db_path

        with self.assertRaises(TypeError):
            list(self.source.extract("SELECT 1", FakeConfig()))


if __name__ == "__main__":
    unittest.main()


class TestSQLiteStreamingExtraction(unittest.TestCase):
    """#765: SQLite iterates the cursor instead of materialising every row.

    "It's a local file" is not the same as "it's free" — the cost being
    removed is holding every row as a Python object, which a local file incurs
    just as readily as a remote warehouse. Measured on 300k rows of ~200B in a
    fresh process: +110.6 MB RSS for ``fetchall()``, +4.4 MB iterating.
    """

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.temp_db.close()
        self.db_path = self.temp_db.name
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE big (id INTEGER)")
            conn.executemany("INSERT INTO big VALUES (?)", [(i,) for i in range(2500)])
            conn.commit()
        self.config = SQLiteProfile(type="sqlite", database=self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_rows_span_many_batches_without_loss(self):
        """fetch_size=100 over 2500 rows means 25 batches — a boundary bug
        shows up as a short read or a duplicate."""
        self.config.fetch_size = 100
        rows = list(SQLiteSource().extract("SELECT id FROM big ORDER BY id", self.config))

        self.assertEqual([r["id"] for r in rows], list(range(2500)))

    def test_arraysize_is_set_from_fetch_size(self):
        """arraysize is what makes sqlite3 fetch in batches rather than one
        row at a time, so it has to carry the configured value."""
        self.config.fetch_size = 250
        with patch("sqlite3.connect") as connect:
            cur = connect.return_value.execute.return_value
            cur.description = [("id",)]
            cur.__iter__ = lambda self: iter([(1,)])

            list(SQLiteSource().extract("SELECT id FROM big", self.config))

        self.assertEqual(cur.arraysize, 250)

    def test_does_not_call_fetchall(self):
        with patch("sqlite3.connect") as connect:
            cur = connect.return_value.execute.return_value
            cur.description = [("id",)]
            cur.__iter__ = lambda self: iter([(1,)])

            rows = list(SQLiteSource().extract("SELECT id FROM big", self.config))

        self.assertEqual(rows, [{"id": 1}])
        cur.fetchall.assert_not_called()

    def test_empty_result_yields_nothing(self):
        rows = list(SQLiteSource().extract("SELECT id FROM big WHERE 0", self.config))
        self.assertEqual(rows, [])
