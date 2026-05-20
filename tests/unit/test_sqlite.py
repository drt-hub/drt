"""Tests for SQLite source connector."""

import os
import sqlite3
import tempfile
import unittest

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
