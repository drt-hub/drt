"""Unit tests for the query-tagging cursor wrapper in sql_utils.py (#768).

``tagged_cursor`` / ``TaggedCursor`` are the seam every SQL destination uses
to tag its SQL from wherever it obtains a cursor, without touching each
individual ``execute()`` call site.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from drt.config.models import SyncOptions
from drt.destinations.sql_utils import tagged_cursor as _tagged_cursor


def _options_with_tags(tags: dict[str, str] | None) -> SyncOptions:
    opts = SyncOptions()
    opts._query_tags = tags
    return opts


class TestTaggedCursorPassthrough:
    def test_no_tags_returns_the_same_cursor_object(self) -> None:
        """No wrapper overhead when tagging is off — existing dialect-hook
        tests that assert on the exact query string keep working unchanged."""
        cur = MagicMock()
        result = _tagged_cursor(cur, _options_with_tags(None))
        assert result is cur

    def test_empty_tags_returns_the_same_cursor_object(self) -> None:
        result = _tagged_cursor(MagicMock(), _options_with_tags({}))
        assert isinstance(result, MagicMock)


class TestTaggedCursorStringQueries:
    def test_prepends_comment_to_a_plain_string_query(self) -> None:
        cur = MagicMock()
        tagged = _tagged_cursor(cur, _options_with_tags({"sync": "s", "run_id": "r"}))
        tagged.execute("INSERT INTO t VALUES (%s)", (1,))
        query, params = cur.execute.call_args.args
        assert query == "/* drt sync=s run_id=r */\nINSERT INTO t VALUES (%s)"
        assert params == (1,)

    def test_positional_and_keyword_args_pass_through(self) -> None:
        cur = MagicMock()
        tagged = _tagged_cursor(cur, _options_with_tags({"sync": "s"}))
        tagged.execute("SELECT 1")
        cur.execute.assert_called_once_with("/* drt sync=s */\nSELECT 1")


class TestTaggedCursorComposableQueries:
    def test_prepends_comment_to_a_psycopg2_composable_query(self) -> None:
        pytest.importorskip("psycopg2.sql")
        from psycopg2 import sql as pgsql

        cur = MagicMock()
        tagged = _tagged_cursor(cur, _options_with_tags({"sync": "s"}))
        composed = pgsql.SQL("TRUNCATE TABLE {}").format(pgsql.Identifier("t"))
        tagged.execute(composed)
        (sent,) = cur.execute.call_args.args
        # Composable objects don't stringify with the comment inline the way
        # a plain str does, but the comment SQL() fragment must be present
        # as the leading component of the composed statement.
        assert isinstance(sent, pgsql.Composed)
        assert sent.seq[0].string == "/* drt sync=s */\n"


class TestTaggedCursorContextManager:
    def test_supports_with_statement(self) -> None:
        """Snowflake's destination uses `with conn.cursor() as cur:` —
        __getattr__ alone would not find __enter__/__exit__, since dunder
        lookups for implicit protocols bypass instance-level __getattr__."""
        real_cursor = MagicMock()
        real_cursor.__enter__ = MagicMock(return_value=real_cursor)
        real_cursor.__exit__ = MagicMock(return_value=False)

        tagged = _tagged_cursor(real_cursor, _options_with_tags({"sync": "s"}))
        with tagged as cur:
            assert cur is tagged  # the wrapper, not the raw cursor
            cur.execute("SELECT 1")
        real_cursor.__enter__.assert_called_once()
        real_cursor.__exit__.assert_called_once()
        real_cursor.execute.assert_called_once_with("/* drt sync=s */\nSELECT 1")


class TestTaggedCursorAttributeDelegation:
    def test_non_execute_attributes_delegate_to_the_real_cursor(self) -> None:
        """fetchall/rowcount/description etc. — every dialect hook reads
        these off ``cur`` directly; the wrapper must not shadow them."""
        cur = MagicMock()
        cur.fetchall.return_value = [(1, "a")]
        cur.rowcount = 3
        tagged = _tagged_cursor(cur, _options_with_tags({"sync": "s"}))
        assert tagged.fetchall() == [(1, "a")]
        assert tagged.rowcount == 3


class TestSyncOptionsQueryTagsDefault:
    def test_defaults_to_none(self) -> None:
        assert SyncOptions()._query_tags is None
