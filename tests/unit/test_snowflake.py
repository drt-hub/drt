"""Unit tests for Snowflake source.

Uses a mock snowflake-connector-python — no real database required.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import SnowflakeProfile
from drt.sources.snowflake import SnowflakeSource


def _config(**overrides: Any) -> SnowflakeProfile:
    defaults: dict[str, Any] = {
        "type": "snowflake",
        "account": "xy12345.us-east-1",
        "user": "analyst",
        "password": "testpassword",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "warehouse": "COMPUTE_WH",
    }
    defaults.update(overrides)
    return SnowflakeProfile(**defaults)


def _fake_cursor(columns, rows):
    cur = MagicMock()
    cur.description = [(col,) for col in columns]
    cur.fetchall.return_value = rows
    # Since #765 rows come from iterating the cursor, not fetchall(). A fresh
    # iterator per call so a retried attempt re-reads rather than finding the
    # cursor exhausted.
    cur.__iter__.side_effect = lambda: iter(rows)
    return cur


def _fake_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestSnowflakeSource:
    def test_extract_returns_rows(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["id", "name"], [(1, "Alice"), (2, "Bob")])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            results = list(source.extract("SELECT * FROM users", config))
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice"}
        assert results[1] == {"id": 2, "name": "Bob"}
        cur.close.assert_called_once()
        conn.close.assert_called_once()

    def test_extract_empty_result(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["id"], [])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            results = list(source.extract("SELECT * FROM empty_table", config))
        assert results == []
        conn.close.assert_called_once()

    def test_test_connection_success(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["1"], [(1,)])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            assert source.test_connection(config) is True
        cur.execute.assert_called_with("SELECT 1")
        cur.close.assert_called_once()
        conn.close.assert_called_once()

    def test_test_connection_failure(self) -> None:
        source = SnowflakeSource()
        config = _config()
        with patch.object(SnowflakeSource, "_connect", side_effect=Exception("fail")):
            assert source.test_connection(config) is False

    def test_connect_import_error(self) -> None:
        source = SnowflakeSource()
        config = _config()
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="Snowflake support requires"):
                source._connect(config)

    def test_connect_parameters(self) -> None:
        source = SnowflakeSource()
        config = _config(role="ADMIN_ROLE")
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        modules = {
            "snowflake": mock_module,
            "snowflake.connector": mock_connector,
        }
        with patch.dict("sys.modules", modules):
            source._connect(config)
            mock_connector.connect.assert_called_once_with(
                account="xy12345.us-east-1",
                user="analyst",
                password="testpassword",
                database="ANALYTICS",
                schema="PUBLIC",
                warehouse="COMPUTE_WH",
                role="ADMIN_ROLE",
            )

    def test_connect_without_role(self) -> None:
        source = SnowflakeSource()
        config = _config()
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        modules = {
            "snowflake": mock_module,
            "snowflake.connector": mock_connector,
        }
        with patch.dict("sys.modules", modules):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert "role" not in call_kwargs

    def test_connect_with_query_tags_sets_session_parameter(self) -> None:
        """#768 — query_tags become the QUERY_TAG session parameter,
        JSON-encoded so QUERY_HISTORY carries structured attribution."""
        source = SnowflakeSource()
        config = _config()
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        modules = {"snowflake": mock_module, "snowflake.connector": mock_connector}
        with patch.dict("sys.modules", modules):
            source._connect(config, query_tags={"sync": "s", "run_id": "r"})
            call_kwargs = mock_connector.connect.call_args[1]
            import json

            assert json.loads(call_kwargs["session_parameters"]["QUERY_TAG"]) == {
                "sync": "s",
                "run_id": "r",
            }

    def test_connect_without_query_tags_omits_session_parameters(self) -> None:
        source = SnowflakeSource()
        config = _config()
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        modules = {"snowflake": mock_module, "snowflake.connector": mock_connector}
        with patch.dict("sys.modules", modules):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert "session_parameters" not in call_kwargs

    def test_extract_passes_query_tags_to_connect(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["id"], [(1,)])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn) as mock_connect:
            list(source.extract("SELECT 1", config, query_tags={"sync": "s"}))
        mock_connect.assert_called_once_with(config, query_tags={"sync": "s"})

    def test_connect_password_from_env(self) -> None:
        source = SnowflakeSource()
        config = _config(password=None, password_env="SNOWFLAKE_PASSWORD")
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        modules = {
            "snowflake": mock_module,
            "snowflake.connector": mock_connector,
        }
        with (
            patch.dict("sys.modules", modules),
            patch.dict("os.environ", {"SNOWFLAKE_PASSWORD": "env_secret"}),
        ):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert call_kwargs["password"] == "env_secret"


class TestSnowflakeSourceKeyPairConnect:
    """Source _connect passes DER private_key for key-pair auth (#737)."""

    @staticmethod
    def _pem() -> str:
        pytest.importorskip("cryptography")
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

    def _profile(self, **auth: Any) -> SnowflakeProfile:
        return SnowflakeProfile(
            type="snowflake",
            account="acct",
            user="svc_user",
            database="DB",
            schema="PUBLIC",
            warehouse="WH",
            **auth,
        )

    def test_private_key_env_wins_and_passes_der(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SF_PK", self._pem())
        fake = MagicMock()
        with patch.dict("sys.modules", {"snowflake": fake, "snowflake.connector": fake.connector}):
            SnowflakeSource()._connect(self._profile(private_key_env="SF_PK", password="ignored"))
        kwargs = fake.connector.connect.call_args.kwargs
        assert isinstance(kwargs["private_key"], bytes)  # DER bytes
        assert "password" not in kwargs

    def test_password_fallback_when_no_key(self) -> None:
        fake = MagicMock()
        with patch.dict("sys.modules", {"snowflake": fake, "snowflake.connector": fake.connector}):
            SnowflakeSource()._connect(self._profile(password="pw"))
        kwargs = fake.connector.connect.call_args.kwargs
        assert kwargs["password"] == "pw"
        assert "private_key" not in kwargs


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


class TestSnowflakeTransientClassification:
    """390114 (token expired) is a DatabaseError — and so are permanent errors.

    That collision is the reason ``with_retry`` takes a predicate rather than
    a tuple of exception types.
    """

    def test_token_expired_390114_is_transient(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.DatabaseError(msg="Authentication token has expired", errno=390114)
        assert SnowflakeSource()._is_transient(exc) is True

    def test_operational_error_is_transient(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        assert SnowflakeSource()._is_transient(sf_errors.OperationalError(msg="conn lost")) is True

    def test_revocation_check_error_is_transient(self) -> None:
        """An unreachable CRL/OCSP endpoint — subclass of OperationalError."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.RevocationCheckError(msg="OCSP responder unreachable")
        assert SnowflakeSource()._is_transient(exc) is True

    def test_programming_error_is_not_transient(self) -> None:
        """ProgrammingError subclasses DatabaseError — must not be swept in."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.ProgrammingError(msg="SQL compilation error", errno=1003)
        assert SnowflakeSource()._is_transient(exc) is False

    def test_database_error_with_other_errno_is_not_transient(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.DatabaseError(msg="something else", errno=1234)
        assert SnowflakeSource()._is_transient(exc) is False

    def test_unrelated_exception_is_not_transient(self) -> None:
        assert SnowflakeSource()._is_transient(ValueError("nope")) is False


class TestSnowflakeSourceRetry:
    def test_expired_token_is_retried_then_succeeds(self) -> None:
        """#654 saw long extracts outstay their session token."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        attempts: list[int] = []

        def connect(_config: Any, **_kwargs: Any) -> MagicMock:
            attempts.append(1)
            if len(attempts) < 3:
                raise sf_errors.DatabaseError(msg="Authentication token has expired", errno=390114)
            return _fake_conn(_fake_cursor(["id"], [(1,)]))

        with patch.object(SnowflakeSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(SnowflakeSource().extract("SELECT id FROM t", _config()))

        assert rows == [{"id": 1}]
        assert len(attempts) == 3

    def test_sql_compilation_error_is_not_retried(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        attempts: list[int] = []

        def connect(_config: Any, **_kwargs: Any) -> MagicMock:
            attempts.append(1)
            raise sf_errors.ProgrammingError(msg="SQL compilation error", errno=1003)

        with patch.object(SnowflakeSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep") as sleep:
                with pytest.raises(sf_errors.ProgrammingError):
                    list(SnowflakeSource().extract("SELECT nope", _config()))

        assert len(attempts) == 1
        sleep.assert_not_called()

    def test_failure_after_first_row_is_not_retried(self) -> None:
        """Scope boundary (#766): a yielded row cannot be un-sent."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        attempts: list[int] = []

        def connect(_config: Any, **_kwargs: Any) -> MagicMock:
            attempts.append(1)

            def exploding_rows():
                yield (1,)
                raise sf_errors.OperationalError(msg="connection reset")

            cur = MagicMock()
            cur.description = [("id",)]
            cur.__iter__.side_effect = exploding_rows
            return _fake_conn(cur)

        with patch.object(SnowflakeSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                gen = SnowflakeSource().extract("SELECT id FROM t", _config())
                assert next(gen) == {"id": 1}
                with pytest.raises(sf_errors.OperationalError):
                    next(gen)

        assert len(attempts) == 1


def _streaming_conn(rows, description=None):
    """A connection whose cursor streams rather than buffering.

    Snowflake's cursor is iterable and honours ``arraysize``, so the shape
    mirrors the Postgres leg rather than needing an explicit fetchmany loop.
    """
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.description = description if description is not None else [("id",), ("name",)]
    cur.__iter__.side_effect = lambda: iter(rows)
    return conn


class TestSnowflakeStreamingExtraction:
    """#765: SnowflakeSource streams instead of calling fetchall().

    ``fetchall()`` materialises the entire result set before the first row
    reaches the engine. The cursor is iterable and respects ``arraysize``, so
    iterating it fetches in batches of that size instead.
    """

    def test_does_not_call_fetchall(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            list(SnowflakeSource().extract("SELECT 1", _config()))

        conn.cursor.return_value.fetchall.assert_not_called()

    def test_arraysize_comes_from_fetch_size(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            list(SnowflakeSource().extract("SELECT 1", _config(fetch_size=2500)))

        assert conn.cursor.return_value.arraysize == 2500

    def test_rows_are_mapped_to_dicts(self):
        conn = _streaming_conn([(1, "Alice"), (2, "Bob")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            rows = list(SnowflakeSource().extract("SELECT 1", _config()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_cursor_is_closed_before_the_connection(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            list(SnowflakeSource().extract("SELECT 1", _config()))

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor was never closed"
        assert names.index("cursor().close") < names.index("close")

    def test_connection_closes_when_the_generator_is_abandoned(self):
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774)."""
        conn = _streaming_conn([(i, "x") for i in range(100)])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            gen = SnowflakeSource().extract("SELECT 1", _config())
            next(gen)
            gen.close()

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor leaked on abandonment"
        assert names.index("cursor().close") < names.index("close")

    def test_empty_result_yields_nothing(self):
        conn = _streaming_conn([], description=[("id",)])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            assert list(SnowflakeSource().extract("SELECT 1", _config())) == []


class TestManagedTableCapable:
    """#960/#1106 — ManagedTableCapable's create-if-absent + escape-hatch
    contract, ported from Postgres's own test_postgres_source.py suite.

    Unlike Postgres, no conn.commit()/rollback() calls are expected anywhere
    here: Snowflake DDL autocommits and has no savepoints (see the module
    docstring on drt/sources/snowflake.py's ManagedTableCapable methods)."""

    def _mock_ddl_conn(self, *, schema_exists: bool = False) -> MagicMock:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = (1,) if schema_exists else None
        conn.cursor.return_value = cur
        return conn

    def test_ensure_managed_schema_creates_when_absent(self) -> None:
        conn = self._mock_ddl_conn(schema_exists=False)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().ensure_managed_schema(_config())

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any("INFORMATION_SCHEMA.SCHEMATA" in sql for sql in executed)
        assert any("CREATE SCHEMA" in sql for sql in executed)
        conn.close.assert_called_once()

    def test_ensure_managed_schema_skips_create_when_present(self) -> None:
        """The escape hatch: a pre-provisioned schema must never see the
        CREATE statement, so a no-CREATE-privilege role can still run."""
        conn = self._mock_ddl_conn(schema_exists=True)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().ensure_managed_schema(_config())

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert not any("CREATE SCHEMA" in sql for sql in executed)
        conn.close.assert_called_once()

    def test_ensure_managed_schema_swallows_the_concurrent_create_race(self) -> None:
        """Two sessions can both pass the initial probe and both attempt
        CREATE; the loser's CREATE raises, and re-probing finds the schema
        already exists (the other session won) — must swallow, not raise."""
        conn = MagicMock()
        probe_cur = MagicMock()
        probe_cur.fetchone.return_value = None  # initial probe: doesn't exist yet
        probe_cur.execute.side_effect = [None, Exception("concurrent create race")]
        reprobe_cur = MagicMock()
        reprobe_cur.fetchone.return_value = (1,)  # now exists -- the other session won
        conn.cursor.side_effect = [probe_cur, reprobe_cur]

        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().ensure_managed_schema(_config())  # must not raise

        conn.close.assert_called_once()

    def test_ensure_managed_schema_reraises_when_still_absent_after_create_fails(self) -> None:
        """A CREATE failure that is NOT a lost concurrent race (e.g. a
        genuine permission error) must propagate, not be swallowed."""
        conn = MagicMock()
        probe_cur = MagicMock()
        probe_cur.fetchone.return_value = None
        probe_cur.execute.side_effect = [None, Exception("insufficient privileges")]
        reprobe_cur = MagicMock()
        reprobe_cur.fetchone.return_value = None  # still absent
        conn.cursor.side_effect = [probe_cur, reprobe_cur]

        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            with pytest.raises(Exception, match="insufficient privileges"):
                SnowflakeSource().ensure_managed_schema(_config())

    def test_ensure_managed_schema_probe_is_upper_normalized_and_database_scoped(self) -> None:
        """Unquoted identifiers fold to uppercase in Snowflake — the probe
        must UPPER()-normalize both sides rather than comparing the raw
        config string byte-for-byte, and must be scoped to config.database
        (Snowflake's information_schema is per-database, not global)."""
        conn = self._mock_ddl_conn(schema_exists=True)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().ensure_managed_schema(_config(managed_schema="custom_schema"))

        sql, params = conn.cursor.return_value.execute.call_args.args
        assert '"ANALYTICS".INFORMATION_SCHEMA.SCHEMATA' in sql
        assert "UPPER(schema_name) = UPPER(%s)" in sql
        assert params == ("custom_schema",)

    def test_managed_table_exists_true(self) -> None:
        conn = self._mock_ddl_conn()
        conn.cursor.return_value.fetchone.return_value = (1,)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            assert SnowflakeSource().managed_table_exists(_config(), "_drt_runs") is True

    def test_managed_table_exists_false(self) -> None:
        conn = self._mock_ddl_conn()
        conn.cursor.return_value.fetchone.return_value = None
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            assert SnowflakeSource().managed_table_exists(_config(), "_drt_runs") is False

    def test_managed_table_exists_probes_the_configured_schema(self) -> None:
        conn = self._mock_ddl_conn()
        conn.cursor.return_value.fetchone.return_value = None
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().managed_table_exists(
                _config(managed_schema="custom_schema"), "_drt_runs"
            )

        sql, params = conn.cursor.return_value.execute.call_args.args
        assert "INFORMATION_SCHEMA.TABLES" in sql
        assert "table_type = 'BASE TABLE'" in sql
        assert params == ("custom_schema", "_drt_runs")

    def test_drop_managed_table_issues_drop_if_exists(self) -> None:
        conn = self._mock_ddl_conn()
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().drop_managed_table(_config(), "_drt_runs")

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert 'DROP TABLE IF EXISTS "ANALYTICS"."_DRT"."_DRT_RUNS"' in executed
        conn.close.assert_called_once()

    def test_managed_identifiers_are_quoted_and_embedded_quotes_are_escaped(self) -> None:
        conn = self._mock_ddl_conn()
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            SnowflakeSource().drop_managed_table(
                _config(database='analytics"prod', managed_schema='drt"managed'),
                'snapshot"name',
            )

        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert sql == ('DROP TABLE IF EXISTS "ANALYTICS""PROD"."DRT""MANAGED"."SNAPSHOT""NAME"')

    def test_managed_table_capable_protocol_satisfied(self) -> None:
        from drt.sources.base import ManagedTableCapable

        assert isinstance(SnowflakeSource(), ManagedTableCapable)


class TestSnapshotDiffSource:
    """#1112 — Snowflake's server-side snapshot-diff SQL contract.

    These mocks verify query shape and lifecycle only. Real Snowflake syntax
    and NULL-vs-empty hashing are covered by the gated DWH smoke test.
    """

    @staticmethod
    def _snapshot_conn(columns: list[str], *, current_exists: bool) -> MagicMock:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [(column,) for column in columns]
        cur.fetchone.return_value = (1,) if current_exists else None
        conn.cursor.return_value = cur
        return conn

    def test_first_run_classifies_every_row_as_added(self) -> None:
        conn = self._snapshot_conn(["id", "note"], current_exists=False)
        source = SnowflakeSource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_connect", return_value=conn),
            patch.object(
                source, "_stream_query", return_value=iter([{"id": 1, "note": None}])
            ) as stream,
        ):
            result = source.extract_snapshot_diff(
                'SELECT id AS "id", note AS "note" FROM users',
                _config(),
                sync_name="daily-users",
                key_columns=["id"],
                hash_columns="all",
            )

        assert result.is_first_run is True
        assert list(result.added) == [{"id": 1, "note": None}]
        assert list(result.changed) == []
        assert list(result.removed_keys) == []
        assert stream.call_args.args[1] == (
            'SELECT * FROM "ANALYTICS"."_DRT"."_DRT_SNAPSHOT_DAILY-USERS_SCRATCH"'
        )
        create_sql = conn.cursor.return_value.execute.call_args_list[0].args[0]
        assert create_sql.startswith(
            'CREATE OR REPLACE TABLE "ANALYTICS"."_DRT".'
            '"_DRT_SNAPSHOT_DAILY-USERS_SCRATCH" COMMENT = \''
        )
        create_call = conn.cursor.return_value.execute.call_args_list[0]
        assert len(create_call.args) == 1  # no bind params: model SQL may contain '%'
        create_token = re.search(r"COMMENT = '([0-9a-f-]{36})' AS ", create_sql).group(1)
        assert stream.call_args.kwargs["expected_token"] == create_token

    def test_model_sql_with_percent_literal_is_not_percent_formatted(self) -> None:
        conn = self._snapshot_conn(["id", "note"], current_exists=False)
        source = SnowflakeSource()
        model_sql = 'SELECT id AS "id", note AS "note" FROM users WHERE note LIKE \'a%\''
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_connect", return_value=conn),
            patch.object(source, "_stream_query", return_value=iter(())),
        ):
            source.extract_snapshot_diff(
                model_sql,
                _config(),
                sync_name="daily-users",
                key_columns=["id"],
                hash_columns="all",
            )

        create_call = conn.cursor.return_value.execute.call_args_list[0]
        assert len(create_call.args) == 1
        assert create_call.args[0].endswith(f" AS {model_sql}")

    def test_existing_snapshot_builds_quoted_join_hash_and_removed_key_queries(self) -> None:
        conn = self._snapshot_conn(["id", "note", "ignored"], current_exists=True)
        source = SnowflakeSource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_connect", return_value=conn),
            patch.object(
                source,
                "_stream_query",
                side_effect=[
                    iter([{"id": 4, "note": "new", "ignored": 0}]),
                    iter([{"id": 3}]),
                    iter([{"id": 2, "note": "changed", "ignored": 0}]),
                ],
            ) as stream,
        ):
            result = source.extract_snapshot_diff(
                "SELECT * FROM users",
                _config(),
                sync_name="daily-users",
                key_columns=["id"],
                hash_columns=["note"],
            )

        assert result.is_first_run is False
        assert [row["id"] for row in result.added] == [4]
        assert [row["id"] for row in result.changed] == [2]
        assert list(result.removed_keys) == [{"id": 3}]

        added_sql, removed_sql, changed_sql = [call.args[1] for call in stream.call_args_list]
        assert 'LEFT JOIN "ANALYTICS"."_DRT"."_DRT_SNAPSHOT_DAILY-USERS" AS c' in added_sql
        assert 's."id" = c."id"' in added_sql
        assert 'WHERE c."id" IS NULL' in added_sql
        assert removed_sql.startswith('SELECT c."id" FROM ')
        assert 'c."id" = s."id"' in removed_sql
        assert 'WHERE s."id" IS NULL' in removed_sql
        assert 'HASH(s."note") <> HASH(c."note")' in changed_sql
        assert "COALESCE" not in changed_sql

    def test_hash_columns_typo_raises_loudly(self) -> None:
        conn = self._snapshot_conn(["id", "note"], current_exists=False)
        source = SnowflakeSource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_connect", return_value=conn),
        ):
            with pytest.raises(ValueError, match=r"hash_columns.*\['ntoe'\].*typo"):
                source.extract_snapshot_diff(
                    "SELECT * FROM users",
                    _config(),
                    sync_name="users",
                    key_columns=["id"],
                    hash_columns=["ntoe"],
                )

    def test_all_hash_columns_excludes_upsert_key(self) -> None:
        conn = self._snapshot_conn(["id", "note", "plan"], current_exists=True)
        source = SnowflakeSource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_connect", return_value=conn),
            patch.object(
                source,
                "_stream_query",
                side_effect=[iter(()), iter(()), iter(())],
            ) as stream,
        ):
            source.extract_snapshot_diff(
                "SELECT * FROM users",
                _config(),
                sync_name="users",
                key_columns=["id"],
                hash_columns="all",
            )

        changed_sql = stream.call_args_list[2].args[1]
        assert 'HASH(s."note", s."plan")' in changed_sql
        assert 'HASH(c."note", c."plan")' in changed_sql
        assert 'HASH(s."id"' not in changed_sql

    def test_token_mismatch_before_classification_yields_no_rows(self) -> None:
        build_conn = self._snapshot_conn(["id", "note"], current_exists=False)
        stream_conn = MagicMock()
        stream_cur = MagicMock()
        stream_cur.fetchone.return_value = ("another-run-token",)
        stream_cur.description = [("id",), ("note",)]
        stream_cur.__iter__.return_value = iter([(1, "must-not-be-yielded")])
        stream_conn.cursor.return_value = stream_cur
        source = SnowflakeSource()

        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_connect", side_effect=[build_conn, stream_conn]),
        ):
            result = source.extract_snapshot_diff(
                "SELECT * FROM users",
                _config(),
                sync_name="users",
                key_columns=["id"],
                hash_columns="all",
            )
            yielded: list[dict[str, Any]] = []
            with pytest.raises(RuntimeError, match=r"must not run concurrently"):
                for row in result.added:
                    yielded.append(row)

        assert yielded == []
        executed = [call.args[0] for call in stream_cur.execute.call_args_list]
        assert len(executed) == 1
        assert executed[0].startswith("SELECT comment FROM ")

    def test_stream_checks_matching_token_before_and_after_query(self) -> None:
        source = SnowflakeSource()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [("run-token",), ("run-token",)]
        cur.description = [("id",), ("note",)]
        cur.__iter__.return_value = iter([(1, "ok")])
        conn.cursor.return_value = cur

        with patch.object(source, "_connect", return_value=conn):
            rows = list(
                source._stream_query(
                    _config(),
                    "SELECT * FROM scratch",
                    scratch_table="_drt_snapshot_users_scratch",
                    expected_token="run-token",
                    sync_name="users",
                    query_tags=None,
                )
            )

        assert rows == [{"id": 1, "note": "ok"}]
        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert executed[1] == "SELECT * FROM scratch"
        assert executed[0].startswith("SELECT comment FROM ")
        assert executed[2].startswith("SELECT comment FROM ")

    def test_commit_swaps_existing_baseline_then_cleans_old_snapshot(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [
            (1,),
            (1,),
            ("run-token",),
            ("run-token",),
        ]  # scratch/current exist, token matches before and after SWAP
        conn.cursor.return_value = cur
        source = SnowflakeSource()
        source._snapshot_diff_tokens[("_DRT", "daily-users")] = "run-token"
        with patch.object(source, "_connect", return_value=conn):
            source.commit_snapshot_diff(_config(), "daily-users")

        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert any(" SWAP WITH " in sql for sql in executed)
        assert executed[-1] == (
            'DROP TABLE IF EXISTS "ANALYTICS"."_DRT"."_DRT_SNAPSHOT_DAILY-USERS_SCRATCH"'
        )

    def test_commit_renames_scratch_on_first_promotion(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [
            (1,),
            None,
            ("run-token",),
            ("run-token",),
        ]  # scratch exists, current absent, token matches before/after RENAME
        conn.cursor.return_value = cur
        source = SnowflakeSource()
        source._snapshot_diff_tokens[("_DRT", "users")] = "run-token"
        with patch.object(source, "_connect", return_value=conn):
            source.commit_snapshot_diff(_config(), "users")

        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert (
            'ALTER TABLE "ANALYTICS"."_DRT"."_DRT_SNAPSHOT_USERS_SCRATCH" '
            'RENAME TO "ANALYTICS"."_DRT"."_DRT_SNAPSHOT_USERS"'
        ) in executed

    def test_commit_token_mismatch_raises_without_swapping(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [(1,), None, ("another-run-token",)]
        conn.cursor.return_value = cur
        source = SnowflakeSource()
        source._snapshot_diff_tokens[("_DRT", "users")] = "this-run-token"

        with (
            patch.object(source, "_connect", return_value=conn),
            pytest.raises(RuntimeError, match=r"must not run concurrently"),
        ):
            source.commit_snapshot_diff(_config(), "users")

        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert not any(" SWAP WITH " in sql or " RENAME TO " in sql for sql in executed)

    def test_commit_detects_replacement_in_final_swap_window(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [
            (1,),
            (1,),
            ("this-run-token",),
            ("another-run-token",),
        ]
        conn.cursor.return_value = cur
        source = SnowflakeSource()
        source._snapshot_diff_tokens[("_DRT", "users")] = "this-run-token"

        with (
            patch.object(source, "_connect", return_value=conn),
            pytest.raises(RuntimeError, match=r"must not run concurrently"),
        ):
            source.commit_snapshot_diff(_config(), "users")

        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert any(" SWAP WITH " in sql for sql in executed)
        assert not any(sql.startswith("DROP TABLE") for sql in executed)

    def test_commit_raises_when_scratch_vanished_and_baseline_is_not_ours(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [None, ("another-run-token",)]
        conn.cursor.return_value = cur
        source = SnowflakeSource()
        source._snapshot_diff_tokens[("_DRT", "users")] = "this-run-token"

        with (
            patch.object(source, "_connect", return_value=conn),
            pytest.raises(RuntimeError, match=r"must not run concurrently"),
        ):
            source.commit_snapshot_diff(_config(), "users")

    def test_commit_tolerates_missing_scratch_when_baseline_carries_our_token(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.side_effect = [None, ("this-run-token",)]
        conn.cursor.return_value = cur
        source = SnowflakeSource()
        source._snapshot_diff_tokens[("_DRT", "users")] = "this-run-token"

        with patch.object(source, "_connect", return_value=conn):
            source.commit_snapshot_diff(_config(), "users")

        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert not any(" SWAP WITH " in sql or " RENAME TO " in sql for sql in executed)

    def test_commit_without_remembered_token_is_noop(self) -> None:
        source = SnowflakeSource()
        with patch.object(source, "_connect") as connect:
            source.commit_snapshot_diff(_config(), "users")

        connect.assert_not_called()

    def test_snapshot_diff_protocol_satisfied(self) -> None:
        from drt.sources.base import SnapshotDiffSource

        assert isinstance(SnowflakeSource(), SnapshotDiffSource)
