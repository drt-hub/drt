"""Tests for Databricks SQL Warehouse source."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import DatabricksProfile
from drt.sources.base import Source
from drt.sources.databricks import DatabricksSource


def _profile(**overrides: object) -> DatabricksProfile:
    defaults: dict = {
        "type": "databricks",
        "server_hostname": "dbc-xxx.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/abc",
        "access_token_env": "DATABRICKS_TOKEN",
        "schema": "default",
    }
    return DatabricksProfile(**{**defaults, **overrides})


def test_implements_source_protocol() -> None:
    assert isinstance(DatabricksSource(), Source)


def test_profile_describe_without_catalog() -> None:
    p = _profile()
    assert "default" in p.describe()
    assert p.describe().startswith("databricks")


def test_profile_describe_with_catalog() -> None:
    p = _profile(catalog="main", schema="analytics")
    assert "main.analytics" in p.describe()


def test_missing_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    src = DatabricksSource()
    with pytest.raises(ValueError, match="access_token"):
        # Force connection attempt by iterating (connection is lazy)
        # We need to actually call _connect to hit the token check
        src._connect(_profile())


def test_connection_import_error_handled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm ImportError propagates when databricks-sql is not installed."""
    import sys

    # Simulate missing databricks module
    monkeypatch.setitem(sys.modules, "databricks", None)
    src = DatabricksSource()
    # _connect should raise ImportError when it tries to import
    # Note: this only hits if databricks-sql-connector isn't installed
    # Skip if it IS installed
    try:
        from databricks import sql  # noqa: F401

        pytest.skip("databricks-sql-connector is installed locally")
    except ImportError:
        monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
        with pytest.raises(ImportError, match="drt-core\\[databricks\\]"):
            src._connect(_profile())


def _mocked_databricks_modules(connect: MagicMock | None = None) -> dict[str, MagicMock]:
    """sys.modules entries satisfying ``from databricks import sql`` — no real
    ``databricks-sql-connector`` install required. CI's default extras
    (`[dev,mcp,duckdb,postgres,mysql,clickhouse,sqlserver]`, see ci.yml) don't
    include ``[databricks]``, so a test gated on the real package would be
    silently skipped there even though it passes in a dev env that happens
    to have it installed — exactly what left the query_tags branch below
    uncovered on #879's own codecov run the first time around."""
    mock_sql = MagicMock()
    if connect is not None:
        mock_sql.connect = connect
    mock_databricks = MagicMock()
    mock_databricks.sql = mock_sql
    return {"databricks": mock_databricks, "databricks.sql": mock_sql}


def test_connect_with_query_tags_passes_native_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#768 — query_tags pass straight through to the driver's own
    `query_tags` connect kwarg."""
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    src = DatabricksSource()
    mock_connect = MagicMock()
    with patch.dict("sys.modules", _mocked_databricks_modules(mock_connect)):
        src._connect(_profile(), query_tags={"sync": "s", "run_id": "r"})
    mock_connect.assert_called_once_with(
        server_hostname="dbc-xxx.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/abc",
        access_token="fake-token",
        schema="default",
        query_tags={"sync": "s", "run_id": "r"},
    )


def test_connect_without_query_tags_omits_native_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    src = DatabricksSource()
    mock_connect = MagicMock()
    with patch.dict("sys.modules", _mocked_databricks_modules(mock_connect)):
        src._connect(_profile())
    assert "query_tags" not in mock_connect.call_args.kwargs


def test_extract_passes_query_tags_to_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    src = DatabricksSource()
    cur = MagicMock()
    cur.description = [("id",)]
    cur.__iter__.side_effect = lambda: iter([(1,)])
    conn = MagicMock()
    conn.cursor.return_value = cur
    with patch.object(DatabricksSource, "_connect", return_value=conn) as mock_connect:
        list(src.extract("SELECT 1", _profile(), query_tags={"sync": "s"}))
    mock_connect.assert_called_once_with(_profile(), query_tags={"sync": "s"})


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


def _install_fake_dbsql_exc(monkeypatch: pytest.MonkeyPatch) -> object:
    """Provide ``databricks.sql.exc`` so classification is testable.

    ``databricks-sql-connector`` is an optional extra and is not installed in
    the default dev environment, so the exception classes are recreated with
    the driver's real shape: OperationalError and ProgrammingError as PEP 249
    siblings under DatabaseError, and RequestError *under* OperationalError —
    which is what makes a 401 look retryable unless it is excluded explicitly.
    The base also carries ``context``, the dict holding ``http-code``.
    """
    import sys
    import types

    class Error(Exception):
        def __init__(self, message=None, context=None, *args):
            super().__init__(message, *args)
            self.message = message
            # The real driver stores the HTTP status here; without it a mock
            # cannot reproduce the 401 path at all.
            self.context = context or {}

    class DatabaseError(Error):
        pass

    class OperationalError(DatabaseError):
        pass

    class ProgrammingError(DatabaseError):
        pass

    class NotSupportedError(DatabaseError):
        pass

    class RequestError(OperationalError):
        # Mirrors the real driver: RequestError subclasses OperationalError,
        # not Error. Getting this wrong hides the auth-retry bug below, since
        # a 401 arrives as a RequestError.
        pass

    exc_mod = types.ModuleType("databricks.sql.exc")
    for cls in (
        Error,
        DatabaseError,
        OperationalError,
        ProgrammingError,
        NotSupportedError,
        RequestError,
    ):
        setattr(exc_mod, cls.__name__, cls)

    databricks_mod = types.ModuleType("databricks")
    sql_mod = types.ModuleType("databricks.sql")
    sql_mod.exc = exc_mod  # type: ignore[attr-defined]
    databricks_mod.sql = sql_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "databricks", databricks_mod)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "databricks.sql.exc", exc_mod)
    return exc_mod


def test_is_transient_operational_error(monkeypatch: pytest.MonkeyPatch) -> None:
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    assert DatabricksSource()._is_transient(exc_mod.OperationalError("service unavailable")) is True


def test_is_transient_request_error_cold_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stopped SQL warehouse resuming — the #654 cold-start case."""
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    exc = exc_mod.RequestError("Warehouse is starting")
    assert DatabricksSource()._is_transient(exc) is True


@pytest.mark.parametrize("exc_name", ["ProgrammingError", "DatabaseError", "NotSupportedError"])
def test_is_transient_false_for_permanent(monkeypatch: pytest.MonkeyPatch, exc_name: str) -> None:
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    exc = getattr(exc_mod, exc_name)("nope")
    assert DatabricksSource()._is_transient(exc) is False


@pytest.mark.parametrize("http_code", [401, 403])
def test_is_transient_false_for_auth_failures(
    monkeypatch: pytest.MonkeyPatch, http_code: int
) -> None:
    """A 401/403 must not be retried.

    databricks-sql-connector's own retry policy classes these as *never*
    retryable — auth/retry.py returns "Received 401 - UNAUTHORIZED. Confirm
    your authentication credentials." and "403 codes are not retried" — and
    then surfaces them as RequestError, which subclasses OperationalError.
    So the driver gives up and drt was retrying anyway, three times, against a
    credential that cannot work. Verified against the real package: before this
    fix, _is_transient returned True for both.
    """
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    exc = exc_mod.RequestError("Received 401 - UNAUTHORIZED", {"http-code": http_code})
    assert DatabricksSource()._is_transient(exc) is False


def test_is_transient_true_for_request_error_without_an_http_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The auth exclusion must not swallow the cold-start case it exists for.

    A resuming warehouse raises RequestError with no http-code (or a 5xx), and
    that is the single most valuable retry in this source (#654).
    """
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    assert DatabricksSource()._is_transient(exc_mod.RequestError("starting", {})) is True
    server_error = exc_mod.RequestError("gateway", {"http-code": 503})
    assert DatabricksSource()._is_transient(server_error) is True


def test_the_fake_exception_hierarchy_matches_the_real_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compare the double against the real driver, class by class.

    The double originally had ``RequestError(Error)``, outside the PEP 249
    tree. The real driver has ``RequestError(OperationalError)``, and that one
    difference is what hid the auth-retry bug this PR fixes: with the wrong
    base a 401 never reaches the isinstance check the exclusion guards, so the
    mocked suite was green while a real 401 was retried three times.

    Comparing against the *real* hierarchy alone would not catch that — the
    double is what every other test in this file actually runs against, so the
    two have to be compared directly.

    Skipped when the extra is absent (CI's default matrix), which is exactly
    why the bug survived — so this is a backstop, not the primary guard. The
    behavioural tests above run everywhere.
    """
    real = pytest.importorskip("databricks.sql").exc
    fake = _install_fake_dbsql_exc(monkeypatch)

    for name in [
        "Error",
        "DatabaseError",
        "OperationalError",
        "ProgrammingError",
        "NotSupportedError",
        "RequestError",
    ]:
        real_cls = getattr(real, name, None)
        fake_cls = getattr(fake, name, None)
        assert real_cls is not None, f"{name} vanished from the real driver"
        assert fake_cls is not None, f"the double is missing {name}"

        real_bases = [b.__name__ for b in real_cls.__mro__[1:] if b is not object]
        fake_bases = [b.__name__ for b in fake_cls.__mro__[1:] if b is not object]
        assert fake_bases == real_bases, (
            f"the double's {name} has bases {fake_bases} but the driver says "
            f"{real_bases} — every classification test here is running against "
            f"a hierarchy the driver does not have"
        )

    # The auth exclusion reads the HTTP status from here.
    assert real.RequestError("m", {"http-code": 401}).context == {"http-code": 401}
    assert fake.RequestError("m", {"http-code": 401}).context == {"http-code": 401}


def test_is_transient_false_for_unrelated_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_dbsql_exc(monkeypatch)
    assert DatabricksSource()._is_transient(ValueError("unrelated")) is False


def test_extract_retries_cold_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """Warehouse cold start: two RequestErrors, then rows come through."""
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object, **_kwargs: object) -> MagicMock:
        attempts.append(1)
        if len(attempts) < 3:
            raise exc_mod.RequestError("Warehouse is starting")
        conn = MagicMock()
        cur = MagicMock()
        cur.description = [("id",)]
        cur.__iter__.side_effect = lambda: iter([(1,)])
        conn.cursor.return_value = cur
        return conn

    with patch.object(DatabricksSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            rows = list(DatabricksSource().extract("SELECT id FROM t", _profile()))

    assert rows == [{"id": 1}]
    assert len(attempts) == 3


def test_extract_does_not_retry_bad_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object, **_kwargs: object) -> MagicMock:
        attempts.append(1)
        raise exc_mod.ProgrammingError("Table or view not found")

    with patch.object(DatabricksSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep") as sleep:
            with pytest.raises(exc_mod.ProgrammingError):
                list(DatabricksSource().extract("SELECT * FROM nope", _profile()))

    assert len(attempts) == 1
    sleep.assert_not_called()


def test_extract_does_not_retry_after_first_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """Scope boundary (#766): a yielded row cannot be un-sent."""
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object, **_kwargs: object) -> MagicMock:
        attempts.append(1)

        def exploding_rows():
            yield (1,)
            raise exc_mod.OperationalError("connection reset")

        conn = MagicMock()
        cur = MagicMock()
        cur.description = [("id",)]
        cur.__iter__.side_effect = exploding_rows
        conn.cursor.return_value = cur
        return conn

    with patch.object(DatabricksSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            gen = DatabricksSource().extract("SELECT id FROM t", _profile())
            assert next(gen) == {"id": 1}
            with pytest.raises(exc_mod.OperationalError):
                next(gen)

    assert len(attempts) == 1


def _streaming_conn(rows, description=None):
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.description = description if description is not None else [("id",), ("name",)]
    cur.__iter__.side_effect = lambda: iter(rows)
    return conn


class TestStreamingExtraction:
    """#765: DatabricksSource streams instead of calling fetchall().

    The cursor's ``__iter__`` delegates to the result set, whose own
    ``__iter__`` is a ``fetchone()`` loop — so iterating genuinely streams
    rather than materialising. Verified against databricks-sql-connector's
    source; there is no live-server measurement for this leg (no local server
    exists), which is why the dwh-smoke leg added alongside matters.

    No ``fetch_size`` knob: this cursor has no ``arraysize`` attribute, and
    ``fetchmany(size)`` takes a required argument rather than reading one, so
    there is nothing for a profile field to set.
    """

    def test_does_not_call_fetchall(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            list(DatabricksSource().extract("SELECT 1", _profile()))

        conn.cursor.return_value.fetchall.assert_not_called()

    def test_rows_are_mapped_to_dicts(self):
        conn = _streaming_conn([(1, "Alice"), (2, "Bob")])
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            rows = list(DatabricksSource().extract("SELECT 1", _profile()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_cursor_is_closed_before_the_connection(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            list(DatabricksSource().extract("SELECT 1", _profile()))

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor was never closed"
        assert names.index("cursor().close") < names.index("close")

    def test_connection_closes_when_the_generator_is_abandoned(self):
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774)."""
        conn = _streaming_conn([(i, "x") for i in range(100)])
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            gen = DatabricksSource().extract("SELECT 1", _profile())
            next(gen)
            gen.close()

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor leaked on abandonment"
        assert names.index("cursor().close") < names.index("close")

    def test_empty_result_yields_nothing(self):
        conn = _streaming_conn([], description=[("id",)])
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            assert list(DatabricksSource().extract("SELECT 1", _profile())) == []


class TestManagedTableCapable:
    """#960/#1108 — ManagedTableCapable's create-if-absent + escape-hatch
    contract, ported from the Snowflake leg's own test_snowflake.py suite.

    No conn.commit()/rollback() calls are expected anywhere here: Delta Lake
    has no multi-statement transactions at all (see the module docstring on
    drt/sources/databricks.py's ManagedTableCapable methods) — an even
    stronger constraint than Snowflake's merely-autocommit-by-default.

    Probes use `fetchall()` truthiness (`SHOW SCHEMAS`/`SHOW TABLES ... LIKE`
    return a result set, not a single EXISTS row), unlike Snowflake's
    `fetchone()`-against-information_schema shape.
    """

    def _mock_ddl_conn(self, *, exists: bool = False) -> MagicMock:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [("s", "n", False)] if exists else []
        conn.cursor.return_value = cur
        return conn

    def test_ensure_managed_schema_creates_when_absent(self) -> None:
        conn = self._mock_ddl_conn(exists=False)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            DatabricksSource().ensure_managed_schema(_profile(catalog="main"))

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any("SHOW SCHEMAS IN main" in sql for sql in executed)
        assert any("CREATE SCHEMA IF NOT EXISTS main._drt" in sql for sql in executed)
        conn.close.assert_called_once()

    def test_ensure_managed_schema_skips_create_when_present(self) -> None:
        """The escape hatch: a pre-provisioned schema must never see the
        CREATE statement, so a no-CREATE-privilege principal can still run."""
        conn = self._mock_ddl_conn(exists=True)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            DatabricksSource().ensure_managed_schema(_profile(catalog="main"))

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert not any("CREATE SCHEMA" in sql for sql in executed)
        conn.close.assert_called_once()

    def test_ensure_managed_schema_swallows_the_concurrent_create_race(self) -> None:
        """Two sessions can both pass the initial probe and both attempt
        CREATE; the loser's CREATE raises, and re-probing finds the schema
        already exists (the other session won) — must swallow, not raise."""
        conn = MagicMock()
        probe_cur = MagicMock()
        probe_cur.fetchall.return_value = []  # initial probe: doesn't exist yet
        probe_cur.execute.side_effect = [None, Exception("concurrent create race")]
        reprobe_cur = MagicMock()
        reprobe_cur.fetchall.return_value = [("s", "n", False)]  # now exists
        conn.cursor.side_effect = [probe_cur, reprobe_cur]

        with patch.object(DatabricksSource, "_connect", return_value=conn):
            DatabricksSource().ensure_managed_schema(_profile(catalog="main"))  # must not raise

        conn.close.assert_called_once()

    def test_ensure_managed_schema_reraises_when_still_absent_after_create_fails(self) -> None:
        """A CREATE failure that is NOT a lost concurrent race (e.g. a
        genuine permission error) must propagate, not be swallowed."""
        conn = MagicMock()
        probe_cur = MagicMock()
        probe_cur.fetchall.return_value = []
        probe_cur.execute.side_effect = [None, Exception("insufficient privileges")]
        reprobe_cur = MagicMock()
        reprobe_cur.fetchall.return_value = []  # still absent
        conn.cursor.side_effect = [probe_cur, reprobe_cur]

        with patch.object(DatabricksSource, "_connect", return_value=conn):
            with pytest.raises(Exception, match="insufficient privileges"):
                DatabricksSource().ensure_managed_schema(_profile(catalog="main"))

    def test_ensure_managed_schema_requires_catalog(self) -> None:
        with pytest.raises(ValueError, match="catalog"):
            DatabricksSource().ensure_managed_schema(_profile())

    def test_ensure_managed_schema_probes_the_configured_catalog_and_schema(self) -> None:
        conn = self._mock_ddl_conn(exists=True)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            DatabricksSource().ensure_managed_schema(
                _profile(catalog="main", managed_schema="custom_schema")
            )

        (sql,) = conn.cursor.return_value.execute.call_args.args
        assert sql == "SHOW SCHEMAS IN main LIKE 'custom_schema'"

    def _mock_table_probe_conn(
        self, *, schema_exists: bool, table_exists: bool = False
    ) -> MagicMock:
        conn = MagicMock()
        cur = MagicMock()
        schema_result = [("main", "custom_schema")] if schema_exists else []
        table_result = [("s", "n", False)] if table_exists else []
        cur.fetchall.side_effect = [schema_result, table_result]
        conn.cursor.return_value = cur
        return conn

    def test_managed_table_exists_true(self) -> None:
        conn = self._mock_table_probe_conn(schema_exists=True, table_exists=True)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            assert (
                DatabricksSource().managed_table_exists(_profile(catalog="main"), "_drt_runs")
                is True
            )

    def test_managed_table_exists_false_when_table_absent_but_schema_present(self) -> None:
        conn = self._mock_table_probe_conn(schema_exists=True, table_exists=False)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            assert (
                DatabricksSource().managed_table_exists(_profile(catalog="main"), "_drt_runs")
                is False
            )
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("SHOW TABLES IN") for sql in executed)

    def test_managed_table_exists_false_when_schema_itself_is_absent(self) -> None:
        """Normal first-use state: the managed schema hasn't been created
        yet. SHOW TABLES IN a nonexistent schema raises on Databricks
        (SCHEMA_NOT_FOUND) rather than returning no rows, unlike a
        Postgres/Snowflake information_schema query — so the schema's own
        existence is probed first and this must short-circuit to False
        without ever issuing the SHOW TABLES query (caught in review)."""
        conn = self._mock_table_probe_conn(schema_exists=False)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            assert (
                DatabricksSource().managed_table_exists(_profile(catalog="main"), "_drt_runs")
                is False
            )
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert not any(sql.startswith("SHOW TABLES IN") for sql in executed)

    def test_managed_table_exists_probes_the_configured_schema(self) -> None:
        conn = self._mock_table_probe_conn(schema_exists=True, table_exists=False)
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            DatabricksSource().managed_table_exists(
                _profile(catalog="main", managed_schema="custom_schema"), "_drt_runs"
            )

        (sql,) = conn.cursor.return_value.execute.call_args.args
        assert sql == "SHOW TABLES IN main.custom_schema LIKE '_drt_runs'"

    def test_managed_table_exists_requires_catalog(self) -> None:
        with pytest.raises(ValueError, match="catalog"):
            DatabricksSource().managed_table_exists(_profile(), "_drt_runs")

    def test_drop_managed_table_issues_drop_if_exists(self) -> None:
        conn = self._mock_ddl_conn()
        with patch.object(DatabricksSource, "_connect", return_value=conn):
            DatabricksSource().drop_managed_table(_profile(catalog="main"), "_drt_runs")

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any("DROP TABLE IF EXISTS main._drt._drt_runs" in sql for sql in executed)
        conn.close.assert_called_once()

    def test_drop_managed_table_requires_catalog(self) -> None:
        with pytest.raises(ValueError, match="catalog"):
            DatabricksSource().drop_managed_table(_profile(), "_drt_runs")

    def test_managed_table_capable_protocol_satisfied(self) -> None:
        from drt.sources.base import ManagedTableCapable

        assert isinstance(DatabricksSource(), ManagedTableCapable)
