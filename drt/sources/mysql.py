"""MySQL source implementation.

Requires: pip install drt-core[mysql]

Example ~/.drt/profiles.yml:
    mysql:
      type: mysql
      host: localhost
      port: 3306
      dbname: analytics
      user: analyst
      password_env: MYSQL_PASSWORD   # export MYSQL_PASSWORD=secret
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import MySQLProfile, ProfileConfig, resolve_env
from drt.config.models import RetryConfig
from drt.destinations.retry import with_retry

# pymysql client-side error numbers that mean "the link to the server broke",
# from pymysql.constants.CR (MySQL's client library errno space, 2000+).
# Kept as literals so this module stays importable without the optional driver.
_MYSQL_TRANSIENT_ERRNOS = frozenset(
    {
        2002,  # CR_CONNECTION_ERROR    — can't connect via socket
        2003,  # CR_CONN_HOST_ERROR     — can't connect to host
        2006,  # CR_SERVER_GONE_ERROR   — server has gone away
        2013,  # CR_SERVER_LOST         — lost connection during query
        2055,  # CR_SERVER_LOST_EXTENDED
    }
)


class MySQLSource:
    """Extract records from a MySQL database."""

    def _is_transient(self, exc: Exception) -> bool:
        """Is ``exc`` worth retrying? (#766)

        ``InterfaceError`` is always transient — pymysql raises it when its
        own connection object is unusable.

        ``OperationalError`` is **not** retried on class alone, unlike the
        Postgres source. pymysql overloads it across both the client errno
        space (2002/2003/2006/2013/2055 — connection lost, server gone away)
        and the server's, where it also covers permanent conditions such as
        1045 access denied and 1049 unknown database. Retrying a bad password
        three times is pure waste and can trip an account-lockout policy, so
        only the client-side connection errnos above qualify.

        pymysql puts the errno in ``args[0]`` (``OperationalError(2013,
        "Lost connection …")``), which is why this reads an attribute rather
        than switching on the class — precisely the case ``retry_on`` exists
        for. An ``OperationalError`` with no usable errno is treated as
        permanent: failing fast beats retrying something unclassifiable.
        """
        try:
            import pymysql
        except ImportError:  # pragma: no cover - driver absent, nothing to classify
            return False
        if isinstance(exc, pymysql.err.InterfaceError):
            return True
        if isinstance(exc, pymysql.err.OperationalError):
            errno = exc.args[0] if exc.args else None
            return errno in _MYSQL_TRANSIENT_ERRNOS
        return False

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run ``query`` and yield rows as dicts, retrying transient failures.

        **Retry scope (#766): connection and query execution only.** A failure
        after the first row has been yielded propagates — those rows are
        already loaded downstream and cannot be un-sent. See the Postgres
        source for the full rationale.

        **Streaming (#765): ``SSCursor``**, pymysql's unbuffered cursor, so
        rows arrive one at a time off the wire instead of the whole result set
        being materialised client-side first — the largest reduction of any
        source here. Measured figures live in
        ``docs/research/extraction-memory.md``, which is the single source for
        them.

        Two things differ from the Postgres leg, both found against a live
        server rather than in any documentation:

        - ``description`` is already populated after ``execute()``, so columns
          can be read up front. A psycopg2 named cursor leaves it ``None``
          until the first batch arrives, which forces a different shape there.
        - **``cursor.close()`` must run before ``conn.close()``.** Closing the
          connection while the cursor still has unread rows makes pymysql's
          teardown read from a socket it has already discarded, and every
          abandoned stream then prints an ``AttributeError`` traceback via
          ``Exception ignored in``. It does not affect correctness — the
          server-side thread goes away either way — but it floods the CLI's
          stderr, which for a tool whose output people pipe is its own kind of
          breakage.

        No ``fetch_size`` knob here, unlike Postgres/Redshift. ``SSCursor``
        streams row by row, and ``fetchmany(n)`` simply calls ``read_next()``
        n times — it does not change the number of server round trips, it just
        holds n rows at once. Measured: iterating peaked at +3.6 MB,
        ``fetchmany(100000)`` at +61.9 MB, with no speed difference. A
        ``fetch_size`` here would only offer users a way to make things worse.
        (These two stay inline because they are the argument for omitting the
        knob, not a benchmark; see ``docs/research/extraction-memory.md``.)

        ``query_tags`` is unused — MySQL has no session/job-level tagging
        primitive drt can reach, so the SQL comment the engine already
        prepended to ``query`` is this connector's only attribution (#768).
        """
        assert isinstance(config, MySQLProfile)

        def _connect_and_execute() -> tuple[Any, Any, list[str]]:
            conn = self._connect(config)
            try:
                import pymysql

                cur = conn.cursor(pymysql.cursors.SSCursor)
                cur.execute(query)
                return conn, cur, [desc[0] for desc in cur.description]
            except BaseException:
                # The failed attempt cleans up after itself — the close is no
                # longer in a `finally` here, since the cursor must outlive it.
                conn.close()
                raise

        conn, cur, columns = with_retry(
            _connect_and_execute, RetryConfig(), retry_on=self._is_transient
        )

        # Iteration stays outside the retry — a yielded row cannot be un-sent.
        # The finally also fires on GeneratorExit, so an abandoned iterator
        # still tears down in the right order.
        try:
            for row in cur:
                yield dict(zip(columns, row))
        finally:
            cur.close()
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, MySQLProfile)
        conn = None
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return True
        except Exception:
            return False
        finally:
            if conn is not None:
                conn.close()

    def _connect(self, config: MySQLProfile) -> Any:
        try:
            import pymysql
        except ImportError as e:
            raise ImportError("MySQL support requires: pip install drt-core[mysql]") from e

        password = resolve_env(config.password, config.password_env) or ""
        return pymysql.connect(
            host=config.host,
            port=config.port,
            database=config.dbname,
            user=config.user,
            password=password,
            charset="utf8mb4",
        )
