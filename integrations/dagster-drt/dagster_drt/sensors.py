"""Change-detection sensors for dagster-drt (Tier 2, ADR 0004, #855).

Polls a cheap, metadata-only change signal on the drt project's source
profile and fires a ``RunRequest`` when it moves — Dagster supplies the
durability, cursoring, and backfill semantics drt itself doesn't have.
This is deliberately not wired to drt's own state (``StateStore`` /
``WatermarkStorage``): the signal here is "has the source moved", tracked
by Dagster's own sensor cursor, which is a different question from "what
has drt itself already synced". The actual sync run, once triggered,
still reads/writes drt's state exactly as it always has — see
``resource.py`` / ``assets.py`` for that half.

Supported profile types today (see
``docs/research/warehouse-trigger-matrix.md`` for the full survey):

- ``deltalake`` — ``DeltaTable.version()``, a monotonic integer already
  read in ``drt/sources/deltalake.py``.
- ``iceberg`` — the catalog's ``current_snapshot().snapshot_id``.
- ``snowflake`` — ``SYSTEM$LAST_CHANGE_COMMIT_TIME('<table>')``, verified
  against a real account in #975: monotonic, no ``CHANGE_TRACKING``
  prerequisite, no stream-style consumption trap (unlike ``STREAM`` /
  ``SYSTEM$STREAM_HAS_DATA()``, which #855 ruled out for exactly that
  reason — see the ADR 0004 amendment). Requires ``watch_table=`` (a
  ``SnowflakeProfile`` has no single table of its own — it's
  account/database/schema/warehouse-scoped) and
  ``minimum_interval_seconds=`` (see the cost note below).
- ``sqlserver`` — ``CHANGE_TRACKING_CURRENT_VERSION()``, also verified in
  #975: monotonic, no consumption trap, no known compute-cost analogue to
  Snowflake's warehouse. Requires ``watch_table=`` too, but only to
  *validate* — the database-scoped version function itself takes no table
  argument, but stays non-NULL as soon as *any* table in the database has
  Change Tracking enabled, saying nothing about whether the one a sync
  actually reads is separately enabled (`ALTER TABLE ... ENABLE
  CHANGE_TRACKING` is a per-table opt-in on top of the per-database one).
  Caught in Codex review (#984): without validating ``watch_table``
  against ``CHANGE_TRACKING_MIN_VALID_VERSION``, a table that was never
  individually enabled would silently never advance the signal, and the
  sensor would look like it's working while never actually seeing that
  table's changes.

Both ``deltalake``/``iceberg`` and ``snowflake``/``sqlserver`` require a
project-wide or explicit table because a ``DeltaLakeProfile`` /
``IcebergProfile`` each map to exactly one physical table
(``drt/config/profiles.py``) and a project has exactly one profile
(``project.profile``, resolved once) — every sync in such a project
shares the same table and the same signal — while ``snowflake``/
``sqlserver`` profiles are account/database-scoped with no single table
of their own, so ``watch_table=`` supplies what the profile can't. Even
with ``watch_table=`` validated, SQL Server's actual *polled* signal
(``CHANGE_TRACKING_CURRENT_VERSION()``) stays database-wide — it fires on
any tracked table's change, not only the validated one — coarser than
Delta/Iceberg, but not unsafe (an extra sensor-triggered run just finds
nothing new). Snowflake is the only one with a real polling cost: its
signal query reuses the profile's ``warehouse=`` exactly like an ordinary
sync, and Snowflake auto-resumes a suspended warehouse for any query when
``AUTO_RESUME=TRUE`` (the default) — see ``minimum_interval_seconds=``'s
required-for-Snowflake check below before assuming this is as free as the
other three.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    JobDefinition,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

if TYPE_CHECKING:
    # dagster's own CoercibleToAssetSelection type alias isn't exported from
    # the top-level dagster package — spelled out inline below instead.
    # Safe as a bare annotation (never evaluated at runtime) thanks to
    # `from __future__ import annotations` above.
    from dagster import AssetsDefinition, SourceAsset


def _current_signal(
    project_dir: Path,
    *,
    watch_table: str | None = None,
    minimum_interval_seconds: int | None = None,
) -> str:
    """Read the cheap, metadata-only change signal for this project's source.

    Never runs a sync's model SQL or reads any row data — only table/catalog
    metadata. Raises ``NotImplementedError`` for profile types with no signal
    wired up yet, and ``ValueError`` for a supported profile type missing a
    required argument (``watch_table=`` / ``minimum_interval_seconds=`` for
    Snowflake) or an unresolvable signal (NULL from the warehouse/database).
    Both are configuration errors and deliberately left to propagate rather
    than being swallowed as a skip (see the sensor body).
    """
    from drt.config.credentials import (
        DeltaLakeProfile,
        IcebergProfile,
        SnowflakeProfile,
        SQLServerProfile,
        load_profile,
        resolve_env,
        resolve_env_dict,
    )
    from drt.config.parser import load_project

    project = load_project(project_dir)
    profile = load_profile(project.profile)

    if isinstance(profile, DeltaLakeProfile):
        from deltalake import DeltaTable

        options = resolve_env_dict(profile.storage_options) or None
        return str(DeltaTable(profile.location, storage_options=options).version())

    if isinstance(profile, IcebergProfile):
        from pyiceberg.catalog import load_catalog

        # Mirrors drt/sources/iceberg.py's _catalog_properties() — kept
        # inline rather than imported since that helper is that module's
        # private implementation detail, not a shared contract.
        catalog_props = resolve_env_dict(profile.properties)
        if profile.catalog_uri:
            catalog_props["uri"] = profile.catalog_uri
        if profile.warehouse:
            catalog_props["warehouse"] = profile.warehouse

        catalog = load_catalog(profile.catalog_name, **catalog_props)
        table = catalog.load_table(profile.table)
        snapshot = table.current_snapshot()
        # None here means "table exists, no data written yet" and will move
        # to a real id the moment it does — unlike the NULLs below, which
        # mean the signal itself is unusable, so 0 would be wrong for those.
        return str(snapshot.snapshot_id) if snapshot is not None else "0"

    if isinstance(profile, SnowflakeProfile):
        # Validate before importing the (optional, driver-requiring) connector
        # — a missing watch_table=/minimum_interval_seconds= is a config
        # error independent of whether snowflake-connector-python is even
        # installed, and should fail with that ValueError rather than a
        # ModuleNotFoundError masking it.
        if not watch_table:
            raise ValueError(
                "build_drt_change_sensor() requires watch_table= for a "
                "Snowflake profile — SnowflakeProfile has no single table "
                "(it's account/database/schema/warehouse-scoped), so unlike "
                "Delta/Iceberg there's no table for a zero-config signal to "
                "point at (#975). Pass the fully-qualified table name, e.g. "
                "'MY_DB.MY_SCHEMA.MY_TABLE'."
            )
        if minimum_interval_seconds is None:
            raise ValueError(
                "build_drt_change_sensor() requires minimum_interval_seconds= "
                "for a Snowflake profile. The signal query reuses this "
                "profile's warehouse= exactly like an ordinary sync "
                "(drt/sources/snowflake.py), and Snowflake auto-resumes a "
                "suspended warehouse for any query when AUTO_RESUME=TRUE "
                "(the default), billing at least one minimum increment — "
                "without an explicit poll interval, Dagster's default tick "
                "cadence would keep the warehouse continuously resumed. "
                "Delta/Iceberg sensors have no such cost; this one does — "
                "pick an interval that reflects what that's worth versus the "
                "free Tier 1 (warehouse-native TASK) / Tier 3 (drt serve) "
                "paths (#975)."
            )

        import snowflake.connector

        connect_args: dict[str, object] = {
            "account": profile.account,
            "user": profile.user,
            "database": profile.database,
            "schema": profile.schema,
        }
        # Mirrors drt/sources/snowflake.py's own connect_args construction —
        # key-pair auth (#737) wins over password when both are configured.
        private_key_pem = resolve_env(None, profile.private_key_env)
        if private_key_pem:
            from drt.config.credentials import load_snowflake_private_key

            connect_args["private_key"] = load_snowflake_private_key(
                private_key_pem, resolve_env(None, profile.private_key_passphrase_env)
            )
        else:
            connect_args["password"] = resolve_env(profile.password, profile.password_env) or ""
        if profile.warehouse:
            connect_args["warehouse"] = profile.warehouse
        if profile.role:
            connect_args["role"] = profile.role

        sf_conn = snowflake.connector.connect(**connect_args)
        try:
            with sf_conn.cursor() as cur:
                # Literal interpolation, not a bind parameter: watch_table is
                # sensor-construction config the developer wrote in their own
                # Python, never external/user input — and this is the exact
                # form verified live against a real account (#975).
                cur.execute(f"SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('{watch_table}')")
                sf_row = cur.fetchone()
                sf_value = sf_row[0] if sf_row is not None else None
        finally:
            sf_conn.close()
        if sf_value is None:
            raise ValueError(
                f"SYSTEM$LAST_CHANGE_COMMIT_TIME returned NULL for "
                f"watch_table={watch_table!r} — likely means the table name "
                "doesn't resolve (check database/schema qualification)."
            )
        return str(sf_value)

    if isinstance(profile, SQLServerProfile):
        # Validate before importing the (optional, driver-requiring) connector
        # — same reasoning as the Snowflake branch above.
        if not watch_table:
            raise ValueError(
                "build_drt_change_sensor() requires watch_table= for a SQL "
                "Server profile. CHANGE_TRACKING_CURRENT_VERSION() is "
                "database-scoped and stays non-NULL as soon as *any* table in "
                "the database has Change Tracking enabled — it says nothing "
                "about whether the specific table a sync reads is actually "
                "tracked. A database can have Change Tracking enabled overall "
                "(`ALTER DATABASE ... SET CHANGE_TRACKING = ON`) while the "
                "table you care about was never separately enabled "
                "(`ALTER TABLE ... ENABLE CHANGE_TRACKING`) — without "
                "watch_table= to validate against, that silently produces a "
                "sensor that looks like it's working but never reports a "
                "change for that table (Codex review, #975/#984). Pass the "
                "table name CHANGETABLE would use, e.g. 'dbo.MyTable'."
            )

        import pymssql

        password = resolve_env(profile.password, profile.password_env) or ""
        mssql_conn = pymssql.connect(
            server=profile.host,
            port=str(profile.port),
            user=profile.user,
            password=password,
            database=profile.database,
        )
        try:
            with mssql_conn.cursor() as cur:
                # CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(...)) is NULL
                # unless watch_table itself has table-level Change Tracking
                # enabled — the per-table check CHANGE_TRACKING_CURRENT_VERSION()
                # alone can't provide, since it's database-scoped.
                cur.execute(
                    "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(%s))",
                    (watch_table,),
                )
                min_valid_row = cur.fetchone()
                min_valid = min_valid_row[0] if min_valid_row is not None else None
                if min_valid is None:
                    raise ValueError(
                        f"CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID({watch_table!r})) "
                        "returned NULL — this table doesn't have Change Tracking "
                        "enabled (or doesn't exist/resolve). Run `ALTER TABLE "
                        f"{watch_table} ENABLE CHANGE_TRACKING` even if the "
                        "database itself already has Change Tracking on "
                        "(Codex review, #975/#984)."
                    )

                cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
                mssql_row = cur.fetchone()
                mssql_value = mssql_row[0] if mssql_row is not None else None
        finally:
            mssql_conn.close()
        if mssql_value is None:
            raise ValueError(
                "CHANGE_TRACKING_CURRENT_VERSION() returned NULL — change "
                f"tracking is not enabled on database {profile.database!r} "
                "(see docs/research/warehouse-trigger-matrix.md's SQL Server "
                "section for how to enable it)."
            )
        return str(mssql_value)

    raise NotImplementedError(
        f"build_drt_change_sensor() doesn't support profile type "
        f"'{profile.type}' yet — supported: deltalake, iceberg, snowflake, "
        "sqlserver."
    )


def build_drt_change_sensor(
    project_dir: str | Path,
    *,
    name: str | None = None,
    minimum_interval_seconds: int | None = None,
    job: JobDefinition | None = None,
    asset_selection: str
    | list[str]
    | list[AssetKey]
    | list[AssetsDefinition | SourceAsset]
    | AssetSelection
    | None = None,
    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
    watch_table: str | None = None,
) -> SensorDefinition:
    """Build a Dagster sensor that fires when a drt project's source table changes.

    Exactly one ``RunRequest`` is emitted per detected change — never one
    per sync. What gets materialized is entirely up to ``job`` /
    ``asset_selection`` passed here, matching every other Dagster sensor:
    this function decides *when* to fire, not *what* runs.

    Only ``job=`` and ``asset_selection=`` are exposed, not dagster's newer
    ``target=`` or multi-job ``jobs=``: ``target=`` doesn't exist before
    Dagster 1.8 (`dagster-drt` still declares ``dagster>=1.6``, and this
    sensor must actually construct on that floor), and ``jobs=`` requires
    each returned ``RunRequest`` to set ``job_name`` to disambiguate, which
    this sensor — one signal, no way to know which job a generic "the
    source changed" event belongs to — has no principled way to choose.

    On the very first evaluation (no prior sensor cursor), this always
    fires once — the same "no watermark yet, so do a full pass" bootstrap
    behaviour drt's own incremental sync has on an unseeded cursor.

    Usage::

        from dagster_drt import build_drt_change_sensor, drt_assets

        @drt_assets(project_dir=".")
        def my_syncs(context, drt): ...

        # asset_selection=, not job=: my_syncs is an AssetsDefinition (a
        # @drt_assets multi_asset), and dagster's job= only accepts a
        # JobDefinition / GraphDefinition / UnresolvedAssetJobDefinition.
        change_sensor = build_drt_change_sensor(
            project_dir=".", asset_selection=[my_syncs]
        )

    Args:
        project_dir: Path to the drt project root.
        name: Optional sensor name (defaults to the decorated function's name).
        minimum_interval_seconds: Minimum seconds between evaluations.
        job: A JobDefinition/GraphDefinition/UnresolvedAssetJobDefinition
            this sensor triggers. For a plain @drt_assets multi_asset, use
            asset_selection= instead (see Usage above).
        asset_selection: Dagster asset selection this sensor triggers —
            accepts a sequence containing an AssetsDefinition (e.g. a
            @drt_assets multi_asset), a sequence of AssetKey, or an
            AssetSelection.
        default_status: Whether the sensor starts running or stopped when
            first deployed. Defaults to STOPPED — same reasoning as
            drt_serve's auth defaults: an orchestrator-launched sync
            hitting real destinations should be an opt-in, not a surprise
            on deploy.
        watch_table: Required, Snowflake and SQL Server profiles only.
            Fully-qualified table name (e.g. ``"MY_DB.MY_SCHEMA.MY_TABLE"``
            for Snowflake, ``"dbo.MyTable"`` for SQL Server). For Snowflake
            it's the table ``SYSTEM$LAST_CHANGE_COMMIT_TIME`` polls
            directly — a ``SnowflakeProfile`` has no single table of its
            own, unlike Delta/Iceberg. For SQL Server it's used only to
            *validate* that this specific table has Change Tracking
            enabled (via ``CHANGE_TRACKING_MIN_VALID_VERSION``) — the
            actual polled signal, ``CHANGE_TRACKING_CURRENT_VERSION()``,
            stays database-wide regardless. Ignored for deltalake/iceberg.

    Raises:
        NotImplementedError: at evaluation time, if the project's source
            profile isn't deltalake, iceberg, snowflake, or sqlserver —
            surfaces as a failed sensor tick in the Dagster UI rather than
            a silent, permanent skip.
        ValueError: at evaluation time, if the profile is snowflake or
            sqlserver and ``watch_table=`` (and, for snowflake,
            ``minimum_interval_seconds=`` too) was not given, if the
            signal query itself comes back NULL (change tracking not
            enabled, or an unresolvable ``watch_table``), or — sqlserver
            only — if ``watch_table`` resolves but doesn't itself have
            Change Tracking enabled. Same propagate-don't-skip treatment
            as ``NotImplementedError`` above.
        ImportError: at evaluation time, if the profile's optional driver
            (``deltalake``, ``pyiceberg``, ``snowflake-connector-python``,
            ``pymssql``) isn't installed — a permanent deploy-config error,
            not something a later tick would fix, so it also propagates
            rather than repeating forever as a skipped tick.
    """
    project_path = Path(project_dir)

    @sensor(
        name=name,
        minimum_interval_seconds=minimum_interval_seconds,
        job=job,
        asset_selection=asset_selection,
        default_status=default_status,
    )
    def _drt_change_sensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
        previous_signal = context.cursor

        try:
            current_signal = _current_signal(
                project_path,
                watch_table=watch_table,
                minimum_interval_seconds=minimum_interval_seconds,
            )
        except (NotImplementedError, ValueError, ImportError):
            # ImportError (including ModuleNotFoundError) means the profile's
            # optional driver — deltalake/pyiceberg/snowflake-connector-python
            # /pymssql — isn't installed. Left uncaught here on purpose
            # (Codex review, #984): dagster-drt's base install pulls in none
            # of them, and this is a permanent deploy-config error, not
            # something a retry on the next tick would ever fix. Catching it
            # below as "transient" would leave the sensor skipping forever,
            # indistinguishable in the Dagster UI from a working sensor that
            # just hasn't seen a change yet.
            raise
        except Exception as exc:
            # Transient I/O (network, auth, a catalog hiccup) skips this
            # tick rather than failing the sensor outright — the next
            # evaluation tries again. NotImplementedError/ValueError/
            # ImportError above are permanent config errors and are not
            # caught here on purpose.
            return SkipReason(f"Could not read change signal: {exc}")

        if previous_signal == current_signal:
            return SkipReason(f"No change since last check (signal={current_signal})")

        context.update_cursor(current_signal)
        # Keyed on the transition, not just the destination value: Dagster
        # dedupes run_key globally across every past evaluation of this
        # sensor, not just consecutive ones. A bare current_signal would
        # silently drop the run if the signal ever revisits an old value —
        # a table rollback (A -> B -> A) or a recreated Delta table
        # restarting its version counter from 0 both do exactly that.
        return RunRequest(run_key=f"{previous_signal}->{current_signal}")

    return _drt_change_sensor
