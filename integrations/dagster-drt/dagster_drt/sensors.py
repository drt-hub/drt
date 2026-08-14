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

Both are project-wide signals, not per-sync: a ``DeltaLakeProfile`` /
``IcebergProfile`` maps to exactly one physical table
(``drt/config/profiles.py``), and a project has exactly one profile
(``project.profile``, resolved once), so every sync in a Delta- or
Iceberg-backed project shares the same table and the same signal.
Snowflake ``STREAM`` and SQL Server Change Tracking are follow-up work —
both have real consumption-semantics caveats (a stream's false-positive
and DML-consumption rules; see the trigger matrix) that don't fit this
same metadata-only-read shape and need their own design pass.
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    DefaultSensorStatus,
    JobDefinition,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)


def _current_signal(project_dir: Path) -> str:
    """Read the cheap, metadata-only change signal for this project's source.

    Never runs a sync's model SQL or reads any row data — only table/catalog
    metadata. Raises ``NotImplementedError`` for profile types with no signal
    wired up yet, which is a configuration error and deliberately left to
    propagate rather than being swallowed as a skip (see the sensor body).
    """
    from drt.config.credentials import (
        DeltaLakeProfile,
        IcebergProfile,
        load_profile,
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
        return str(snapshot.snapshot_id) if snapshot is not None else "0"

    raise NotImplementedError(
        f"build_drt_change_sensor() doesn't support profile type "
        f"'{profile.type}' yet — supported: deltalake, iceberg."
    )


def build_drt_change_sensor(
    project_dir: str | Path,
    *,
    name: str | None = None,
    minimum_interval_seconds: int | None = None,
    job: JobDefinition | None = None,
    jobs: list[JobDefinition] | None = None,
    asset_selection: object | None = None,
    target: object | None = None,
    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
) -> SensorDefinition:
    """Build a Dagster sensor that fires when a drt project's source table changes.

    Exactly one ``RunRequest`` (keyed by the new signal value, so Dagster
    dedupes re-evaluation) is emitted per detected change — never one per
    sync. What gets materialized is entirely up to the ``job`` / ``jobs`` /
    ``asset_selection`` / ``target`` passed here, matching every other
    Dagster sensor: this function decides *when* to fire, not *what* runs.

    On the very first evaluation (no prior sensor cursor), this always
    fires once — the same "no watermark yet, so do a full pass" bootstrap
    behaviour drt's own incremental sync has on an unseeded cursor.

    Usage::

        from dagster_drt import build_drt_change_sensor, drt_assets

        @drt_assets(project_dir=".")
        def my_syncs(context, drt): ...

        # target=, not job=: my_syncs is an AssetsDefinition (a @drt_assets
        # multi_asset), and dagster's job= only accepts a JobDefinition /
        # GraphDefinition / UnresolvedAssetJobDefinition — target= is the
        # parameter that accepts an AssetsDefinition directly.
        change_sensor = build_drt_change_sensor(project_dir=".", target=my_syncs)

    Args:
        project_dir: Path to the drt project root.
        name: Optional sensor name (defaults to the decorated function's name).
        minimum_interval_seconds: Minimum seconds between evaluations.
        job: A JobDefinition/GraphDefinition/UnresolvedAssetJobDefinition
            this sensor triggers. For a plain @drt_assets multi_asset, use
            target= instead (see Usage above).
        jobs: Multiple jobs this sensor can trigger (requires job_name on
            each RunRequest — not done here; use one sensor per job instead
            if you need per-job cursors).
        asset_selection: Dagster asset selection this sensor triggers.
        target: Dagster's unified job/asset-selection target argument —
            accepts a JobDefinition, an UnresolvedAssetJobDefinition, an
            AssetsDefinition (e.g. a @drt_assets multi_asset), or a
            CoercibleToAssetSelection. The usual choice for this sensor.
        default_status: Whether the sensor starts running or stopped when
            first deployed. Defaults to STOPPED — same reasoning as
            drt_serve's auth defaults: an orchestrator-launched sync
            hitting real destinations should be an opt-in, not a surprise
            on deploy.

    Raises:
        NotImplementedError: at evaluation time, if the project's source
            profile isn't deltalake or iceberg (Snowflake / SQL Server are
            follow-up work) — surfaces as a failed sensor tick in the
            Dagster UI rather than a silent, permanent skip.
    """
    project_path = Path(project_dir)

    @sensor(
        name=name,
        minimum_interval_seconds=minimum_interval_seconds,
        job=job,
        jobs=jobs,
        asset_selection=asset_selection,  # type: ignore[arg-type]
        target=target,  # type: ignore[arg-type]
        default_status=default_status,
    )
    def _drt_change_sensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
        try:
            current_signal = _current_signal(project_path)
        except NotImplementedError:
            raise
        except Exception as exc:
            # Transient I/O (network, auth, a catalog hiccup) skips this
            # tick rather than failing the sensor outright — the next
            # evaluation tries again. A NotImplementedError above is a
            # permanent config error and is not caught here on purpose.
            return SkipReason(f"Could not read change signal: {exc}")

        if context.cursor == current_signal:
            return SkipReason(f"No change since last check (signal={current_signal})")

        context.update_cursor(current_signal)
        return RunRequest(run_key=current_signal)

    return _drt_change_sensor
