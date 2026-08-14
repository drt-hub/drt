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
    asset_selection: str
    | list[str]
    | list[AssetKey]
    | list[AssetsDefinition | SourceAsset]
    | AssetSelection
    | None = None,
    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
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
        asset_selection=asset_selection,
        default_status=default_status,
    )
    def _drt_change_sensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
        previous_signal = context.cursor

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
