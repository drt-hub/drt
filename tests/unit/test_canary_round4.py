"""Round-four canary snapshot, descriptor, and lease regressions."""

from __future__ import annotations

import json
import os
import socket
import struct
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from drt.cli._apply_claims import (
    UnsafeApplyArtifacts,
    ensure_claims_dir,
    sweep_canary_spools,
)
from drt.cli._apply_flow import ApplyRefused, _validate_canary_compatibility
from drt.cli._canary_spool import (
    SnapshotSpoolSource,
    SnapshotSpoolWriter,
    SpoolError,
    open_spool,
    verify_spool,
)
from drt.config.credentials import BigQueryProfile
from drt.config.models import SyncConfig
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync


class _Source:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls = 0

    def extract(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls += 1
        yield from self.rows


class _Destination:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def load(self, records: list[dict[str, Any]], *_args: Any) -> SyncResult:
        self.rows.extend(records)
        return SyncResult(success=len(records))


def _sync(**sync_overrides: Any) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "canary",
            "model": "select 1",
            "destination": {"type": "rest_api", "url": "https://example.test"},
            "sync": {
                "computed_fields": {"label": "{{ row.name }}!"},
                "field_mappings": {"name": "display_name"},
                "mask": {"display_name": "hash"},
                **sync_overrides,
            },
        }
    )


def _profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def test_record_sink_is_post_transform_and_replay_never_reads_source(tmp_path: Path) -> None:
    source = _Source([{"id": 1, "name": "Ada"}])
    verified: list[dict[str, Any]] = []
    first_destination = _Destination()
    run_sync(
        _sync(metadata_columns={"run_id": "run_marker"}),
        source,
        first_destination,
        _profile(),
        tmp_path,
        dry_run=True,
        run_id="verified-run",
        record_sink=lambda row: verified.append(dict(row)),
    )
    assert source.calls == 1
    assert verified[0]["label"] == "Ada!"
    assert "display_name" in verified[0] and "name" not in verified[0]
    assert verified[0]["run_marker"] == "verified-run"

    source.rows[:] = [{"id": 999, "name": "mutated"}]
    replay_destination = _Destination()
    run_sync(
        _sync(metadata_columns={"run_id": "run_marker"}),
        source,
        replay_destination,
        _profile(),
        tmp_path,
        replay_records=verified,
        run_id="wave-run",
    )
    assert source.calls == 1
    assert replay_destination.rows == verified


def test_default_run_sync_does_not_invoke_a_sink(tmp_path: Path) -> None:
    source = _Source([{"id": 1, "name": "Ada"}])
    destination = _Destination()
    run_sync(_sync(), source, destination, _profile(), tmp_path)
    assert source.calls == 1 and len(destination.rows) == 1


def test_explicit_new_defaults_match_the_legacy_call_shape(tmp_path: Path) -> None:
    rows = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
    legacy_destination = _Destination()
    default_destination = _Destination()
    legacy = run_sync(
        _sync(), _Source([dict(row) for row in rows]), legacy_destination, _profile(), tmp_path
    )
    explicit = run_sync(
        _sync(),
        _Source([dict(row) for row in rows]),
        default_destination,
        _profile(),
        tmp_path,
        record_sink=None,
        replay_records=None,
    )
    assert legacy_destination.rows == default_destination.rows
    assert (legacy.rows_extracted, legacy.success, legacy.failed, legacy.skipped) == (
        explicit.rows_extracted,
        explicit.success,
        explicit.failed,
        explicit.skipped,
    )


def test_replay_skips_destination_lookup_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = SyncConfig.model_validate(
        {
            "name": "lookup-canary",
            "model": "select 1",
            "destination": {
                "type": "postgres",
                "host": "localhost",
                "dbname": "d",
                "table": "target",
                "upsert_key": ["id"],
                "lookups": {
                    "account_id": {
                        "table": "accounts",
                        "match": {"name": "account"},
                        "select": "id",
                    }
                },
            },
            "sync": {"mode": "upsert"},
        }
    )
    calls = 0

    def build(*_args: Any, **_kwargs: Any) -> dict[tuple[Any, ...], Any]:
        nonlocal calls
        calls += 1
        return {("acme",): 42}

    monkeypatch.setattr("drt.engine.sync.build_lookup_map", build)
    verified: list[dict[str, Any]] = []
    run_sync(
        sync,
        _Source([{"id": 1, "account": "acme"}]),
        _Destination(),
        _profile(),
        tmp_path,
        dry_run=True,
        record_sink=lambda row: verified.append(dict(row)),
    )
    assert calls == 1 and verified[0]["account_id"] == 42

    destination = _Destination()
    run_sync(
        sync,
        _Source([{"id": 999, "account": "changed"}]),
        destination,
        _profile(),
        tmp_path,
        replay_records=verified,
    )
    assert calls == 1 and destination.rows == verified


def _writer(path: Path) -> SnapshotSpoolWriter:
    return SnapshotSpoolWriter(path, max_rows=10, max_bytes=100_000, bucket_of=lambda _r: 0)


def test_spool_descriptor_survives_path_replacement(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.spool.jsonl"
    writer = _writer(path)
    writer.append({"id": 1})
    _rows, _bytes, digest = writer.close()
    with pytest.raises(OSError):
        os.write(writer.fd, b"not writable")
    verify_spool(writer.fd, digest)
    original = tmp_path / "original"
    path.rename(original)
    path.write_text('{"t":"dict","v":[["id",{"t":"int","v":"999"}]]}\n')

    verify_spool(writer.fd, digest)
    assert list(SnapshotSpoolSource(writer.fd).extract("", object())) == [{"id": 1}]
    writer.dispose()


def test_spool_float_bits_and_named_datetime_become_fixed_offset(tmp_path: Path) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    payload = 0xFFF8000000000042
    value = struct.unpack(">d", payload.to_bytes(8, "big"))[0]
    moment = datetime(2026, 7, 1, 12, tzinfo=ZoneInfo("Asia/Tokyo"))
    path = tmp_path / "snapshot.spool.jsonl"
    writer = _writer(path)
    naive = datetime(2026, 7, 1, 3, 4, 5, fold=1)
    writer.append({"float": value, "moment": moment, "naive": naive})
    writer.close()
    decoded = next(SnapshotSpoolSource(writer.fd).extract("", object()))
    assert struct.pack(">d", decoded["float"]) == payload.to_bytes(8, "big")
    assert decoded["moment"] == moment
    assert type(decoded["moment"].tzinfo).__name__ == "timezone"
    assert decoded["naive"] == naive and decoded["naive"].tzinfo is None
    writer.dispose()


def test_spool_rejects_invalid_float_payload_and_exposes_source_protocol(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.spool.jsonl"
    path.write_text('{"t":"dict","v":[["value",{"t":"float","v":"00"}]]}\n')
    path.chmod(0o600)
    fd = open_spool(path)
    source = SnapshotSpoolSource(fd)
    assert source.test_connection(object())
    with pytest.raises(SpoolError, match="IEEE-754"):
        list(source.extract("", object()))
    os.close(fd)


def test_unsupported_spool_value_writes_no_partial_line(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.spool.jsonl"
    writer = _writer(path)
    with pytest.raises(SpoolError, match="cannot preserve"):
        writer.append({"bad": object()})
    assert path.read_bytes() == b""
    writer.dispose()


def test_open_spool_refuses_symlink_nonregular_mode_and_hardlink(tmp_path: Path) -> None:
    good = tmp_path / "good"
    good.write_bytes(b"x")
    good.chmod(0o600)
    symlink = tmp_path / "symlink"
    symlink.symlink_to(good)
    with pytest.raises(SpoolError):
        open_spool(symlink)
    with pytest.raises(SpoolError, match="regular"):
        open_spool(tmp_path)
    good.chmod(0o640)
    with pytest.raises(SpoolError, match="permissions"):
        open_spool(good)
    good.chmod(0o600)
    hardlink = tmp_path / "hardlink"
    os.link(good, hardlink)
    with pytest.raises(SpoolError, match="one hard link"):
        open_spool(good)


def test_open_spool_refuses_wrong_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "spool"
    path.write_bytes(b"")
    path.chmod(0o600)
    real_fstat = os.fstat

    def wrong_owner(fd: int) -> Any:
        info = real_fstat(fd)
        values = list(info)
        values[4] = os.getuid() + 1
        return os.stat_result(values)

    monkeypatch.setattr(os, "fstat", wrong_owner)
    with pytest.raises(SpoolError, match="owned"):
        open_spool(path)


def test_writer_cleans_up_when_initial_descriptor_validation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _canary_spool

    path = tmp_path / "spool"
    monkeypatch.setattr(
        _canary_spool,
        "_validate_spool_fd",
        lambda _fd: (_ for _ in ()).throw(SpoolError("unsafe")),
    )
    with pytest.raises(SpoolError, match="unsafe"):
        _writer(path)
    assert not path.exists()


def test_writer_refuses_path_swap_while_sealing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _canary_spool

    path = tmp_path / "spool"
    writer = _writer(path)
    writer.append({"id": 1})
    real_open = _canary_spool.open_spool

    def swapped_open(candidate: Path) -> int:
        candidate.rename(tmp_path / "original")
        candidate.write_bytes(b"replacement")
        candidate.chmod(0o600)
        return real_open(candidate)

    monkeypatch.setattr(_canary_spool, "open_spool", swapped_open)
    with pytest.raises(SpoolError, match="path changed"):
        writer.close()
    writer.dispose()


def test_writer_safe_unlink_handles_disposed_missing_and_replaced_paths(tmp_path: Path) -> None:
    disposed = _writer(tmp_path / "disposed")
    disposed.dispose()
    assert not disposed.unlink_owned()
    disposed.dispose()

    missing = _writer(tmp_path / "missing")
    missing.close()
    missing.path.unlink()
    assert missing.unlink_owned()
    missing.dispose()

    replaced = _writer(tmp_path / "replaced")
    replaced.append({"id": 1})
    replaced.close()
    replaced.path.rename(tmp_path / "original")
    replaced.path.write_text("replacement")
    assert not replaced.unlink_owned()
    replaced.dispose()


def _claim(directory: Path, plan_id: str, **fields: Any) -> Path:
    path = directory / f"{plan_id}.json"
    path.write_text(json.dumps({"plan_id": plan_id, **fields}))
    return path


def test_sweep_removes_terminal_dead_and_old_unclaimed_spools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = ensure_claims_dir(tmp_path)
    terminal = directory / "terminal.spool.jsonl"
    terminal.write_text("secret")
    _claim(directory, "terminal", state="success")
    dead = directory / "dead.spool.jsonl"
    dead.write_text("secret")
    dead_claim = _claim(
        directory,
        "dead",
        state="pending",
        hostname=socket.gethostname(),
        pid=999_999_999,
    )
    old = directory / "old.spool.jsonl"
    old.write_text("secret")
    old_time = (datetime.now(timezone.utc) - timedelta(days=2)).timestamp()
    os.utime(old, (old_time, old_time))

    def dead_pid(_pid: int, _signal: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(os, "kill", dead_pid)

    sweep_canary_spools(tmp_path)

    assert not terminal.exists() and not dead.exists() and not old.exists()
    assert json.loads(dead_claim.read_text())["reason"] == "orphaned"


def test_sweep_preserves_live_remote_and_recent_unclaimed_spools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = ensure_claims_dir(tmp_path)
    live = directory / "live.spool.jsonl"
    live.write_text("secret")
    _claim(directory, "live", state="pending", hostname=socket.gethostname(), pid=123)
    remote = directory / "remote.spool.jsonl"
    remote.write_text("secret")
    _claim(directory, "remote", state="pending", hostname="another-host", pid=456)
    recent = directory / "recent.spool.jsonl"
    recent.write_text("secret")
    notices: list[str] = []
    monkeypatch.setattr(os, "kill", lambda _pid, _signal: None)

    sweep_canary_spools(tmp_path, notify=notices.append)

    assert live.exists() and remote.exists() and recent.exists()
    assert any("live" in notice for notice in notices)
    assert any("another-host" in notice for notice in notices)


def test_sweep_treats_permission_error_as_a_live_pid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = ensure_claims_dir(tmp_path)
    spool = directory / "protected.spool.jsonl"
    spool.write_text("secret")
    _claim(
        directory,
        "protected",
        state="pending",
        hostname=socket.gethostname(),
        pid=321,
    )

    def protected_pid(_pid: int, _signal: int) -> None:
        raise PermissionError

    monkeypatch.setattr(os, "kill", protected_pid)
    sweep_canary_spools(tmp_path)
    assert spool.exists()


def test_sweep_ignores_an_unstatable_unclaimed_spool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = ensure_claims_dir(tmp_path)
    spool = directory / "unstatable.spool.jsonl"
    spool.write_text("secret")
    real_stat = Path.stat

    def fail_selected(path: Path, *args: Any, **kwargs: Any) -> os.stat_result:
        if path == spool:
            raise OSError("gone")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", fail_selected)
    sweep_canary_spools(tmp_path)
    assert spool in list(directory.iterdir())


def test_apply_artifact_directory_is_private_and_refuses_symlink(tmp_path: Path) -> None:
    directory = ensure_claims_dir(tmp_path)
    assert directory.stat().st_mode & 0o777 == 0o700
    other = tmp_path / "other"
    other.mkdir()
    directory.rmdir()
    directory.symlink_to(other, target_is_directory=True)
    with pytest.raises(UnsafeApplyArtifacts, match="real directory"):
        ensure_claims_dir(tmp_path)


def test_apply_artifact_directory_refuses_group_write(tmp_path: Path) -> None:
    directory = ensure_claims_dir(tmp_path)
    directory.chmod(0o720)
    with pytest.raises(UnsafeApplyArtifacts, match="group/other writable"):
        ensure_claims_dir(tmp_path)


def test_apply_artifact_directory_refuses_wrong_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = ensure_claims_dir(tmp_path)
    real_lstat = Path.lstat

    def wrong_owner(path: Path) -> os.stat_result:
        info = real_lstat(path)
        if path == directory:
            values = list(info)
            values[4] = os.getuid() + 1
            return os.stat_result(values)
        return info

    monkeypatch.setattr(Path, "lstat", wrong_owner)
    with pytest.raises(UnsafeApplyArtifacts, match="wrong owner"):
        ensure_claims_dir(tmp_path)


def test_compatibility_wrapper_refuses_a_missing_problem_and_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("drt.cli._apply_flow.compatibility_problem", lambda *_args: (None, None))
    with pytest.raises(ApplyRefused, match="incompatible"):
        _validate_canary_compatibility(object(), object())


def test_compatibility_wrapper_returns_the_verified_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("drt.cli._apply_flow.compatibility_problem", lambda *_args: (["id"], None))
    assert _validate_canary_compatibility(object(), object()) == ["id"]
