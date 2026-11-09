"""Encrypted exact-replay sidecars (#1221, ADR 0014)."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pyrage import x25519

from drt import __version__
from drt.cli import _plan_payload
from drt.cli._apply_flow import ApplyRefused, load_exact_payload
from drt.cli._plan_payload import (
    PlanPayloadError,
    _decode_value,
    decrypt_rows,
    encrypt_rows,
    resolve_recipients,
    serialize_rows,
)
from drt.engine.diff import DiffResult
from drt.engine.plan import PlanDocumentError, build_plan, load_plan_document


def _artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, Any]] | None = None,
) -> tuple[str, Path, Any]:
    if rows is None:
        rows = [{"id": 1, "email": "alice@example.com"}]
    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_PLAN_KEY", "plan-key")
    monkeypatch.setenv("DRT_SECRETS_KEY", str(identity))
    ciphertext, metadata = encrypt_rows(rows, [str(identity.to_public())])
    plan = build_plan(
        DiffResult(added=rows, total_source_rows=len(rows), total_destination_rows=0),
        sync_name="orders",
        sync_mode="upsert",
        match_policy="upsert",
        destination="postgres",
        config_fingerprint="sha256:cfg",
        environment_fingerprint="sha256:env",
        plan_key=b"plan-key",
        drt_version=__version__,
        key_columns=["id"],
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    plan.payload = metadata
    path = tmp_path / "plan.json.payload"
    path.write_bytes(ciphertext)
    return plan.to_json(), path, identity


def test_exact_payload_round_trips_only_in_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)

    loaded = load_exact_payload(plan_text, path, project_dir=tmp_path)
    assert loaded.records == [{"id": 1, "email": "alice@example.com"}]
    sidecar_stat = path.stat()
    assert loaded.identity == (sidecar_stat.st_dev, sidecar_stat.st_ino)
    assert b"alice@example.com" not in path.read_bytes()


def test_exact_payload_preserves_common_warehouse_scalar_types(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identifier = UUID("12345678-1234-5678-1234-567812345678")
    occurred_at = datetime(2026, 10, 28, 12, 34, 56, tzinfo=timezone.utc)
    rows = [
        {
            "amount": Decimal("123.4500"),
            "occurred_at": occurred_at,
            "day": date(2026, 10, 28),
            "clock": time(12, 34, 56),
            "identifier": identifier,
            "binary": b"\x00payload\xff",
            "coordinates": (35.6, 139.7),
            "collision": {"__drt_exact_replay_type__": "user-value"},
        }
    ]
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch, rows)

    assert load_exact_payload(plan_text, path, project_dir=tmp_path).records == rows


def test_payload_metadata_is_covered_by_the_plan_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, _path, _identity = _artifact(tmp_path, monkeypatch)
    document = json.loads(plan_text)
    document["payload"]["rows"] += 1

    with pytest.raises(PlanDocumentError, match="seal"):
        load_plan_document(json.dumps(document), b"plan-key")


def test_edited_and_swapped_sidecars_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, identity = _artifact(tmp_path, monkeypatch)
    original = path.read_bytes()
    path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
    with pytest.raises(ApplyRefused, match="edited or swapped|larger than"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)

    other, _metadata = encrypt_rows([{"id": 2}], [str(identity.to_public())])
    path.write_bytes(other)
    with pytest.raises(ApplyRefused, match="edited or swapped|larger than"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)


def test_wrong_key_and_missing_sidecar_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)
    monkeypatch.setenv("DRT_SECRETS_KEY", str(x25519.Identity.generate()))
    with pytest.raises(ApplyRefused, match="Check that the key matches"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)

    path.unlink()
    with pytest.raises(ApplyRefused, match="missing"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)


def test_symlink_and_oversized_sidecars_are_refused_without_unbounded_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)
    target = tmp_path / "real.payload"
    path.replace(target)
    path.symlink_to(target)
    with pytest.raises(ApplyRefused, match="not a symlink"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)


def test_symlink_is_refused_on_platforms_without_nofollow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)
    target = tmp_path / "real.payload"
    path.replace(target)
    path.symlink_to(target)
    monkeypatch.setattr(os, "O_NOFOLLOW", 0)

    with pytest.raises(ApplyRefused, match="not a symlink"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)

    path.unlink()
    target.replace(path)
    path.write_bytes(path.read_bytes() + b"extra bytes that must not be drained")
    with pytest.raises(ApplyRefused, match="larger than the byte count"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)


def test_non_regular_sidecar_is_refused_after_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)
    real_fstat = os.fstat

    class _NotRegular:
        st_mode = stat.S_IFSOCK

    monkeypatch.setattr(os, "fstat", lambda _fd: _NotRegular())
    with pytest.raises(ApplyRefused, match="must be a regular file"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)
    monkeypatch.setattr(os, "fstat", real_fstat)


def test_sidecar_open_and_read_errors_are_clean_refusals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)
    real_open = os.open

    def denied_open(target: os.PathLike[str] | str, flags: int, *args: Any) -> int:
        if Path(target) == path:
            raise PermissionError("denied")
        return real_open(target, flags, *args)

    monkeypatch.setattr(os, "open", denied_open)
    with pytest.raises(ApplyRefused, match="Cannot read exact replay sidecar"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)
    monkeypatch.setattr(os, "open", real_open)

    real_fdopen = os.fdopen

    class _BrokenReader:
        def __init__(self, fd: int) -> None:
            self.fd = fd

        def __enter__(self) -> _BrokenReader:
            return self

        def __exit__(self, *_args: Any) -> None:
            os.close(self.fd)

        def fileno(self) -> int:
            return self.fd

        def read(self, _size: int) -> bytes:
            raise OSError("read failed")

    monkeypatch.setattr(os, "fdopen", lambda fd, *_a, **_k: _BrokenReader(fd))
    with pytest.raises(ApplyRefused, match="Cannot read exact replay sidecar"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)
    monkeypatch.setattr(os, "fdopen", real_fdopen)


def test_exact_payload_uses_the_plan_max_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)

    with pytest.raises(ApplyRefused, match="--max-age"):
        load_exact_payload(plan_text, path, project_dir=tmp_path, max_age=timedelta(0))


def test_recipient_resolution_and_payload_serialization_error_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DRT_PLAN_RECIPIENTS", " age1first,age1second\nage1third ")
    assert resolve_recipients(None) == ["age1first", "age1second", "age1third"]
    assert resolve_recipients([" age1cli ", ""]) == ["age1cli"]

    collision = {"__drt_exact_replay_type__": "user value"}
    rows = [
        {
            "collision": collision,
            "date": date(2026, 10, 29),
            "time": time(12, 34, 56),
            "bytearray": bytearray(b"a"),
            "memoryview": memoryview(b"b"),
        }
    ]
    assert json.loads(serialize_rows(rows))["schema_version"] == 1

    with pytest.raises(PlanPayloadError, match="cannot serialize"):
        serialize_rows([{"bad": object()}])
    with pytest.raises(PlanPayloadError, match="at least one"):
        encrypt_rows([], [])
    monkeypatch.setattr(_plan_payload, "MAX_PAYLOAD_BYTES", 1)
    identity = x25519.Identity.generate()
    with pytest.raises(PlanPayloadError, match="byte cap"):
        encrypt_rows([{"id": 1}], [str(identity.to_public())])


@pytest.mark.parametrize(
    "encoded",
    [
        {"__drt_exact_replay_type__": "tuple", "value": [], "extra": True},
        {"__drt_exact_replay_type__": "datetime", "value": "not-a-date"},
        {"__drt_exact_replay_type__": "unknown", "value": "x"},
    ],
)
def test_malformed_typed_payload_values_are_refused(encoded: Any) -> None:
    with pytest.raises(PlanPayloadError, match="malformed|unsupported"):
        _decode_value(encoded)


@pytest.mark.parametrize(
    ("plaintext", "rows", "message"),
    [
        (b"not-json", 0, "not valid JSON"),
        (b'{"schema_version":99,"rows":[]}', 0, "unsupported schema"),
        (b'{"schema_version":1,"rows":{}}', 0, "malformed rows"),
        (b'{"schema_version":1,"rows":[]}', 1, "row count"),
    ],
)
def test_decrypted_payload_document_validation(
    monkeypatch: pytest.MonkeyPatch, plaintext: bytes, rows: int, message: str
) -> None:
    import pyrage

    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_SECRETS_KEY", str(identity))
    ciphertext = pyrage.encrypt(plaintext, [identity.to_public()])
    expected = {
        "sha256": "sha256:" + hashlib.sha256(ciphertext).hexdigest(),
        "bytes": len(ciphertext),
        "rows": rows,
    }

    with pytest.raises(PlanPayloadError, match=message):
        decrypt_rows(ciphertext, expected)


def test_decrypt_rejects_authenticated_byte_count_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_SECRETS_KEY", str(identity))
    ciphertext, expected = encrypt_rows([], [str(identity.to_public())])
    expected["bytes"] += 1

    with pytest.raises(PlanPayloadError, match="byte count"):
        decrypt_rows(ciphertext, expected)
