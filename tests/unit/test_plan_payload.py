"""Encrypted exact-replay sidecars (#1221, ADR 0014)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pyrage import x25519

from drt import __version__
from drt.cli._apply_flow import ApplyRefused, load_exact_payload
from drt.cli._plan_payload import encrypt_rows
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

    assert load_exact_payload(plan_text, path, project_dir=tmp_path) == [
        {"id": 1, "email": "alice@example.com"}
    ]
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
            "identifier": identifier,
            "binary": b"\x00payload\xff",
            "coordinates": (35.6, 139.7),
        }
    ]
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch, rows)

    assert load_exact_payload(plan_text, path, project_dir=tmp_path) == rows


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
    with pytest.raises(ApplyRefused, match="edited or swapped"):
        load_exact_payload(plan_text, path, project_dir=tmp_path)

    other, _metadata = encrypt_rows([{"id": 2}], [str(identity.to_public())])
    path.write_bytes(other)
    with pytest.raises(ApplyRefused, match="edited or swapped"):
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


def test_exact_payload_uses_the_plan_max_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_text, path, _identity = _artifact(tmp_path, monkeypatch)

    with pytest.raises(ApplyRefused, match="--max-age"):
        load_exact_payload(plan_text, path, project_dir=tmp_path, max_age=timedelta(0))
