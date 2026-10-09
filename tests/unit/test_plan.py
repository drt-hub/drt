"""Unit tests for the reviewable change-set artifact (#1216)."""

from __future__ import annotations

import json
from typing import Any

from drt.engine.diff import DiffResult
from drt.engine.plan import PLAN_SCHEMA_VERSION, build_plan, config_hash, render_text

_BASE: dict[str, Any] = {
    "sync_name": "orders_to_pg",
    "sync_mode": "upsert",
    "match_policy": "upsert",
    "destination": "postgres",
    "config_fingerprint": "sha256:cfg",
    "drt_version": "1.1.0",
    "key_columns": ["id"],
    "created_at": "2026-10-09T00:00:00+00:00",
}


def _diff(**overrides: Any) -> DiffResult:
    diff = DiffResult(
        added=[{"id": 3, "email": "c@example.com"}],
        updated=[({"id": 2, "email": "old@example.com"}, {"id": 2, "email": "new@example.com"})],
        deleted=[{"id": 9}],
        total_source_rows=3,
        total_destination_rows=5,
        delete_reason="mirror",
    )
    for name, value in overrides.items():
        setattr(diff, name, value)
    return diff


def test_entries_cover_every_action_without_row_values() -> None:
    plan = build_plan(_diff(), **_BASE)
    data = plan.to_dict()

    assert data["schema_version"] == PLAN_SCHEMA_VERSION
    assert data["summary"]["create"] == 1
    assert data["summary"]["update"] == 1
    assert data["summary"]["delete"] == 1
    assert [e["action"] for e in data["entries"]] == ["create", "update", "delete"]
    assert data["entries"][1]["changed_columns"] == ["email"]
    assert data["entries"][2]["delete_reason"] == "mirror"
    # No row values anywhere: only keys and column names.
    assert "new@example.com" not in plan.to_json()
    assert "old@example.com" not in plan.to_json()


def test_append_only_and_replace_actions() -> None:
    diff = DiffResult(
        inserted=[{"id": 1}],
        replaced=[({"id": 2, "a": 1, "b": 2}, {"id": 2, "a": 5})],
        writes_full_row=True,
    )
    plan = build_plan(diff, **_BASE)

    assert [e.action for e in plan.entries] == ["insert", "replace"]
    assert plan.entries[1].changed_columns == ["a", "b"]


def test_same_inputs_produce_identical_output_apart_from_created_at() -> None:
    shuffled = _diff(added=[{"id": 4}, {"id": 3}], deleted=[{"id": 11}, {"id": 9}])
    ordered = _diff(added=[{"id": 3}, {"id": 4}], deleted=[{"id": 9}, {"id": 11}])

    a = build_plan(shuffled, **_BASE)
    b = build_plan(ordered, **{**_BASE, "created_at": "2030-01-01T00:00:00+00:00"})

    assert a.digest == b.digest
    assert a.plan_id == b.plan_id
    strip = lambda p: {k: v for k, v in p.to_dict().items() if k != "created_at"}  # noqa: E731
    assert json.dumps(strip(a), sort_keys=True) == json.dumps(strip(b), sort_keys=True)


def test_digest_changes_with_the_action_set_and_with_config() -> None:
    base = build_plan(_diff(), **_BASE)
    fewer = build_plan(_diff(deleted=[]), **_BASE)
    other_cfg = build_plan(_diff(), **{**_BASE, "config_fingerprint": "sha256:other"})

    assert base.digest != fewer.digest
    assert base.digest != other_cfg.digest


def test_masked_and_redacted_keys_are_hashed() -> None:
    diff = _diff(added=[{"id": 1, "email": "a@example.com"}])
    masked = build_plan(diff, **{**_BASE, "key_columns": ["id", "email"]}, mask_columns={"email"})
    key = masked.entries[0].key
    assert key["id"] == 1
    assert key["email"].startswith("sha256:")
    assert "a@example.com" not in masked.to_json()

    redacted = build_plan(diff, **_BASE, redact_keys=True)
    assert redacted.entries[0].key["id"].startswith("sha256:")


def test_unsupported_destination_is_unavailable_with_reason() -> None:
    diff = DiffResult(
        sample=[{"id": 1}], supported=False, fallback_reason="rest_api: no comparison available"
    )
    plan = build_plan(diff, **_BASE)

    assert plan.available is False
    assert plan.entries == []
    assert plan.unavailable_reason == "rest_api: no comparison available"
    assert "unavailable" in render_text(plan)


def test_unknown_delete_set_never_yields_a_partial_plan() -> None:
    plan = build_plan(_diff(delete_preview_unavailable_reason="permission denied"), **_BASE)

    assert plan.available is False
    assert plan.entries == []
    assert "permission denied" in (plan.unavailable_reason or "")


def test_truncated_diff_and_missing_key_are_unavailable() -> None:
    assert build_plan(_diff(truncated=True), **_BASE).available is False
    assert build_plan(_diff(), **{**_BASE, "key_columns": []}).available is False


def test_no_changes_plan_is_available_and_empty() -> None:
    plan = build_plan(DiffResult(total_source_rows=2, total_destination_rows=2), **_BASE)

    assert plan.available is True
    assert plan.has_changes is False
    assert "No changes." in render_text(plan)


def test_config_hash_tracks_sync_definition() -> None:
    class _Cfg:
        def __init__(self, value: int) -> None:
            self.value = value

        def model_dump(self, mode: str = "python") -> dict[str, int]:
            return {"batch_size": self.value}

    assert config_hash(_Cfg(1)) == config_hash(_Cfg(1))
    assert config_hash(_Cfg(1)) != config_hash(_Cfg(2))
