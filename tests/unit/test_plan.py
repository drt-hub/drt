"""Unit tests for the reviewable change-set artifact (#1216)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from drt.engine.diff import DiffResult
from drt.engine.plan import (
    PLAN_JSON_SCHEMA,
    PLAN_SCHEMA_VERSION,
    build_plan,
    config_hash,
    render_markdown,
    render_text,
    unsupported_reason,
)

_KEY = b"test-key"
_BASE: dict[str, Any] = {
    "sync_name": "orders_to_pg",
    "sync_mode": "upsert",
    "match_policy": "upsert",
    "destination": "postgres",
    "config_fingerprint": "sha256:cfg",
    "environment_fingerprint": "sha256:env",
    "plan_key": b"test-key",
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
    volatile = {"created_at", "plan_id", "seal"}  # unique per artifact, not per content
    strip = lambda p: {k: v for k, v in p.to_dict().items() if k not in volatile}  # noqa: E731
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
    assert key["email"].startswith("hmac-sha256:")
    assert "a@example.com" not in masked.to_json()

    redacted = build_plan(diff, **_BASE, redact_keys=True)
    assert redacted.entries[0].key["id"].startswith("hmac-sha256:")


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


def test_cursor_is_hashed_never_written_raw() -> None:
    plan = build_plan(_diff(), **_BASE, cursor_value="alice@example.com")

    assert "alice@example.com" not in plan.to_json()
    assert plan.to_dict()["fingerprints"]["cursor_hash"].startswith("hmac-sha256:")
    other = build_plan(_diff(), **_BASE, cursor_value="bob@example.com")
    assert plan.plan_id != other.plan_id


def test_duplicate_keys_sort_deterministically() -> None:
    a = ({"id": 2, "x": 1, "y": 1}, {"id": 2, "x": 2, "y": 1})
    b = ({"id": 2, "x": 1, "y": 1}, {"id": 2, "x": 1, "y": 2})

    forward = build_plan(_diff(added=[], deleted=[], updated=[a, b]), **_BASE)
    backward = build_plan(_diff(added=[], deleted=[], updated=[b, a]), **_BASE)

    assert forward.digest == backward.digest


class _Opts:
    def __init__(self, **kw: Any) -> None:
        self.mode = kw.get("mode", "upsert")
        self.match_policy = kw.get("match_policy", "upsert")
        self.incremental_strategy = kw.get("incremental_strategy", "cursor")
        self.metadata_columns = kw.get("metadata_columns")


class _Sync:
    def __init__(self, upsert_key: list[str] | None = None, **kw: Any) -> None:
        self.sync = _Opts(**kw)
        self.destination = type("D", (), {"upsert_key": upsert_key or ["id"]})()


def test_unsupported_reasons_are_reported_before_anything_runs() -> None:
    assert unsupported_reason(_Sync()) is None
    assert "match_policy" in (unsupported_reason(_Sync(match_policy="update_only")) or "")
    assert "incremental_strategy" in (
        unsupported_reason(_Sync(mode="incremental", incremental_strategy="diff")) or ""
    )
    meta = type("M", (), {"synced_at": "synced_at", "run_id": None, "sync_name": None})()
    assert "metadata column" in (
        unsupported_reason(_Sync(["id", "synced_at"], metadata_columns=meta)) or ""
    )
    assert unsupported_reason(_Sync(["id"], metadata_columns=meta)) is None


def test_plan_conforms_to_its_published_json_schema() -> None:
    import jsonschema

    validator = jsonschema.Draft202012Validator(PLAN_JSON_SCHEMA)
    for diff in (_diff(), DiffResult(total_source_rows=0), DiffResult(supported=False)):
        validator.validate(build_plan(diff, **_BASE).to_dict())


def test_markdown_lists_actions() -> None:
    diff = _diff(added=[{"id": 7}], updated=[], deleted=[])
    text = render_markdown(build_plan(diff, **_BASE))

    assert "| create | 1 |" in text
    assert "| create | `id=7` | - |" in text


@pytest.mark.parametrize(
    "value",
    ["a|b", "a`b", "``", "safe\r\r### injected\r", "line1\nline2", "<img src=x>", "[x](http://e)"],
)
def test_markdown_keys_cannot_break_out_of_their_cell(value: str) -> None:
    diff = _diff(added=[{"id": value}], updated=[], deleted=[])
    lines = render_markdown(build_plan(diff, **_BASE)).splitlines()

    rows = [ln for ln in lines if ln.startswith("| create |") and "`" in ln]
    assert len(rows) == 1  # one physical row: no raw CR/LF escaped the key
    assert not any(ln.startswith("###") and "injected" in ln for ln in lines)
    assert "\r" not in "".join(rows) and "\n" not in "".join(rows)


def test_markdown_distinct_keys_render_distinctly() -> None:
    a = render_markdown(build_plan(_diff(added=[{"id": "a`b"}], updated=[], deleted=[]), **_BASE))
    b = render_markdown(build_plan(_diff(added=[{"id": "a'b"}], updated=[], deleted=[]), **_BASE))

    assert a != b


def test_markdown_shortens_very_long_values_and_untrusted_names() -> None:
    huge = "x" * 1_000_000
    diff = _diff(added=[{"id": huge}], updated=[], deleted=[])
    plan = build_plan(
        diff,
        **{**_BASE, "sync_name": "n`<script>"},
    )
    text = render_markdown(plan)

    assert len(text) < 2_000
    assert "(+" in text and "chars)" in text
    assert "<script>" not in text.replace("`<script>`", "")  # only ever inside a code span


def test_markdown_truncates_long_plans_and_reports_unavailable() -> None:
    many = _diff(added=[{"id": i} for i in range(60)], updated=[], deleted=[])
    assert "10 more entries not shown" in render_markdown(build_plan(many, **_BASE))
    unavailable = build_plan(DiffResult(supported=False, fallback_reason="why"), **_BASE)
    assert "Plan unavailable" in render_markdown(unavailable)


def test_published_schema_file_matches_the_code() -> None:
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "docs" / "schemas" / "plan.schema.json"
    assert json.loads(path.read_text()) == json.loads(json.dumps(PLAN_JSON_SCHEMA))


def test_markdown_for_an_empty_plan() -> None:
    plan = build_plan(DiffResult(total_source_rows=1, total_destination_rows=1), **_BASE)

    assert "No changes." in render_markdown(plan)


def test_plan_document_round_trips_and_detects_tampering() -> None:
    from drt.engine.plan import PlanDocumentError, load_plan_document

    plan = build_plan(_diff(), **_BASE)
    doc = load_plan_document(plan.to_json(), _KEY)
    assert doc["plan_id"] == plan.plan_id

    tampered = json.loads(plan.to_json())
    tampered["entries"].pop()
    with pytest.raises(PlanDocumentError, match="modified"):
        load_plan_document(json.dumps(tampered), _KEY)

    # Envelope fields are sealed too: bumping created_at cannot skip --max-age.
    envelope = json.loads(plan.to_json())
    envelope["created_at"] = "2030-01-01T00:00:00+00:00"
    with pytest.raises(PlanDocumentError, match="modified"):
        load_plan_document(json.dumps(envelope), _KEY)
    version = json.loads(plan.to_json())
    version["drt_version"] = "9.9.9"
    with pytest.raises(PlanDocumentError, match="modified"):
        load_plan_document(json.dumps(version), _KEY)

    for text in ("not json", "[]", "{}"):
        with pytest.raises(PlanDocumentError):
            load_plan_document(text, _KEY)

    wrong_version = json.loads(plan.to_json())
    wrong_version["schema_version"] = 99
    with pytest.raises(PlanDocumentError, match="schema_version"):
        load_plan_document(json.dumps(wrong_version), _KEY)

    unavailable = build_plan(DiffResult(supported=False, fallback_reason="x"), **_BASE)
    with pytest.raises(PlanDocumentError, match="unavailable"):
        load_plan_document(unavailable.to_json(), _KEY)


def test_plan_id_is_unique_per_artifact_but_the_digest_is_deterministic() -> None:
    first = build_plan(_diff(), **_BASE)
    later = build_plan(_diff(), **{**_BASE, "created_at": "2026-10-10T00:00:00+00:00"})

    assert first.digest == later.digest
    assert first.plan_id != later.plan_id


def test_value_change_is_drift_even_when_key_action_and_columns_match() -> None:
    def plan_with(email: str) -> Any:
        diff = _diff(
            added=[],
            deleted=[],
            updated=[({"id": 2, "email": "old"}, {"id": 2, "email": email})],
        )
        return build_plan(diff, **_BASE)

    assert plan_with("b").digest != plan_with("c").digest
    assert "value_hash" in plan_with("b").to_dict()["entries"][0]
    assert '"b"' not in plan_with("b").to_json()  # the value itself is never written


def test_engine_written_columns_are_not_part_of_the_value_hash() -> None:
    def plan_with(run_id: str) -> Any:
        diff = _diff(added=[{"id": 1, "name": "a", "run": run_id}], updated=[], deleted=[])
        return build_plan(diff, **_BASE, exclude_columns={"run"})

    assert plan_with("r1").digest == plan_with("r2").digest


def test_environment_fingerprint_is_part_of_the_digest() -> None:
    a = build_plan(_diff(), **_BASE)
    b = build_plan(_diff(), **{**_BASE, "environment_fingerprint": "sha256:other"})

    assert a.digest != b.digest


def test_drift_report_counts_duplicates() -> None:
    from drt.engine.plan import drift_report

    entry = {"key": {"id": 1}, "action": "create"}
    report = drift_report([entry, entry], [entry])

    assert report["drifted"] == 1
    assert len(report["removed"]) == 1
    assert report["added"] == []
    assert drift_report([entry], [entry])["drifted"] == 0
    changed = drift_report([entry], [{**entry, "action": "update"}])
    assert changed["drifted"] == 1 and len(changed["added"]) == 1


def test_a_resealed_forgery_still_fails_the_digest_and_plan_id_checks() -> None:
    from drt.engine.plan import PlanDocumentError, load_plan_document, seal_of

    plan = build_plan(_diff(), **_BASE)

    def forged(edit: Any) -> str:
        doc = json.loads(plan.to_json())
        edit(doc)
        doc["seal"] = seal_of(doc, _KEY)
        return json.dumps(doc)

    with pytest.raises(PlanDocumentError, match="digest"):
        load_plan_document(forged(lambda d: d["entries"].pop()), _KEY)
    with pytest.raises(PlanDocumentError, match="plan_id"):
        load_plan_document(forged(lambda d: d.update(plan_id="plan-0000000000000000")), _KEY)


def test_hashes_are_keyed_so_the_artifact_alone_cannot_confirm_a_guess() -> None:
    import hashlib

    def plan_with(key: bytes) -> Any:
        diff = _diff(added=[{"id": 1, "email": "a@example.com"}], updated=[], deleted=[])
        return build_plan(diff, **{**_BASE, "plan_key": key}, cursor_value="alice@example.com")

    a, b = plan_with(b"key-one"), plan_with(b"key-two")
    assert a.digest != b.digest
    assert a.to_dict()["options"]["key_id"] != b.to_dict()["options"]["key_id"]
    # A plain SHA-256 of a guessed value (what a public salt would allow) matches nothing.
    guess = hashlib.sha256(b'"alice@example.com"').hexdigest()
    assert guess[:16] not in a.to_json() and guess[:32] not in a.to_json()
    assert b"key-one" not in a.to_json().encode()


def test_duplicate_keys_with_different_values_sort_by_value_not_source_order() -> None:
    first = {"id": 5, "name": "x"}
    second = {"id": 5, "name": "y"}

    forward = build_plan(_diff(added=[first, second], updated=[], deleted=[]), **_BASE)
    backward = build_plan(_diff(added=[second, first], updated=[], deleted=[]), **_BASE)

    assert forward.digest == backward.digest
