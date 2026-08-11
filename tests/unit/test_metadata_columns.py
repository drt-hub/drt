"""Unit tests for opt-in engine metadata columns (#762)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from drt.config.credentials import BigQueryProfile
from drt.config.models import DestinationConfig, MetadataColumnsConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.engine.metadata_columns import apply_metadata_columns


def _rows() -> list[dict[str, Any]]:
    return [{"id": 1}, {"id": 2}]


class TestApplyMetadataColumns:
    def test_none_config_is_noop_returns_same_list(self) -> None:
        rows = _rows()
        out = apply_metadata_columns(
            rows, None, synced_at="2026-08-11T00:00:00Z", run_id="r1", sync_name="s"
        )
        assert out is rows
        assert out == [{"id": 1}, {"id": 2}]

    def test_all_fields_unset_is_noop(self) -> None:
        rows = _rows()
        out = apply_metadata_columns(
            rows,
            MetadataColumnsConfig(),
            synced_at="2026-08-11T00:00:00Z",
            run_id="r1",
            sync_name="s",
        )
        assert out == [{"id": 1}, {"id": 2}]

    def test_adds_only_the_configured_columns(self) -> None:
        out = apply_metadata_columns(
            _rows(),
            MetadataColumnsConfig(synced_at="_drt_synced_at"),
            synced_at="2026-08-11T00:00:00Z",
            run_id="r1",
            sync_name="s",
        )
        assert out == [
            {"id": 1, "_drt_synced_at": "2026-08-11T00:00:00Z"},
            {"id": 2, "_drt_synced_at": "2026-08-11T00:00:00Z"},
        ]

    def test_adds_all_three_when_configured(self) -> None:
        out = apply_metadata_columns(
            _rows(),
            MetadataColumnsConfig(
                synced_at="_drt_synced_at", run_id="_drt_run_id", sync_name="_drt_sync_name"
            ),
            synced_at="2026-08-11T00:00:00Z",
            run_id="r1",
            sync_name="post_users",
        )
        assert out[0] == {
            "id": 1,
            "_drt_synced_at": "2026-08-11T00:00:00Z",
            "_drt_run_id": "r1",
            "_drt_sync_name": "post_users",
        }

    def test_run_id_none_writes_null(self) -> None:
        """Library callers that don't pass run_id to run_sync() get None —
        same nullability as SyncResult.run_id — not a synthetic fallback."""
        out = apply_metadata_columns(
            _rows(),
            MetadataColumnsConfig(run_id="_drt_run_id"),
            synced_at="2026-08-11T00:00:00Z",
            run_id=None,
            sync_name="s",
        )
        assert out[0]["_drt_run_id"] is None

    def test_same_value_across_every_record_in_the_run(self) -> None:
        """One synced_at/run_id per run_sync() call, not per-record."""
        out = apply_metadata_columns(
            _rows(),
            MetadataColumnsConfig(synced_at="_drt_synced_at"),
            synced_at="2026-08-11T00:00:00Z",
            run_id="r1",
            sync_name="s",
        )
        assert {r["_drt_synced_at"] for r in out} == {"2026-08-11T00:00:00Z"}

    def test_empty_records_list(self) -> None:
        out = apply_metadata_columns(
            [],
            MetadataColumnsConfig(synced_at="_drt_synced_at"),
            synced_at="2026-08-11T00:00:00Z",
            run_id="r1",
            sync_name="s",
        )
        assert out == []

    def test_can_overwrite_an_existing_column_name(self) -> None:
        """Same in-place-overwrite allowance computed_fields/field_mappings
        give — metadata_columns is not special-cased to forbid collisions
        with source columns, only with each other (config-time check)."""
        out = apply_metadata_columns(
            [{"id": 1, "_drt_synced_at": "stale"}],
            MetadataColumnsConfig(synced_at="_drt_synced_at"),
            synced_at="2026-08-11T00:00:00Z",
            run_id="r1",
            sync_name="s",
        )
        assert out[0]["_drt_synced_at"] == "2026-08-11T00:00:00Z"


class TestMetadataColumnsConfigValidation:
    def test_default_is_all_none(self) -> None:
        cfg = MetadataColumnsConfig()
        assert cfg.synced_at is None
        assert cfg.run_id is None
        assert cfg.sync_name is None

    def test_rejects_duplicate_target_column_names(self) -> None:
        with pytest.raises(ValidationError, match="distinct column names"):
            MetadataColumnsConfig(synced_at="_drt_meta", run_id="_drt_meta")

    @pytest.mark.parametrize("blank", ["", "   "])
    def test_rejects_blank_column_name(self, blank: str) -> None:
        """An empty/whitespace name (e.g. from a ${VAR} that resolved empty)
        must fail config validation, not silently disable the column — the
        engine's own truthy check in apply_metadata_columns would otherwise
        treat it as "not configured" and skip it without a warning."""
        with pytest.raises(ValidationError, match="non-empty column name"):
            MetadataColumnsConfig(synced_at=blank)

    def test_distinct_names_are_fine(self) -> None:
        cfg = MetadataColumnsConfig(synced_at="_drt_synced_at", run_id="_drt_run_id")
        assert cfg.synced_at == "_drt_synced_at"
        assert cfg.run_id == "_drt_run_id"

    def test_sync_options_default_is_none(self) -> None:
        opts = SyncOptions.model_validate(
            {"mode": "full"},
        )
        assert opts.metadata_columns is None


class TestEndToEndThroughRunSync:
    """metadata_columns wired into run_sync(), verified after mask/field_mappings.

    Mirrors TestPipelineOrder in test_computed_fields.py — same run_sync()
    harness, extended to prove metadata_columns lands last: after mask (so
    the injected column is never itself masked) and untouched by
    field_mappings (its column name is already destination-facing).
    """

    def _run(
        self, sync_options: dict[str, Any], row: dict[str, Any], **run_sync_kwargs: Any
    ) -> list[dict[str, Any]]:
        from drt.engine.sync import run_sync

        captured: list[dict[str, Any]] = []

        class FakeSource:
            def extract(
                self, query: str, config: object, *, query_tags: dict[str, str] | None = None
            ) -> Iterator[dict]:
                yield dict(row)

            def test_connection(self, config: object) -> bool:
                return True

        class CapturingDestination:
            def load(
                self,
                records: list[dict],
                config: DestinationConfig,
                sync_options: SyncOptions,
            ) -> SyncResult:
                captured.extend(records)
                result = SyncResult()
                result.success = len(records)
                return result

        sync = SyncConfig.model_validate(
            {
                "name": "metadata_demo",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"batch_size": 10, **sync_options},
            }
        )
        run_sync(
            sync,
            FakeSource(),
            CapturingDestination(),
            BigQueryProfile(type="bigquery", project="p", dataset="d"),
            Path("."),
            **run_sync_kwargs,
        )
        return captured

    def test_survives_mask_untouched(self) -> None:
        """A mask rule on a source field must not reach the metadata column
        added after it, even though both are simple string values."""
        loaded = self._run(
            {
                "mask": {"email": "redact"},
                "metadata_columns": {"synced_at": "_drt_synced_at"},
            },
            {"email": "ada@example.com"},
        )
        assert loaded[0]["email"] == "[REDACTED]"
        assert loaded[0]["_drt_synced_at"] != "[REDACTED]"

    def test_column_name_is_not_renamed_by_field_mappings(self) -> None:
        loaded = self._run(
            {
                "field_mappings": {"email": "contact_email"},
                "metadata_columns": {"run_id": "_drt_run_id"},
            },
            {"email": "ada@example.com"},
            run_id="run-abc",
        )
        assert loaded[0]["contact_email"] == "ada@example.com"
        assert loaded[0]["_drt_run_id"] == "run-abc"

    def test_run_id_flows_from_run_sync_caller(self) -> None:
        loaded = self._run(
            {"metadata_columns": {"run_id": "_drt_run_id"}},
            {"id": 1},
            run_id="run-xyz",
        )
        assert loaded[0]["_drt_run_id"] == "run-xyz"

    def test_sync_name_is_the_sync_config_name(self) -> None:
        loaded = self._run(
            {"metadata_columns": {"sync_name": "_drt_sync_name"}},
            {"id": 1},
        )
        assert loaded[0]["_drt_sync_name"] == "metadata_demo"

    def test_all_records_in_a_run_share_the_same_synced_at(self) -> None:
        from drt.engine.sync import run_sync

        captured: list[dict[str, Any]] = []

        class FakeSource:
            def extract(
                self, query: str, config: object, *, query_tags: dict[str, str] | None = None
            ) -> Iterator[dict]:
                yield {"id": 1}
                yield {"id": 2}
                yield {"id": 3}

            def test_connection(self, config: object) -> bool:
                return True

        class CapturingDestination:
            def load(
                self, records: list[dict], config: DestinationConfig, sync_options: SyncOptions
            ) -> SyncResult:
                captured.extend(records)
                result = SyncResult()
                result.success = len(records)
                return result

        sync = SyncConfig.model_validate(
            {
                "name": "multi_batch",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {
                    "batch_size": 1,
                    "metadata_columns": {"synced_at": "_drt_synced_at"},
                },
            }
        )
        run_sync(
            sync,
            FakeSource(),
            CapturingDestination(),
            BigQueryProfile(type="bigquery", project="p", dataset="d"),
            Path("."),
        )
        assert len({r["_drt_synced_at"] for r in captured}) == 1

    def test_disabled_by_default_no_columns_added(self) -> None:
        loaded = self._run({}, {"id": 1})
        assert loaded[0] == {"id": 1}
