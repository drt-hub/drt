"""Unit tests for the opt-in telemetry module."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer

from drt import telemetry


@pytest.fixture(autouse=True)
def _isolate_user_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect ~/.drt to a temp dir; clear cached config; clear env."""
    user_dir = tmp_path / ".drt"
    monkeypatch.setattr(telemetry, "_user_dir", lambda: user_dir)
    telemetry._load_config_cached.cache_clear()
    for var in ("DO_NOT_TRACK", "DRT_TELEMETRY", "DRT_TELEMETRY_ENDPOINT", "DRT_TELEMETRY_API_KEY"):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# is_enabled() decision tree
# ---------------------------------------------------------------------------


class TestIsEnabled:
    def test_default_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        assert telemetry.is_enabled() is False

    def test_no_api_key_short_circuits_to_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY", "1")
        assert telemetry.is_enabled() is False

    def test_env_var_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        monkeypatch.setenv("DRT_TELEMETRY", "1")
        assert telemetry.is_enabled() is True

    def test_env_var_explicit_disable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        monkeypatch.setenv("DRT_TELEMETRY", "0")
        telemetry.set_enabled(True)
        assert telemetry.is_enabled() is False

    def test_config_file_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        telemetry.set_enabled(True)
        assert telemetry.is_enabled() is True

    def test_do_not_track_overrides_everything(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        monkeypatch.setenv("DRT_TELEMETRY", "1")
        telemetry.set_enabled(True)
        monkeypatch.setenv("DO_NOT_TRACK", "1")
        assert telemetry.is_enabled() is False

    def test_set_enabled_invalidates_cache(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        assert telemetry.is_enabled() is False
        telemetry.set_enabled(True)
        assert telemetry.is_enabled() is True


# ---------------------------------------------------------------------------
# anonymous_id stability
# ---------------------------------------------------------------------------


class TestAnonymousId:
    def test_generated_on_first_call(self) -> None:
        assert not telemetry._anon_id_path().exists()
        first = telemetry.get_anonymous_id()
        assert telemetry._anon_id_path().exists()
        assert len(first) == 36

    def test_stable_across_calls(self) -> None:
        first = telemetry.get_anonymous_id()
        second = telemetry.get_anonymous_id()
        assert first == second


# ---------------------------------------------------------------------------
# Payload allow-list — privacy-critical
# ---------------------------------------------------------------------------


class TestPayload:
    def test_allow_list_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        payload = telemetry.build_sync_completed_payload(
            distinct_id="fixed-id",
            sync_mode="incremental",
            source_type="bigquery",
            destination_type="slack",
            rows_synced=42,
            duration_seconds=1.5,
            status="success",
        )
        assert set(payload.keys()) == {"api_key", "event", "distinct_id", "properties", "timestamp"}
        assert set(payload["properties"].keys()) == {
            "drt_version",
            "python_version",
            "os",
            "source_type",
            "destination_type",
            "sync_mode",
            "rows_synced",
            "duration_seconds",
            "status",
        }
        assert payload["distinct_id"] == "fixed-id"

    def test_event_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        payload = telemetry.build_sync_completed_payload(
            distinct_id="fixed-id",
            sync_mode="full",
            source_type="duckdb",
            destination_type="rest_api",
            rows_synced=0,
            duration_seconds=0.0,
            status="success",
        )
        assert payload["event"] == "sync_completed"
        assert payload["api_key"] == "phc_test"

    def test_build_is_pure_no_disk_write(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """build_sync_completed_payload must not generate ~/.drt/.anonymous_id."""
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        assert not telemetry._anon_id_path().exists()
        telemetry.build_sync_completed_payload(
            distinct_id="placeholder",
            sync_mode="full",
            source_type="duckdb",
            destination_type="rest_api",
            rows_synced=1,
            duration_seconds=0.1,
            status="success",
        )
        assert not telemetry._anon_id_path().exists()
        assert not telemetry._config_path().exists()

    def test_status_partial_round_trips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        payload = telemetry.build_sync_completed_payload(
            distinct_id="fixed-id",
            sync_mode="full",
            source_type="duckdb",
            destination_type="rest_api",
            rows_synced=2,
            duration_seconds=0.1,
            status="partial",
        )
        assert payload["properties"]["status"] == "partial"

    def test_no_pii_field_names_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Defensive guard: if a future change adds a PII-shaped field, fail loudly."""
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        payload = telemetry.build_sync_completed_payload(
            distinct_id="fixed-id",
            sync_mode="full",
            source_type="duckdb",
            destination_type="rest_api",
            rows_synced=1,
            duration_seconds=0.1,
            status="success",
        )
        forbidden = {
            "sync_name", "name", "query", "sql",
            "url", "endpoint", "destination_url",
            "credentials", "password", "token", "api_secret",
            "hostname", "username", "ip",
            "path", "project_path", "project_dir",
            "table", "column", "schema",
        }
        all_keys = set(payload.keys()) | set(payload.get("properties", {}).keys())
        leaks = all_keys & forbidden
        assert not leaks, f"PII-shaped fields leaked into telemetry payload: {leaks}"


# ---------------------------------------------------------------------------
# track_sync_completed — end-to-end via httpserver
# ---------------------------------------------------------------------------


class TestTrack:
    def test_disabled_by_default_no_post(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[bytes] = []
        httpserver.expect_request("/").respond_with_handler(
            lambda req: (calls.append(req.data), _ok())[1]  # type: ignore[no-any-return,no-untyped-call]
        )
        monkeypatch.setenv("DRT_TELEMETRY_ENDPOINT", httpserver.url_for("/"))
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        # Telemetry preference unset → default off
        telemetry.track_sync_completed(
            sync_mode="full",
            source_type="duckdb",
            destination_type="rest_api",
            rows_synced=1,
            duration_seconds=0.1,
            status="success",
        )
        time.sleep(0.2)
        assert calls == []

    def test_enabled_posts_payload(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[dict[str, object]] = []

        def handler(req):  # type: ignore[no-untyped-def]
            captured.append(json.loads(req.data.decode()))
            return _ok()

        httpserver.expect_request("/").respond_with_handler(handler)
        monkeypatch.setenv("DRT_TELEMETRY_ENDPOINT", httpserver.url_for("/"))
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        monkeypatch.setenv("DRT_TELEMETRY", "1")

        telemetry.track_sync_completed(
            sync_mode="incremental",
            source_type="bigquery",
            destination_type="slack",
            rows_synced=10,
            duration_seconds=0.5,
            status="success",
        )

        for _ in range(50):
            if captured:
                break
            time.sleep(0.05)

        assert len(captured) == 1
        body = captured[0]
        assert body["event"] == "sync_completed"
        assert body["api_key"] == "phc_test"
        assert body["properties"]["source_type"] == "bigquery"
        assert body["properties"]["destination_type"] == "slack"
        assert body["properties"]["rows_synced"] == 10

    def test_unreachable_backend_swallowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DRT_TELEMETRY_ENDPOINT", "http://127.0.0.1:1")  # closed port
        monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
        monkeypatch.setenv("DRT_TELEMETRY", "1")
        telemetry.track_sync_completed(
            sync_mode="full",
            source_type="duckdb",
            destination_type="rest_api",
            rows_synced=0,
            duration_seconds=0.0,
            status="failed",
        )
        # Non-daemon thread keeps process alive but eventually times out.


def _ok():  # type: ignore[no-untyped-def]
    from werkzeug.wrappers import Response

    return Response("", status=204)
