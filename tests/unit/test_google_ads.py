"""Tests for Google Ads offline conversion destination."""

from __future__ import annotations

import json

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import GoogleAdsDestinationConfig, SyncOptions
from drt.destinations.google_ads import GoogleAdsDestination


def _options() -> SyncOptions:
    return SyncOptions()


def _config(httpserver: HTTPServer, **overrides: str) -> GoogleAdsDestinationConfig:
    defaults = {
        "type": "google_ads",
        "customer_id": "1234567890",
        "conversion_action": "customers/1234567890/conversionActions/987",
    }
    return GoogleAdsDestinationConfig(**{**defaults, **overrides})


class TestGoogleAdsDestination:
    def test_success(self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v17/customers/1234567890:uploadClickConversions",
            method="POST",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [
            {"gclid": "abc123", "conversion_time": "2024-01-01 12:00:00"},
        ]
        result = GoogleAdsDestination().load(records, config, _options())
        assert result.success == 1
        assert result.failed == 0

    def test_missing_gclid(self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")
        config = _config(httpserver)
        records = [{"conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())
        assert result.failed == 1
        assert any("Missing" in e.error_message for e in result.row_errors)

    def test_missing_developer_token(
        self, monkeypatch: pytest.MonkeyPatch, httpserver: HTTPServer
    ) -> None:
        monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)
        config = _config(httpserver)
        with pytest.raises(ValueError, match="GOOGLE_ADS_DEVELOPER_TOKEN"):
            GoogleAdsDestination().load(
                [{"gclid": "x", "conversion_time": "t"}],
                config,
                _options(),
            )

    def test_partial_failure(self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v17/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "details": [{"message": "Invalid gclid"}],
                },
            }
        )

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [
            {"gclid": "good", "conversion_time": "2024-01-01 12:00:00"},
            {"gclid": "bad", "conversion_time": "2024-01-01 12:00:00"},
        ]
        result = GoogleAdsDestination().load(records, config, _options())
        assert result.failed == 1
        assert result.success == 1

    def test_with_conversion_value(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v17/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = GoogleAdsDestinationConfig(
            type="google_ads",
            customer_id="1234567890",
            conversion_action="customers/1234567890/conversionActions/987",
            conversion_value_field="revenue",
            currency_code="JPY",
        )
        records = [
            {
                "gclid": "abc",
                "conversion_time": "2024-01-01",
                "revenue": 9800,
            },
        ]
        result = GoogleAdsDestination().load(records, config, _options())
        assert result.success == 1

        req = httpserver.log[0][0]
        body = json.loads(req.data)
        conv = body["conversions"][0]
        assert conv["conversionValue"] == 9800.0
        assert conv["currencyCode"] == "JPY"
