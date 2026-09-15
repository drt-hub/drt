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
        """#1152: partialFailureError.details always has exactly ONE
        GoogleAdsFailure entry regardless of failure count -- its own nested
        `errors` array (matched back to a conversion via
        location.fieldPathElements[].index) is what actually enumerates
        failures. This is the real documented response shape (see
        https://developers.google.com/google-ads/api/docs/best-practices/partial-failures),
        not the flat `{"message": ...}` shape the pre-fix test invented."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v17/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v17.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 1}
                                        ]
                                    },
                                }
                            ],
                        }
                    ],
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
        assert len(result.row_errors) == 1
        assert result.row_errors[0].batch_index == 1
        assert result.row_errors[0].error_message == "Invalid gclid"

    def test_partial_failure_with_multiple_failed_conversions(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """#1152: the pre-fix code counted `len(details)` (always 1) as the
        failure count, silently undercounting whenever more than one
        conversion in a batch failed. Four conversions here, two fail."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v17/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v17.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid (index 1)",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 1}
                                        ]
                                    },
                                },
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid (index 3)",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 3}
                                        ]
                                    },
                                },
                            ],
                        }
                    ],
                },
            }
        )

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [{"gclid": f"g{i}", "conversion_time": "2024-01-01 12:00:00"} for i in range(4)]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 2
        assert result.success == 2
        assert sorted(e.batch_index for e in result.row_errors) == [1, 3]

    def test_partial_failure_unparseable_shape_fails_whole_batch(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A partialFailureError that doesn't match the documented shape
        (e.g. Google changes it, or a proxy mangles it) must degrade to a
        conservative whole-batch failure rather than silently guessing at
        which conversions actually failed."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v17/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"partialFailureError": {"message": "something went wrong"}})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [{"gclid": "g1", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert result.success == 0

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
