"""Tests for Google Ads offline conversion destination."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

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
            "/v25/customers/1234567890:uploadClickConversions",
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

    def test_missing_developer_token_no_longer_raises(
        self, monkeypatch: pytest.MonkeyPatch, httpserver: HTTPServer
    ) -> None:
        """#1154: Google's Cloud-project-based access model made the
        developer-token header optional and server-ignored -- requiring it
        client-side would block anyone onboarding under the new model with
        no token to give. The request proceeds without the header."""
        monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        result = GoogleAdsDestination().load(
            [{"gclid": "x", "conversion_time": "t"}],
            config,
            _options(),
        )

        assert result.success == 1
        assert result.failed == 0
        req = httpserver.log[0][0]
        assert "developer-token" not in req.headers

    def test_developer_token_sent_when_configured(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        GoogleAdsDestination().load(
            [{"gclid": "x", "conversion_time": "t"}],
            config,
            _options(),
        )

        req = httpserver.log[0][0]
        assert req.headers["developer-token"] == "dev-tok"

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
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
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
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
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
            "/v25/customers/1234567890:uploadClickConversions",
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
            "/v25/customers/1234567890:uploadClickConversions",
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

    def test_partial_failure_multiple_errors_for_one_conversion_counted_once(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153: Google can return more than one
        GoogleAdsError for the same conversion index. Recording one RowError
        per *error* instead of per conversion would inflate `failed` past
        the batch size and enqueue the same record into the DLQ twice."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 0}
                                        ]
                                    },
                                },
                                {
                                    "errorCode": {"conversionActionError": "INVALID_ARGUMENT"},
                                    "message": "Invalid conversion action",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 0}
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
        records = [{"gclid": "bad", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1
        assert "Invalid gclid" in result.row_errors[0].error_message
        assert "Invalid conversion action" in result.row_errors[0].error_message

    def test_partial_failure_malformed_nested_shape_does_not_crash(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153: a non-dict `location` (or any other nested
        type mismatch) must be validated, not dereferenced with an unchecked
        `.get()` -- an unhandled AttributeError here would fall through to
        the outer generic `except Exception`, which records `failed` but no
        `row_errors`, silently dropping these records from the DLQ."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid",
                                    "location": "not-a-dict",
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
        records = [{"gclid": "bad", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert len(result.row_errors) == 1
        assert result.row_errors[0].batch_index == 0

    def test_partial_failure_empty_errors_list_is_unparseable(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153: a non-null partialFailureError with an
        empty `errors` array is not a legitimate all-clear -- treat it the
        same as any other unparseable shape rather than silently crediting
        every conversion as a success."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [],
                        }
                    ],
                },
            }
        )

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [{"gclid": "g1", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1

    def test_partial_failure_multiple_details_entries_is_unparseable(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153, round 2: `details` is documented to always
        contain exactly one packed GoogleAdsFailure. Silently reading
        `details[0]` and ignoring a second entry would drop real errors from
        an undocumented multi-entry response and wrongly credit those
        conversions as successfully delivered."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        one_failure = {
            "@type": "type.googleapis.com/google.ads.googleads.v25.errors.GoogleAdsFailure",
            "errors": [
                {
                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                    "message": "Invalid gclid",
                    "location": {"fieldPathElements": [{"fieldName": "conversions", "index": 0}]},
                }
            ],
        }
        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "message": "Request contains an invalid argument.",
                    "details": [one_failure, one_failure],
                },
            }
        )

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [
            {"gclid": "bad0", "conversion_time": "2024-01-01 12:00:00"},
            {"gclid": "bad1", "conversion_time": "2024-01-01 12:00:00"},
        ]
        result = GoogleAdsDestination().load(records, config, _options())

        # Unparseable -> conservative whole-batch failure, not "index 0
        # failed, index 1 silently succeeded".
        assert result.failed == 2
        assert result.success == 0
        assert len(result.row_errors) == 2

    def test_partial_failure_empty_object_is_not_a_success(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153, round 3: `partialFailureError: {}` is
        falsey in Python but still a *present* field -- a truthiness check
        would take the success branch and credit every conversion as
        delivered, contradicting the conservative-fallback philosophy the
        rest of this parser follows."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"partialFailureError": {}})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [{"gclid": "g1", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1

    def test_partial_failure_non_object_payload_does_not_crash(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153, round 3: a truthy but non-dict
        `partialFailureError` must not be handed to `.get()` (an
        AttributeError there would fall through to the outer generic
        `except Exception`, which records `failed` but no `row_errors`)."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"partialFailureError": "unexpected string payload"})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [{"gclid": "g1", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert len(result.row_errors) == 1

    def test_partial_failure_explicit_null_is_not_a_success(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153, round 4: `resp_data.get("partialFailureError")`
        returns the same `None` for an absent key and an explicit
        `"partialFailureError": null` -- a malformed/intermediary response
        sending the latter must still route through the conservative
        fallback, not the absent-field success branch."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"partialFailureError": None})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        records = [{"gclid": "g1", "conversion_time": "2024-01-01 12:00:00"}]
        result = GoogleAdsDestination().load(records, config, _options())

        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1

    def test_partial_failure_boolean_index_is_unparseable(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review of #1153, round 4: `bool` is an `int` subclass in
        Python, so an unvalidated `"index": true` would otherwise be
        accepted as index 1 and misattribute the error to an arbitrary
        record instead of hitting the whole-batch fallback."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": True}
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
            {"gclid": "g0", "conversion_time": "2024-01-01 12:00:00"},
            {"gclid": "g1", "conversion_time": "2024-01-01 12:00:00"},
        ]
        result = GoogleAdsDestination().load(records, config, _options())

        # Unparseable -> conservative whole-batch failure, not "index 1
        # (== True) failed, index 0 silently succeeded".
        assert result.failed == 2
        assert result.success == 0
        assert len(result.row_errors) == 2


class TestGoogleAdsNativeIdempotencyKey:
    """#897: native_idempotency_key is rendered per record and sent as
    ClickConversion's orderId dedup field."""

    def test_sent_as_order_id(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver, native_idempotency_key="order-{{ row.id }}")
        result = GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01", "id": "42"}],
            config,
            _options(),
        )

        assert result.success == 1
        body = httpserver.log[0][0].get_json()
        assert body["conversions"][0]["orderId"] == "order-42"

    def test_sync_name_is_in_template_scope(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver, native_idempotency_key="{{ sync_name }}-{{ row.id }}")
        options = _options()
        options._sync_name = "my-sync"
        GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01", "id": "42"}], config, options
        )

        body = httpserver.log[0][0].get_json()
        assert body["conversions"][0]["orderId"] == "my-sync-42"

    def test_omitted_when_not_configured(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json({"results": [{}]})

        from drt.destinations import google_ads

        monkeypatch.setattr(google_ads, "_BASE_URL", httpserver.url_for(""))

        config = _config(httpserver)
        GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01"}], config, _options()
        )

        body = httpserver.log[0][0].get_json()
        assert "orderId" not in body["conversions"][0]

    def test_template_error_is_a_row_error(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")
        config = _config(httpserver, native_idempotency_key="order-{{ row.missing_field }}")

        result = GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01"}], config, _options()
        )

        assert result.failed == 1
        assert result.success == 0
        assert "Template error" in result.row_errors[0].error_message

    def test_order_id_already_in_use_counts_as_success(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A retry that reuses a previously-delivered orderId is a
        successful, deduplicated delivery, not a failure."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {
                                        "conversionUploadError": "ORDER_ID_ALREADY_IN_USE"
                                    },
                                    "message": "Order ID already recorded",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 0}
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

        config = _config(httpserver, native_idempotency_key="order-{{ row.id }}")
        result = GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01", "id": "42"}], config, _options()
        )

        assert result.success == 1
        assert result.failed == 0
        assert result.row_errors == []

    def test_order_id_already_in_use_mixed_with_real_error_still_fails(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the same conversion also carries a genuine error alongside
        ORDER_ID_ALREADY_IN_USE, it must not be waved through as dedup."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {
                                        "conversionUploadError": "ORDER_ID_ALREADY_IN_USE"
                                    },
                                    "message": "Order ID already recorded",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 0}
                                        ]
                                    },
                                },
                                {
                                    "errorCode": {"conversionUploadError": "INVALID_GCLID"},
                                    "message": "Invalid gclid",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 0}
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

        config = _config(httpserver, native_idempotency_key="order-{{ row.id }}")
        result = GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01", "id": "42"}], config, _options()
        )

        assert result.success == 0
        assert result.failed == 1

    def test_duplicate_order_id_is_not_treated_as_dedup(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DUPLICATE_ORDER_ID ("multiple events include multiple
        conversions with the same order ID ... were not processed") is a
        real config problem -- e.g. a native_idempotency_key template that
        renders the same value for two different rows in one batch -- not
        drt's own redelivery, and must not be swept into the
        ORDER_ID_ALREADY_IN_USE dedup path."""
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")

        httpserver.expect_request(
            "/v25/customers/1234567890:uploadClickConversions",
        ).respond_with_json(
            {
                "partialFailureError": {
                    "code": 3,
                    "message": "Request contains an invalid argument.",
                    "details": [
                        {
                            "@type": (
                                "type.googleapis.com/google.ads.googleads."
                                "v25.errors.GoogleAdsFailure"
                            ),
                            "errors": [
                                {
                                    "errorCode": {"conversionUploadError": "DUPLICATE_ORDER_ID"},
                                    "message": "Duplicate order ID in request",
                                    "location": {
                                        "fieldPathElements": [
                                            {"fieldName": "conversions", "index": 0}
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

        config = _config(httpserver, native_idempotency_key="order-{{ row.id }}")
        result = GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01", "id": "42"}], config, _options()
        )

        assert result.success == 0
        assert result.failed == 1


class TestGoogleAdsRateLimiterKeying:
    """#1154, Codex review round 5: a round-3 attempt to split tokenless
    configs' rate-limit key by OAuth-client identity was reverted -- Google's
    Cloud-project-based access model scopes quota to the project, which can
    own several OAuth clients, so per-client keys can under-share a quota
    that is really shared. No config field currently identifies the
    underlying Cloud project, so every google_ads config (tokenless or not)
    deliberately shares one bucket per rate_limit_key(); see that method's
    docstring in destinations_saas.py."""

    @staticmethod
    def _patch_common(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        from drt.destinations import google_ads

        spy = MagicMock(return_value=MagicMock())
        monkeypatch.setattr(google_ads, "resolve_rate_limiter", spy)
        monkeypatch.setattr(google_ads, "AuthHandler", lambda auth: MagicMock(get_headers=dict))
        monkeypatch.setattr(google_ads.httpx, "Client", MagicMock())
        return spy

    def test_tokenless_configs_share_one_bucket_regardless_of_oauth_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from drt.config.base import OAuth2ClientCredentialsAuth

        monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)
        spy = self._patch_common(monkeypatch)

        def make_config(client_id_env: str) -> GoogleAdsDestinationConfig:
            return GoogleAdsDestinationConfig(
                type="google_ads",
                customer_id="123",
                conversion_action="customers/123/conversionActions/456",
                auth=OAuth2ClientCredentialsAuth(
                    type="oauth2_client_credentials",
                    token_url="https://oauth2.googleapis.com/token",
                    client_id_env=client_id_env,
                    client_secret_env="SECRET",
                ),
            )

        records = [{"gclid": "g", "conversion_time": "2024-01-01"}]
        GoogleAdsDestination().load(records, make_config("CLIENT_A_ID"), _options())
        GoogleAdsDestination().load(records, make_config("CLIENT_B_ID"), _options())

        assert "key_override" not in spy.call_args_list[0].kwargs
        assert "key_override" not in spy.call_args_list[1].kwargs

    def test_config_with_token_does_not_override_the_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")
        spy = self._patch_common(monkeypatch)

        config = GoogleAdsDestinationConfig(
            type="google_ads",
            customer_id="123",
            conversion_action="customers/123/conversionActions/456",
        )
        GoogleAdsDestination().load(
            [{"gclid": "g", "conversion_time": "2024-01-01"}], config, _options()
        )

        assert "key_override" not in spy.call_args.kwargs
