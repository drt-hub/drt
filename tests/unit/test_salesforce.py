"""Unit tests for Salesforce destination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import json
import pytest

from drt.config.models import (
    SalesforceDestinationConfig, 
    SyncOptions, 
    RetryConfig
)
from drt.destinations.salesforce import SalesforceDestination


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def make_config(**overrides):
    base = {
        "type": "salesforce",
        "object": "Contact",
        "external_id_field": "Email",
        "auth": {
            "type": "bearer",
            "token": "test-token",
        },
    }
    base.update(overrides)
    return SalesforceDestinationConfig(**base)


# ---------------------------------------------------------------------
# REST API tests
# ---------------------------------------------------------------------

def test_rest_upsert_success(httpx_mock):
    config = make_config()
    dest = SalesforceDestination()

    httpx_mock.add_response(status_code=204)

    result = dest.load(
        [{"Email": "test@example.com", "FirstName": "Test"}],
        config,
        SyncOptions(),
    )

    assert result.success == 1
    assert result.failed == 0


def test_missing_external_id_skip(httpx_mock):
    config = make_config()
    dest = SalesforceDestination()

    result = dest.load(
        [{"FirstName": "No Email"}],
        config,
        SyncOptions(on_error="skip"),
    )

    assert result.success == 0
    assert result.failed == 1
    assert "Missing external_id_field" in result.row_errors[0].error_message


def test_missing_external_id_fail(httpx_mock):
    config = make_config()
    dest = SalesforceDestination()

    with pytest.raises(ValueError):
        dest.load(
            [{"FirstName": "No Email"}],
            config,
            SyncOptions(on_error="fail"),
        )


def test_retry_on_429(httpx_mock):
    config = make_config()
    dest = SalesforceDestination()

    # First request = 429, second = success
    httpx_mock.add_response(status_code=429, headers={"Retry-After": "0"})
    httpx_mock.add_response(status_code=204)

    result = dest.load(
        [{"Email": "retry@example.com"}],
        config,
        SyncOptions(
            retry=RetryConfig(max_attempts=2, initial_backoff=0)
        ),
    )

    assert result.success == 1
    assert result.failed == 0


# ---------------------------------------------------------------------
# Bulk API tests
# ---------------------------------------------------------------------

def test_bulk_upsert_success(httpx_mock):
    config = make_config(bulk_threshold=1)  # force bulk
    dest = SalesforceDestination()

    # Create job
    httpx_mock.add_response(
        status_code=200,
        json={"id": "job123"},
    )

    # Upload batch
    httpx_mock.add_response(status_code=201)

    # Close job
    httpx_mock.add_response(status_code=200)

    # Poll status (complete immediately)
    httpx_mock.add_response(
        status_code=200,
        json={"state": "JobComplete"},
    )

    records = [
        {"Email": "bulk1@example.com"},
        {"Email": "bulk2@example.com"},
    ]

    result = dest.load(records, config, SyncOptions())

    assert result.success == len(records)
    assert result.failed == 0


def test_bulk_upsert_failure(httpx_mock):
    config = make_config(bulk_threshold=1)
    dest = SalesforceDestination()

    # Create job
    httpx_mock.add_response(status_code=200, json={"id": "job123"})

    # Upload batch
    httpx_mock.add_response(status_code=201)

    # Close job
    httpx_mock.add_response(status_code=200)

    # Poll status → failed
    httpx_mock.add_response(
        status_code=200,
        json={"state": "Failed"},
    )

    records = [{"Email": "fail@example.com"}]

    result = dest.load(records, config, SyncOptions())

    assert result.success == 0
    assert result.failed == len(records)


# ---------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------

def test_rest_http_error(httpx_mock):
    config = make_config()
    dest = SalesforceDestination()

    httpx_mock.add_response(
        status_code=400,
        json={"error": "bad request"},
    )

    result = dest.load(
        [{"Email": "bad@example.com"}],
        config,
        SyncOptions(on_error="skip"),
    )

    assert result.success == 0
    assert result.failed == 1
    assert result.row_errors[0].http_status == 400


def test_empty_records(httpx_mock):
    config = make_config()
    dest = SalesforceDestination()

    result = dest.load([], config, SyncOptions())

    assert result.success == 0
    assert result.failed == 0