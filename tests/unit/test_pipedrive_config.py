"""Unit tests for the Pipedrive destination config."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from drt.config.models import PipedriveDestinationConfig, SyncConfig


def test_defaults() -> None:
    config = PipedriveDestinationConfig(type="pipedrive")
    assert config.api_token_env == "PIPEDRIVE_API_TOKEN"
    assert config.object == "person"
    assert config.match_field == "email"


def test_requires_a_token_source() -> None:
    with pytest.raises(ValidationError, match="api_token or api_token_env"):
        PipedriveDestinationConfig(type="pipedrive", api_token=None, api_token_env=None)


def test_only_person_is_supported_for_now() -> None:
    with pytest.raises(ValidationError):
        PipedriveDestinationConfig(type="pipedrive", object="deal")


def test_parses_from_a_sync_dict() -> None:
    sync = SyncConfig.model_validate(
        {
            "name": "people_to_pipedrive",
            "model": "SELECT 1 AS id",
            "destination": {"type": "pipedrive"},
        }
    )
    assert isinstance(sync.destination, PipedriveDestinationConfig)


def test_base_url_defaults_to_none_and_must_be_http() -> None:
    assert PipedriveDestinationConfig(type="pipedrive").base_url is None
    with pytest.raises(ValidationError, match="base_url"):
        PipedriveDestinationConfig(type="pipedrive", base_url="ftp://example.com")


@pytest.mark.parametrize(
    "url",
    [
        "https://api.pipedrive.com/api/v2",
        "https://example.com",
        "http://localhost:8080/api/v2",
        "http://127.0.0.1:9000",
    ],
)
def test_base_url_accepts_https_and_local_http(url: str) -> None:
    assert PipedriveDestinationConfig(type="pipedrive", base_url=url).base_url == url


@pytest.mark.parametrize(
    "url",
    [
        "http://api.pipedrive.com/api/v2",
        "ftp://example.com",
        "https://",
        "http://localhost.evil.example",
        "http://localhost@evil.example",
        "api.pipedrive.com",
    ],
)
def test_base_url_rejects_plain_http_and_other_schemes(url: str) -> None:
    with pytest.raises(ValidationError, match="base_url"):
        PipedriveDestinationConfig(type="pipedrive", base_url=url)
