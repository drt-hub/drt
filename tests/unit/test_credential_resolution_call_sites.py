"""Regression tests for the provider-URI call sites fixed in #965."""

from __future__ import annotations

import sys
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from drt.config.credentials import PostgresProfile, RedshiftProfile
from drt.config.models import (
    BearerAuth,
    DiscordDestinationConfig,
    EmailSmtpDestinationConfig,
    GoogleAdsDestinationConfig,
    GoogleSheetsDestinationConfig,
    IntercomDestinationConfig,
    JiraDestinationConfig,
    MetaConversionsDestinationConfig,
    SalesforceBulkDestinationConfig,
    SlackDestinationConfig,
    SyncOptions,
    TeamsDestinationConfig,
    TwilioDestinationConfig,
)
from drt.destinations import (
    discord,
    email_smtp,
    google_ads,
    intercom,
    jira,
    meta_conversions,
    salesforce_bulk,
    slack,
    teams,
    twilio,
)
from drt.destinations.discord import DiscordDestination
from drt.destinations.email_smtp import EmailSmtpDestination
from drt.destinations.google_ads import GoogleAdsDestination
from drt.destinations.intercom import IntercomDestination
from drt.destinations.jira import JiraDestination
from drt.destinations.meta_conversions import MetaConversionsDestination
from drt.destinations.salesforce_bulk import SalesforceBulkDestination
from drt.destinations.slack import SlackDestination
from drt.destinations.teams import TeamsDestination
from drt.destinations.twilio import TwilioDestination
from drt.sources import postgres, redshift
from drt.sources.postgres import PostgresSource
from drt.sources.redshift import RedshiftSource

_URI = "aws-sm://prod/drt/credentials#value"


@pytest.mark.parametrize(
    ("module", "destination", "config", "resolved", "expected_calls"),
    [
        (
            discord,
            DiscordDestination(),
            DiscordDestinationConfig(type="discord", webhook_url_env=_URI),
            "https://discord.example/webhook",
            [call(None, _URI)],
        ),
        (
            email_smtp,
            EmailSmtpDestination(),
            EmailSmtpDestinationConfig(
                type="email_smtp",
                host="smtp.example.com",
                sender="sender@example.com",
                recipients=["recipient@example.com"],
                subject_template="subject",
                body_template="body",
                username_env=_URI,
                password_env=_URI,
            ),
            "resolved-credential",
            [call(None, _URI), call(None, _URI)],
        ),
        (
            google_ads,
            GoogleAdsDestination(),
            GoogleAdsDestinationConfig(
                type="google_ads",
                customer_id="123",
                conversion_action="customers/123/conversionActions/456",
                developer_token_env=_URI,
            ),
            "developer-token",
            [call(None, _URI)],
        ),
        (
            meta_conversions,
            MetaConversionsDestination(),
            MetaConversionsDestinationConfig(
                type="meta_conversions",
                pixel_id="123",
                event_name="Purchase",
                event_id_field="event_id",
                email_field="email",
                access_token_env=_URI,
            ),
            "meta-token",
            [call(None, _URI)],
        ),
        (
            intercom,
            IntercomDestination(),
            IntercomDestinationConfig(
                type="intercom",
                auth=BearerAuth(type="bearer", token_env=_URI),
                properties_template='{"email": "test@example.com"}',
            ),
            "intercom-token",
            [call(None, _URI)],
        ),
        (
            slack,
            SlackDestination(),
            SlackDestinationConfig(type="slack", webhook_url_env=_URI),
            "https://slack.example/webhook",
            [call(None, _URI)],
        ),
        (
            teams,
            TeamsDestination(),
            TeamsDestinationConfig(type="teams", webhook_url_env=_URI),
            "https://teams.example/webhook",
            [call(None, _URI)],
        ),
        (
            twilio,
            TwilioDestination(),
            TwilioDestinationConfig(
                type="twilio",
                account_sid_env=_URI,
                auth_token_env=_URI,
                from_number="+1234567890",
                to_template="{{ row.phone }}",
                message_template="hello",
            ),
            "resolved-credential",
            [call(None, _URI), call(None, _URI)],
        ),
    ],
)
def test_destination_provider_uris_route_through_resolve_env(
    module: Any,
    destination: Any,
    config: Any,
    resolved: str,
    expected_calls: list[Any],
) -> None:
    resolver = MagicMock(return_value=resolved)
    client = MagicMock()
    http_client = client.return_value.__enter__.return_value
    http_client.post.return_value.json.return_value = {}

    with (
        patch.object(module, "resolve_env", resolver),
        patch.object(module.httpx, "Client", client) if hasattr(module, "httpx") else nullcontext(),
        patch.object(module.smtplib, "SMTP") if hasattr(module, "smtplib") else nullcontext(),
    ):
        destination.load([{"phone": "+1234567890"}], config, SyncOptions())

    assert resolver.call_args_list == expected_calls


def test_jira_provider_uris_route_through_resolve_env() -> None:
    config = JiraDestinationConfig(
        type="jira",
        base_url_env="aws-sm://prod/drt/jira#base_url",
        email_env="aws-sm://prod/drt/jira#email",
        token_env="aws-sm://prod/drt/jira#token",
        project_key="ENG",
        summary_template="summary",
        description_template="description",
    )
    values = {
        config.base_url_env: "https://example.atlassian.net/",
        config.email_env: "bot@example.com",
        config.token_env: "api-token",
    }
    resolver = MagicMock(side_effect=lambda _value, uri: values[uri])

    with (
        patch.object(jira, "resolve_env", resolver),
        patch.object(jira.httpx, "Client") as client,
    ):
        http_client = client.return_value.__enter__.return_value
        JiraDestination().load([{}, {"issue_id": "ENG-1"}], config, SyncOptions())

    assert resolver.call_args_list == [
        call(None, config.base_url_env),
        call(None, config.email_env),
        call(None, config.token_env),
    ]
    assert http_client.post.call_args.args[0] == "https://example.atlassian.net/rest/api/3/issue"
    assert http_client.put.call_args.args[0] == (
        "https://example.atlassian.net/rest/api/3/issue/ENG-1"
    )


def test_salesforce_bulk_provider_uris_route_through_resolve_env() -> None:
    config = SalesforceBulkDestinationConfig(
        type="salesforce_bulk",
        instance_url_env="aws-sm://prod/drt/salesforce#instance_url",
        object_name="Contact",
        client_id_env="aws-sm://prod/drt/salesforce#client_id",
        client_secret_env="aws-sm://prod/drt/salesforce#client_secret",
        username_env="aws-sm://prod/drt/salesforce#username",
        password_env="aws-sm://prod/drt/salesforce#password",
    )
    values = {
        config.instance_url_env: "https://example.my.salesforce.com/",
        config.client_id_env: "client-id",
        config.client_secret_env: "client-secret",
        config.username_env: "user@example.com",
        config.password_env: "password",
    }
    resolver = MagicMock(side_effect=lambda _value, uri: values[uri])
    auth_response = MagicMock(status_code=401, text="stop after credential resolution")
    client = MagicMock()
    client.return_value.__enter__.return_value.post.return_value = auth_response
    destination = SalesforceBulkDestination()
    destination.stage([{"Id": "001"}], config, SyncOptions())

    with (
        patch.object(salesforce_bulk, "resolve_env", resolver),
        patch.object(salesforce_bulk.httpx, "Client", client),
        pytest.raises(RuntimeError, match="Salesforce auth failed"),
    ):
        destination.finalize(config, SyncOptions())

    assert resolver.call_args_list == [
        call(None, config.instance_url_env),
        call(None, config.client_id_env),
        call(None, config.client_secret_env),
        call(None, config.username_env),
        call(None, config.password_env),
    ]


def test_google_ads_missing_resolved_token_keeps_existing_error() -> None:
    config = GoogleAdsDestinationConfig(
        type="google_ads",
        customer_id="123",
        conversion_action="customers/123/conversionActions/456",
        developer_token_env=_URI,
    )

    with patch.object(google_ads, "resolve_env", return_value=None):
        with pytest.raises(ValueError, match=_URI):
            GoogleAdsDestination().load([{}], config, SyncOptions())


def test_google_sheets_provider_uri_routes_through_resolve_env() -> None:
    from drt.destinations import google_sheets

    config = GoogleSheetsDestinationConfig(
        type="google_sheets",
        spreadsheet_id="sheet-id",
        credentials_env=_URI,
    )
    service_account = MagicMock()
    discovery = MagicMock()
    modules = {
        "google.oauth2": SimpleNamespace(service_account=service_account),
        "google.oauth2.service_account": service_account,
        "googleapiclient": SimpleNamespace(discovery=discovery),
        "googleapiclient.discovery": discovery,
    }

    with (
        patch.object(google_sheets, "resolve_env", return_value="/resolved/key.json") as resolver,
        patch.dict(sys.modules, modules),
    ):
        google_sheets._build_sheets_service(config)

    resolver.assert_called_once_with(None, _URI)
    service_account.Credentials.from_service_account_file.assert_called_once()


@pytest.mark.parametrize(
    ("module", "source", "profile"),
    [
        (
            postgres,
            PostgresSource(),
            PostgresProfile(type="postgres", password_env=_URI),
        ),
        (
            redshift,
            RedshiftSource(),
            RedshiftProfile(type="redshift", password_env=_URI),
        ),
    ],
)
def test_sql_source_provider_uri_routes_through_resolve_env(
    module: Any, source: Any, profile: Any
) -> None:
    connect = MagicMock()
    fake_psycopg2 = SimpleNamespace(connect=connect)

    with (
        patch.object(module, "resolve_env", return_value="db-password") as resolver,
        patch.dict(sys.modules, {"psycopg2": fake_psycopg2}),
    ):
        source._connect(profile)

    resolver.assert_called_once_with(None, _URI)
    assert connect.call_args.kwargs["password"] == "db-password"
