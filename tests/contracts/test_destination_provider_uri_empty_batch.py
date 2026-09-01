"""Empty batches must not resolve provider-URI destination credentials.

Secret-provider resolution can perform network I/O before a destination opens
its own transport.  The transport tripwires in the sibling empty-batch
contracts therefore cannot catch this ordering regression on their own.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from drt.config.models import (
    BearerAuth,
    DiscordDestinationConfig,
    EmailSmtpDestinationConfig,
    GoogleAdsDestinationConfig,
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

_URI = "aws-sm://prod/drt/credentials#value"


PROVIDER_URI_DESTINATIONS: list[Any] = [
    pytest.param(
        discord,
        DiscordDestination(),
        DiscordDestinationConfig(type="discord", webhook_url_env=_URI),
        False,
        id="discord",
    ),
    pytest.param(
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
        False,
        id="email_smtp",
    ),
    pytest.param(
        google_ads,
        GoogleAdsDestination(),
        GoogleAdsDestinationConfig(
            type="google_ads",
            customer_id="123",
            conversion_action="customers/123/conversionActions/456",
            developer_token_env=_URI,
        ),
        False,
        id="google_ads",
    ),
    pytest.param(
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
        False,
        id="meta_conversions",
    ),
    pytest.param(
        intercom,
        IntercomDestination(),
        IntercomDestinationConfig(
            type="intercom",
            auth=BearerAuth(type="bearer", token_env=_URI),
            properties_template='{"email": "{{ row.email }}"}',
        ),
        False,
        id="intercom",
    ),
    pytest.param(
        jira,
        JiraDestination(),
        JiraDestinationConfig(
            type="jira",
            base_url_env=_URI,
            email_env=_URI,
            token_env=_URI,
            project_key="ENG",
            summary_template="summary",
            description_template="description",
        ),
        False,
        id="jira",
    ),
    pytest.param(
        slack,
        SlackDestination(),
        SlackDestinationConfig(type="slack", webhook_url_env=_URI),
        False,
        id="slack",
    ),
    pytest.param(
        teams,
        TeamsDestination(),
        TeamsDestinationConfig(type="teams", webhook_url_env=_URI),
        False,
        id="teams",
    ),
    pytest.param(
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
        False,
        id="twilio",
    ),
    pytest.param(
        salesforce_bulk,
        SalesforceBulkDestination(),
        SalesforceBulkDestinationConfig(
            type="salesforce_bulk",
            instance_url_env=_URI,
            object_name="Contact",
            client_id_env=_URI,
            client_secret_env=_URI,
            username_env=_URI,
            password_env=_URI,
        ),
        True,
        id="salesforce_bulk",
    ),
]


@pytest.mark.parametrize("module,destination,config,is_staged", PROVIDER_URI_DESTINATIONS)
def test_empty_batch_does_not_resolve_provider_uri(
    module: Any,
    destination: Any,
    config: Any,
    is_staged: bool,
) -> None:
    """Credential resolution stays behind the empty-input short-circuit."""
    resolver_error = AssertionError("provider URI resolved for an empty batch")

    with patch.object(module, "resolve_env", side_effect=resolver_error) as resolver:
        if is_staged:
            destination.stage([], config, SyncOptions())
            result = destination.finalize(config, SyncOptions())
        else:
            result = destination.load([], config, SyncOptions())

    resolver.assert_not_called()
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0
