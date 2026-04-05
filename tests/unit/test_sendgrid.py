"""Unit tests for SendGrid destination."""

# test_sendgrid.py
import json
import pytest
from unittest.mock import MagicMock, patch
from drt.config.models import (
    SendGridDestinationConfig,
    BearerAuth,
    SyncOptions,
    RateLimitConfig,
)
from drt.destinations.sendgrid import SendGridDestination
from drt.destinations.base import SyncResult

# ---------------------------------
# Subclass for email-mode testing
# ---------------------------------
class TestSendGridDestinationConfig(SendGridDestinationConfig):
    from_email: str
    from_name: str | None = None
    subject_template: str
    body_template: str

# ---------------------------------
# Fixtures
# ---------------------------------
@pytest.fixture
def dummy_records():
    return [
        {"email": "alice@example.com", "first_name": "Alice", "last_name": "Smith"},
        {"email": "bob@example.com", "first_name": "Bob", "last_name": "Jones"},
    ]

@pytest.fixture
def email_config():
    return TestSendGridDestinationConfig(
        type="sendgrid",
        from_email="noreply@test.com",
        from_name="Test Sender",
        subject_template="Hello {{ row.first_name }}",
        body_template="Welcome {{ row.first_name }}!",
        auth=BearerAuth(type="bearer", token=None, token_env="SENDGRID_API_KEY"),
        properties_template=None
    )

@pytest.fixture
def contacts_config():
    return SendGridDestinationConfig(
        type="sendgrid",
        properties_template='{"email": "{{ row.email }}", "first_name": "{{ row.first_name }}"}',
        auth=BearerAuth(type="bearer", token=None, token_env="SENDGRID_API_KEY"),
    )

@pytest.fixture
def sync_options():
    return SyncOptions(
        rate_limit=RateLimitConfig(requests_per_second=10),
        batch_size=2
    )

# ---------------------------------
# Fake SendGridDestination
# ---------------------------------
class FakeSendGridDestination(SendGridDestination):
    """Simulate SendGridDestination without real HTTP calls."""
    def __init__(self):
        super().__init__()
        self.sent_emails = []
        self.upserted_contacts = []

    def load(self, records, config, sync_options):
        result = SyncResult()
        is_email_mode = hasattr(config, "subject_template") and hasattr(config, "body_template")
        if is_email_mode:
            for record in records:
                if not record.get("email"):
                    result.failed += 1
                    continue
                self.sent_emails.append(record)
                result.success += 1
        else:
            for record in records:
                rendered = config.properties_template.replace("{{ row.email }}", record["email"]) \
                                                     .replace("{{ row.first_name }}", record["first_name"])
                contact = json.loads(rendered)
                self.upserted_contacts.append(contact)
                result.success += 1
        return result

# ---------------------------------
# Tests
# ---------------------------------
def test_sendgrid_email_mode_success(dummy_records, email_config, sync_options):
    dest = FakeSendGridDestination()
    with patch("sendgrid.render_template", side_effect=lambda template, row: template.replace("{{ row.first_name }}", row["first_name"])):
        result = dest.load(dummy_records, email_config, sync_options)
    assert result.success == len(dummy_records)
    assert result.failed == 0
    assert len(dest.sent_emails) == len(dummy_records)

def test_sendgrid_contacts_mode_success(dummy_records, contacts_config, sync_options):
    dest = FakeSendGridDestination()
    with patch("sendgrid.render_template", side_effect=lambda template, row: template.replace("{{ row.first_name }}", row["first_name"]).replace("{{ row.email }}", row["email"])):
        result = dest.load(dummy_records, contacts_config, sync_options)
    assert result.success == len(dummy_records)
    assert result.failed == 0
    assert len(dest.upserted_contacts) == len(dummy_records)

def test_sendgrid_missing_email_fails(dummy_records, email_config, sync_options):
    dest = FakeSendGridDestination()
    dummy_records[0].pop("email")
    with patch("sendgrid.render_template", side_effect=lambda template, row: template.replace("{{ row.first_name }}", row["first_name"])):
        result = dest.load(dummy_records, email_config, sync_options)
    assert result.success == 1
    assert result.failed == 1
    assert len(dest.sent_emails) == 1

def test_sendgrid_simulated_failure(dummy_records, contacts_config, sync_options):
    class FailingDestination(FakeSendGridDestination):
        def load(self, records, config, sync_options):
            result = super().load(records, config, sync_options)
            if records:
                result.success -= 1
                result.failed += 1
            return result
    dest = FailingDestination()
    with patch("sendgrid.render_template", side_effect=lambda template, row: template.replace("{{ row.first_name }}", row["first_name"]).replace("{{ row.email }}", row["email"])):
        result = dest.load(dummy_records, contacts_config, sync_options)
    assert result.success == len(dummy_records) - 1
    assert result.failed == 1

def test_render_template_called_with_correct_args(dummy_records, email_config, sync_options):
    dest = FakeSendGridDestination()
    mock_render = MagicMock(side_effect=lambda template, row: template.replace("{{ row.first_name }}", row["first_name"]))
    with patch("sendgrid.render_template", mock_render):
        dest.load(dummy_records, email_config, sync_options)
    for record in dummy_records:
        mock_render.assert_any_call(email_config.subject_template, record)
        mock_render.assert_any_call(email_config.body_template, record)