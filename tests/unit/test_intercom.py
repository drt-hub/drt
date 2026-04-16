from unittest.mock import patch

import httpx
import pytest

from drt.config.models import IntercomDestinationConfig, SyncOptions, BearerAuth
from drt.destinations.intercom import IntercomDestination
from drt.templates.renderer import render_template


class DummyResponse:
    def __init__(self, status_code=200, text="ok"):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise httpx.HTTPStatusError(
                message="error",
                request=httpx.Request("POST", "https://test"),
                response=self,
            )


@pytest.fixture
def config():
    return IntercomDestinationConfig(
        type="intercom",
        auth=BearerAuth(type="bearer", token="secret"),
        properties_template="""
        {
            "email": "{{ row.email }}",
            "name": "{{ row.name }}",
            "custom_attributes": {
                "plan": "{{ row.plan }}"
            }
        }
        """,
    )


# ----------------------------
# 1. SUCCESS CASE
# ----------------------------
def test_success(config):
    records = [{"email": "a@test.com", "name": "Alice", "plan": "pro"}]

    with patch("httpx.Client.post", return_value=DummyResponse(200)):
        result = IntercomDestination().load(records, config, SyncOptions())

    assert result.success == 1
    assert result.failed == 0


# ----------------------------
# 2. INVALID JSON TEMPLATE → SHOULD FAIL AT JINJA LEVEL
# ----------------------------
def test_invalid_json_template():
    config = IntercomDestinationConfig(
        type="intercom",
        auth=BearerAuth(type="bearer", token="secret"),
        # invalid Jinja syntax (this is correct expectation)
        properties_template="{ invalid json {{ }",
    )

    records = [{"email": "a@test.com"}]

    with pytest.raises(Exception):  # TemplateSyntaxError bubbles up
        IntercomDestination().load(records, config, SyncOptions())


# ----------------------------
# 3. MISSING FIELD → STRICT UNDEFINED (FAIL FAST)
# ----------------------------
def test_missing_field_error(config):
    records = [{"email": "a@test.com"}]  # missing name + plan

    with pytest.raises(ValueError, match="Template error"):
        IntercomDestination().load(records, config, SyncOptions())


# ----------------------------
# 4. HTTP ERROR (ONLY AFTER TEMPLATE SUCCESS)
# ----------------------------
def test_http_error(config):
    records = [{"email": "a@test.com", "name": "Alice", "plan": "pro"}]

    with patch(
        "httpx.Client.post",
        return_value=DummyResponse(400, "bad request"),
    ):
        with pytest.raises(httpx.HTTPStatusError):
            IntercomDestination().load(records, config, SyncOptions())