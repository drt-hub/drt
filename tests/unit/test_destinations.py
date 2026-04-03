"""Unit tests for Slack, HubSpot, and GitHub Actions destinations.

Uses pytest-httpserver to spin up a local HTTP server — no mocking.
"""

from __future__ import annotations

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import (
    BearerAuth,
    DiscordDestinationConfig,
    GitHubActionsDestinationConfig,
    HubSpotDestinationConfig,
    SlackDestinationConfig,
    SyncOptions,
)
from drt.destinations.discord import DiscordDestination
from drt.destinations.github_actions import GitHubActionsDestination
from drt.destinations.hubspot import HubSpotDestination
from drt.destinations.slack import SlackDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _options() -> SyncOptions:
    return SyncOptions()


# ---------------------------------------------------------------------------
# SlackDestination
# ---------------------------------------------------------------------------


class TestSlackDestination:
    def test_success(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            message_template="hello {{ row.name }}",
        )
        result = SlackDestination().load([{"name": "Alice"}], config, _options())
        assert result.success == 1
        assert result.failed == 0

    def test_on_error_skip(self, httpserver: HTTPServer) -> None:
        httpserver.expect_ordered_request("/webhook").respond_with_data("", status=500)
        httpserver.expect_ordered_request("/webhook").respond_with_data("ok", status=200)
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            message_template="{{ row.msg }}",
        )
        opts = SyncOptions(on_error="skip")
        result = SlackDestination().load(
            [{"msg": "a"}, {"msg": "b"}], config, opts
        )
        assert result.failed == 1
        assert result.success == 1

    def test_missing_webhook_raises(self) -> None:
        config = SlackDestinationConfig(type="slack", message_template="hi")
        with pytest.raises(ValueError, match="webhook_url"):
            SlackDestination().load([{"x": 1}], config, _options())

    def test_block_kit_payload(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=(
                '{"blocks": [{"type": "section",'
                ' "text": {"type": "mrkdwn", "text": "{{ row.msg }}"}}]}'
            ),
        )
        result = SlackDestination().load([{"msg": "hello"}], config, _options())
        assert result.success == 1


# ---------------------------------------------------------------------------
# DiscordDestination
# ---------------------------------------------------------------------------


class TestDiscordDestination:
    def test_success(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        config = DiscordDestinationConfig(
            type="discord",
            webhook_url=httpserver.url_for("/webhook"),
            message_template="hello {{ row.name }}",
        )
        result = DiscordDestination().load([{"name": "Alice"}], config, _options())
        assert result.success == 1
        assert result.failed == 0

    def test_on_error_skip(self, httpserver: HTTPServer) -> None:
        httpserver.expect_ordered_request("/webhook").respond_with_data("", status=500)
        httpserver.expect_ordered_request("/webhook").respond_with_data("ok", status=200)
        config = DiscordDestinationConfig(
            type="discord",
            webhook_url=httpserver.url_for("/webhook"),
            message_template="{{ row.msg }}",
        )
        opts = SyncOptions(on_error="skip")
        result = DiscordDestination().load(
            [{"msg": "a"}, {"msg": "b"}], config, opts
        )
        assert result.failed == 1
        assert result.success == 1

    def test_missing_webhook_raises(self) -> None:
        config = DiscordDestinationConfig(type="discord", message_template="hi")
        with pytest.raises(ValueError, match="webhook_url"):
            DiscordDestination().load([{"x": 1}], config, _options())

    def test_embeds_payload(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        config = DiscordDestinationConfig(
            type="discord",
            webhook_url=httpserver.url_for("/webhook"),
            embeds=True,
            message_template=(
                '{"embeds": [{"title": "{{ row.title }}",'
                ' "description": "{{ row.desc }}", "color": 3447003}]}'
            ),
        )
        result = DiscordDestination().load(
            [{"title": "New Alert", "desc": "Something happened"}], config, _options()
        )
        assert result.success == 1


# ---------------------------------------------------------------------------
# HubSpotDestination
# ---------------------------------------------------------------------------


class TestHubSpotDestination:
    def test_success_upsert(self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HUBSPOT_TOKEN", "test-token")
        httpserver.expect_request("/crm/v3/objects/contacts").respond_with_data(
            '{"id": "1"}', status=200, content_type="application/json"
        )
        config = HubSpotDestinationConfig(
            type="hubspot",
            object_type="contacts",
            id_property="email",
            properties_template='{"email": "{{ row.email }}"}',
            auth=BearerAuth(type="bearer", token_env="HUBSPOT_TOKEN"),
        )
        # Patch API base URL to point at test server
        import drt.destinations.hubspot as hs_mod
        monkeypatch.setattr(hs_mod, "_HUBSPOT_API", httpserver.url_for("/crm/v3/objects"))
        result = HubSpotDestination().load(
            [{"email": "alice@example.com"}], config, _options()
        )
        assert result.success == 1
        assert result.failed == 0

    def test_missing_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("HUBSPOT_TOKEN", raising=False)
        config = HubSpotDestinationConfig(
            type="hubspot",
            object_type="contacts",
            auth=BearerAuth(type="bearer", token_env="HUBSPOT_TOKEN"),
        )
        with pytest.raises(ValueError, match="HUBSPOT_TOKEN"):
            HubSpotDestination().load([{"email": "x@x.com"}], config, _options())

    def test_template_error_skipped(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HUBSPOT_TOKEN", "test-token")
        config = HubSpotDestinationConfig(
            type="hubspot",
            object_type="contacts",
            properties_template="not valid json {{ row.email }}",
            auth=BearerAuth(type="bearer", token_env="HUBSPOT_TOKEN"),
        )
        import drt.destinations.hubspot as hs_mod
        monkeypatch.setattr(hs_mod, "_HUBSPOT_API", httpserver.url_for("/crm/v3/objects"))
        result = HubSpotDestination().load(
            [{"email": "x@x.com"}], config, _options()
        )
        assert result.failed == 1
        assert result.success == 0


# ---------------------------------------------------------------------------
# GitHubActionsDestination
# ---------------------------------------------------------------------------


class TestGitHubActionsDestination:
    def test_success(self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GITHUB_TOKEN", "test-token")
        httpserver.expect_request(
            "/repos/myorg/myapp/actions/workflows/deploy.yml/dispatches"
        ).respond_with_data("", status=204)
        config = GitHubActionsDestinationConfig(
            type="github_actions",
            owner="myorg",
            repo="myapp",
            workflow_id="deploy.yml",
            ref="main",
            auth=BearerAuth(type="bearer", token_env="GITHUB_TOKEN"),
        )
        import drt.destinations.github_actions as ga_mod
        monkeypatch.setattr(ga_mod, "_GITHUB_API", httpserver.url_for(""))
        result = GitHubActionsDestination().load(
            [{"env": "prod", "version": "1.0"}], config, _options()
        )
        assert result.success == 1
        assert result.failed == 0

    def test_missing_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        config = GitHubActionsDestinationConfig(
            type="github_actions",
            owner="myorg",
            repo="myapp",
            workflow_id="deploy.yml",
            auth=BearerAuth(type="bearer", token_env="GITHUB_TOKEN"),
        )
        with pytest.raises(ValueError, match="GITHUB_TOKEN"):
            GitHubActionsDestination().load([{}], config, _options())

    def test_inputs_template_error_skipped(
        self, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GITHUB_TOKEN", "test-token")
        config = GitHubActionsDestinationConfig(
            type="github_actions",
            owner="myorg",
            repo="myapp",
            workflow_id="deploy.yml",
            inputs_template="not valid json {{ row.env }}",
            auth=BearerAuth(type="bearer", token_env="GITHUB_TOKEN"),
        )
        import drt.destinations.github_actions as ga_mod
        monkeypatch.setattr(ga_mod, "_GITHUB_API", httpserver.url_for(""))
        result = GitHubActionsDestination().load([{"env": "prod"}], config, _options())
        assert result.failed == 1
        assert result.success == 0
