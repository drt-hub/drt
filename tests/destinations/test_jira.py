"""Unit tests for JiraDestination with mocked httpx client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import JiraDestinationConfig, RateLimitConfig, RetryConfig, SyncOptions
from drt.destinations.jira import JiraDestination


def _options() -> SyncOptions:
    return SyncOptions(
        rate_limit=RateLimitConfig(requests_per_second=1000),
        retry=RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        on_error="skip",
    )


def _config(**overrides: object) -> JiraDestinationConfig:
    data: dict[str, object] = {
        "type": "jira",
        "base_url_env": "JIRA_BASE_URL",
        "email_env": "JIRA_EMAIL",
        "token_env": "JIRA_API_TOKEN",
        "project_key": "ENG",
        "issue_type": "Task",
        "summary_template": "Alert: {{ row.metric }} exceeded threshold",
        "description_template": "Value: {{ row.value }}, Threshold: {{ row.threshold }}",
    }
    data.update(overrides)
    return JiraDestinationConfig(**data)


class TestJiraDestination:
    def test_create_issue_uses_post(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("JIRA_BASE_URL", "https://myorg.atlassian.net")
        monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "token-123")

        row = {"metric": "cpu", "value": 95, "threshold": 80}
        config = _config()

        with patch("drt.destinations.jira.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            response = MagicMock()
            response.raise_for_status.return_value = None
            mock_client.post.return_value = response

            result = JiraDestination().load([row], config, _options())

        assert result.success == 1
        assert result.failed == 0
        mock_client.post.assert_called_once()
        args, kwargs = mock_client.post.call_args
        assert args[0] == "https://myorg.atlassian.net/rest/api/3/issue"
        assert kwargs["json"]["fields"]["project"]["key"] == "ENG"
        assert kwargs["json"]["fields"]["issuetype"]["name"] == "Task"
        assert kwargs["json"]["fields"]["summary"] == "Alert: cpu exceeded threshold"

    def test_update_issue_uses_put_when_issue_id_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("JIRA_BASE_URL", "https://myorg.atlassian.net")
        monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "token-123")

        row = {
            "issue_id": "ENG-123",
            "metric": "memory",
            "value": 88,
            "threshold": 75,
        }
        config = _config()

        with patch("drt.destinations.jira.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            response = MagicMock()
            response.raise_for_status.return_value = None
            mock_client.put.return_value = response

            result = JiraDestination().load([row], config, _options())

        assert result.success == 1
        assert result.failed == 0
        mock_client.put.assert_called_once()
        args, kwargs = mock_client.put.call_args
        assert args[0] == "https://myorg.atlassian.net/rest/api/3/issue/ENG-123"
        assert kwargs["json"]["fields"]["summary"] == "Alert: memory exceeded threshold"

    def test_templates_render_for_project_and_issue_type(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("JIRA_BASE_URL", "https://myorg.atlassian.net")
        monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "token-123")

        row = {
            "project": "OPS",
            "itype": "Bug",
            "metric": "latency",
            "value": 420,
            "threshold": 200,
        }
        config = _config(
            project_key="{{ row.project }}",
            issue_type="{{ row.itype }}",
        )

        with patch("drt.destinations.jira.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            response = MagicMock()
            response.raise_for_status.return_value = None
            mock_client.post.return_value = response

            result = JiraDestination().load([row], config, _options())

        assert result.success == 1
        _, kwargs = mock_client.post.call_args
        assert kwargs["json"]["fields"]["project"]["key"] == "OPS"
        assert kwargs["json"]["fields"]["issuetype"]["name"] == "Bug"

    def test_missing_env_var_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("JIRA_BASE_URL", raising=False)
        monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "token-123")

        with pytest.raises(ValueError, match="JIRA_BASE_URL"):
            JiraDestination().load(
                [{"metric": "cpu", "value": 90, "threshold": 80}],
                _config(),
                _options(),
            )
