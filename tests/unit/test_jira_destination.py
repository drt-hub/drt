"""Unit tests for JiraDestination with mocked httpx client."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import JiraDestinationConfig, RateLimitConfig, RetryConfig, SyncOptions
from drt.destinations.jira import JiraDestination


def _options(on_error: str = "skip") -> SyncOptions:
    return SyncOptions(
        rate_limit=RateLimitConfig(requests_per_second=1000),
        retry=RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        on_error=on_error,
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


def _http_error(status: int, text: str) -> httpx.HTTPStatusError:
    response = httpx.Response(
        status_code=status,
        text=text,
        request=httpx.Request("POST", "http://x"),
    )
    return httpx.HTTPStatusError(
        message=f"HTTP {status}",
        request=response.request,
        response=response,
    )


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
        # Updates should not attempt project/type reassignment by default.
        assert "project" not in kwargs["json"]["fields"]
        assert "issuetype" not in kwargs["json"]["fields"]
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

    def test_on_error_fail_stops_within_batch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("JIRA_BASE_URL", "https://myorg.atlassian.net")
        monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "token-123")

        rows = [
            {"metric": "cpu", "value": 95, "threshold": 80},
            {"metric": "memory", "value": 88, "threshold": 75},
        ]

        with patch("drt.destinations.jira.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(400, "bad request")

            result = JiraDestination().load(rows, _config(), _options(on_error="fail"))

        assert result.failed == 1
        assert result.success == 0
        assert mock_client.post.call_count == 1

    def test_non_serializable_row_preview_is_safe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("JIRA_BASE_URL", "https://myorg.atlassian.net")
        monkeypatch.setenv("JIRA_EMAIL", "bot@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "token-123")

        row = {
            "metric": "cpu",
            "value": 95,
            "threshold": 80,
            "seen_at": datetime(2026, 1, 1, 12, 0, 0),
        }

        with patch("drt.destinations.jira.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(400, "bad request")

            result = JiraDestination().load([row], _config(), _options())

        assert result.failed == 1
        assert result.row_errors
        assert "seen_at" in result.row_errors[0].record_preview
