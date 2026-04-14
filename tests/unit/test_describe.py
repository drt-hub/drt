import pytest

from drt.config.models import (
    ClickHouseDestinationConfig,
    DiscordDestinationConfig,
    FileDestinationConfig,
    GitHubActionsDestinationConfig,
    GoogleAdsDestinationConfig,
    GoogleSheetsDestinationConfig,
    HubSpotDestinationConfig,
    JiraDestinationConfig,
    LinearDestinationConfig,
    MySQLDestinationConfig,
    ParquetDestinationConfig,
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SendGridDestinationConfig,
    SlackDestinationConfig,
    TeamsDestinationConfig,
)

ALL_DESTINATIONS = [
    RestApiDestinationConfig(type="rest_api", url="https://api.test.com"),
    SlackDestinationConfig(type="slack", webhook_url="https://hooks.slack.com/test"),
    DiscordDestinationConfig(type="discord", webhook_url="https://discord.com/api/webhooks/test"),
    GitHubActionsDestinationConfig(type="github_actions", owner="owner", repo="repo", 
        workflow_id="workflow.yml"),
    GoogleAdsDestinationConfig(type="google_ads", customer_id="1234567890", 
        conversion_action="customers/123/conversionActions/456"),
    GoogleSheetsDestinationConfig(type="google_sheets", spreadsheet_id="sheet123"),
    HubSpotDestinationConfig(type="hubspot"),
    SendGridDestinationConfig(type="sendgrid", from_email="test@example.com", 
        subject_template="subject", body_template="body"),
    LinearDestinationConfig(type="linear", title_template="title",
        description_template="description"),
    PostgresDestinationConfig(type="postgres", host="localhost", dbname="db",
        table="public.table", upsert_key=["id"]),
    MySQLDestinationConfig(type="mysql", host="localhost", dbname="db", table="table",
        upsert_key=["id"]),
    TeamsDestinationConfig(type="teams", webhook_url="https://teams.test/webhook"),
    JiraDestinationConfig(type="jira", base_url_env="JIRA_URL", email_env="JIRA_EMAIL", 
        token_env="JIRA_TOKEN", project_key="TEST", summary_template="summary",
        description_template="description"),
    ClickHouseDestinationConfig(type="clickhouse", host="localhost", database="db",
        table="table"),
    ParquetDestinationConfig(type="parquet", path="output.parquet"),
    FileDestinationConfig(type="file", path="output.csv"),
]

@pytest.mark.parametrize("dest", ALL_DESTINATIONS, ids=lambda d: d.type)
def test_destination_describe_returns_string(dest):
    result = dest.describe()

    assert isinstance(result, str)
    assert len(result) > 0
    assert dest.type in result.lower()