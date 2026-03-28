# Examples

Ready-to-run drt configurations. Each directory is a self-contained project.

| Example | Source | Destination | Notes |
|---|---|---|---|
| [quickstart](./quickstart/) | DuckDB | httpbin.org/post | **Start here** — zero external credentials |
| [duckdb_to_slack](./duckdb_to_slack/) | DuckDB | Slack | Requires `SLACK_WEBHOOK_URL` |
| [notify_slack](./notify_slack/) | DuckDB | Slack | Alert-style notification pattern |
| [bigquery_to_hubspot](./bigquery_to_hubspot/) | BigQuery | HubSpot | Requires GCP + HubSpot credentials |
| [bigquery_to_github_actions](./bigquery_to_github_actions/) | BigQuery | GitHub Actions | Trigger workflows from query results |

Start with `quickstart/` if you are new to drt — it requires no cloud accounts and
runs end-to-end in about 5 minutes.
