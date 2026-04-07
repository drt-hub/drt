# PostgreSQL → Slack Alert Pipeline

Send a Slack notification whenever new high-value orders appear in your PostgreSQL database.

## What it does

1. Queries PostgreSQL for orders above a threshold (e.g. > £1,000)
2. Sends a Slack message per order via Incoming Webhook
3. Uses incremental sync — only processes new orders since last run

## Setup

### 1. Configure your PostgreSQL connection

```bash
drt init   # select "postgres" as source
```

Edit `profiles.yml`:

```yaml
default:
  type: postgres
  host: localhost
  port: 5432
  dbname: your_database
  user: your_user
  password_env: POSTGRES_PASSWORD
```

### 2. Set your Slack webhook

Create an [Incoming Webhook](https://api.slack.com/messaging/webhooks) in Slack, then set the environment variable:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### 3. Run

```bash
drt run
```

## Files

```
postgres_to_slack/
├── drt_project.yml
├── models/
│   └── high_value_orders.sql
├── syncs/
│   └── alert_high_value_orders.yml
└── README.md
```
