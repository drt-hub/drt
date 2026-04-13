# BigQuery → Discord Alert Pipeline

Send a Discord notification whenever new errors are detected in your BigQuery table.

## What it does

1. Queries BigQuery for recent error records
2. Sends a Discord message per error via Incoming Webhook
3. Uses incremental sync — only processes new errors since last run

## Setup

### 1. Configure your BigQuery connection

```bash
drt init   # select "bigquery" as source
```

Edit `~/.drt/profiles.yml`:

```yaml
default:
  type: bigquery
  project: your_gcp_project_id
  dataset: your_dataset
  keyfile_env: GOOGLE_APPLICATION_CREDENTIALS
```

### 2. Set your Discord webhook

Create an [Incoming Webhook](https://support.discord.com/hc/en-us/articles/228383668) in Discord, then set the environment variable:

```bash
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/YOUR/WEBHOOK"
```

### 3. Run

```bash
drt run
```

## Files

```
bigquery_to_discord/
├── drt_project.yml
├── syncs/
│   ├── alert_errors.yml
│   └── models/
│       └── recent_errors.sql
└── README.md
```
