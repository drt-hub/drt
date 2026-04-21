# MySQL → Slack Alert Pipeline

Send a Slack notification whenever orders fail in your MySQL database.

## What it does

1. Queries MySQL for failed orders
2. Sends a Slack message per failed order via Incoming Webhook
3. Uses incremental sync — only processes new failures since last run

## Setup

### 1. Configure your MySQL connection

Edit `~/.drt/profiles.yml`:

```yaml
default:
  type: mysql
  host: localhost
  port: 3306
  dbname: your_database
  user: your_user
  password_env: MYSQL_PASSWORD
```

### 2. Set Slack webhook URL

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"
```

Create an [Incoming Webhook](https://api.slack.com/messaging/webhooks) in your Slack workspace.

### 3. Create sample data (optional)

```sql
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100),
    order_total DECIMAL(10,2),
    status VARCHAR(20),
    error_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (customer_name, order_total, status, error_reason) VALUES
    ('Alice', 99.99, 'failed', 'Payment declined'),
    ('Bob', 149.50, 'failed', 'Insufficient inventory'),
    ('Carol', 75.00, 'completed', NULL);
```

### 4. Run

```bash
cd examples/mysql_to_slack
drt run --dry-run   # preview
drt run             # send alerts
drt status          # check results
```

## Customization

- Edit `syncs/models/failed_orders.sql` to change the query
- Edit `syncs/alert_failed_orders.yml` to customize the Slack message format
- Change `batch_size` for bulk processing
- Use `block_kit: true` for rich Slack messages with Block Kit JSON