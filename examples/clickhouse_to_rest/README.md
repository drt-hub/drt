# ClickHouse to REST API Example

This example demonstrates how to sync data from a ClickHouse database to a REST API.

## Setup

1.  **Install ClickHouse support**:
    ```bash
    pip install "drt-core[clickhouse]"
    ```

2.  **Configure Profile**:
    Copy `profiles.yml.example` to `~/.drt/profiles.yml` (or append to it) and update the credentials.

3.  **Run Sync**:
    ```bash
    drt run --select ch_sync --dry-run   # Preview
    drt run --select ch_sync             # Execute
    ```
