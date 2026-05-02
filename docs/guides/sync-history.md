# Sync execution history

Every `drt run` invocation appends a record to a per-sync history file. Operators can browse past executions from the CLI, pipe the output into shell scripts, or query it from an AI agent via the MCP server. This is the on-call companion to [sync failure alerts](alerts.md): alerts notify you in real time, history answers the follow-up "what happened last week?".

## What gets recorded

For every non-dry-run sync, drt appends one JSON object to `.drt/history/<sync_name>.jsonl` with these fields:

```json
{
  "sync_name": "post_users",
  "started_at": "2026-05-02T15:30:00.000+00:00",
  "completed_at": "2026-05-02T15:30:42.318+00:00",
  "duration_seconds": 42.3,
  "status": "success",
  "records_synced": 1500,
  "records_failed": 0,
  "errors": [],
  "cursor_value_used": "2026-05-01T00:00:00",
  "dry_run": false
}
```

`status` is one of `success` / `partial` / `failed`. `errors` is truncated to the first 5 messages to bound disk growth on long-failing syncs.

> **Dry-run is skipped.** `drt run --dry-run` does not write to history — it is for previewing, not auditing.

## Configure retention

By default drt keeps 30 days of history per sync, pruning older entries on each append. Override in `drt_project.yml`:

```yaml
name: my-project
profile: default
history:
  enabled: true            # default: true
  retention_days: 30       # default: 30
```

Set `enabled: false` to disable history entirely (the directory will not be created).

## CLI usage

### Show recent runs across all syncs

```bash
drt status --history
```

```
                          Execution history (all syncs)
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Started             ┃ Sync         ┃ Status  ┃ Synced ┃ Failed ┃ Duration ┃ Error           ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ 2026-05-02 15:30:00 │ post_users   │ success │   1500 │      0 │   42.3s │                 │
│ 2026-05-02 02:00:00 │ alert_errors │ partial │     12 │      3 │    1.9s │ HTTP 500 ...    │
│ 2026-05-01 15:30:00 │ post_users   │ success │   1490 │      0 │   41.2s │                 │
└─────────────────────┴──────────────┴─────────┴────────┴────────┴─────────┴─────────────────┘
```

### Show only one sync

```bash
drt status --history --sync post_users --limit 50
```

### Pipe to JSON (CI / scripts)

```bash
drt status --history --output json | jq '.entries[] | select(.status == "failed")'
```

The `entries` array is always newest-first.

## MCP tool: `drt_get_history`

When `drt mcp run` is connected to Claude / Cursor / any MCP client, the `drt_get_history` tool exposes the same data programmatically:

```jsonc
// MCP request
{ "tool": "drt_get_history", "arguments": { "sync_name": "post_users", "limit": 5 } }

// Response (abbreviated)
{
  "entries": [
    { "sync_name": "post_users", "started_at": "...", "status": "success", ... },
    ...
  ]
}
```

This makes "did the daily user_sync run last night?" a question your AI assistant can answer without leaving the chat.

## Operational tips

- **Disk usage is bounded.** Per-sync JSONL with default retention is typically well under 1 MB even on chatty hourly syncs (~720 entries × 30 days × ~500 B/entry ≈ 11 MB worst case).
- **Per-sync files are append-safe.** POSIX `O_APPEND` makes individual line writes atomic, so `drt run --threads 8` running multiple syncs in parallel won't corrupt history.
- **History never blocks a sync.** Disk-full or permission errors are logged at WARNING and swallowed — sync correctness must not depend on telemetry persistence.
- **Pair with alerts.** Use [`alerts.on_failure`](alerts.md) for real-time pages and `drt status --history` for the morning review of "what failed overnight".

## See also

- [API reference: `drt_project.yml` history block](../llm/API_REFERENCE.md)
- [Alerts guide](alerts.md) — the real-time counterpart to history
- [`drt/state/history.py`](../../drt/state/history.py) — `HistoryEntry` model and `HistoryManager`
