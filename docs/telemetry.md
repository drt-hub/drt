# Telemetry

drt is opt-in for telemetry. Nothing is sent until you enable it.

## TL;DR

```bash
# opt in
drt config set telemetry.enabled true

# opt out (or never opt in)
drt config set telemetry.enabled false

# universal kill switch (overrides everything, including env var)
DO_NOT_TRACK=1 drt run

# preview the next payload without sending
drt config show-telemetry
```

## What is collected

When you opt in, drt sends one event per `drt run` invocation per sync:

| Field | Example | Why |
|---|---|---|
| `event` | `"sync_completed"` | Event name |
| `distinct_id` | `550e8400-e29b-41d4-a716-446655440000` | Random UUID generated once per machine, stored in `~/.drt/.anonymous_id`. Lets us count active machines without identifying them. Delete the file to rotate. |
| `drt_version` | `"0.6.2"` | Helps us know which versions are still in use |
| `python_version` | `"3.12"` | Distribution decisions for Python support matrix |
| `os` | `"linux"` / `"darwin"` / `"windows"` | OS distribution |
| `source_type` | `"bigquery"` | Which source connectors are popular |
| `destination_type` | `"slack"` | Which destination connectors are popular |
| `sync_mode` | `"incremental"` / `"full"` / `"upsert"` / `"replace"` | Which modes get used |
| `rows_synced` | `42` | Approximate scale of usage. Not aggregated to a person. |
| `duration_seconds` | `1.5` | Distribution of sync durations (perf priorities) |
| `status` | `"success"` / `"partial"` / `"failed"` | Reliability signal |
| `timestamp` | `"2026-05-01T12:34:56Z"` | When the event happened |

## What is NOT collected

The payload is built by an **allow-list** function, [`build_sync_completed_payload()`](../drt/telemetry.py). To add a field, the function signature has to change — there is no other path. Specifically excluded from the body drt sends:

- ❌ Sync names (e.g. `post_users`)
- ❌ SQL queries / model contents
- ❌ Destination URLs (no webhook URLs, API endpoints)
- ❌ Credentials of any kind
- ❌ Project file paths
- ❌ Hostname / username
- ❌ IP address (drt does not include client IP in the request body)
- ❌ Row contents
- ❌ Column names
- ❌ Schema names

### A note on IP addresses

PostHog's capture endpoint (`/i/v0/e/`) auto-attaches a `$ip` property server-side from the TCP source IP, even though drt never sends one. This is verifiable: a `sync_completed` event captured by a self-hosted PostHog shows `"$ip": "192.168.x.x"` in the stored properties despite the request body containing only the allow-list above.

If the maintainer-side ingestion endpoint should not retain client IPs:
- Configure the PostHog project with **GeoIP/IP capture disabled**, or
- Run a thin proxy in front of capture that strips `$ip` before forwarding, or
- Substitute the backend with a custom collector (drt does not require PostHog specifically — `DRT_TELEMETRY_ENDPOINT` accepts any URL that returns 2xx for a JSON POST).

The privacy claim is "drt does not transmit your IP." It is not "the backend you POST to will not log it." Operators of the receiving service are responsible for IP retention policy.

## GDPR disclosure (EU / EEA opt-ins)

Lawful basis for processing is your opt-in consent (GDPR Art. 6(1)(a); Art. 49(1)(a) for the US transfer).

- **Destination**: `https://eu.i.posthog.com/i/v0/e/` — PostHog Cloud EU, operated by PostHog Inc. (EU data residency). Override with `DRT_TELEMETRY_ENDPOINT` (e.g. `https://us.i.posthog.com/i/v0/e/` for PostHog US).
- **Data controller**: K. Masuda (natural person, drt OSS maintainer).
  drt is currently a single-maintainer OSS project. If drt is transferred
  to a legal entity in the future, the data controller role transfers
  with it; the affected release will update this section and the
  CHANGELOG will note the controller change. Users opted in at the time
  of transfer can re-confirm or revoke via `drt config unset telemetry.enabled`.
- **Retention**: 1 year. Events stored in PostHog Cloud EU are deleted
  after 1 year by the project-level data retention policy (Free plan
  default). A followup (#TODO_FILL_WITH_FOLLOWUP_NUMBER) will reduce this
  to 90 days via scheduled API-based deletion.
- **Erasure / data subject requests**: `drt.hub.dev@gmail.com`. Deleting
  `~/.drt/.anonymous_id` rotates your `distinct_id` going forward but
  does not retroactively scrub past events; use the contact above for
  past events.

## How to verify

Before opting in, you can see exactly what would be sent:

```bash
drt config show-telemetry
```

You can also point telemetry at your own listener and watch the wire:

```bash
# terminal 1: capture POSTed bodies
python3 -c "
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n=int(self.headers.get('content-length',0))
        print(self.rfile.read(n).decode())
        self.send_response(204); self.end_headers()
HTTPServer(('127.0.0.1',8000), H).serve_forever()
"

# terminal 2: run with telemetry redirected
DRT_TELEMETRY_ENDPOINT=http://localhost:8000/ \
DRT_TELEMETRY_API_KEY=phc_local_test \
DRT_TELEMETRY=1 \
drt run
```

The full request body will print in terminal 1.

## Self-host PostHog (full E2E)

```bash
git clone https://github.com/PostHog/posthog /tmp/posthog
cd /tmp/posthog && docker compose -f docker-compose.dev.yml up -d
# visit http://localhost:8000, sign up, copy the project API key (phc_...)

DRT_TELEMETRY_ENDPOINT=http://localhost:8000/i/v0/e/ \
DRT_TELEMETRY_API_KEY=phc_<your_key> \
drt config set telemetry.enabled true
drt run
# events appear under Activity → Live events
```

## How to opt out

Any of the following disables telemetry:

- `drt config set telemetry.enabled false` (persistent)
- `DRT_TELEMETRY=0` (per-invocation)
- `DO_NOT_TRACK=1` (universal kill switch — overrides config and env)
- Delete `~/.drt/telemetry.json` and `~/.drt/.anonymous_id`

## Implementation

All telemetry code lives in a single file: [`drt/telemetry.py`](../drt/telemetry.py). It uses only the Python standard library (`urllib.request`). The POST runs on a daemon thread joined via `atexit` with a 2 s timeout: normal `drt run` exits wait briefly for the POST to complete, while abnormal exits (SIGTERM, SIGINT) skip the wait. All exceptions on the send path are swallowed at DEBUG level so telemetry can never crash the user's command.

Wire format follows PostHog's capture endpoint (`POST /i/v0/e/`), which works against PostHog Cloud and self-hosted PostHog with no code changes. The endpoint and API key are both overridable via environment variables.

## For maintainers

### Release-time API key injection

`_DEFAULT_API_KEY` ships as `None` in source. Without an injection step at release time, `is_enabled()` short-circuits to `False` regardless of user opt-in — so the package on PyPI is physically incapable of sending until a maintainer wires in a key.

Recommended release flow:

1. Store the PostHog write key as a repository secret named `POSTHOG_WRITE_KEY`.
2. In the release workflow, before `python -m build`, substitute the placeholder:
   ```bash
   python -c "import pathlib, os; \
   p = pathlib.Path('drt/telemetry.py'); \
   p.write_text(p.read_text().replace('_DEFAULT_API_KEY: str | None = None', \
     f'_DEFAULT_API_KEY: str | None = \"{os.environ[\"POSTHOG_WRITE_KEY\"]}\"'))"
   ```
3. Add a smoke check that fails the release if the substitution did not happen — for example, `python -c "from drt import telemetry; assert telemetry._DEFAULT_API_KEY"`.

If the inject step is skipped, telemetry silently no-ops forever — fail-safe but invisible. The smoke check is what catches a missed inject.

### PostHog project setup (one-time)

1. Disable IP data capture: Settings > Project > General ([source](https://posthog.com/docs/privacy/gdpr-compliance)).
2. Sign the self-serve DPA at `app.posthog.com/legal` ([source](https://posthog.com/dpa)).

Before each release with telemetry enabled, populate the controller / retention / erasure-contact placeholders in the GDPR disclosure section above.
