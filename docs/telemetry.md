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
- ❌ IP address (drt does not include client IP in the request body AND explicitly suppresses PostHog server-side IP capture — see the "A note on IP addresses" section below)
- ❌ Geo data (country / region / city — explicitly suppressed via `$geoip_disable`)
- ❌ PostHog person profile (explicitly suppressed via `$process_person_profile: false`)
- ❌ Row contents
- ❌ Column names
- ❌ Schema names

### A note on IP addresses

PostHog's capture endpoint (`/i/v0/e/`) historically auto-attached a `$ip` property server-side from the TCP source IP, even though drt never sent one in the request body. To prevent that capture, drt now **explicitly includes three PostHog meta-properties in every payload**:

| Property | Value | Effect |
|---|---|---|
| `$ip` | `""` (empty string) | Tells PostHog to record an empty IP for this event rather than the TCP source. |
| `$geoip_disable` | `true` | Disables PostHog's GeoIP resolution for this event so no country / region / city is derived. |
| `$process_person_profile` | `false` | Prevents PostHog from materializing a per-`distinct_id` profile, further reducing what's stored downstream. |

These three properties form a **defense-in-depth** layer in front of the maintainer-side PostHog project setting (**Settings > Project > General > Privacy > "IP data capture configuration" > "Discard client IP data"**, which is ON for the drt project), so even if the project setting is ever flipped back off the payload still tells PostHog not to capture IPs. The combination is verifiable with a self-hosted PostHog or via the `drt config show-telemetry` preview.

The privacy claim is therefore stronger than "drt does not transmit your IP": **drt actively instructs the backend not to record one.** This makes it possible to keep drt's GDPR posture clean without depending on a configurable backend setting that an operator might change later.

drt does not require PostHog specifically — `DRT_TELEMETRY_ENDPOINT` accepts any URL that returns 2xx for a JSON POST. Backends that ignore the PostHog meta-property convention should be paired with their own IP-stripping logic.

## GDPR disclosure (EU / EEA opt-ins)

Lawful basis for processing is your opt-in consent (GDPR Art. 6(1)(a)). The data is stored in PostHog Cloud EU (EU-hosted). The processor, PostHog Inc. (United States), may have technical access from outside the EU; the international transfer safeguard is the Standard Contractual Clauses (Art. 46(2)(c)) included in the signed Data Processing Agreement.

- **Destination**: `https://eu.i.posthog.com/i/v0/e/` — PostHog Cloud EU, operated by PostHog Inc. (EU data residency). Override with `DRT_TELEMETRY_ENDPOINT` (e.g. `https://us.i.posthog.com/i/v0/e/` for PostHog US).
- **Data controller**: K. Masuda (natural person, drt OSS maintainer).
  drt is currently a single-maintainer OSS project. If drt is transferred
  to a legal entity in the future, the data controller role transfers
  with it; the affected release will update this section and the
  CHANGELOG will note the controller change. Users opted in at the time
  of transfer can re-confirm or revoke via `drt config unset telemetry.enabled`.
- **Retention**: 1 year. Events stored in PostHog Cloud EU are deleted
  after 1 year by the project-level data retention policy (Free plan
  default). Earlier drafts of this document committed to reducing
  retention to 90 days via a scheduled API cleanup (tracked in
  [#482](https://github.com/drt-hub/drt/issues/482)), but the public
  PostHog API does not expose an endpoint to bulk-delete events by
  `event` name and `timestamp` — the deletion workaround turned out to
  be infeasible. Instead, drt removed the PII at the source by sending
  PostHog the `$ip` / `$geoip_disable` / `$process_person_profile`
  meta-properties on every event (see the "A note on IP addresses"
  section above), so no IP, no GeoIP-derived geo, and no person profile
  is created or stored on the maintainer side. The 1-year retention is
  therefore on **PII-free** data: `distinct_id` is a random install
  UUID with no identity link, and the allow-list payload carries no
  user content. We consider the data-minimization concern that
  originally motivated the 90-day target satisfied at the
  data-collection layer, which is a stronger posture than retention
  alone.
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

1. **Disable IP data capture** at the project level: **Settings > Project > General > Privacy > "IP data capture configuration"** → turn on **"Discard client IP data"** ([source](https://posthog.com/docs/privacy/gdpr-compliance)). On PostHog Cloud EU this is the default for new projects, so the toggle may already be on. drt also sends the `$ip` / `$geoip_disable` / `$process_person_profile` meta-properties on every event as defense-in-depth, but the project-level setting is the belt to those suspenders — keep both on.
2. Sign the self-serve DPA at `app.posthog.com/legal` ([source](https://posthog.com/dpa)).

### Why no automated retention cleanup

The original plan ([#482](https://github.com/drt-hub/drt/issues/482)) was to schedule a daily PostHog API call deleting `sync_completed` events older than 90 days, on the assumption that PostHog exposed a public bulk-delete endpoint. It does not — only person-based deletion is public (the `async_deletion` model is backend-only and not surfaced via REST). A first implementation attempt ([PR #535](https://github.com/drt-hub/drt/pull/535), reverted by the same PR that introduced this paragraph) hit 404 against the assumed endpoint. drt's privacy posture now rests on **not collecting PII in the first place** rather than retention cleanup, which is a stronger guarantee and removes the operational burden.

Before each release with telemetry enabled, populate the controller / retention / erasure-contact placeholders in the GDPR disclosure section above.
