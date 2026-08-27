# Triggering drt syncs via HTTP

`drt serve` starts a lightweight HTTP endpoint so you can trigger syncs from webhooks, CI systems, or other orchestrators.

No extra dependencies — stdlib only.

## Start the server

```bash
drt serve --port 8080 --token-env DRT_WEBHOOK_TOKEN
```

- `--host` (default `127.0.0.1`)
- `--port` (default `8080`)
- `--auth` (default `auto`) — `none`, `bearer`, `hmac`, or `auto` (bearer if the token env var is set, else none)
- `--token-env` (default `DRT_WEBHOOK_TOKEN`) — env var holding the bearer token
- `--hmac-secret-env` (default `DRT_WEBHOOK_HMAC_SECRET`) — env var holding the HMAC signing secret
- `--hmac-header` (defaults to `X-Hub-Signature-256`, or `Stripe-Signature` under `--hmac-scheme stripe`) — header carrying the HMAC signature
- `--hmac-scheme` (default `generic`) — `generic` (HMAC of the body: GitHub, Shopify, bare hex) or `stripe` (timestamped `t=...,v1=...`)
- `--hmac-tolerance` (default `300`) — replay window in seconds for `--hmac-scheme stripe`

```bash
export DRT_WEBHOOK_TOKEN="$(openssl rand -hex 32)"
drt serve --port 8080
```

## The delivery contract

This is a promise, not an implementation detail (#854):

- **A trigger accepted with `202` will run.** Nothing accepted is silently dropped.
- **Different syncs run concurrently.** Only same-sync overlap is serialized.
- **Same-sync triggers coalesce.** While a sync is running, at most one *pending*
  run exists per sync. Any trigger that doesn't start a run of its own is answered
  with that pending run's id and `"coalesced": true`. drt syncs are watermark-driven,
  so the one pending run picks up everything that accumulated: a queue of N would do
  the same work as a queue of 1.

  `"coalesced": true` therefore means **"your trigger did not start a run of its
  own; a pending run covers it"**. It does *not* distinguish "my trigger created the
  pending run" from "my trigger joined one already there", and clients shouldn't try
  to infer which happened. Either way the work is covered, and either way the run id
  you're given is the one to poll.
- Dry-run and real triggers coalesce **separately** — a real trigger is never folded
  into a dry-run preview.

Run ids are valid **for the lifetime of the `drt serve` process** — they are held in
memory, so `GET /runs/<id>` goes blank after a restart. Don't build a durable
workflow on them yet; [#762](https://github.com/drt-hub/drt/issues/762) (first-class
run ids) is the upgrade path, and only the id's durability will change, not this
contract.

## Endpoints

### `GET /health`

```bash
curl http://localhost:8080/health
# {"status": "ok", "version": "0.8.4"}
```

### `POST /sync/<name>`

Trigger a sync by name. Returns `202` immediately — the sync runs in the background,
so a sender with a delivery timeout (Pub/Sub push, GitHub, EventBridge) records
success as soon as the trigger is accepted.

```bash
curl -X POST http://localhost:8080/sync/sync_users \
  -H "Authorization: Bearer $DRT_WEBHOOK_TOKEN"
```

```json
{
  "run_id": "3f2a9c...",
  "sync_name": "sync_users",
  "state": "running",
  "dry_run": false,
  "coalesced": false,
  "url": "/runs/3f2a9c..."
}
```

Optional query parameters:

- `?dry_run=true` — preview without writing
- `?wait=true` — block until the run finishes and return the full result with the
  pre-#854 semantics (`200` success, `207` partial/failed). If the sync is already
  running, the request coalesces into the pending run and blocks until *that*
  completes.

### `GET /runs/<id>`

Authenticated, like the `POST` that created the run. The run id is a uuid4, but it
travels in the URL path, so it reaches every proxy access log in front of drt: it
identifies a run, it isn't a credential for reading one. The response carries the
full `SyncResult` and any error text.

```bash
curl http://localhost:8080/runs/3f2a9c... \
  -H "Authorization: Bearer $DRT_WEBHOOK_TOKEN"
```

```json
{
  "run_id": "3f2a9c...",
  "sync_name": "sync_users",
  "state": "success",
  "dry_run": false,
  "created_at": "2026-08-03T12:00:00+00:00",
  "started_at": "2026-08-03T12:00:00+00:00",
  "finished_at": "2026-08-03T12:00:01+00:00",
  "result": {"sync_name": "sync_users", "status": "success", "rows_synced": 42, "...": "..."},
  "error": null
}
```

`state` is one of `pending`, `running`, `success`, `partial`, `failed`, `error`.

### Status codes

| Code | Meaning |
|------|---------|
| 202  | trigger accepted — it **will** run; poll `GET /runs/<id>` |
| 200  | `?wait=true` only: sync succeeded / run found |
| 207  | `?wait=true` only: sync partial or failed (result body has details) |
| 400  | sync name missing from URL |
| 401  | auth missing or wrong (when auth enabled); applies to every route except `GET /health` |
| 404  | sync name not found in project, or unknown run id |
| 413  | request body over 1 MiB |
| 500  | unexpected error |

> Before v0.8.5, concurrent requests were answered `423 Locked` and **dropped** —
> the sender believed it delivered, and nothing ran. That status is gone; a
> concurrent trigger now coalesces and is never lost.

## Auth schemes

`GET /health` is always open, so a load balancer or `docker healthcheck` needs no
credential. Every other route is authenticated under `bearer` and `hmac`, including
`GET /runs/<id>`.

### Bearer token

```bash
drt serve --auth bearer --token-env DRT_WEBHOOK_TOKEN
```

Static shared secret in the `Authorization: Bearer ...` header, compared in
constant time.

### HMAC body signature

```bash
export DRT_WEBHOOK_HMAC_SECRET="$(openssl rand -hex 32)"
drt serve --auth hmac
```

Verifies an HMAC-SHA256 signature of the raw request body. Accepts GitHub's
`sha256=<hex>` format (default header `X-Hub-Signature-256`), bare hex, and
base64 digests — so GitHub and Shopify (`--hmac-header X-Shopify-Hmac-Sha256`)
work out of the box. Stripe signs a different payload and is a separate scheme —
see below.

`POST /sync/<name>` signs its raw body, including when that body is empty —
which is the normal way to trigger a sync:

```bash
SIG="sha256=$(printf '' | openssl dgst -sha256 -hmac "$DRT_WEBHOOK_HMAC_SECRET" | sed 's/^.*= //')"
curl -X POST http://localhost:8080/sync/my_sync -H "X-Hub-Signature-256: $SIG"
```

A `GET` has no body, so `GET /runs/<id>` signs the **request path** instead, under
a key derived from your secret. Binding the path in means a signature issued for
one run id cannot read another (#936), and the derived key means a captured `GET`
signature cannot be replayed as a `POST` whose body is crafted to match:

```bash
RUN_ID=3f2a9c...
# The GET key is HMAC(secret, "drt/serve/v1/get-path"); openssl prints it as hex.
GET_KEY=$(printf 'drt/serve/v1/get-path' \
  | openssl mac -digest SHA256 -macopt "key:$DRT_WEBHOOK_HMAC_SECRET" HMAC)
SIG="sha256=$(printf "/runs/$RUN_ID" \
  | openssl mac -digest SHA256 -macopt "hexkey:$GET_KEY" HMAC | tr 'A-Z' 'a-z')"
curl "http://localhost:8080/runs/$RUN_ID" -H "X-Hub-Signature-256: $SIG"
```

The signature covers the path exactly as sent, so it is per-run rather than a
constant you can compute once and reuse everywhere.

It proves knowledge of the secret without putting the secret on the wire, but it is
a static value, not a per-request signature. If you poll run state from somewhere
you wouldn't trust with a replayable credential, run `--auth bearer` for that path
and verify webhook bodies at a proxy instead.

### Stripe timestamped signature

```bash
export DRT_WEBHOOK_HMAC_SECRET="whsec_..."   # from the Stripe Dashboard
drt serve --auth hmac --hmac-scheme stripe
```

Stripe does not sign the body alone. Its `Stripe-Signature` header carries a
timestamp and one or more signatures:

```
Stripe-Signature: t=1492774577,v1=5257a869e7ec...,v0=6ffbb59b2300...
```

and the signed payload is `<t>.<body>`. drt verifies the `v1` signature over that
string and rejects a delivery whose `t` is outside `--hmac-tolerance` (default
300s, matching Stripe's own libraries), which is what stops a captured request
from being replayed later.

Three details worth knowing:

- **`v0` is ignored.** Stripe attaches a deliberately fake `v0` to test events;
  its docs say to ignore every scheme that isn't `v1`, because accepting one
  would be a downgrade attack. A header carrying only `v0` is rejected.
- **Secret rotation works.** Rolling an endpoint secret leaves the old one live
  for up to 24 hours and Stripe sends one `v1` per active secret. Any matching
  `v1` is accepted, so a roll doesn't drop deliveries.
- **Don't set `--hmac-tolerance 0`.** It is rejected: zero would disable the
  recency check rather than tighten it.

The raw body must reach drt byte-for-byte. Any proxy that re-encodes, reformats
or re-serializes JSON on the way through invalidates the signature — this is the
most common cause of Stripe verification failures. `drt serve` speaks plain HTTP,
and Stripe requires HTTPS for live endpoints, so a TLS-terminating proxy is
required; make sure it passes the body through untouched.

**`--hmac-scheme stripe` covers POSTs only.** Stripe never sends a `GET`, so it
defines no signature shape for one, and `GET /runs/<id>` answers `401` under this
scheme — deliberately, even if you send a `Stripe-Signature` header. A Stripe
signature is computed over `<t>.<body>`, which for a bodyless `GET` carries no
path: one captured header would then read *any* run id for the whole tolerance
window, which is exactly the replay the generic scheme's path-bound `GET`
signature avoids. If you need to poll run state, run a second listener with
`--auth bearer` for that, or query it from the same process that triggered the
sync using the run id returned by the 202.

Pub/Sub push authenticates with an **OIDC JWT**, not a body signature — that
verification path is [#903](https://github.com/drt-hub/drt/issues/903), and until it lands Pub/Sub still needs
a verifying proxy in front.

## Use cases

### GitHub webhook → run sync on push

Point a repository webhook at `https://your-drt-host/sync/sync_users` with a
secret, and run `drt serve --auth hmac` with the same secret in
`DRT_WEBHOOK_HMAC_SECRET`. GitHub signs each delivery; drt verifies it.

### dbt Cloud job completion → sync downstream

Add a post-job webhook in dbt Cloud pointing to `https://your-drt-host/sync/sync_users`
with the bearer token. If your workflow needs the result in the webhook response,
use `?wait=true`.

## Design notes

- **No persistent state** beyond what `StateManager` already does (`.drt/state.json`).
  One `StateManager` instance is shared across all runs so concurrent syncs can't
  race the state file.
- **Stdlib only** — `http.server.ThreadingHTTPServer`, no FastAPI/uvicorn. Keeps
  drt-core dependency-free.
- **Ctrl+C exits immediately**; in-flight runs are abandoned (unchanged from
  previous behaviour). Pending coalesced runs die with the process — the 202
  promise holds only while the process lives, which is why senders should still
  monitor sync freshness (`alerts.on_degraded`).

For production, run behind a reverse proxy (nginx, Caddy) for TLS and rate limiting.
