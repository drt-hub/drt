# ADR 0014 — Exact-replay apply: encrypted plan payloads (`--exact`)

- **Status:** Accepted 2026-10-28 (owner confirmed all four decisions as proposed — see "Decisions").
- **Issue:** [#1221](https://github.com/drt-hub/drt/issues/1221) (epic [#1227](https://github.com/drt-hub/drt/issues/1227)).
- **Relates to:** [ADR 0013](0013-plan-apply-lineage.md) (plan/apply lineage); the plan
  guide (`docs/guides/plan.md`); `docs/guides/secrets-encryption.md` (age/X25519, #303);
  [ADR 0004](0004-streaming-and-event-triggered-syncs.md) (no daemon, no hosted store).

## Context

`drt apply` today is **verify-by-replan**: it re-extracts the source, recomputes the change
set and refuses to write if it differs from the reviewed plan. That guarantees the *change
set* (keys and actions, plus a keyed hash of each row to write) is unchanged. It does not
guarantee the *row values written are the ones a reviewer saw*, because nobody saw values:
`plan.json` is deliberately redacted so it can be posted as a PR comment or CI artifact.

Two situations need more:

1. **Four-eyes / regulated changes.** An approver signs off on a plan, and the apply must
   write *exactly that data*, not whatever the source holds a minute later.
2. **Expensive or non-repeatable extraction.** Re-running the model at apply time is slow,
   costs warehouse credits, or reads a source that has moved on.

## Decision

### 1. Opt-in, and never in `plan.json`

`plan.json` stays redacted and shareable. Values live in a **sidecar** next to it:

```
drt plan orders --out plan.json --include-values --recipient age1...   # also writes plan.json.payload
drt apply plan.json --exact --approved-by alice                          # reads plan.json.payload
```

`plan.json` gains only `payload: {sha256, rows, bytes}` (omitted without
`--include-values`). The existing HMAC `seal` covers it, so the sidecar cannot be swapped
for another one. Plaintext values are never written to disk by drt.

### 2. Encryption: age X25519, recipients separate from the decryptor

The payload is encrypted to one or more **age recipient public keys** (`--recipient`, or
`DRT_PLAN_RECIPIENTS`), using the same `pyrage` dependency as `secrets.toml` encryption.
The planner needs only public keys. `drt apply --exact` decrypts with the age identity in
`DRT_SECRETS_KEY`. So the machine (or CI job) that plans need not be able to read the data
it staged, and the approver's apply environment is the only place the values reappear. A
rejected recipient is never echoed; if it starts with `AGE-SECRET-KEY-`, drt explains that
it is a private identity and asks for the `age1...` public key instead.

### 3. Apply sends the payload and re-verifies the destination

`--exact` skips source extraction entirely. The decrypted rows are fed to the same
`compute_diff` the plan used, in place of the extracted records, against the destination as
it is now. The result must equal the reviewed entries (same keys, actions and `value_hash`)
or apply aborts exactly as it does today on drift. So drift in the **destination** is still
caught; drift in the **source** is, by design, ignored (that is the point of the mode).
Exact delivery bypasses the warehouse idempotency ledger: the live destination re-diff is
the correctness check, while ledger filtering after that check could silently skip a row the
verified plan requires. Because replay does not read the source, it also never resolves or
advances incremental cursor/watermark state. It still records `last_run_at`, status and
records synced while carrying the previous cursor forward unchanged.

### 4. Bounds and hygiene

- `sync.mask` and `metadata_columns` are applied **before** encryption; capturing metadata
  after injection keeps verification's row classification identical for existing rows. The
  delivery run replaces run-specific metadata values through the normal metadata step.
- Limits: `--max-payload-rows` (default 10,000) and a byte cap; planning fails clearly
  beyond them rather than producing a huge file.
- `--max-age` already bounds how old a plan may be; it covers the payload too.
- Apply opens the sidecar without following symlinks, requires a regular file, and reads at
  most the sealed byte count plus one byte. Directory apply decrypts one plan at a time.
- Sidecar writes use a private temporary sibling plus `os.replace`. Replacing an existing
  plan/payload pair is not atomic across both files, but the plan's sealed payload SHA-256
  means a mixed pair is always refused.
- The payload is **deleted after a successful apply, including a no-op** (`--keep-payload`
  to retain it), but only when the path still names the exact file that was verified; a
  replacement is never unlinked. A failed apply leaves it, so a retry with a new plan can
  reuse nothing by accident.
- Exact replay does not install the DLQ observer: persisting a failed exact row would write
  the decrypted payload back to durable local or remote state.
- The docs state the PII-at-rest trade-off plainly: an encrypted file of real rows exists
  between plan and apply.

### 5. Surfaces

- CLI: `drt plan --include-values --recipient`, `drt apply --exact`.
- MCP: **not exposed** in v1.3. An agent should not stage or replay row values; `drt_plan`
  / `drt_apply` stay value-free. This is recorded as a parity exclusion with the reason.
- `run_results.json` / history (ADR 0013): `exact: true` is recorded with the run.

### 6. Non-goals

- Not a transaction and not a lock: a concurrent writer to the destination between plan and
  apply is still possible (apply refuses only if the diff no longer matches).
- Not key management: drt does not generate, store or rotate age keys.
- No hosted payload store (ADR 0004); the sidecar is a local/CI artifact like `plan.json`.

## Consequences

- One additive CLI surface; `plan.json` schema gains an optional `payload` object (schema
  version bump per the plan-schema rules).
- A new class of sensitive artifact; the opt-in, the recipient model and the deletion rule
  exist to keep it small and short-lived.
- Warehouse destinations need no new code: the verification reuses `compute_diff`.

## Decisions

Confirmed by the maintainer on 2026-10-28, all as proposed:

1. **Sidecar file**, not embedded in `plan.json`, so the shareable plan can never carry values.
2. **Public-key recipients at plan time, `DRT_SECRETS_KEY` identity at apply**; the planner
   cannot read the data.
3. **Size cap 10,000 rows**; over the cap exact-replay is unavailable (a result, not an error).
   The payload is **deleted after a successful apply (including a no-op)**; a failed apply
   leaves it.
4. **No MCP exposure in v1.3**, recorded as a parity exclusion with the reason.
