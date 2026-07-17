# PII Masking — Obscure Sensitive Fields Before They Leave

When you activate warehouse data out to a third-party service, some
columns carry personal data you'd rather not hand over verbatim — an
email address for matching, a name, a phone number. Rewriting the source
query to hash or drop those columns couples the privacy rule to the SQL
and makes it easy to forget on the next sync.

**`sync.mask`** moves the rule into the sync config, applied in flight:
each named field is hashed, redacted, or truncated **after extraction and
just before the destination**, without touching the source query or the
warehouse.

## Quick Start

```yaml
name: users_to_hubspot
model: |
  SELECT email, full_name, phone
  FROM `project.dataset.users`

destination:
  type: hubspot
  object_type: contacts

sync:
  mode: upsert
  mask:
    email: hash                              # SHA-256 hex — stable, so it still joins
    phone: redact                            # replaced with "[REDACTED]"
    full_name: { strategy: truncate, length: 1 }   # keep the first character only
```

The destination receives the masked values; the warehouse row is
unchanged, and the source SQL never mentions masking.

## Strategies

| Strategy | Config | Result | Use when |
|----------|--------|--------|----------|
| `hash` | `field: hash` | SHA-256 hex of the value | You need a **stable pseudonym** — the same input always hashes the same, so it still deduplicates / joins on the far side, but the original isn't recoverable. |
| `redact` | `field: redact` | the literal `[REDACTED]` | The field must be **present but carry no information** (schema compatibility, a placeholder). |
| `truncate` | `field: { strategy: truncate, length: N }` | the first `N` characters | A **partial** value is enough — a name initial, a postcode prefix. `length` is required and must be ≥ 0. |

The **flat form** (`field: hash` / `field: redact`) covers the
parameter-less strategies; the **object form**
(`field: { strategy: truncate, length: N }`) is for strategies that take
options.

## How it composes

- **Runs last.** Masking is the final transform before the load —
  after `lookups` and after [`field_mappings`](field-mappings.md). So
  `mask` keys reference the field name **as it leaves drt** (the
  destination-facing name). If you rename `email_address → email` with
  `field_mappings`, mask the key `email`.
- **Nulls pass through.** A `null` carries no PII, and masking it would
  only hide that the source value was absent — so `null` stays `null`.
- **Best-effort per record.** A configured field that a given row doesn't
  have is simply left alone for that row; the source query is allowed to
  omit it.
- **Every destination.** Masking is a pure in-engine transform, so it
  works identically for REST, SQL, file, and warehouse destinations — no
  per-connector support needed.
- **Non-strings are stringified first**, so an integer id masks the same
  way its text form would.

## What masking is and isn't

`sync.mask` protects data **in the activation path** — what the
third-party destination receives. It does **not** modify the warehouse,
and a `hash` is a pseudonym, not encryption: identical inputs produce
identical hashes (that's what makes them joinable), so it resists casual
exposure, not a determined re-identification attack against a small,
known value space. For values that must be unrecoverable even in
principle, drop the column in the source query instead.

For masking secrets in the **generated docs site** rather than in synced
data, see the docs-safe labels behaviour (`drt docs generate` redacts
endpoints / phones / emails by default) — a separate concern from
`sync.mask`.

## Related

- [Field Mappings](field-mappings.md) — the rename step `mask` runs after
- `docs/llm/API_REFERENCE.md` — the full `sync:` schema
