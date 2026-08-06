# Sync Unit Tests — Fixture Rows In, Expected Payload Out

The sync pipeline stopped being trivial once `mask`, `field_mappings`, and destination lookups
could all touch a record before it ships. The only feedback loop for "does this mapping do what I
think" was `drt run --dry-run` against a real warehouse — slow, and unavailable to a contributor
without cloud credentials, or to CI on a pull request.

**`sync.unit_tests`** runs fixture rows through the real transform pipeline and checks the
output — no network call, no credential, no destination connection. Prior art: dbt unit tests
(1.8+), Census/Hightouch mapper previews with sample records.

## Quick start

```yaml
name: users_to_hubspot
model: ref('users')
destination: { type: hubspot, api_key_env: HUBSPOT_KEY, object_type: contacts }

sync:
  field_mappings: { first: given_name }
  mask: { email: hash }

unit_tests:
  - name: masks_email_and_renames
    given:
      - { id: 1, email: "alice@example.com", first: "Alice", last: "Doe" }
    expect:
      - { id: 1, given_name: "Alice" }
```

```bash
drt test --unit
#
# users_to_hubspot
#   ✓ masks_email_and_renames: ok
```

## How it works

`given` rows are run through the *same* engine transform chain a real sync uses —
`field_mappings` → `mask` (and any other stage the engine applies before `destination.load()`) —
via a fake in-memory source and a destination stand-in that captures what it would have sent
instead of sending it. The captured output is compared against `expect`.

```
given (source names) → field_mappings → mask → captured, compared to expect
```

That ordering is why `given` uses **source** column names — the same names `cursor_field` and
`lookups` already read — while `expect` uses **destination-facing** names, the ones that exist
after the rename and the mask have both run.

## Ordering example: renamed, then masked

```yaml
sync:
  field_mappings: { full_name: name }
  mask: { name: redact }

unit_tests:
  - name: renamed_then_masked
    given:
      - { id: 1, full_name: "Ada Lovelace" }
    expect:
      - { id: 1, name: "[REDACTED]" }   # renamed full_name -> name, then masked
```

## Rules and edge cases

- **`expect` is a subset match, per row.** Only the keys a test declares are checked; keys the
  real pipeline produces that aren't listed in `expect` are ignored. A sync's source columns grow
  over time, and requiring every unit test to enumerate every column it doesn't care about would
  turn each one into a maintenance burden against unrelated schema growth.
- **Row count is checked exactly.** A transform that drops a row (an `on_error: skip` path) or
  somehow duplicates one is exactly the kind of change a unit test exists to catch — `expect`
  must have the same number of rows as the real output, even though each row's *fields* are
  matched loosely.
- **A config error becomes a failed test, not a crashed CLI.** An unsupported `match_policy` on a
  destination that can't honour it, or anything else the pipeline can raise, is caught and
  reported as a mismatch on that test, so one broken fixture doesn't stop `drt test --unit` before
  later tests get a chance to run.
- **`given` and `expect` must each have at least one row.** An empty `given` would make the test
  vacuously pass forever, which defeats the point.

## What's not supported yet

- **`destination.lookups`.** Lookups resolve FK values by querying the real destination — there
  is no fake lookup table (yet). A sync with `lookups:` configured reports its unit tests as
  failed, naming the reason, rather than silently running without the lookup step and asserting
  against output the real sync would never produce. Test the lookup-resolved fields with a real
  `drt run --dry-run --diff` instead.
- **`expect_body`** — asserting the *rendered* payload text for templated destinations (REST
  `body_template` and similar) isn't implemented. `expect` checks the record's field values, not
  a destination's own serialization of them.
- **Aggregations or cross-row logic.** Each fixture row is transformed independently; there's no
  way to assert something about the fixture set as a whole. That's what `sync.tests:` (against
  real destination data) or the source SQL model itself are for.

## `--unit` vs `sync.tests:`

|  | `drt test` | `drt test --unit` |
|---|---|---|
| Touches | the real destination | nothing — no network, no credentials |
| Checks | data already synced (row counts, freshness, uniqueness…) | the transform pipeline's output for given fixture rows |
| Needs | a live sync to have run first | nothing — runs standalone, in CI with no secrets |
| `--dry-run` / `--store-failures` | supported | rejected (exit 2) — destination-connected concepts |

They answer different questions and don't compose: `--unit` runs *instead of* `sync.tests:`, never
alongside it in the same invocation.

## MCP

`drt_run_test(sync_name=None, unit=False)` mirrors the CLI — pass `unit=True` to run
`unit_tests:` instead of `tests:`. Each result entry is `{name, passed, mismatches}` (no
`severity` tier, since unit tests don't have one).

## See also

- [Field Mappings](field-mappings.md), [PII Masking](pii-masking.md) — the transform stages a unit test exercises, in order
- [`docs/llm/API_REFERENCE.md`](../llm/API_REFERENCE.md) — full `unit_tests:` field reference
- [Destination Lookup](destination-lookup.md) — the one transform stage unit tests can't exercise yet
