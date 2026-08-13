# Computed Fields — Declarative Derived Columns

Destination payloads constantly need small shape adjustments that the
warehouse model has no business owning: a `full_name` concatenated for a
CRM, a phone number in E.164 for Twilio, a timestamp as epoch millis for
Amplitude, an environment stamp so you can tell staging rows apart.

Without a way to express those in the sync, you have two bad options.
Push the formatting into the dbt model — and now you need one mart per
destination, because the next destination wants the same data shaped
differently. Or use the REST destination's `body_template` — which only
the REST destination has, leaving the other 33 structured destinations
with nothing.

**`computed_fields`** derives columns in the sync config, between
extraction and load, for every destination. Prior art: dlt's `add_map`,
Census and Hightouch's computed mapper fields.

## Quick start

```yaml
name: users_to_crm
model: |
  SELECT first_name, last_name, phone, signup_ts
  FROM `project.dataset.users`

destination:
  type: hubspot
  api_key_env: HUBSPOT_API_KEY
  object_type: contacts

sync:
  mode: upsert
  computed_fields:
    full_name:     "{{ row.first_name }} {{ row.last_name }}"
    phone_e164:    "+81{{ row.phone | replace('-', '') }}"
    signup_ms:     "{{ (row.signup_ts.timestamp() * 1000) | int }}"
    source_system: "drt-${ENV}"
```

Columns are read as `{{ row.column_name }}`, with the same Jinja
environment, filters, and strictness as the REST destination's
[`body_template`](../connectors/rest-api.md) — including `tojson_safe`
for `datetime` / `Decimal` / `UUID`.

## Values keep their type

A template that is a **single expression** returns the Python value, not
its text. Anything else renders as a string:

| Template | Result | Type |
| --- | --- | --- |
| `"{{ row.n }}"` | `5` | `int` |
| `"{{ (row.ts.timestamp() * 1000) \| int }}"` | `1754000000000` | `int` |
| `"{{ row.is_active }}"` | `True` | `bool` |
| `"{{ row.first }} {{ row.last }}"` | `"Ada Lovelace"` | `str` |
| `"+81{{ row.phone }}"` | `"+8109012345678"` | `str` |
| `"drt-prod"` | `"drt-prod"` | `str` |

This matters because a computed field becomes a **column a destination
writes**. `signup_ms` arriving at a BIGINT column as the string
`"1754000000000"` is a failed load, not a formatting quibble.

The rule keys on the template's structure, never on what the value looks
like — so a column holding the string `"123"` stays the string `"123"`,
and a zip code written as `"01"` stays `"01"`. Nothing is ever re-parsed
out of the rendered text.

## Order: computed → renamed → masked

```
extract → cursor tracking → lookups → computed_fields → field_mappings → mask → metadata_columns → load()
            (source names)  (source)   (source names)     (rename)      (dest names)   (engine-injected)
```

`computed_fields` runs **first** of the three payload transforms, so:

- Templates read **source** column names — the same names `cursor_field`
  and `lookups` use.
- [`field_mappings`](field-mappings.md) can rename a computed field, and
  [`mask`](pii-masking.md) can mask one. Both keep referencing the
  destination-facing name, exactly as before.

```yaml
sync:
  computed_fields:
    full_name: "{{ row.first }} {{ row.last }}"   # built from source names
  field_mappings:
    full_name: name                               # renamed for the destination
  mask:
    name: redact                                  # masked under the mapped name
```

## Rules and edge cases

- **No chaining.** Every template is evaluated against the record as it
  arrived, so one computed field can never read another. This keeps
  results independent of YAML key order — the same guarantee
  `field_mappings` makes. If you need two steps, write the composed
  expression.

- **Replacing a source column is allowed** and reads the original value,
  which is what makes in-place normalisation work:

  ```yaml
  computed_fields:
    phone: "+81{{ row.phone | replace('-', '') }}"   # phone -> E.164, same name
  ```

- **`${VAR}` substitution applies**, like every other string field in
  sync YAML — `source_system: "drt-${ENV}"` resolves at config load.

- **Constants need no expression.** `source_system: "drt"` is a valid
  template that renders to `"drt"`.

## ⚠️ Nulls passed through a filter become the string `"None"`

```yaml
computed_fields:
  p: "{{ row.phone | replace('-', '') }}"   # phone is NULL -> "None", not null
```

Jinja stringifies a value before a filter sees it, so a NULL reaching a
filter comes out as the literal text `"None"`. A bare `{{ row.phone }}`
correctly stays null — it is only filters and arithmetic that force the
conversion.

This is plain Jinja behaviour, shared with `body_template`, and drt
deliberately does not special-case it: two different templating
semantics in one tool would be worse than one documented sharp edge.
Give the value an explicit default — parenthesize it, since Jinja's `|`
binds tighter than `or` and `row.phone or '' | replace(...)` would apply
the filter to `''` instead of to `row.phone`, leaving a non-null value's
dashes untouched:

```yaml
computed_fields:
  p: "{{ (row.phone or '') | replace('-', '') }}"
```

## Errors

**Template syntax** is checked when the config loads, so a malformed
template fails `drt validate` rather than surfacing on the first row of a
production run:

```bash
drt validate
# computed_fields['broken']: Template syntax error: ... (line 1)
```

**Everything else is a run-time error** governed by `sync.on_error`,
because whether `row.foo` exists depends on the query:

| `on_error` | Behaviour |
| --- | --- |
| `fail` (default) | The run stops, naming the field: `computed_fields['x']: ...` |
| `skip` | The row is dropped and recorded as a row error; other rows continue |

`fail` is the default for a reason: a template error is nearly always one
config defect that affects every row alike, and naming it once is more
useful than reporting *N* skipped rows and zero synced.

Rows dropped under `skip` are never half-derived — fields are assigned
only once every template for that row succeeded.

## What it does not do

- **Aggregations or cross-row logic** — no window functions, no totals.
  That is SQL's job; do it in the model.
- **Lookups / joins** — use [`lookups`](destination-lookup.md).
- **Arbitrary Python** — templates only. Escaping to Python is the
  plugin-system boundary, tracked separately.

## See also

- [Field Mappings](field-mappings.md) — rename columns (runs *after*
  `computed_fields`)
- [PII Masking](pii-masking.md) — obscure fields (runs *after* renaming)
- [Metadata Columns](metadata-columns.md) — engine-injected bookkeeping
  columns (runs *last*, after masking)
- [`docs/llm/API_REFERENCE.md`](../llm/API_REFERENCE.md) — full sync
  options reference
