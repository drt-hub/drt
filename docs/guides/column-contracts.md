# Column contracts

Column contracts stop model-output drift before it reaches a destination. They
are opt-in: a sync with no `columns:` block behaves exactly as it did before.

```yaml
name: customers_to_crm
model: ref('customers')

columns:
  - name: email
    type: string
    description: Primary CRM identifier
  - name: lifetime_value
    type: number
  - name: updated_at
    type: timestamp

contract:
  enforced: true
  on_extra_columns: fail

destination:
  type: hubspot
  object_type: contacts
  id_property: email
```

`columns` without `contract` is a documentation-only declaration. To check it
during `drt run`, `drt plan`, and `drt apply`, set `contract.enforced: true`.
Declaring `contract` without a non-empty `columns` list is a validation error,
as are duplicate names, unsupported types, and unknown keys in either block.

## When the check runs

drt compares the declaration with the raw first extracted batch, after
extraction and before destination lookups, computed fields, field mappings,
masking, metadata columns, or the first destination write. A violation fails
the run through the normal result/observer path; nothing from that run has been
sent. An empty extraction has no batch to inspect and therefore passes without
a contract check. Diff-based incremental extraction is the exception: drt
preflights the materialized snapshot's sample or column names before any
removal is finalized, then still checks the first added/changed batch when one
exists. If a diff run has no added or changed rows and its source provides
neither snapshot metadata field, an enforced contract fails closed; the source
must return `snapshot_columns`, or the contract must be disabled.

The check unions keys across every row in the first batch, which supports
sparse records. It checks all non-null values in that batch and reports every
missing column, incompatible type, and (under the default policy) extra column
in one error. A null value is compatible with every declared type; value-level
rules such as null rates, ranges, and uniqueness belong in `drt test`.

## Portable types

| Declared type | Compatible Python values returned by a source |
|---|---|
| `string` | `str` |
| `number` | `int`, `float`, `decimal.Decimal` (but not `bool`) |
| `integer` | `int` (but not `bool`) |
| `boolean` | `bool` |
| `timestamp` | `datetime.datetime` |
| `date` | `datetime.date` (a `datetime` remains a timestamp) |
| `json` | `dict`, `list` |

Unsupported Python values are reported by class name in the violation. This
mapping is intentionally small and portable across sources and destinations;
cast source-specific values in the model query when necessary.

## Extra columns

`contract.on_extra_columns` defaults to `fail`:

- `fail` reports undeclared keys and sends nothing.
- `ignore` permits undeclared keys and sends them unchanged.
- `drop` removes undeclared keys from every batch before destination-facing
  transforms and loading. The first batch is still checked for declared names
  and types.

Only the first batch establishes the schema/type check. With `drop`, filtering
continues for later batches so an undeclared key can never leak to the
destination. Use a representative first batch and a stable model schema.

## Plans and catalog output

`drt plan` uses the same engine path, so a contract violation makes the plan
unavailable with the contract error and does not produce a partial change set.
Because `columns` and `contract` live in the sync file, changing either also
changes the plan `config_hash`; `drt apply` refuses an older plan.

`drt docs generate` publishes declared names, portable types, and descriptions
on the sync page and in manifest schema v6 under `syncs[].columns`. The block is
omitted when absent, and manifests from schema v1 through v5 still load.
