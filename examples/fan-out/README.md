# Zero-credential fan-out

This mini-project sends one model to two independently named file syncs. Both
syncs share the `customer_fanout` tag, so one selector runs them together.

From this directory, stage the included SQLite profile in an example-local
home and run the project:

```bash
mkdir -p .demo-home/.drt
cp profiles.yml.example .demo-home/.drt/profiles.yml
env HOME="$PWD/.demo-home" drt validate --strict
env HOME="$PWD/.demo-home" drt run --select tag:customer_fanout
```

The outputs are `output/active_customers.csv` and
`output/active_customers.jsonl`. SQLite runs in memory, and the local
`syncs/models/active_customers.sql` file supplies deterministic rows, so no
database server or credentials are required.

The local SQL file wins over dbt resolution to keep this example
self-contained. In a dbt project, remove that override, materialize
`active_customers`, and let both exact `ref('active_customers')` declarations
resolve through the same `target/manifest.json`.

See the [fan-out guide](../../docs/guides/fan-out.md) for independent reads,
partial failures, per-leg watermarks, retries, rate limits, and ordering.
