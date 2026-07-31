# Extraction memory — measured peak RSS per source

- **Issue:** [#765](https://github.com/drt-hub/drt/issues/765) (streaming
  extraction), with the Delta Lake row from
  [#679](https://github.com/drt-hub/drt/issues/679)
- **Status:** Reference. This page is the **single source of truth** for the
  memory figures quoted anywhere in this repository. Source docstrings,
  connector docs and `docs/llm/API_REFERENCE.md` describe the *shape* of the
  improvement and link here rather than repeating digits.

## Why this page exists

The #765 figures were originally written into five places independently —
source docstrings, `docs/connectors/*.md`, `docs/llm/API_REFERENCE.md`,
`CHANGELOG.md`, and the closing comment on the issue. Nothing generated them,
so they drifted apart, and two of them ended up provably wrong (see
[Corrections](#corrections)). Collapsing them to one table makes the next
update a single edit instead of five.

## Methodology

- **Row shape:** 300,000 rows of roughly 200 bytes.
- **Metric:** peak resident set size (`ru_maxrss`), reported as the delta over
  the process baseline.
- **One variant per process.** This is the part that matters. `ru_maxrss` is a
  process-wide *high-water mark*, so measuring the buffered variant and then the
  streaming variant in the same process makes the second look free — the mark
  set by the first never comes down. Every figure below was taken in a fresh
  process.
- **Measured through `extract()`**, not through a hand-rolled driver loop, so
  the number includes drt's own per-row `dict` construction.

## Results

| Source | Engine | Before | After | Mechanism | Landed |
|---|---|---|---|---|---|
| Postgres / Redshift | Postgres 16 | +196 MB | +16 MB | named (server-side) cursor, `itersize` | [#860](https://github.com/drt-hub/drt/pull/860) |
| MySQL | MySQL 8 | +112 MB | +3 MB | `SSCursor` (unbuffered) | [#863](https://github.com/drt-hub/drt/pull/863) |
| ClickHouse | ClickHouse 24 | +224 MB | +149 MB | `query_rows_stream()` | [#864](https://github.com/drt-hub/drt/pull/864) |
| SQL Server | SQL Server 2022 | +151 MB | +39 MB | cursor iteration, `arraysize` | [#866](https://github.com/drt-hub/drt/pull/866) |
| DuckDB | local file | +150.9 MB | +42.2 MB | explicit `fetchmany` loop | [#866](https://github.com/drt-hub/drt/pull/866) |
| SQLite | local file | +110.6 MB | +4.4 MB | cursor iteration, `arraysize` | [#866](https://github.com/drt-hub/drt/pull/866) |
| Delta Lake | local table | +244 MB | +120 MB | lazy `to_pyarrow_dataset()` | [#868](https://github.com/drt-hub/drt/pull/868) |

**Snowflake and Databricks carry no figures.** Neither has a local server to
measure against, so both were validated against live accounts through the
`dwh-smoke` harness ([#654](https://github.com/drt-hub/drt/issues/654),
[#865](https://github.com/drt-hub/drt/pull/865)) for correctness rather than
memory. A number here would be a number from someone's warehouse on some day,
which is worse than no number.

**Iceberg is absent because it does not stream.** `pyiceberg` exposes only a
full `to_arrow()` or a *single-pass* batch reader that DuckDB drains on the
first scan — a query reading the table twice would return silently wrong
results rather than an error. Correctness over memory; see
`drt/sources/iceberg.py`.

### Reading the ClickHouse row

The ~1.5× is much smaller than the Postgres (12×) or MySQL (37×) legs, and that
is reported as-is rather than dressed up. The residual is `clickhouse-connect`
buffering the HTTP response internally, which drt does not control;
`max_block_size` does not move it (measured identical at 8192, 65536 and the
default). That is also why ClickHouse exposes no `fetch_size` — there would be
nothing for it to control.

## Two figures deliberately kept inline

These are *arguments*, not benchmarks, and live next to the code they justify:

- `drt/sources/mysql.py` — iterating peaked at +3.6 MB against
  `fetchmany(100000)` at +61.9 MB, with no speed difference. This is why MySQL
  exposes no `fetch_size`: the knob would only offer a way to use more memory.
- `drt/sources/deltalake.py` — DuckDB's `delta_scan()` measured +19 MB, better
  than the +120 MB above, but it fetches the `delta` extension from DuckDB's
  repository on first use. That implicit network call at sync time would break
  air-gapped installs, so it was rejected.

## Corrections

Superseded values, recorded so the digits in git history are traceable:

| Where | Said | Now | Why |
|---|---|---|---|
| `drt/sources/sqlserver.py` | +110.8 MB → +4.3 MB | +151 MB → +39 MB | Its own connector doc, the CHANGELOG and #765 all said `+151 → +39`. The discarded pair sits within 0.2 MB of **SQLite's** — implausible as an independent measurement of a different engine, and [#866](https://github.com/drt-hub/drt/pull/866) landed SQL Server, DuckDB and SQLite together. |
| `drt/sources/mysql.py` | +0.8 MB after | +3 MB | Contradicted the same docstring's undisputed *"iterating peaked at +3.6 MB"* for the same `SSCursor` path. |
| `CHANGELOG.md`, `docs/connectors/postgres.md`, `docs/llm/API_REFERENCE.md` | Postgres +182 MB → +18 MB | +196 MB → +16 MB | The original run measured both variants in one process. Re-run one variant per process, the buffered baseline reads *higher* — exactly what a shared high-water mark predicts. |
| `CHANGELOG.md` | MySQL +109 MB → +8 MB | +112 MB → +3 MB | Same re-measurement. The `+8` reads as a lost decimal point from the `+0.8` above. |

## Status of these numbers

They are **consistent and attributed, not independently re-verified.** No
harness is committed that can regenerate them: `docker-compose.yml` defines only
the `drt` app service, so the Postgres / MySQL / ClickHouse / SQL Server
instances behind these runs were stood up ad hoc and are not reproducible from
this repository.

Making them reproducible is
[#280](https://github.com/drt-hub/drt/issues/280) — a benchmark suite that
seeds a fixed row shape, runs one variant per subprocess, and emits this table.
Until that lands, treat the digits as the best available record rather than a
regression gate. The in-process guard in
`tests/integration/test_large_batch.py` (a `tracemalloc` peak ceiling) is what
actually protects against an accidental O(N) buffering regression today.
