# ADR 0010: Rust Migration Decision

- **Status:** Proposed recommendation; final migration decision deferred to the
  repository owner.
- **Issue:** [#301](https://github.com/drt-hub/drt/issues/301).
- **Relates to:** [#280](https://github.com/drt-hub/drt/issues/280), whose
  reproducible benchmark scenarios and unmeasured `execute_scenario()` seam are
  reused unchanged.
- **Profile date:** 2026-08-22, commit
  `dc4ad41abcc1e0ee01b5b593fcc0eb7522097a52`, Python 3.12.12, Darwin 25.5.0
  arm64.

## Question

Is drt's sync path materially CPU-bound, such that rewriting
`drt/engine/sync.py` in Rust through PyO3 is likely to improve end-to-end
performance, or is the path dominated by source/destination I/O that a rewrite
cannot remove?

This ADR records a measured recommendation. It does **not** accept or commit to
a Rust migration: cost, maintenance, packaging, contributor accessibility, and
roadmap priority remain business/project-owner decisions outside #301.

## Methodology

`make profile` uses stdlib `cProfile` around #280's existing
`execute_scenario(scenario, work_dir)`. It profiles the same deterministic
SQLite `:memory:` → real `engine.run_sync()` → batched JSONL persistence path,
the same four-field synthetic records, the same 100-row batch size, and the
same three `SCENARIOS`:

| Scenario | Rows | Destination calls |
|---|---:|---:|
| Small | 100 | 1 |
| Medium | 10,000 | 100 |
| Large | 100,000 | 1,000 |

OpenTelemetry is pinned to the same no-op providers as the benchmark harness
before profiling. The destination's persisted row count is verified after the
profile and outside the measured call.

cProfile's call graph is attributed into three non-overlapping wall-time
buckets:

1. **Source extraction — CPU-bound:** cumulative time beneath
   `SQLiteSource.extract`, including SQLite query execution/iteration and the
   `dict(zip(columns, row))` construction that occurs at that source boundary.
   Because this harness always uses SQLite `:memory:`, this is in-process
   SQLite VM and Python work, not disk or network wait.
2. **Destination I/O — I/O-bound:** cumulative time in each directly called
   `os.makedirs` (including its `mkdir`/`stat` subtree), plus self time in
   `_io.open`, `TextIOWrapper.write`, and the close/flush context exit called
   directly by `CountingFileDestination.load`.
3. **Transformation/serialization — CPU-bound:** remaining profiled time,
   including engine batching and record handling, fixed scenario setup, the
   destination's Python loop, and `json.dumps`/JSON encoder work.

This assignment is deliberately exclusive, so the three percentages sum to
100%. The JSON artifact also records inclusive SQLite extraction,
`json.dumps`, and full destination `load` call-tree times as diagnostic
components; those overlap and must not be summed. The version-1 schema is
[`benchmarks/profile-result-schema.json`](../../benchmarks/profile-result-schema.json),
and each local run writes ignored artifacts under `benchmarks/profiles/`.

## Results

These are the actual `make profile` results from the environment identified
above, not projections:

| Scenario | Total | SQLite extraction (CPU) | Transform + serialization (CPU) | Destination file I/O | Combined CPU |
|---|---:|---:|---:|---:|---:|
| Small (100) | 0.003122 s | 0.000658 s (21.08%) | 0.002210 s (70.80%) | 0.000253 s (8.12%) | 0.002868 s (91.88%) |
| Medium (10,000) | 0.051703 s | 0.014715 s (28.46%) | 0.029960 s (57.95%) | 0.007028 s (13.59%) | 0.044675 s (86.41%) |
| Large (100,000) | 0.447126 s | 0.125501 s (28.07%) | 0.252146 s (56.39%) | 0.069479 s (15.54%) | 0.377647 s (84.46%) |

For this workload, the claim that the bottleneck is I/O rather than CPU is
**refuted**. CPU-classified work accounts for 84.46% to 91.88%; the only
I/O-classified bucket, local destination filesystem work, accounts for 8.12%
to 15.54%. This strong local CPU majority follows in part from the benchmark
shape: its source deliberately performs no genuine storage or network I/O.

### Where the CPU time is

The clearest scalable CPU hotspot is JSON serialization:

| Scenario | Inclusive `json.dumps` time | Share of total |
|---|---:|---:|
| Small | 0.000363 s | 11.63% |
| Medium | 0.020620 s | 39.88% |
| Large | 0.174138 s | 38.95% |

At 100,000 rows, JSON serialization is the largest identified CPU component:
0.174138 seconds (38.95% of total), or 69.06% of the
transformation/serialization bucket. SQLite extraction contributes another
0.125501 seconds (28.07% of total). It belongs to the CPU classification in
this `:memory:` workload, but remains source implementation work outside
`engine/sync.py`; cProfile cannot further separate SQLite VM stepping from the
Python record construction in the same generator.

After subtracting JSON from the transformation/serialization bucket, 0.078008
seconds (17.45% of total) remains across engine batching/record handling,
destination-loop work, and fixed setup. That residual includes the proposed
Rust boundary, but is not exclusive to it. The engine-only opportunity is
therefore smaller than 17.45%, not the full 84.46% CPU-classified share.

The full destination `load` call tree takes 0.270963 seconds (60.60% of the
large run), but it contains both the 0.174138-second JSON CPU component and the
0.069479-second file-I/O component. A Rust rewrite limited to
`engine/sync.py` would leave both stdlib JSON serialization in the benchmark
destination and the physical write outside the Rust boundary. Moving records
through PyO3 can also introduce conversion/copy overhead, so the theoretical
engine-only share is an upper bound, not an expected speedup.

## What the profile does not establish

This is intentionally a reproducible local workload, not a production traffic
model. The in-memory database performs no storage or network wait; the only
I/O-classified work is a small local buffered file write plus its repeated
directory metadata operations. The resulting CPU percentage is an accurate
description of this synthetic compute-to-file path, but it cannot settle
whether production drt traffic is CPU- or I/O-bound. In particular, it should
not be read as stronger evidence for a Rust migration merely because correcting
the source classification made the reported CPU share larger.

Real warehouse extraction and SaaS/API destinations add network latency,
server scheduling, rate limiting, retries, and remote commit time; all make a
production sync more I/O-heavy and reduce the end-to-end fraction a local CPU
optimization can improve. Conversely, heavier `computed_fields`, masking,
lookups, schema-aware serialization, or larger/wider payloads could increase
CPU share and create a better native-code candidate than this four-field
pass-through workload.

cProfile instruments every Python call and therefore changes absolute timing.
Its single-process deterministic call attribution answers where this workload
spends time, while #280's unprofiled benchmark remains the appropriate tool
for throughput/regression comparisons. These measurements are one run on one
machine; exact durations are not portable, which is why tests validate schema
and invariants rather than performance values.

## Recommendation

**Do not use this profile as justification for a broad Rust rewrite of
`engine/sync.py`.** The local workload is CPU-majority, so it refutes the bare
“I/O, not CPU” assumption only for this deliberately I/O-light shape. Its two
largest measured CPU components are destination-side JSON serialization and
in-memory source extraction—neither is the engine module proposed for PyO3.
The engine-only opportunity is smaller than the 17.45% non-JSON
transformation residual in the large run and would be subject to Python/Rust
boundary costs. For remote warehouse/API workloads, the achievable end-to-end
benefit is likely smaller still.

If performance becomes a roadmap priority, the next evidence should be profiles
of representative remote destinations and CPU-heavy transforms using real
payload widths. A narrow prototype is justified only after those profiles: the
best candidate exposed here is native/batched serialization together with
record processing, measured end to end against the Python implementation and
including PyO3 conversion overhead. Workloads dominated by wide JSON payloads,
expensive per-record computed fields/masking, or sustained local file exports
are the shapes most likely to benefit; rate-limited SaaS APIs and high-latency
warehouses are least likely.

The repository owner retains the final migration and roadmap decision. This
ADR's recommendation is to defer that call rather than interpret #301 as an
accepted Rust commitment.

## Follow-up: real I/O and a PyO3 prototype (#1008)

The recommendation above named its own gap: this profile's source performs no
genuine storage or network I/O, so it cannot settle whether production drt
traffic is CPU- or I/O-bound. This section reports two follow-up experiments
run against that gap directly. Neither commits to a Rust migration; both
exist to replace assumption with measurement before that decision is made.

**Run date:** 2026-08-23, commit `ab9d27c7` (worktree
`feat/1008-adr0010-followup-profiling`), Python 3.12.12, Darwin 25.5.0 arm64,
Docker Desktop, `postgres:16-alpine` via `testcontainers`. This section was
rewritten once after its first draft: the first pass mislabeled controlled
network latency as CPU time and, separately, a same-process threaded HTTP
test server produced schema-invalid measurements at two data points. Both
are fixed below (a three-way split anchored on the known injected latency,
and a process-isolated server); the numbers and conclusions in this section
supersede that draft.

### Experiment 1a — real warehouse source (Postgres, real TCP + real query)

`benchmarks/profile_real_io.py::profile_postgres_scenario` profiles
`PostgresSource.extract()` reading the same four-field synthetic records back
from a real, ephemeral `postgres:16-alpine` container over a real TCP
connection — not `:memory:` SQLite. `psycopg2`'s C driver does not expose its
own socket calls to cProfile, so `connection_query_setup` and
`row_streaming_and_conversion` are both reported as `mixed_io_cpu` — an
honest aggregate of network wait, server-side execution, driver conversion,
and Python record construction, not a further-separable I/O/CPU split:

| Scenario | Total | Connection + query setup (mixed) | Row streaming + conversion (mixed) | Consumer CPU |
|---|---:|---:|---:|---:|
| Small (100) | 0.010920 s | 0.009237 s (84.59%) | 0.001423 s (13.03%) | 0.000259 s (2.38%) |
| Medium (10,000) | 0.026981 s | 0.006535 s (24.22%) | 0.019685 s (72.96%) | 0.000761 s (2.82%) |
| Large (100,000) | 0.177117 s | 0.004036 s (2.28%) | 0.166423 s (93.96%) | 0.006658 s (3.76%) |

Read this narrowly. `consumer_cpu` is only the row-counting loop *outside*
`extract()` in the test harness — it is not drt's own conversion work, which
happens inside `extract()` itself and is folded into `row_streaming_and_conversion`,
an aggregate this method cannot split further. So this table does not isolate
what fraction of extraction is CPU versus network wait; it only shows that
the overwhelming majority of time is inside the source boundary (connection
setup, the real round trip, and driver-level row materialization) rather than
in code a Rust rewrite of `engine/sync.py` would touch. That much is real and
matches ADR 0010's original prediction that a genuine warehouse source would
look I/O-and-driver-dominated, in contrast to the `:memory:` SQLite benchmark.
It is also a *local Docker* container on the same machine (sub-millisecond
network hop) — a real cloud warehouse (Snowflake, BigQuery, a managed
Postgres) adds real network transit, auth, and server queueing on top of
this, which would only shrink the harness-owned share further, not grow it.

### Experiment 1b — real REST destination under controlled latency

`benchmarks/profile_real_io.py::profile_rest_scenario` profiles
`RestApiDestination.load()` sending real HTTP POST requests over a real
loopback TCP socket to a controlled-latency server running in a genuinely
separate OS process (`multiprocessing`, spawned), whose handler adds a real
`time.sleep()` delay before responding — 0 / 10 / 50 / 200 ms, where 0 ms is
an added baseline (see below) and 10/50/200 ms bracket "fast internal API"
through "typical public SaaS API" (chosen values, not measured from a live
vendor). The client process runs one untimed warm-up request before any
profiled scenario, so import and first-connection cost lands there instead
of contaminating whichever scenario happens to run first.

The split is built from what's known by construction rather than from
cProfile trying to separate CPU from a blocking socket call inside one
function's frame (the first draft's mistake): `known_network_wait` is
`controlled_latency_ms × request_count` — an exact lower bound on wall time
spent waiting on the injected delay. `load_overhead` is everything else
inside `RestApiDestination.load()`'s cumulative time: httpx, the Jinja
`{{ rows | tojson_safe }}` render, JSON encoding, header construction, *and*
any real local-transport wait (loopback round trip, handler read/write) not
covered by the injected sleep. `harness_cpu` is everything outside `load()`
(the batching loop) — structurally tiny by construction, since almost all
per-request work happens inside `load()`; a small `harness_cpu` says nothing
about the CPU/IO split of the leg as a whole, it only confirms the batching
loop itself is thin.

| Scenario | Latency | Total | known_network_wait | load_overhead | harness_cpu |
|---|---:|---:|---:|---:|---:|
| Small (100) | 0 ms | 0.005 s | 0.00% | 99.48% | 0.52% |
| Medium (10,000) | 0 ms | 0.438 s | 0.00% | 99.91% | 0.09% |
| Large (100,000) | 0 ms | 3.617 s | 0.00% | 99.94% | 0.06% |
| Small (100) | 10 ms | 0.020 s | 49.67% | 50.23% | 0.10% |
| Medium (10,000) | 10 ms | 2.060 s | 48.55% | 51.43% | 0.02% |
| Large (100,000) | 10 ms | 20.597 s | 48.55% | 51.40% | 0.05% |
| Small (100) | 50 ms | 0.065 s | 77.07% | 22.89% | 0.04% |
| Medium (10,000) | 50 ms | 6.896 s | 72.51% | 27.47% | 0.02% |
| Large (100,000) | 50 ms | 68.609 s | 72.88% | 27.05% | 0.07% |
| Small (100) | 200 ms | 0.213 s | 93.70% | 6.27% | 0.03% |
| Medium (10,000) | 200 ms | 21.850 s | 91.53% | 8.46% | 0.01% |
| Large (100,000) | 200 ms | 219.887 s | 90.96% | 9.02% | 0.02% |

All twelve scenarios completed and passed schema validation this run — the
process-isolated server removed the same-process multi-thread contention
that broke the large/50ms and large/200ms measurements in the first draft.

**`load_overhead` is not a CPU measurement, and this method cannot cleanly
split it into CPU versus local-transport wait.** The discriminating check is
per-request cost, which a pure-CPU bucket should hold flat across latency
settings — it does not. Using the large scenario's 1,000 requests:

| Latency | load_overhead / request |
|---:|---:|
| 0 ms | 3.61 ms |
| 10 ms | 10.59 ms |
| 50 ms | 18.56 ms |
| 200 ms | 19.84 ms |

(medium agrees: 4.38 / 10.59 / 18.94 / 18.48 ms/request at the same four
latencies.) Per-request cost climbs with the injected latency instead of
staying flat, so `load_overhead` demonstrably contains latency-correlated
wait — most likely the handler's `rfile.read()`/response write sitting
outside the measured `time.sleep()`, plus scheduling delay from the server's
single-threaded, `timeout=0.1`-polling accept loop. That rules out reading
`load_overhead` as either "it's all CPU" or "it's all network wait." The 0 ms
row is the one defensible number here: with no injected delay,
`load_overhead` is CPU plus bare loopback transit, which puts an **upper
bound of roughly 3.6–4.8 ms per request** on the REST leg's true CPU cost at
this record shape (small/medium/large agree: 4.77 / 4.38 / 3.61 ms/request).
This is a limitation of the measurement, stated plainly rather than
resolved: neither Experiment 1a nor 1b isolates a clean CPU/IO percentage
split for a real destination or source, and this ADR does not lean on either
table for that number. What both experiments do establish is where the time
is *not*: not in code outside the source/destination boundary, and (for REST)
not more than ~5ms/request even at the boundary once network wait is
subtracted out.

### Experiment 2 — scoped PyO3 prototype of the confirmed JSON hotspot

A throwaway `pyo3`+`serde_json` extension (`fastjson.dumps_records`,
built with `maturin develop --release`, Rust 1.75.0, `pyo3 = "0.20.3"`) was
benchmarked head-to-head against `json.dumps(records, default=str)` on the
identical four-field record shape, at the same three row counts. The
measured number includes the full Python→Rust call boundary: extracting each
field of each record from its Python `dict` via PyO3's `extract()`, not just
the isolated Rust-side `serde_json::to_string` call — this is deliberate,
since that boundary cost is exactly what ADR 0010's original text warned
would make a naive port's estimate an upper bound, not an expected speedup.
The code was not committed (scratch-only, per #1008's scope) — the numbers
below are real, from three independent runs in this environment:

| Records | Run 1 (py→rust speedup) | Run 2 | Run 3 |
|---:|---:|---:|---:|
| 100 | 1.38x | 1.56x | (not re-run) |
| 10,000 | 1.26x | 1.24x | 1.15x |
| 100,000 | 0.95x | 1.07x | 1.09x |

At small batch sizes, the prototype is consistently faster — roughly
1.2×–1.6×, likely `serde_json`'s per-call serialization speed genuinely
outrunning Python's C-accelerated `json` module by a real but modest margin.
**At 100,000 records, the result is statistical noise around parity (0.95×
to 1.09× across three runs) — not a reliable win.** The absolute gap at that
scale is small in both directions (roughly 24–27 ms either way). The
per-record PyO3 extraction cost scales linearly with record count exactly
like the serialization work it's paired with, so the fixed per-call boundary
overhead that helps the speedup look good at 100 records stops mattering at
100,000 — and CPython's own `json` module is already a mature C extension,
not a naive pure-Python implementation, so there was never a large gap to
close in the first place.

### Does this change the recommendation?

**No.** The primary evidence for that answer is Experiment 2, not Experiment
1: Experiment 1 confirms real sources and destinations are dominated by
connection/driver/network work rather than by code a Rust rewrite of
`engine/sync.py` would touch, but — as stated above — it does not isolate a
CPU/IO percentage for either leg, so it cannot by itself prove or disprove
that a rewrite would help. Experiment 2 is the one experiment in this ADR
that measures a concrete "port this to Rust" candidate end-to-end, including
the realistic PyO3 call-boundary cost, and it does not deliver a reliable
win: a real but modest 1.2–1.6× at small batches, and statistical noise
around parity (0.95×–1.09× across three runs) at 100,000 records — the scale
where a win would matter most for throughput-sensitive syncs.

That result stands on its own regardless of how Experiment 1's CPU/IO
question eventually resolves: even the one hotspot this ADR's original
profile identified as a plausible Rust candidate did not clear the bar once
measured honestly. **Do not use this profiling work, before or after this
follow-up, as justification for a broad `engine/sync.py` Rust rewrite.** The
evidence gathered so far identifies no workload shape in drt's actual
codebase where a native rewrite has been shown to deliver a measured,
boundary-cost-inclusive win. If a future workload shape is a better
candidate — very large batches of a genuinely CPU-heavy, allocation-light
transform, profiled with the same rigor applied here — that would be new
evidence, not an extension of what this ADR already covers. Whether it is
worth building a cleaner CPU/IO split for a real destination or source (the
gap this section leaves open) is itself a candidate for a future, separate
ADR follow-up, not assumed here. The repository owner's final call remains
unchanged and undecided by this ADR.

