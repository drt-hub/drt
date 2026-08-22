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
