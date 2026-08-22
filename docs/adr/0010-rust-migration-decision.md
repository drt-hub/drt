# ADR 0010: Rust Migration Decision

- **Status:** Proposed recommendation; final migration decision deferred to the
  repository owner.
- **Issue:** [#301](https://github.com/drt-hub/drt/issues/301).
- **Relates to:** [#280](https://github.com/drt-hub/drt/issues/280), whose
  reproducible benchmark scenarios and unmeasured `execute_scenario()` seam are
  reused unchanged.
- **Profile date:** 2026-08-22, commit
  `d4d32f0d4c618ce8f6816141ab2867b2a2bbe2b4`, Python 3.12.12, Darwin 25.5.0
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

1. **Source extraction — I/O-bound:** cumulative time beneath
   `SQLiteSource.extract`, including SQLite query execution/iteration and the
   `dict(zip(columns, row))` construction that occurs at that source boundary.
2. **Destination I/O — I/O-bound:** self time in `_io.open`,
   `TextIOWrapper.write`, and the close/flush context exit called directly by
   `CountingFileDestination.load`.
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

| Scenario | Total | SQLite extraction (I/O) | Transform + serialization (CPU) | Destination file I/O | Combined I/O |
|---|---:|---:|---:|---:|---:|
| Small (100) | 0.002690 s | 0.000581 s (21.61%) | 0.001734 s (64.45%) | 0.000375 s (13.94%) | 0.000956 s (35.55%) |
| Medium (10,000) | 0.052322 s | 0.015026 s (28.72%) | 0.030324 s (57.95%) | 0.006972 s (13.33%) | 0.021998 s (42.05%) |
| Large (100,000) | 0.419169 s | 0.121406 s (28.96%) | 0.243418 s (58.08%) | 0.054345 s (12.96%) | 0.175751 s (41.92%) |

For this workload, the claim that the bottleneck is I/O rather than CPU is
**refuted**. CPU-classified work is the largest bucket in every scenario,
ranging from 57.95% to 64.45%; the two I/O buckets together account for
35.55% to 42.05%.

### Where the CPU time is

The clearest scalable CPU hotspot is JSON serialization:

| Scenario | Inclusive `json.dumps` time | Share of total |
|---|---:|---:|
| Small | 0.000426 s | 15.84% |
| Medium | 0.019604 s | 37.47% |
| Large | 0.163497 s | 39.00% |

At 100,000 rows, JSON serialization alone is roughly two-thirds of the entire
CPU-classified bucket (0.163497 of 0.243418 seconds). The remainder is 0.079921
seconds (19.07% of total) across engine batching/record handling,
destination-loop work, and fixed setup. Source-record construction is charged
to the extraction bucket because cProfile cannot separate it from SQLite
cursor stepping inside the same generator function. It is therefore not valid
to treat the 19.07% remainder—or the mixed extraction bucket—as an
`engine/sync.py` rewrite opportunity.

The full destination `load` call tree takes 0.252180 seconds (60.16% of the
large run), but it contains both the 0.163497-second JSON CPU component and the
0.054345-second file-I/O component. A Rust rewrite limited to
`engine/sync.py` would leave both stdlib JSON serialization in the benchmark
destination and the physical write outside the Rust boundary. Moving records
through PyO3 can also introduce conversion/copy overhead, so the theoretical
engine-only share is an upper bound, not an expected speedup.

## What the profile does not establish

This is intentionally a reproducible local workload, not a production traffic
model. SQLite is in-memory and the JSONL destination is a local buffered file.
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
“I/O, not CPU” assumption, but the largest measured CPU component is
destination-side JSON serialization—not the engine module proposed for PyO3.
The engine-only opportunity is smaller than the 19.07% non-JSON remainder in
the large run and would be subject to Python/Rust boundary costs. For remote
warehouse/API workloads, the achievable end-to-end benefit is likely smaller
still.

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
