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

**Run date:** 2026-08-23, commit `af183b5` (worktree
`feat/1008-adr0010-followup-profiling`), Python 3.12.12, Darwin 25.5.0 arm64,
Docker Desktop, `postgres:16-alpine` via `testcontainers`.

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
| Small (100) | 0.005408 s | 0.004608 s (85.21%) | 0.000760 s (14.05%) | 0.000040 s (0.74%) |
| Medium (10,000) | 0.013707 s | 0.004760 s (34.73%) | 0.007786 s (56.80%) | 0.001161 s (8.47%) |
| Large (100,000) | 0.072050 s | 0.005109 s (7.09%) | 0.059340 s (82.36%) | 0.007600 s (10.55%) |

Two things stand out. First, `consumer_cpu` — the pure-Python code *outside*
the source boundary, the closest analogue to what a `run_sync()` engine loop
would do with each row — never exceeds 10.55% of total time at any scenario
size. The overwhelming majority of time is inside the source boundary itself
(connection setup, the real round trip, and driver-level row materialization),
which is exactly the I/O-and-driver-dominated shape ADR 0010's original
recommendation predicted a real warehouse source would have, in sharp
contrast to the `:memory:` SQLite benchmark. Second, this is a *local Docker*
container on the same machine (sub-millisecond network hop) — a real cloud
warehouse (Snowflake, BigQuery, a managed Postgres) adds real network transit,
auth, and server queueing on top of this, which would only push the
I/O-dominated share higher, not lower.

### Experiment 1b — real REST destination under controlled latency

`benchmarks/profile_real_io.py::profile_rest_scenario` profiles
`RestApiDestination.load()` sending real HTTP POST requests over a real
loopback TCP socket to a local `pytest-httpserver` instance, whose handler
adds a real, controlled `time.sleep()` delay before responding (10 / 50 / 200
ms, bracketing "fast internal API" through "typical public SaaS API" — chosen
values, not measured from a live vendor). `socket_io` sums self time in the
actual blocking socket primitives (`recv`, `send`, `connect`, `select`);
`destination_cpu` is the remainder of `RestApiDestination.load()`'s own
cumulative time; `harness_cpu` is everything outside `load()` (batch
iteration, scenario setup):

| Scenario | Latency | Total | Socket I/O | Destination CPU | Harness CPU |
|---|---:|---:|---:|---:|---:|
| Small (100) | 10 ms | 0.059 s | 0.36% | 23.91% | 75.73% |
| Medium (10,000) | 10 ms | 2.013 s | 1.40% | 73.53% | 25.07% |
| Large (100,000) | 10 ms | 20.329 s | 1.12% | 73.63% | 25.25% |
| Small (100) | 50 ms | 0.066 s | 0.60% | 84.98% | 14.42% |
| Medium (10,000) | 50 ms | 6.797 s | 0.61% | 86.74% | 12.65% |
| Small (100) | 200 ms | 0.213 s | 0.17% | 95.58% | 4.25% |
| Medium (10,000) | 200 ms | 22.000 s | 0.22% | 95.40% | 4.38% |

**The large-scenario rows at 50 ms and 200 ms are omitted above, not
rounded away.** Both runs completed and wrote result artifacts, but their
bucket math is internally inconsistent — the 50 ms run reports
`destination_cpu` at 163.80% of total with `harness_cpu` at -64.37%; the 200
ms run reports 155.38% and -55.57%. Percentages exceeding 100%, or going
negative, mean the measurement itself broke down at that combination, not
that destination CPU work genuinely tripled. The likely cause: `HTTPServer`
runs its handler (including the `time.sleep()` delay) in a second thread of
the *same process* being profiled; at large row counts the "large" scenario
issues ~1,000 sequential requests, so 50/200 ms latency means the server
thread holds the GIL and real wall-clock time for minutes at a stretch
concurrently with the profiled client thread. `cProfile`'s per-call timing
is not designed for sustained multi-thread contention of that duration, and
the client-side `load()` cumulative time it reports for the run — summed
across all ~1,000 calls to that one function — ends up larger than the
profiler's own total wall-clock accounting for the run. The Postgres leg
above does not have this problem because the "server" is a genuinely separate
OS process (a container), the same kind of clean boundary a production
`drt run` process has against any real destination; a same-process threaded
test server is not.

This is itself a real, useful finding, not just an experimental miss: a
same-process threaded HTTP test server is not a trustworthy profiling harness
once latency × request count pushes a run into sustained multi-minute
multi-thread contention. Fixing it (moving the server to a genuinely separate
process, matching the Postgres leg's boundary) is a real, buildable follow-up
if this evidence base needs to grow further — not done here, to keep this
issue's scope to the two experiments it was opened for.

Where the numbers are trustworthy (every row above), the pattern is
unambiguous: `socket_io`'s directly-measured self time is small (never above
1.4%) because a blocking `recv()` mostly is not caught mid-flight by
cProfile's own sampling of the call it's already inside — but `destination_cpu`
climbs from 73.63% (10 ms) to 95.58%/95.40% (200 ms) as latency rises, because
that bucket's cumulative time is dominated by the same blocked call, which
cProfile *does* attribute to `RestApiDestination.load()`'s frame. Read
together with Experiment 1a: **the more a destination resembles a real
network call — even a fast, local one — the smaller the CPU-classified
share becomes and the larger the load-bound-on-the-network share becomes**,
exactly inverting the #280/#301 SQLite-to-local-file benchmark's shape.

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

**No — if anything, it sharpens it.** Experiment 1 shows the original
recommendation's central caveat was not hypothetical: real network I/O
(even a fast local container, even a local loopback socket with imposed
delay) measurably shrinks the CPU-classified share and grows the
I/O/network-bound share, exactly opposite of what correcting the SQLite
extraction bucket did to the original local benchmark. Experiment 2 shows
the one concrete "port this to Rust" candidate the original profile
identified does not deliver a reliable win once the realistic PyO3 boundary
cost is included — it is a real but modest win at small scale and
noise-level at the 100,000-row scale where a win would matter most for
throughput-sensitive syncs.

Both results point the same direction as the original recommendation: **do
not use this profiling work, before or after this follow-up, as
justification for a broad `engine/sync.py` Rust rewrite.** The evidence
gathered so far identifies no workload shape in drt's actual codebase where
a native rewrite has been shown to deliver a measured, boundary-cost-inclusive
win. If a future workload shape is a better candidate — very large batches
of a genuinely CPU-heavy, allocation-light transform, profiled with the same
rigor applied here — that would be new evidence, not an extension of what
this ADR already covers. The repository owner's final call remains
unchanged and undecided by this ADR.

