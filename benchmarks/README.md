# Sync performance benchmarks

This local harness measures drt's real extract → batch → load path without network access or
credentials. A recursive query generates deterministic rows through the built-in SQLite source;
the engine streams them into a benchmark JSONL destination that persists every batch and counts
`Destination.load()` calls as an API-call proxy. OpenTelemetry is pinned to its fallback no-op
providers before measurement, regardless of the operator's environment or profile configuration.

It is maintainer tooling, not a shipped `drt` command, and deliberately does not run in CI. Compare
results only when the machine, Python version, drt dependencies, and scenario configuration are
equivalent.

## Run it

```bash
make benchmark
```

That runs all three fixed scenarios with the engine's default 100-row batch size:

| Scenario | Rows |
|---|---:|
| `small` | 100 |
| `medium` | 10,000 |
| `large` | 100,000 |

Run a subset by invoking the script directly; repeat `--scenario` to select more than one:

```bash
python3 scripts/run_benchmarks.py --scenario small --scenario medium
```

Each selected scenario is one benchmark run and writes one ignored JSON file under
`benchmarks/results/`. Temporary destination files are removed after the run.

## Result schema

[`result-schema.json`](result-schema.json) is the machine-readable JSON Schema. Schema version 1
has this shape:

```json
{
  "schema_version": 1,
  "scenario": "small",
  "row_count": 100,
  "git_commit": "0123456789abcdef0123456789abcdef01234567",
  "timestamp": "2026-08-21T12:34:56.789012Z",
  "measurements": {
    "duration_seconds": 0.123456,
    "rows_per_second": 810.01,
    "peak_memory_bytes": 123456,
    "destination_call_count": 1
  }
}
```

- `duration_seconds` is total wall-clock time around `execute_scenario()`, including SQLite
  extraction, engine transforms/batching, and destination serialization/writes. The persisted-row
  verification happens afterward and is excluded from timing and peak-memory measurement.
- `rows_per_second` is extracted rows divided by that duration.
- `peak_memory_bytes` is peak Python allocation tracked by `tracemalloc` during the scenario. It
  does not include native SQLite allocations, allocator fragmentation, or operating-system caches.
- `destination_call_count` is the number of benchmark destination `load()` calls. It represents
  the batch/API-call shape, not a network latency estimate.

The commit is `unknown` only when the harness is run outside a Git checkout. Timestamps are UTC.
Result files are measurements, not fixtures: do not commit them or assert exact performance values
in tests.

## Profile I/O vs CPU

Issue #301's profiler wraps the unmeasured runner directly, so it uses the exact same source,
destination, engine path, row payload, batch size, and three scenario sizes:

```bash
make profile
```

Run a subset with the script directly:

```bash
python3 scripts/run_profiling.py --scenario medium --scenario large
```

Each scenario produces one ignored, schema-versioned JSON artifact under `benchmarks/profiles/`;
[`profile-result-schema.json`](profile-result-schema.json) documents the machine-readable shape.
The three non-overlapping wall-time buckets are attributed from cProfile's call graph:

- **Source extraction (CPU-bound):** cumulative time beneath `SQLiteSource.extract`, including
  SQLite execution/iteration and construction of each source record. This harness uses SQLite
  `:memory:`, so that work is in-process computation rather than disk or network waiting.
- **Transformation/serialization (CPU-bound):** the remaining Python/engine time, including batch
  construction, fixed scenario setup, and `json.dumps`/JSON encoder calls. The JSON artifact also
  records JSON serialization's inclusive cumulative time as a component.
- **Destination I/O (I/O-bound):** time in the destination's repeated `os.makedirs` filesystem
  operations plus self time in `_io.open`, `TextIOWrapper.write`, and the file context's
  close/flush path.

The buckets sum to 100%; the component timings are inclusive call-tree diagnostics and should not
be added together. cProfile adds deterministic call instrumentation, and both SQLite `:memory:` and
the local buffered JSONL destination are much lower-latency than a remote warehouse or SaaS API.
Use these results to locate CPU work in this workload, not as a universal model of production
network latency. Like `make benchmark`, profiling is manual maintainer tooling and does not run in
CI.

## Profile real local network I/O

Issue #1008's follow-up keeps the same four-field records, 100-row batch size, and 100 / 10,000 /
100,000-row scenarios, but replaces the I/O-light endpoints with real local sockets:

```bash
make profile-real-io
```

- The source leg starts an ephemeral `postgres:16-alpine` container, seeds the largest scenario
  once, and profiles the real `PostgresSource.extract()` server-side cursor over TCP. Its source
  boundary includes database and TCP wait *and* psycopg2/Python row conversion because cProfile
  cannot split those operations inside the driver.
- The destination leg starts `pytest-httpserver`, sleeps in the server thread for a controlled
  10, 50, or 200 ms before every response, and profiles the real `RestApiDestination.load()` in
  batch mode. This produces real socket wait, but the delay is a chosen fixed input—not a measured
  vendor RTT and not a model of jitter, retries, rate limits, or remote service variance.

The full run requires Docker and takes several minutes because the large 200 ms REST scenario
makes 1,000 sequential requests. It is manual maintainer tooling and is not collected by pytest or
CI. Select smaller portions with `--leg`, `--scenario`, and `--latency-ms`, for example:

```bash
python3 scripts/run_real_io_profiling.py --leg rest --scenario small --latency-ms 10
```

Artifacts are ignored JSON files under `benchmarks/profiles/`; their version-1 shape is documented
by [`real-io-profile-result-schema.json`](real-io-profile-result-schema.json). Buckets are
non-overlapping and sum to 100%. The Postgres I/O buckets are I/O-bearing connector boundaries,
not pure syscall time; the REST socket bucket is self time in the profiled thread's blocking
socket/select primitives.
