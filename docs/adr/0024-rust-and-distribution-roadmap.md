# ADR 0024 — Rust and distribution roadmap

- **Status:** Accepted 2026-11-20 (owner approved every recommendation in the final section).
- **Date:** 2026-11-18.
- **Relates to:** [ADR 0007](0007-protocol-stability-policy.md) (frozen public
  Protocols), [ADR 0009](0009-plugin-config-union-blocker.md) (Python entry-point
  plugins), [ADR 0010](0010-rust-migration-decision.md) (profile and PyO3
  evidence), and [#1017](https://github.com/drt-hub/drt/issues/1017) ("Rust
  distribution UX: cold-start + single-binary packaging", parked; the title and
  parked state were supplied for this ADR, but no reference to #1017 remains in
  the checked-out repository).
- **Measurement environment:** commit
  `301c48f58830bc57a2a92f3488609f9574203760`, Python 3.12.12, macOS 27.2.1,
  Darwin 27.2.0 arm64.

## Context

The maintainer wants to consider a "v2 built for Rust" direction. The immediate
prompt is that dbt Core v2, described as the Rust "Fusion" engine, reportedly
reached GA in 2026, and that many voices now prefer Rust to Python. **Those are
external, unverified claims in this ADR**: neither the dbt release state nor the
sentiment claim is evidenced by this repository. They may be useful market
signals, but they are not measurements of drt.

The repository has deliberately kept a Rust option open without committing to
it. The architecture notes identify `drt/engine/sync.py` as a possible future
PyO3 boundary and keep state/watermark persistence behind `SyncObserver` where
the current Protocol permits it. The `Source`, `Destination` and `StateManager`
interfaces are frozen under ADR 0007. The roadmap says that the Rust decision is
pending owner review and that no rewrite is scheduled. ADR 0010 and its #1008
follow-up are the existing evidence; this ADR does not supersede their numbers.

At the same time, drt is not only an engine. The current package supports Python
3.10–3.13 and has a Python CLI, Pydantic config system, Jinja templates, HTTP
client, state backends, 37 built-in destinations and 13 built-in sources. Its
optional extras include the vendor SDKs and drivers for BigQuery, Snowflake,
Databricks, PostgreSQL/Redshift, ClickHouse, Delta Lake, Iceberg, SQL Server,
MySQL, Google Cloud, Azure and AWS. Seven Python entry-point groups are public:
`drt.sources`, `drt.destinations`, `drt.secret_providers`,
`drt.permission_checkers`, `drt.audit_loggers`, `drt.rate_limiter_backends` and
`drt.observers`. A distribution or rewrite decision therefore affects connector
availability, config compatibility and plugin loading, not just a batch loop.

The current contribution surface is Python-centric. As a local indicator rather
than a claim about every community PR, 53 of the latest 100 non-merge commits
touch Python under `drt/` or `tests/`, 76 touch Markdown, and none touch Rust;
there are no tracked `.rs` files. Moving the primary implementation language
would change who can make a small connector or correctness contribution and
would require a second toolchain in development and CI.

Three questions are commonly collapsed into "should drt use Rust?" They have
different evidence and may have different answers:

1. **Speed:** is a material part of an important sync CPU-bound, and can native
   code improve end-to-end time after boundary costs?
2. **Distribution:** can users install and start drt without managing Python,
   and can CI acquire it quickly and reproducibly?
3. **Positioning/brand:** would a Rust v2 story create enough user and contributor
   value to justify rebuilding a mature Python integration surface?

This ADR keeps those decisions separate. A single binary does not prove a faster
sync; a fast native kernel does not remove the Python runtime; and a Rust brand
does not create connector or plugin parity.

## Decision

No Rust rewrite or "v2 is Rust" commitment is accepted by this ADR.

The proposed direction is an evidence-gated ladder:

1. keep the Python implementation as the supported default;
2. optimize eager imports and prototype rewrite-free distribution first;
3. add an in-process Rust extension only for a measured hotspot that clears the
   thresholds below;
4. consider an experimental selectable Rust engine only after a versioned YAML
   conformance suite exists; and
5. consider a separate Rust v2 repository only after the experimental engine has
   demonstrated both demand and a maintainable compatibility boundary.

A full rewrite is not recommended on the evidence available today. Each later
step must be optional and must retain the last proven Python path until its own
continue gate is met.

## Question 1: speed

### Evidence already measured

ADR 0010 profiled a deterministic four-field, SQLite `:memory:` to JSONL path.
Its large scenario processed 100,000 rows in 0.447126 seconds:

| Bucket | Time | Share |
|---|---:|---:|
| SQLite extraction and row construction | 0.125501 s | 28.07% |
| Transform and serialization | 0.252146 s | 56.39% |
| Destination file I/O | 0.069479 s | 15.54% |
| Combined CPU-classified work | 0.377647 s | 84.46% |

The local workload is CPU-majority, but that does not identify
`engine/sync.py` as the hotspot. `json.dumps` alone took 0.174138 seconds
(38.95%). After subtracting JSON, only 0.078008 seconds (17.45%) remained
across engine batching, record handling, the destination loop and setup. The
engine-only opportunity is smaller than that residual.

The #1008 follow-up then tested the boundaries ADR 0010 said were missing:

- A real PostgreSQL source over TCP spent 93.96% of its large run in the mixed
  row-streaming/driver/conversion boundary and 3.76% in the outside consumer
  loop. cProfile could not split driver/network wait from conversion CPU, but it
  did show that the time was not in engine orchestration.
- A real loopback REST destination put an upper bound of roughly 3.6–4.8 ms per
  request on CPU plus bare local transport. In the large scenario, the known
  injected wait alone was 48.55% of wall time at 10 ms latency, 72.88% at
  50 ms and 90.96% at 200 ms. The remaining bucket was mixed and was not
  presented as CPU.
- A throwaway PyO3/Serde JSON prototype included Python-to-Rust field extraction.
  It was approximately 1.2–1.6x faster at small batches, but only 0.95–1.09x at
  100,000 records: noise around parity at the scale where throughput matters.

The existing evidence therefore rejects two shortcuts: "drt is all I/O" and
"Rust will make the engine fast." Some local shapes are CPU-heavy, but the only
native prototype did not show a large-batch win, and remote work is substantially
inside source, driver, destination and network boundaries.

### New measurements required

The next suite must use unprofiled wall time for the decision and a profiler only
for attribution. Every scenario must record throughput, p50/p95 wall time, peak
RSS, output equality, row width, batch size and Python/Rust boundary conversions.
It must run at least five measured repetitions after one warm-up and retain raw
results with the commit, machine and compiler versions.

| Scenario | Required shape | What it answers |
|---|---|---|
| Wide JSON serialization | 10k and 100k records; 50, 100 and 250 fields; nested arrays/objects, decimals, datetimes and nulls; batch sizes 100/1k/10k | Whether a schema-aware batched serializer has enough work per boundary crossing to beat CPython's mature C JSON path |
| Transform-heavy sync | 100k records with field mappings, multiple `computed_fields`, and hash/redact/truncate masks; include narrow and wide rows | Whether Python per-record expression and masking work is a real native kernel rather than an assumed one |
| Large local export | At least 1 GiB logical input to JSONL and CSV, plus Parquet where the extra is installed; warm and cold filesystem runs reported separately | Whether sustained local export is CPU-, allocation- or storage-bound, and whether native code improves throughput or memory |
| Local SQL-to-file end to end | SQLite/DuckDB to file with 100k and 1m rows, including batching, contracts and serialization | Whether an isolated kernel win survives the real engine path |
| Remote warehouse/API controls | Real PostgreSQL plus one cloud warehouse when credentials are available; REST at 0/10/50/200 ms | The Amdahl-law ceiling and regression risk for the production shapes least likely to benefit |
| Diff/mirror and contract work | Large key sets, sparse updates, wide column contracts and scoped mirror where supported | Whether planning, hashing or validation—rather than ordinary loading—contains a better native candidate |

### Speed decision criteria

The cost bar rises with the scope of native code:

| Change | Continue threshold | Why this threshold matches the cost |
|---|---|---|
| Pure-Python optimization | At least 10% end-to-end or 50 ms startup improvement on a stable benchmark, with no semantic change | Low build and contributor cost; modest wins are worth taking |
| Narrow PyO3 kernel | At least 2.0x in the isolated hotspot **and** at least 25% end-to-end in two representative CPU-heavy scenarios; no regression over 5% in remote controls | A compiled extension adds platform wheels, unsafe/FFI review, Rust CI and fallback maintenance |
| Selectable Rust engine | At least 2.0x end-to-end in two important user workloads, or a separately validated distribution capability the Python line cannot meet; at least 90% of the agreed core conformance matrix passing | A second engine duplicates orchestration and creates behavior-drift risk |
| Rust v2/full rewrite | At least 2.0x across the weighted representative suite, no important scenario over 10% slower, and an independently justified adoption/distribution benefit | Speed alone must repay connector, plugin, migration and contributor costs; one synthetic win is insufficient |

Any result below a threshold is still useful profiling evidence, but it stops that
branch of the roadmap. Thresholds are evaluated on end-to-end work, not a
microbenchmark that excludes conversion, allocation or I/O.

## Question 2: distribution

Distribution has its own desired outcomes: a user should be able to install drt
without first understanding Python environments; `drt --version` and config-only
commands should feel immediate; CI should install a pinned version reproducibly;
and the artifact must state which built-in drivers and third-party plugins it can
load. Rust is one way to build an executable, not the definition of success.

### Cold-start measurements

The following are fresh-process measurements on the environment in this ADR's
header. Each command ran 20 times with stdout/stderr discarded. They use warm OS
filesystem caches; they are process-start measurements, not a claim about a
machine immediately after boot. `drt validate` ran against
`examples/review-demo` without connection checks.

| Command | First | Median | p95 | Range |
|---|---:|---:|---:|---:|
| `drt --version` | 155.2 ms | 154.1 ms | 159.5 ms | 151.0–162.8 ms |
| `drt validate` | 270.2 ms | 229.7 ms | 245.2 ms | 222.6–270.2 ms |

CPython import-time tracing (`PYTHONPROFILEIMPORTTIME=1`, equivalent to
`python -X importtime`) perturbs absolute timing, so its cumulative values must
not be added together. It does identify the import graph responsible for startup:

| Path | Representative import-time observation |
|---|---|
| `drt --version` | `drt.cli.main` 170.6 ms cumulative; `drt.cli.commands` 135.6 ms; `commands.apply` 121.3 ms; `drt.cli.output` 120.0 ms; `drt.config.models` 92.4 ms. Largest self times were `drt.config.base` 14.5 ms, `sync_options` 10.5 ms and SaaS destination configs 10.4 ms. |
| `drt validate` | Connector-registry import was 59.7 ms cumulative even for config validation. `psycopg2._psycopg` cost 15.5 ms self; the Airtable destination path was 24.8 ms cumulative/5.1 ms self; `sync_options` cost 10.2 ms self. |

This follows directly from the code: importing `drt.cli.main` imports the
`commands` package, which imports all 23 command modules for decorator side
effects. Importing the connector registry imports and registers every built-in
source and destination, including optional driver paths that happen to be
installed.

A lower-bound import probe does not implement a usable CLI, but it shows the
available order of magnitude. Across 20 fresh processes, `import drt` took
31.3 ms median and importing `drt` plus the shared Typer app took 39.0 ms,
versus 150.3 ms for importing the current `drt.cli.main`. Thus roughly 111 ms of
the current module import is in eager CLI registration, not irreducible Python
startup. A lazy-command prototype should be measured before a Rust launcher is
credited with that win.

The first distribution gate is successful if, on this same benchmark machine,
lazy imports make `drt --version` median at most 75 ms and `drt validate` median
at most 175 ms, without changing command help, shell completion, plugin discovery
or validation. These are prototype gates, not cross-machine SLAs.

### Rewrite-free and low-rewrite options

The characteristics of the external packaging tools below are **external and
unverified in this repository**. A spike must confirm them on supported platforms
and with drt's actual native drivers and entry-point plugins.

| Option | What it can deliver | Cost and plugin/driver trade-off |
|---|---|---|
| `uv tool install` | Simple isolated CLI installation and pinning; a strong default install instruction | Not one binary. It delegates environment/runtime management to uv. Python entry-point plugins and ordinary wheels can retain their current model, subject to uv's environment rules. |
| `pipx` | Familiar isolated CLI environment with the existing wheel and extras | Requires a Python/pipx installation and does not improve interpreter startup. It preserves the current plugin and driver ecosystem most directly. |
| `shiv` / zipapp | One distributable Python archive and reproducible base dependency set | Still needs a compatible Python. Native wheels/drivers and extraction/cache behavior need platform testing. Adding plugins outside the archive either needs a supported side environment or a rebuilt archive. |
| PyInstaller | Standalone directory or one-file application with a bundled interpreter | OS/architecture-specific builds, hidden-import hooks and one-file extraction/startup cost. Bundling all warehouse SDKs could make artifacts large; excluding them creates connector editions. Ordinary Python entry-point plugins cannot simply be installed into an opaque frozen executable without a designed sidecar/plugin path. |
| Nuitka | Standalone/one-file Python application compiled through C | More complex compiler and release matrix; it does not remove Python semantics or automatically solve dynamic discovery. Native SDKs and entry points need the same explicit compatibility matrix as PyInstaller. |
| Thin Rust launcher | A small native command can select/download a drt runtime, verify it, and exec it | Fast launcher startup does not remove the Python child startup or environment. It adds a second updater, cache, signing and failure surface. Plugins remain possible only in the managed Python payload; a truly standalone launcher must define how they are installed. |
| Container image | No host Python, pinned base dependencies, natural CI/server execution and optional full-driver images | Image pull and CVE/rebuild cost; weak fit for an interactive local CLI. Third-party plugins require a derived image or an explicit runtime install/mount. Multi-architecture images and cloud credentials remain release work. |
| Homebrew / winget | Discoverable native package-manager UX and upgrades | These are channels, not a packaging format. A formula/manifest must still install a wheel environment, launcher or frozen artifact. Every release adds checksum, signing/notarization and channel maintenance. Plugin behavior follows the underlying artifact. |

No single frozen artifact can simultaneously be small, bundle every optional
warehouse SDK, and accept arbitrary Python entry-point plugins without some form
of external environment. A packaging spike must choose and name tiers, for
example:

- a base executable for HTTP/file and dependency-light connectors;
- connector-specific or "full" artifacts with selected vendor SDKs; and
- the ordinary Python wheel as the extensible distribution for third-party
  plugins.

That is a product contract, not an implementation detail. If "one binary" means
"every built-in and every third-party Python plugin works," the requirement is
internally inconsistent unless the binary embeds and exposes a managed Python
environment.

### Distribution decision criteria

The rewrite-free spike must build on Linux x86_64, macOS arm64 and Windows
x86_64 before it can be called a user-facing distribution. macOS x86_64 and Linux
arm64 may be added when demand or CI runners justify them. For each artifact it
must publish:

- size, clean-install time and `--version`/`validate` startup;
- included connector/driver matrix;
- plugin support or an explicit statement that the artifact is closed;
- SBOM, signing/notarization plan and vulnerability rebuild procedure; and
- a smoke test for config parsing plus at least one source/destination of every
  included native-driver family.

Continue with a frozen/launcher distribution only if it removes the host-Python
requirement on all three initial platforms, meets the declared connector matrix,
and keeps median `--version` below 100 ms on the reference machine. Stop or retain
it as an experimental convenience if hidden imports, driver size, code signing or
plugin incompatibility makes the artifact harder to support than `uv tool`/pipx
plus the existing wheel.

## Question 3: positioning and brand

A credible "v2 is Rust" statement requires more than a Rust executable name. It
requires a meaningful Rust-owned execution path, a supported feature/connector
matrix, a migration contract, operational evidence and a reason users experience.
Calling a Python payload through a thin Rust launcher may improve installation,
but presenting that as a Rust engine would confuse distribution with
implementation.

The cost is unusually high for an integration product:

- Thirty-seven destinations contain vendor-specific authentication, retry,
  pagination, rate-limit, idempotency, serialization, schema and mirror behavior.
  Many use Python SDKs or the shared Python HTTP stack.
- Thirteen sources depend on Python database/lakehouse drivers and their exact
  streaming, retry and type-conversion behavior.
- Seven entry-point groups make installed Python packages part of the public
  ecosystem. A standalone Rust engine needs a stable cross-language plugin ABI,
  a subprocess/RPC bridge, WebAssembly, or an explicit decision to abandon that
  compatibility.
- `pyproject.toml` shows the current driver ecosystem: Snowflake, Databricks,
  BigQuery, psycopg2, ClickHouse, Delta Lake, Iceberg, pymssql, PyMySQL, Google,
  Azure and AWS libraries. The claim that Rust has no equivalents of equal
  maturity for all of these exact drt needs is **external and unverified here**;
  what the repository proves is that replacing them requires revalidating every
  current behavior against a different client.
- The contributor path today is "add a Python config/class/test and register it."
  A Rust-first core raises the toolchain and ownership threshold, while a dual
  implementation splits reviews and bug fixes.

Possible brand benefit is also **external and unverified** until measured with
users. Stars, social enthusiasm, or another project's language choice do not show
that drt users will accept fewer connectors, a closed plugin binary, or a migration
to obtain Rust. Before using "v2" publicly, the project should test concrete
messages separately: "installs as one executable," "starts in N ms," "uses less
memory," and "runs workload X Y times faster." Those are falsifiable benefits;
"built for Rust" alone is not.

## Roadmap options

### A. Stay Python; optimize imports and ship better packaging

| Dimension | Assessment |
|---|---|
| Effort | Small for lazy CLI/registry imports; medium for a signed multi-platform frozen artifact and package-manager channels |
| Risk | Low for imports when help/completion tests are retained; medium for frozen-driver and plugin behavior |
| Reversibility | High. The wheel and existing runtime remain authoritative. |
| Contributor impact | Minimal; current Python connector/plugin funnel remains intact |
| Must be true before starting | Startup baseline is reproducible; packaging editions and plugin promises are explicit |
| Continue | Startup gates are met; at least one no-host-Python artifact passes the declared connector matrix; release maintenance is affordable |
| Stop | Eager-import work does not move startup, or frozen packaging creates an unbounded driver/plugin support matrix |

**Recommendation:** start here. It directly tests #1017's stated distribution
problem and preserves all later Rust choices.

### B. Add in-process Rust extensions for measured hotspots

| Dimension | Assessment |
|---|---|
| Effort | Medium per kernel plus wheel, fallback, CI and release work |
| Risk | Medium: conversion copies, platform wheel gaps and semantic drift can erase the microbenchmark win |
| Reversibility | High if the Python implementation remains a tested fallback behind one internal interface |
| Contributor impact | Most work stays Python; hotspot maintainers need Rust/PyO3; source builds become harder on unsupported platforms |
| Must be true before starting | A scenario in the new suite identifies a stable hotspot and a throwaway prototype clears the narrow-extension threshold |
| Continue | Two representative end-to-end scenarios improve by at least 25%, correctness is byte/behavior equivalent, and remote controls do not regress over 5% |
| Stop | Large-batch results remain near parity, boundary conversion dominates, or wheels cannot cover supported Python/platform versions |

**Recommendation:** allow only evidence-selected kernels. Do not begin with a
port of the whole engine.

### C. Add a separate selectable engine in this repository

The user surface would be explicitly experimental, for example
`drt run --engine=rust`, with Python remaining the default. Initially the Rust
engine may own orchestration/transforms while adapting to Python connectors; that
tests semantics and performance but does **not** produce a standalone binary.

| Dimension | Assessment |
|---|---|
| Effort | Large: engine interface, adapter boundary, dual execution tests and conformance fixtures |
| Risk | High: two engines can produce subtly different rows, checkpoints, retries, plans or finalization outcomes |
| Reversibility | High for users while the flag is experimental; medium for maintainers once both paths acquire users |
| Contributor impact | Engine work splits by language; connector authors can remain in Python while the adapter exists |
| Must be true before starting | B or an engine-specific prototype meets the speed/distribution gate; the versioned YAML and behavior conformance suite exists; ownership is funded |
| Continue | At least 90% of the agreed core matrix passes, two important workloads are at least 2x faster or distribution proves uniquely better, and bug fixes can be shared rather than duplicated |
| Stop | The adapter consumes the win, state/plan semantics diverge, or the experimental path cannot keep up with two consecutive feature releases |

**Recommendation:** keep as a gated experiment, not a v2 promise.

### D. Develop a parallel `drt v2` line in a separate repository

A repository such as `drt-rs` would read the same versioned YAML contract. Users
would opt in per project and could keep v1 for connector/plugin coverage.

| Dimension | Assessment |
|---|---|
| Effort | Very large: standalone CLI, config/template/state implementation, connectors, packaging and migration tooling |
| Risk | High product fragmentation and schema/behavior drift; lower risk of destabilizing v1 |
| Reversibility | High at project level because users choose; low for work invested in duplicated integrations |
| Contributor impact | Clean Rust contributor path, but splits issue triage, docs and the existing Python community |
| Must be true before starting | C demonstrates demand or a standalone prototype demonstrates a capability unavailable in-process; a maintainer owns the new line; a minimum connector set has named users |
| Continue | Early adopters run real projects, pinned conformance stays green, the minimum connector set grows without starving v1, and releases can be made independently |
| Stop | Adoption remains experimental-only, most fixes must be hand-ported, or same-YAML compatibility repeatedly blocks independent design |

**Recommendation:** if a genuinely standalone v2 earns a start, use a separate
repository rather than turning the stable Python repository into two products.

### E. Full rewrite and replacement

| Dimension | Assessment |
|---|---|
| Effort | Extra-large and multi-release: every engine, state, CLI, config, driver, connector and plugin contract must be replaced or dropped |
| Risk | Very high correctness, parity, migration and contributor risk |
| Reversibility | Low after the Python line is deprecated or maintainers/users migrate |
| Contributor impact | Maximizes the Rust funnel but discards the easiest Python contribution path and makes existing plugins incompatible without a bridge |
| Must be true before starting | D has already proved broad demand, performance/distribution thresholds, feature parity and sustainable staffing; a published migration/deprecation policy is accepted |
| Continue | Weighted benchmarks remain at least 2x, no important scenario regresses over 10%, connector/state/plan conformance reaches the agreed release bar, and v1 users demonstrably migrate |
| Stop | Any prerequisite is absent. After work begins, missing two consecutive parity milestones triggers scope reduction back to D rather than an indefinite rewrite. |

**Recommendation:** do not start. It is the least reversible answer to three
questions that can all be tested more cheaply.

## Staged plan and decision gates

Decisions are evidence-triggered, not tied to a marketing date.

### Gate 0 — accept the questions and preserve the option

Accept this ADR as the measurement roadmap, not as approval for Rust. Keep the
observer/protocol boundary clean and keep v1 compatibility tests authoritative.
Do not announce a Rust v2.

### Gate 1 — distribution and import spike

Unpark or replace #1017 with two bounded experiments:

1. lazy command registration, a pre-import `--version` path, and lazy built-in
   connector implementation imports; and
2. one environment-based install (`uv tool` or pipx), one frozen artifact
   (PyInstaller or Nuitka), and the existing container path.

Decide A after publishing the startup and artifact matrices. If lazy imports meet
the 75/175 ms gates, cold start is not a reason to choose Rust. If a frozen
artifact fails, retain the environment-based install rather than widening the
scope to a rewrite automatically.

### Gate 2 — representative performance suite

Add the six scenario families above to the benchmark harness and collect at least
one user-provided production shape with redacted/synthetic data. Decide whether
any hotspot qualifies for B. No qualifying hotspot means stop the speed branch
and continue Python optimization.

### Gate 3 — throwaway native kernel

Prototype only the highest-ranked hotspot, include conversion and allocation,
and do not commit the prototype as product code until it clears the 2x-kernel and
25%-end-to-end thresholds. If it passes, ship it as optional/invisible B with a
Python fallback and remeasure after one release before expanding the native
surface.

### Gate 4 — experimental engine

Before C, publish a conformance version covering config parsing, field mapping,
computed fields/masks, batching, modes, errors, state/watermarks, history, DLQ,
guards, plan/apply and observer-visible outcomes. Start with dependency-light
local source/destination pairs. The flag remains experimental and non-default
until it passes 90% of the agreed core matrix and its benefit survives real
connectors.

### Gate 5 — standalone v2 and repository decision

Only after Gate 4 should the maintainer decide whether the Rust implementation is
a kernel, a second engine or a separate product. D requires named early adopters,
a funded maintainer and a versioned release of the shared spec. E requires D to
have already proved migration, parity and demand; it is not a shortcut around D.

## Repository and conformance ownership

For B and C, a monorepo is preferable. It permits atomic changes to YAML schema,
golden fixtures, Python behavior and Rust behavior; one PR can show the
compatibility result. Rust jobs can be path-filtered, while a required conformance
job runs when config/engine/spec files change. Python wheels and any Rust crate or
binary should still have independent release artifacts so a native experiment
does not force a drt-core release.

The monorepo cost is heavier checkout/toolchain/CI, coupled review permissions
and temptation to make every Python contributor install Rust. Rust should
therefore remain optional for ordinary Python development until C becomes a
supported engine.

For D, a separate repository is preferable. It gives the standalone CLI an
independent issue tracker, release cadence, binary signing workflow and language
identity, and prevents v1 maintenance from being blocked by Rust releases. The
cost is drift and non-atomic changes.

The current repository should initially own the normative, implementation-neutral
YAML schema and golden conformance fixtures because it owns the shipped contract.
They should be published as a versioned artifact that `drt-rs` pins. The Rust repo
should run pinned conformance on every PR and latest-main conformance nightly; the
Python repo should run the last released Rust consumer in a compatibility job when
the shared spec changes. If both implementations become independently governed,
the spec can then move to a small neutral `drt-spec` repository. Creating that
third repository before two real consumers exist would add process without
reducing drift.

Release cadence should remain independent: Python v1 releases must not wait for
experimental Rust parity, and Rust releases must declare the exact conformance
version and connector matrix they implement. "Reads the same YAML" means passes
that declared version; it must not mean that either implementation silently
accepts fields it cannot execute.

## Consequences

- Rust remains a real option, but each increase in scope requires stronger
  end-to-end evidence.
- Distribution can improve without changing the engine language, and plugin-rich
  wheel installs can coexist with a smaller closed executable.
- The current Python plugin and connector ecosystem remains the default while a
  native path is experimental.
- Maintaining a conformance suite becomes prerequisite work for dual engines and
  also improves v1 compatibility testing on its own.
- A future Rust v2 may intentionally support a smaller connector matrix, but it
  must say so by artifact/release rather than implying parity from shared YAML.
- The roadmap rejects language choice as a proxy for performance, installation
  quality or market demand; each is measured independently.

## Decisions (owner-approved 2026-11-20)

The maintainer accepted the recommended answer to every question below. They are
normative for the implementation PRs.

1. **Should speed, distribution and brand remain three independent decisions?**
   **Recommendation: yes.** It prevents a packaging problem or external narrative
   from being used as performance evidence.
2. **Should #1017 be unparked as a lazy-import plus packaging spike before Rust
   engine work?** **Recommendation: yes.** Current imports expose roughly 111 ms
   of non-essential CLI-module loading, a cheaper and reversible first target.
3. **Should the speed thresholds in this ADR be the gates for native work?**
   **Recommendation: yes, provisionally.** They price PyO3/dual-engine maintenance
   against end-to-end benefit and can be amended before a funded prototype.
4. **Which option is the default roadmap now?** **Recommendation: A, with B
   permitted only for a qualifying hotspot.** This preserves connector, driver
   and contributor reach while collecting better evidence.
5. **May option C start before a conformance suite and a qualifying benchmark?**
   **Recommendation: no.** A selectable engine without both would create an
   unmeasurable second implementation.
6. **Where should Rust work live?** **Recommendation: monorepo for B/C; a separate
   `drt-rs` repository for D.** Atomic compatibility matters while code is shared;
   independent cadence matters once it is a standalone product.
7. **Who owns same-YAML compatibility?** **Recommendation: keep the versioned
   schema and golden fixtures in this repository initially, publish them, and
   split to `drt-spec` only after two active consumers exist.** This avoids both
   premature governance and silent drift.
8. **When may the project use a "v2 built for Rust" message?** **Recommendation:
   only after Gate 4 proves a material Rust-owned path, its measured user benefit
   and its connector/conformance matrix.** Until then, describe concrete startup,
   packaging or hotspot improvements rather than an unverified language promise.
