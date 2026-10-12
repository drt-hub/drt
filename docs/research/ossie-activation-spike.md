# Apache Ossie activation spike (#1254)

## Question

Can drt turn an Apache Ossie (formerly Open Semantic Interchange, OSI)
semantic-model document into a useful, reviewable activation scaffold without
making an unstable specification or an unavailable Python package part of the
runtime?

This spike implements the narrowest reversible answer: `drt init
--from-ossie`. It reads YAML/JSON once, writes ordinary drt YAML, and leaves no
Ossie dependency on the run path. The reader is isolated in
`drt/integrations/ossie.py` so the official package can replace it later.

## Findings

### What mapped cleanly

- A dataset maps naturally to one drt sync and its `source` maps to the SQL
  `FROM` relation (or a wrapped subquery).
- Dataset fields map to a deterministic `SELECT <expression> AS <field>` list.
  Per-field dialect selection works cleanly, including a documented
  `ANSI_SQL` fallback. The spike requires lowercase, non-reserved aliases so
  emitted aliases and destination keys remain identical across SQL dialects.
- A dataset `primary_key` maps directly to SQL destination `upsert_key`.
  Composite keys survive for PostgreSQL; a single key maps to HubSpot's
  `id_property`.
- Dataset description plus model name/version gives useful sync provenance.
- Unknown extension keys can be ignored safely by a consumer that reads only
  this subset. `relationships`, `unique_keys`, AI context, and custom
  extensions remain available to the reader even though this scaffold does
  not yet turn them into writes.

### Gaps and unsafe inferences

- **No entity/grain concept for activation.** A dataset key identifies rows,
  but the model does not say which business entity or destination object those
  rows should update. Destination table, HubSpot object type, and property
  names therefore remain TODOs.
- **Primary-key columns are not guaranteed to appear in `fields`.** The
  canonical TPC-DS example keys `store_sales` by `ss_item_sk` and
  `ss_ticket_number` but declares an expression only for the former. The
  scaffold adds a missing key as a direct source-column selection and reports
  it; a future adapter should ask Ossie whether this omission is intentionally
  valid or an example/schema integrity gap.
- **No stable sensitivity/PII flag yet.** drt cannot infer `sync.mask` safely.
  Some proposals have discussed personal/confidential flags, but the core
  input shape used by this spike cannot be treated as a stable governance
  contract.
- **Datatype names do not map to destination types.** Ossie's logical
  `String`, `Integer`, `Decimal`, and temporal types are not enough to choose
  PostgreSQL DDL, HubSpot property definitions, API serialization, precision,
  or nullability. The spike creates no destination schema.
- **Field expressions are dialect-specific.** A field can have several opaque
  dialect expressions. drt can select the requested one or fall back to
  `ANSI_SQL`, but it cannot prove that an ANSI expression runs unchanged on
  every configured warehouse. The scaffold does not try to emulate each
  database parser. It conservatively refuses dialect-specific quoting and
  escaping, control characters, separators, or comment markers when it cannot
  tokenize them unambiguously; the accepted subset requires balanced quotes
  and doubled-quote escapes.
- **Metrics need a group-by grain.** Model-level metrics may span datasets and
  contain aggregation/window logic, but Ossie does not provide the activation
  entity grain needed to build one row per destination object. Metrics are
  listed as “available but not generated (entity grain unclear).”
- **A source can be a query.** The spec's `source` is one string for either a
  table/view or query and does not declare a query dialect. The spike accepts a
  single uncommented `SELECT`/`WITH` query and wraps it; one trailing semicolon
  is stripped, while separators inside quoted literals are preserved.
  Table-like values may have at most three strictly validated unquoted
  identifier segments.
- **The specification is unstable.** `0.2.0.dev0` is explicitly unreleased and
  has already changed the document root from a `semantic_model` array to one
  flat model. The repository's older flights example still uses that nested
  document shape and is rejected, while the current flat TPC-DS example
  scaffolds successfully. Unknown versions warn instead of failing;
  structurally unusable documents still fail clearly.
- **There is no write/activation concept.** Ossie describes analytical
  semantics, not destination credentials, endpoints, object/table names,
  create-vs-update policy, field mappings, rate limits, error policy, or data
  movement. Those choices cannot be inferred from semantic metadata alone.
  Accordingly, the spike offers only PostgreSQL and HubSpot scaffolds with
  real keyed-upsert configuration. It does not offer a generic REST scaffold:
  a plain POST per record would duplicate on replay, and designing a keyed URL
  plus API-specific idempotency contract is outside this metadata format.
- **Import is intentionally bounded.** A model file larger than 5 MB is
  refused, and parser recursion or memory failures are reported as model
  errors. This is a one-time metadata scaffold, not a bulk-data ingestion path.
- **The official package is not currently a viable drt dependency.** It is not
  on PyPI and requires Python 3.11+, while drt supports Python 3.10+. Vendoring
  its full schema/runtime would enlarge core for a one-time scaffold operation.

## Recommendation: B now, preserve C, defer D

Using issue #1254's B/C/D decision frame:

- **B — bounded CLI scaffolding: recommend.** Keep the delivered import as a
  spike/preview on `drt init`. It provides immediate value, produces normal
  reviewable drt YAML, and makes every uncertain activation decision an
  explicit TODO. Gather real models and destination edits before expanding the
  contract.
- **C — official-package adapter: preserve as the next implementation step,
  not a current dependency.** When Apache Ossie publishes a compatible package
  and stabilizes a release, replace the isolated reader behind the same data
  boundary. Before doing so, add versioned compatibility fixtures and decide
  whether drt keeps Python 3.10 support.
- **D — runtime-native semantic activation: defer.** Do not add `model:
  ossie(...)`, automatic joins/metrics, destination schema creation, or an
  always-live Ossie dependency until the spec defines (or drt explicitly asks
  users for) entity grain, sensitivity, query dialect, and activation mapping.
  Any of those can be designed later without invalidating generated YAML.

The next useful evidence is not more parser breadth. It is a small field trial:
run option B against several production-shaped models, record which TODO edits
operators make, and use those edits to decide whether C is merely a parser swap
or whether a separate, explicit activation-mapping extension should be
proposed before any part of D.

## Input-safety posture of the scaffold (added after review)

The reader treats the Ossie file as semi-trusted input and refuses rather than guesses:

- Expressions and source queries are checked by a deliberately small scanner. Anything whose
  tokenization differs between warehouses is refused (backslashes, `$`, prefixed string
  literals, unterminated quotes, `;` / comment markers outside quoted spans), as are Jinja
  markers (`{{`, `{%`, `{#`), because drt renders a sync's `model` through Jinja.
- Generated YAML is emitted through the YAML serializer; text that lands in comments is
  split on every newline kind and prefixed per line.
- Reserved-word checking is best-effort (SQL:2016 plus the common warehouse extras and the
  GoogleSQL list). A missed word produces SQL the warehouse rejects when the sync is
  validated or run, which is a loud failure rather than an unsafe one; the user renames the
  field in the semantic model.
- Dataset names are limited to 64 characters because they become file names.
