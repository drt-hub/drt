# ADR 0006 — `drt docs` manifest schema v3

- **Status:** Accepted
- **Issue:** [#772](https://github.com/drt-hub/drt/issues/772)
- **Implementation:** `drt/docs/manifest.py` (schema), `drt/docs/builder.py`
  (producer)

## Context

The `state:modified` selector needs to compare the current definition of each
sync with a baseline produced by an earlier CI run. Task 1 of #772 established
the canonical, deterministic per-sync fingerprint in
`drt/config/fingerprint.py`, but that value must live in a stable artifact a CI
job can save and read later.

`manifest.json` is already drt's public, versioned artifact for project catalog
facts. Exposing the fingerprint there avoids a second baseline format and lets
later selector work load it through the existing `Manifest.from_dict()` path.

## Decision

### One additive field per sync, one version bump

```
sync.config_hash  str, omitted when absent
```

v3 is a **pure superset of v2**: nothing is renamed or removed, every v2
consumer keeps working unchanged, and `schema_version: 3` signals that the
field is available. No migration is required. As with v2, the version moves
because the public shape of `Sync` changed, even though the change is additive.
Like `state`/`runs`/`fields`/`dlq_depth` before it, `config_hash` is present
in the serialized dict only when it has a value — never emitted as `null` —
so a re-serialized v1/v2 manifest never gains a schema-v3-only key its
declared `schema_version` doesn't actually support.

The builder computes the fingerprint map once per manifest and looks up each
entry by the resolved sync name. Filesystem-backed syncs are expected to have a
string value; an absent (`None`) in-memory value is defensive for a sync whose
source file cannot be found or read, or for a future non-file-backed sync
source — in the serialized JSON, that absence means the key is omitted
entirely rather than written as `null` (see the omission note above).

### The fingerprint is repo-derived, not run state

`config_hash` is emitted whether or not machine-local state is included.
`--no-state` continues to remove the latest-state snapshot, history, and DLQ
depth only; it does not remove the fingerprint, just as it does not remove tags,
fields, or edges. This lets CI generate a state-free baseline without losing the
content comparison primitive it needs.

The hash deliberately does not depend on `generated_at`, `drt_version`, run
history, watermark state, or DLQ depth. Those guarantees are owned and tested by
`drt.config.fingerprint`; the manifest only transports the resulting string.

### Older manifests remain loadable

`Manifest.from_dict()` accepts v1/v2 sync records with no `config_hash` key and
sets the in-memory value to `None`, while preserving the manifest's original
`schema_version`. This is parsing compatibility only: the selector decides how
to handle a baseline too old to contain comparable fingerprints.

### Fingerprint semantics stay outside the manifest

The manifest builder only exposes `sync_fingerprints(project_dir)` output. It
does not reinterpret inputs, hash resolved runtime state, or add secret and
determinism rules of its own; those semantics belong to the already-reviewed
fingerprint module.

## Consequences

- CI can persist one `manifest.json` artifact and later compare stable per-sync
  content hashes for `state:modified`.
- v1/v2 manifests still deserialize, allowing callers to produce a clear
  version error instead of failing on a missing dictionary key.
- `--no-state` manifests remain sufficient baselines because `config_hash` is a
  repository fact rather than machine-local state.
- A v2 consumer that ignores unknown fields continues to work. A consumer that
  pins `schema_version == 2` will refuse v3, as intended by ADR 0001's versioning
  contract.
