# ADR 0009: Third-party connector configs are blocked by closed config unions

## Status

Resolved by [#997](https://github.com/drt-hub/drt/issues/997) — see
[Resolution](#resolution). Accepted before that as a record of the blocker;
the Context and Decision below are kept as written so the reasoning that led
here stays readable.

## Context

Issue #297 asks for entry-point based plugin registration so that `pip
install drt-salesforce-premium` can register a new source or destination
without a code change to drt-core, mirroring how `SecretProvider.register()`
already lets third-party secret backends register themselves
([`drt/config/secret_providers/base.py`](../../drt/config/secret_providers/base.py)).

The connector registry itself
([`drt/connectors/registry.py`](../../drt/connectors/registry.py)) is not the
obstacle: `register_destination()` / `register_source()` are plain
dict-keyed functions that accept any `type_name` at runtime, entry points or
not. The obstacle is upstream of the registry, in config parsing.

### Destinations: closed discriminated union

`SyncConfig.destination` ([`drt/config/sync_options.py:632`](../../drt/config/sync_options.py))
is typed as `DestinationConfig`, defined at
[`drt/config/sync_options.py:588`](../../drt/config/sync_options.py) as:

```python
DestinationConfig = Annotated[
    RestApiDestinationConfig | SlackDestinationConfig | ... | KlaviyoDestinationConfig,
    Field(discriminator="type"),
]
```

All ~34 members are hand-listed at import time (flagged in the file's own
`# PARITY:` comment as required to match the registry, guarded by
`tests/unit/test_cli_list_connectors.py::test_DESTINATIONS_matches_registry`).
Pydantic validates a discriminated union by matching the payload's `type`
field against this fixed member list. A sync YAML naming an unrecognized
`type` (e.g. `type: salesforce_premium` from a third-party package) fails
`SyncConfig` validation outright — **before** `drt run` ever reaches
`get_destination()`. A plugin can register itself in the connector registry
and still be permanently unreachable from any sync YAML.

### Sources/profiles: closed hand dispatch

`ProfileConfig` ([`drt/config/profiles.py:261`](../../drt/config/profiles.py))
is a plain `|` union of all profile dataclasses, but the more binding
constraint is `load_profile()`
([`drt/config/credentials.py:244`](../../drt/config/credentials.py)): a
linear `if source_type == "bigquery": ... if source_type == "duckdb": ...`
chain ending in `raise ValueError(f"Unsupported source type '{source_type}'. Supported: ...")`.
This is not a pydantic discriminated union, but it is the same shape of
problem — a closed, hand-enumerated set of recognized `type` strings, with
no path for a plugin's source type to reach construction.

**The blocker is symmetric.** Both destinations and sources hit the same
wall — a closed set of recognized `type` values checked before the
connector registry is consulted — via two different mechanisms (pydantic
discriminated union vs. hand-written dispatch).

## Why this ADR does not resolve the blocker

Making `SyncConfig.destination` / `load_profile()` dynamically extensible
means replacing the closed union/dispatch with a registry-driven validator:
look up `type` in a (now plugin-populated) registry, then validate the
remaining fields against whatever config class the registry returns. That
change:

- alters parse-error messages for all ~34 built-in destination types (today
  pydantic's discriminated union produces "unknown type, expected one of
  [...]"; a registry-driven validator produces a different message shape,
  and its exact wording depends on what's been imported/registered by the
  time parsing runs — a strictly worse error for the common case of a
  plain typo, unless carefully engineered);
- removes mypy's static knowledge of `sync.destination`'s concrete type at
  call sites that narrow on it today, including the six per-dialect
  dispatchers in `drt/config/query.py` (deliberately left untouched by
  #469 — see [ADR 0007](0007-protocol-stability-policy.md)'s consequence
  note) and the `isinstance()` narrowing used throughout `drt/destinations/`;
- is exactly the kind of frozen-surface signature change ADR 0007 treats
  conservatively by default — `DestinationConfig` is a public, imported type,
  not an internal implementation detail.

This is a config-layer redesign with blast radius across the whole
codebase, not a plugin-discovery PR. It needs its own design (and its own
review) separate from #297's entry-point mechanics.

## Decision

1. **State the blocker explicitly (this ADR)** rather than silently shipping
   a plugin system that can register third-party connectors but can never
   use them from a sync YAML.
2. **#297 ships the part that works today**: entry-point discovery for the
   registries that do *not* go through a closed config union —
   `PermissionChecker` / `AuditLogger` / extra `SyncObserver`s
   ([ADR 0008](0008-enterprise-boundary-rbac-and-audit-hooks.md) named
   #297 as the eventual fix for exactly these, plus `SecretProvider`, whose
   `register()` pattern already accepts runtime registration and needs no
   config-union change since a secret provider is selected by URI scheme,
   not by a pydantic discriminated field). `drt plugins list` reports what
   was discovered per entry-point group, and connector entry points
   (`drt.sources` / `drt.destinations`) are surfaced as **registered, not
   yet usable in sync YAML** rather than presented as fully working.
3. **Defer the config-union question to a follow-up issue, [#997](https://github.com/drt-hub/drt/issues/997).** Candidate
   directions, none chosen here:
   - Two-pass validation: parse `type: str` + `config: dict[str, Any]`
     loosely first, resolve the concrete config class via the (by-then
     populated) connector registry, then `model_validate()` the dict against
     it — closed union stays as the fast/typed path for built-ins, dynamic
     resolution is a fallback only reached when `type` isn't a static union
     member.
   - A catch-all fallback member appended to the union (e.g. a
     `GenericDestinationConfig` model accepting an open `dict`) that defers
     strict validation until the registry resolves the real type — keeps a
     single pydantic validation pass but weakens error messages for a
     plugin type with a typo'd field.
   - Replace `Annotated[Union[...], Field(discriminator=...)]` with a
     `model_validator(mode="before")` that dispatches through the registry
     unconditionally, built-ins included — most extensible, but is the
     signature change flagged above as needing its own review.
4. **Correct #297's issue scope** to reflect this: entry-point discovery for
   the four non-connector registries and `drt plugins list` in this PR;
   the config-union redesign filed as a separate issue and out of scope
   here.

## Consequences

- #297 ships a real, complete win for `PermissionChecker` / `AuditLogger` /
  extra observers / `SecretProvider` — those are the exact four registries
  ADR 0008 identified as sharing the "requires an explicit operator-side
  import" gap, and this closes it for all four.
- Third-party **connector** packages (the `pip install
  drt-salesforce-premium` scenario in #297's original issue text) remain
  blocked until [#997](https://github.com/drt-hub/drt/issues/997)'s design is chosen and implemented.
  `drt plugins list` must not imply otherwise.
- [#997](https://github.com/drt-hub/drt/issues/997) inherits this ADR's three candidate directions as a
  starting point, not a decision — the choice affects error-message quality,
  mypy narrowing, and validation performance, and deserves its own review
  cycle rather than being folded into #297's PR.

## Resolution

[#997](https://github.com/drt-hub/drt/issues/997) took the **second** candidate
direction above — a catch-all member appended to the union — with one addition
that changes its trade-off materially.

### Destinations

`DestinationConfig` keeps its 34 concrete members and gains a 35th,
[`GenericDestinationConfig`](../../drt/config/base.py). `Field(discriminator="type")`
becomes a *callable* `Discriminator(_destination_tag)`
([`drt/config/sync_options.py`](../../drt/config/sync_options.py)), and every
member carries an explicit `Tag` — pydantic requires one per choice under a
callable discriminator and raises `PydanticUserError` at import without it, so
the annotation cannot silently rot.

`_destination_tag` is three-way, and the middle branch is what this ADR did not
anticipate:

| `type` | routes to | error |
|---|---|---|
| a built-in | that concrete config | unchanged, per-field |
| registered in the connector registry | `GenericDestinationConfig` | — |
| anything else | — | `union_tag_invalid`, as today |

That third row is the addition. This ADR predicted the option would weaken
errors "for a plugin type with a typo'd field", and it does — but the far more
common case, a typo'd *built-in* (`type: postgress`), was also at risk, because
a catch-all that swallows every unknown type makes `drt validate` pass on it and
fail much later at `get_destination()`. Consulting the registry for
membership — not for validation — keeps that case reporting exactly the error
shape it reports today. The constraint at the top of this ADR is therefore met
rather than traded away.

What *is* still weakened is unchanged from the prediction: a plugin's own fields
are `extra="allow"` and carried verbatim, so a typo in one is kept, not
rejected. drt-core does not know the plugin's schema. The registry already
stores a `config_class` that could tighten this in a second pass; #997
deliberately does not implement it.

### Sources / profiles

`load_profile()` keeps its hand-written `if source_type == ...` chain verbatim
and gains a registry lookup *after* it
([`drt/config/credentials.py`](../../drt/config/credentials.py)). Built-ins keep
their exact construction, including per-type defaults, and a plugin cannot
shadow one. Profiles are plain dataclasses with no validator to hook, so there
is no generic model here — the registered profile class is constructed directly
from the YAML mapping, which is the source-side equivalent of accepting extra
fields.

This also fixed a latent gap the audit turned up: `rest_api` was registered as a
source and present in the `ProfileConfig` union, but had no branch in the
dispatch chain, so `load_profile()` rejected it. It now loads through the same
fallback.

### What did not change

The three constraints this ADR raised held:

- **Error messages** — a typo'd or unregistered type still produces
  `union_tag_invalid` at `('destination',)`; a missing `type` still produces
  `union_tag_not_found`; an invalid built-in still produces its per-field error
  inside its own member.
- **mypy narrowing** — the union is still a union of concrete classes written
  out literally, so `isinstance()` narrowing in
  [`drt/destinations/query.py`](../../drt/destinations/query.py) and throughout
  `drt/destinations/` is untouched. The tags are hand-written rather than
  derived precisely so this stays true.
- **ADR 0007 frozen surface** — `DestinationConfig` gains a member; no existing
  config class was modified, and none was removed.

One knock-on the design did not anticipate: `SyncConfig`'s generated JSON Schema
renders the union as `oneOf`, so an open catch-all made every built-in payload
match two members at once and fail `drt validate`'s schema pass.
`GenericDestinationConfig.__get_pydantic_json_schema__` pins its `type` to the
plugin types the registry currently holds, which mirrors the discriminator's
decision and keeps the schema and the parser in agreement. The consequence is
that `drt schema` output reflects the plugins installed when it runs — a static
file cannot describe types that arrive by `pip install`.
