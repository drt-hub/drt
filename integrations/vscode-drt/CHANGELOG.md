# Changelog

All notable changes to the **drt — Reverse ETL** VS Code extension are documented
here. This extension versions independently of `drt-core`; each release notes the
drt-core version its bundled schemas were generated from.

## [0.1.13] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: the `klaviyo` destination
  gains an `endpoint: profile | event` mode plus `metric_name`,
  `metric_name_field`, `time_field`, `value_field`, and `unique_id_field` for
  sending events (not just profile updates) to Klaviyo's Events API
  (drt-hub/drt#1052).
- Bundled JSON Schemas regenerated from drt-core: the destination union gains
  the `meta_conversions` connector for batched Meta Conversions API events,
  including field mappings, hashing inputs, retry, and rate-limit configuration
  (drt-hub/drt#1054).
- Bundled JSON Schemas regenerated from drt-core: `meta_conversions.event_time_field`
  is now required (previously optional and nullable) — without an explicit
  mapping, every row was silently stamped with the current sync time instead of
  its real transaction time (drt-hub/drt#1077).
- Bundled JSON Schemas regenerated from drt-core: `staged_upload` and
  `salesforce_bulk` destinations now validate destination-level `retry` and
  `rate_limit` overrides, matching every other rate-limited destination type
  (drt-hub/drt#1048).
- Bundled JSON Schemas regenerated from drt-core: `StateConfig` gains a new
  `state.profile` field, reserved for a future `warehouse` state backend
  (drt-hub/drt#920) — currently rejected in every configuration, since
  `state.backend`'s enum does not accept `warehouse` yet.
- Bundled JSON Schemas regenerated: the `destination` union's schema also picked
  up the #997 callable-discriminator shape (a flat `oneOf` list including
  `GenericDestinationConfig`, no `discriminator`/`mapping` block) — the
  previously-committed copy hadn't actually reflected that change, because the
  exact JSON Schema this union serializes to depends on the installed pydantic
  version and isn't pinned (drt-hub/drt#1070). No behavior change: draft-07
  doesn't use `discriminator` as a validation keyword either way.
- Bundled JSON Schemas regenerated from drt-core: `sync.batch_size` now has a
  minimum of `1` (`exclusiveMinimum: 0`) — `0` or a negative value used to
  either crash or silently insert nothing on some destinations while
  reporting success (drt-hub/drt#961).
- Bundled JSON Schemas regenerated from drt-core: the `destination` union gains
  a `GenericDestinationConfig` member for third-party connector types, and the
  OpenAPI-style `discriminator` block is gone — drt-core now discriminates the
  union with a callable rather than a field name (drt-hub/drt#997). No
  validation change for the built-in types: `discriminator` is not a draft-07
  keyword, so it was inert here, and each member still pins its own `type`.
  Note the bundled schemas are static, so a *plugin's* destination type is not
  known to them and an editor will still flag it — `drt validate` accepts it.
- Bundled JSON Schemas regenerated from drt-core: `rest_api` destinations now
  validate `body_mode`, `batch_template`, `max_records_per_request`, and
  `error_path` for batch request bodies (drt-hub/drt#770).
- Bundled JSON Schemas regenerated from drt-core: `drt_project.yml` now validates
  the `state` backend block (`backend`, `bucket`, `prefix`) introduced for
  drt-hub/drt#756.
- Bundled JSON Schemas regenerated from drt-core: `state.backend` now accepts
  `gcs` in addition to `local`, and `HistoryConfig` gains `max_entries`
  (drt-hub/drt#756).
- Bundled JSON Schemas regenerated from drt-core: `state.backend` now accepts
  `s3`, with new `region`, `aws_profile`, `aws_access_key_id_env`,
  `aws_secret_access_key_env`, `aws_session_token_env`, and `endpoint_url`
  fields (drt-hub/drt#756).
- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates a
  `sync.metadata_columns` block (`{synced_at, run_id, sync_name}`) for opt-in
  engine-injected bookkeeping columns (drt-hub/drt#762).

## [0.1.12] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates a
  `sync.computed_fields` block (`{field_name: jinja_template}`) for declarative
  derived columns (drt-hub/drt#763).
- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates a
  `sync.unit_tests` block (`{name, given, expect}`) for offline transform-pipeline
  tests (drt-hub/drt#780).

## [0.1.11] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml`'s `mirror`
  block docs now describe `strategy: tracked` + `scope` composition
  (drt-hub/drt#694).

## [0.1.10] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `drt_project.yml` now validates
  the `query_tagging` block (`enabled`, `extra`) for cost-attribution query
  tagging (drt-hub/drt#768).

## [0.1.9] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates a
  `rate_limit` block on each destination (not just under `sync:`), plus
  `rate_limit.burst`; `requests_per_second` widened from integer to number
  (destination-level rate limiting, drt-hub/drt#769).

## [0.1.8] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` tests now
  validate `name`, `query`, and `severity` (custom SQL query tests + warn
  severity, drt-hub/drt#779).

## [0.1.7] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates the
  `alerts.on_degraded` block (degraded-sync alert conditions, drt-hub/drt#784).

## [0.1.6] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `drt_project.yml` now validates
  the `vars:` block (project vars, drt-hub/drt#783).
- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates the
  `sync.match_policy` field (`upsert` | `update_only` | `create_only`,
  drt-hub/drt#757).

## [0.1.0] - Unreleased

Initial release.

- YAML validation, autocomplete, and hover for `drt_project.yml` and `syncs/*.yml`
  via a `yamlValidation` contribution to `redhat.vscode-yaml`.
- Bundled JSON Schemas generated from drt-core **0.7.9**
  (`drt.config.schema.generate_sync_schema()` / `generate_project_schema()`).
- No runtime code — declarative schema association only.
