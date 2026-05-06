> **Pre-1.0 Notice:** Until v1.0, drt may introduce breaking changes in MINOR
> releases (per [semver §4](https://semver.org/#spec-item-4)). The deprecation
> cycle and communication rules below still apply — we simply relax the
> "MAJOR-only" guarantee. v1.0 will freeze the Protocol (see #300); from that
> point, all rules in this document are firm.

# Versioning Policy

drt follows **semantic versioning** (MAJOR.MINOR.PATCH). This document defines what constitutes breaking changes, how deprecations work, and how we communicate version updates.

## Semantic Versioning

- **MAJOR** — breaking changes to public APIs, schemas, or CLI output
- **MINOR** — new features, new destinations/sources, backwards-compatible additions
- **PATCH** — bug fixes, internal improvements, non-breaking tweaks

## Breaking Changes (MAJOR bump required)

A breaking change is anything that requires users to change their code, YAML configs, or scripts to keep drt working.

### Public API Breaking Changes

Removing or renaming methods in:

- `Source` protocol (e.g., `Source.extract()`)
- `Destination` protocol (e.g., `Destination.load()`)
- `StateManager` class
- Any public class or method imported from `drt.*`

**Example:** Renaming `Destination.load(records, config, sync_options)` → `Destination.process_records()` requires a MAJOR bump.

> See also: [#300 — Protocol stability review and freeze preparation](https://github.com/drt-hub/drt/issues/300) (v0.9 ROADMAP).

### Sync YAML Schema Breaking Changes

Removing or renaming a sync config key that is not already deprecated.

**Examples that trigger MAJOR:**

- Removing `sync.batch_size`
- Renaming `model: ref('users')` → `source: ref('users')`
- Changing how `mode: full|incremental` is parsed

**Examples that do NOT trigger MAJOR:**

- Adding optional keys with defaults
- Adding a new value to an enum (unless removing existing values)

### CLI Interface Breaking Changes

Removing or changing:

- Exit codes (e.g., exit 0 for success must stay 0)
- `--output json` schema format (structure of JSON output)
- `--log-format json` output format (structure of log output)
- Required vs optional flags

**Examples that trigger MAJOR:**

- Changing `drt run` to exit code 1 on dry-run success
- Changing `--output json` output structure
- Changing `--log-format json` output structure
- Removing `--profile` flag

**Examples that do NOT trigger MAJOR:**

- Adding new CLI flags
- Adding new commands
- Changing help text

## Non-Breaking Changes (MINOR or PATCH)

### Always Safe

- Adding optional YAML keys with sensible defaults
- Adding new CLI flags (existing scripts continue working)
- Adding new destinations or sources
- Adding new `--output` format options
- Adding optional parameters to public functions (with defaults)
- Internal module reorganization (non-public imports)
- Bug fixes that make behavior match documentation

### Safe When Deprecated

See "Deprecation Cycle" below.

## Deprecation Cycle

Removing a feature safely requires this process:

### Step 1: Announce [DEPRECATED]

Add to `CHANGELOG.md` with `[DEPRECATED]` tag:

```markdown
### v0.5.0

**Deprecated:**

- [DEPRECATED] `sync.batch_size` key — will be removed in v0.7.0. Use `sync.batch_config.size` instead.
- [DEPRECATED] `--legacy-auth` flag — use environment variables instead.
```

### Step 2: Add Tooling Support

- `drt validate` should emit a warning if deprecated features are used (planned — tracked in #467; not yet wired up in current releases)
- Documentation should link to migration guide
- Error messages should suggest the replacement

### Step 3: Maintain for Two Minor Releases

A deprecated feature announced in v0.5.0 must continue working through at least v0.6.x. Removal can happen in v0.7.0 or later.

**Minimum timeline:**

- v0.5.0: Announced deprecated
- v0.5.x + v0.6.x: Maintained with [DEPRECATED] warning
- v0.7.0+: Can be removed

### Step 4: Communicate Removal

When actually removing a feature, GitHub release notes must have a "Breaking Changes" section:

```markdown
## v0.7.0

### Breaking Changes

- Removed `sync.batch_size` (deprecated in v0.5.0). Use `sync.batch_config.size` instead.
- Removed `--legacy-auth` flag. Use `DRT_AUTH_TOKEN` env var instead.

### Migration Guide

See [MIGRATION_v0.6_to_v0.7.md](./docs/migration/v0.6-to-v0.7.md) for step-by-step instructions.
```

## Communication

Every release notes entry must clarify:

1. **"Breaking Changes" section** — even if empty, always include it
2. **Migration path** — how to adapt code/configs
3. **Timeline** — when feature was deprecated vs removed

### Breaking Changes Section Example

```markdown
## Breaking Changes

**None.** This is a safe upgrade.
```

or

```markdown
## Breaking Changes

- **CLI:** Exit codes changed:
  - `drt run` now exits 2 (not 1) on validation error
  - Migrate: Check `$?` == 2 instead of `$?` == 1 in scripts

- **YAML:** `incremental.lookback` renamed to `sync.cursor_lookback`
  - Migrate: Update all syncs with find-and-replace
  - Timeline: Announced deprecated in v0.4.0; removed in v0.5.0

For more details, see migration guide: `docs/migration/v0.4-to-v0.5.md`
```

### Migration Guides

Create a file `docs/migration/v{OLD}-to-v{NEW}.md` for MAJOR version bumps:

````markdown
# Migrating from v0.4 to v0.5

## CLI: Exit Codes

**Old behavior:**

- Exit 0: success
- Exit 1: validation error
- Exit 2: runtime error

**New behavior:**

- Exit 0: success
- Exit 2: validation error (changed!)
- Exit 1: runtime error (changed!)

**Update your scripts:**

```bash
# Old
drt run || [ $? -eq 1 ] && handle_validation_error

# New
drt run || [ $? -eq 2 ] && handle_validation_error
```
````

```

```

**[Similar sections for each breaking change]**

````

## Examples

### Example 1: Safe MINOR Release

```markdown
## v0.5.0

### New Features

- Added `sync.retry_backoff` optional YAML key (defaults to exponential)
- Added `--max-retries` CLI flag
- Added support for Salesforce Bulk API destination

### Bug Fixes

- Fixed rare race condition in state manager

## Breaking Changes

None. This is a safe upgrade.
````

### Example 2: Breaking MAJOR Release

```markdown
## v1.0.0

### Removed

- `sync.batch_size` (use `sync.batch_config.size`)
- `--legacy-auth` flag (use `DRT_AUTH_TOKEN`)

### Changed

- Exit codes for validation errors (now exit 2, not 1)
- `--output json` includes new `metadata` field

## Breaking Changes

- **Exit codes:** `drt run` validation errors now exit 2 (not 1)
- **YAML:** `sync.batch_size` removed; use `sync.batch_config.size`
- **CLI:** `--legacy-auth` removed; use `DRT_AUTH_TOKEN` environment variable

Migration guide: See `docs/migration/v0.1-to-v1.0.md`
```

## Policy Enforcement

- **Release checklist:** Before releasing, manually verify the CHANGELOG includes a "Breaking Changes" section when the release contains breaking changes
- **PR reviews:** Check CHANGELOG for `[DEPRECATED]` tags if removing features
- **CI:** `make release-check` validates release metadata consistency, the presence of a CHANGELOG entry, and required version/documentation references
- **Docs:** This file is the source of truth for versioning decisions and release documentation expectations

## Questions?

- Technical questions: [GitHub Discussions](https://github.com/drt-hub/drt/discussions)
- Policy questions: Open an issue tagged `versioning`
