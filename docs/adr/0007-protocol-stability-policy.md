# ADR 0007 — Protocol stability policy

- **Status:** Accepted 2026-08-18.
- **Issues:** [#300](https://github.com/drt-hub/drt/issues/300) (this ADR's
  deliverable — review and freeze preparation), feeds
  [#304](https://github.com/drt-hub/drt/issues/304) (the v1.0 freeze itself).
- **Relates to:** [#992](https://github.com/drt-hub/drt/pull/992) (the
  mechanical half of #300 — `@runtime_checkable` consistency and `Raises:`
  documentation across all 17 Protocols, merged ahead of this ADR so it
  describes a surface that's already consistent).
- **Implementation:** none directly. This ADR sets the policy #304 enforces
  and the freeze-scope call each of the 17 Protocols below needs.

## Context

drt has 17 `typing.Protocol` interfaces spanning destinations (`Destination`,
`ConnectionTestable`, `MatchPolicyCapable`, `StagedDestination`,
`OrphanCleanup`, `RowCountable`, `RateLimitKeyed`, `LimiterFactory`), sources
(`Source`, `IncrementalSource`), state (`StateStore`, `HistoryStore`,
`DlqBackend`, `WatermarkStorage`, `ObjectClient`), secrets (`SecretProvider`),
and the engine (`SyncObserver`). #304 commits drt to freezing three of these
("Source, Destination, StateManager") at v1.0 with a semver guarantee and a
"deprecated methods stay for at least 2 minor versions" removal policy — but
neither #304 nor anything else in this repo currently defines **what a
breaking change to a Protocol actually is**, and no deprecation mechanism
exists to enforce the 2-minor-version promise. #300 exists to close that gap
before v1.0 makes the freeze real.

## What makes a Protocol change breaking

`Protocol` is structural typing, not inheritance. This has one consequence
that most breaking-change checklists (written for classes/ABCs) miss:

**Adding a required method to a Protocol breaks every existing
implementer, immediately, with no deprecation window possible.** An ABC can
add a method with a default implementation and every subclass keeps working.
A `Protocol` has no such path — there is no shared base every implementer
inherits from that a default could live on. The moment `Destination` gains a
required method, every one of drt's own connectors and every third-party one
built against it fails `isinstance()` checks and (if constructed directly)
type-checks under mypy.

Concretely, for any of the 17 Protocols:

| Change | Breaking? |
|---|---|
| Add a required method | **Yes — no default-method escape hatch exists** |
| Add an optional method (with `...` body but callers use `getattr`/`hasattr`) | Yes in practice — nothing in Python enforces "optional" on a `Protocol` method the way a `@property` with a default would on a class |
| Remove or rename a method | Yes |
| Narrow a parameter's accepted type | Yes (existing callers passing the old wider type now fail) |
| Widen a parameter's accepted type | No — existing implementations already handle the narrower case |
| Narrow a return type | No — existing callers handle the wider type already |
| Widen a return type | Yes (existing callers may pattern-match / narrow on the old type) |
| Add a new, optional-capability Protocol (`FooCapable`) checked via `isinstance()` | **No** — this is the sanctioned extension path, see below |
| Change documented `Raises:` behavior (e.g. a method that never raised starts raising) | Yes — treat it the same as a signature change |

## The sanctioned extension mechanism

5 of the 17 Protocols already exist specifically to route around the
no-default-method problem: `ConnectionTestable`, `MatchPolicyCapable`,
`StagedDestination`, `OrphanCleanup` (destinations), and `IncrementalSource`
(sources). Each is checked structurally —
`isinstance(dest, MatchPolicyCapable)` — rather than being a required part of
`Destination`/`Source`. A destination that doesn't implement the capability
is simply not that shape; the engine branches on it rather than requiring it.

**This is the only way to add capability to a frozen Protocol without a major
version bump.** New capability needed post-v1.0 (RBAC hooks, audit hooks —
see below) must ship as a new optional-capability Protocol, not as an
addition to `Destination`/`Source`/`StateStore` directly. This is also why
[#992](https://github.com/drt-hub/drt/pull/992)'s `@runtime_checkable`
consistency fix matters as a prerequisite: an optional-capability Protocol
that isn't `@runtime_checkable` can't be `isinstance()`-checked, so it isn't
usable as an extension point at all. All 17 Protocols now have it.

## Deprecation workflow

Codifying what the codebase already does three times, rather than inventing
something new: `StateManager = LocalStateManager`
(`drt/state/manager.py:165`), `HistoryManager = LocalHistoryManager`
(`drt/state/history.py:180`), `DlqStore = LocalDlqStore`
(`drt/state/dlq.py:311`) are all back-compat aliases kept so a rename or
refactor doesn't break existing imports.

For a Protocol method deprecation (post-v1.0, to honor #304's "2 minor
versions" promise):

1. The new shape ships alongside the old one. If it's a method rename, the
   old name becomes a thin alias/wrapper calling the new one; if it's a
   whole-Protocol replacement, the old Protocol is kept as-is and the new one
   is introduced separately (as `FooV2` or similarly named — never silently
   redefining `Foo`).
2. The old path's docstring gets a `Deprecated since vX.Y — use ... instead.
   Will be removed no earlier than vX.(Y+2).` line, and a `DeprecationWarning`
   is raised at call time (not just documented) so it surfaces in CI for any
   downstream user running with warnings-as-errors.
3. CHANGELOG entry under `## [Unreleased]` naming the deprecation and the
   removal-eligible version.
4. The old path is removed no earlier than 2 minor versions after the
   deprecation shipped — matching #304's stated promise exactly, not a
   different number invented here.

## Freeze-scope table

Not every Protocol in the codebase is a public, frozen-at-v1.0 interface.
One is explicitly internal:

| Protocol | File | Freeze scope at v1.0 |
|---|---|---|
| `Source` | `drt/sources/base.py` | **Public, frozen** (#304 names it explicitly) |
| `IncrementalSource` | `drt/sources/base.py` | Public, frozen (optional-capability extension of `Source`) |
| `Destination` | `drt/destinations/base.py` | **Public, frozen** (#304 names it explicitly) |
| `ConnectionTestable` | `drt/destinations/base.py` | Public, frozen (optional-capability) |
| `MatchPolicyCapable` | `drt/destinations/base.py` | Public, frozen (optional-capability) |
| `StagedDestination` | `drt/destinations/base.py` | Public, frozen (optional-capability) |
| `OrphanCleanup` | `drt/destinations/base.py` | Public, frozen (optional-capability) |
| `RowCountable` | `drt/destinations/sql_utils.py` | Public, frozen (optional-capability) |
| `RateLimitKeyed` | `drt/destinations/rate_limiter.py` | Public, frozen — implemented by every `DestinationConfig` member |
| `LimiterFactory` | `drt/destinations/rate_limiter.py` | Internal — a callable injection point for tests (`resolve_rate_limiter`'s `limiter_factory` param), not implemented by connectors |
| `StateStore` | `drt/state/manager.py` | **Public, frozen** (#304 names it "StateManager") |
| `HistoryStore` | `drt/state/history.py` | Public, frozen — same #756 backend-selection surface as `StateStore` |
| `DlqBackend` | `drt/state/dlq.py` | Public, frozen — same surface |
| `WatermarkStorage` | `drt/state/watermark.py` | Public, frozen — same surface, already has 3 backends |
| `ObjectClient` | `drt/state/_objectstore.py` | **Internal, not frozen.** Underscore-prefixed module; not a public extension point today — only GCS/S3 implement it, both inside drt-core. May be reconsidered as a public plugin surface later (see #297), but that is a new decision, not inherited from this freeze. |
| `SecretProvider` | `drt/config/secret_providers/base.py` | Public, frozen — third-party secret backends are an expected extension |
| `SyncObserver` | `drt/engine/observer.py` | Public, frozen — explicitly designed as the Rust-migration seam and the OTel/ErrorFormatter plug-in point |

## Known asymmetry, frozen as-is

`Source.test_connection(config) -> bool` (never raises, caller checks the
return) and `ConnectionTestable.test_connection(config) -> None` (raises on
failure) share a method name but have opposite error-handling contracts.
Verified (2026-08-18) that the two never meet at a shared call site — sources
are checked via `drt/cli/commands/profile.py:166` and
`drt/mcp/tools/test_profile.py:24`, destinations via
`drt/cli/commands/validate.py:302` — so no caller has ever had to
branch on which contract it's dealing with. Unifying them now would be a
breaking signature change to one of two already-shipped, already-frozen-at-v1.0
Protocols, for a cost (two similarly-named methods) that is purely cosmetic.
**Decision: frozen as two independent, differently-shaped contracts.** A
future major version could rename one to remove the collision if it ever
becomes a real source of confusion in practice; nothing found in this
review makes that case today.

## RBAC / audit hooks — explicitly deferred

#300's scope includes "identify any missing methods needed for Enterprise
features (RBAC hooks, audit hooks)." **This ADR does not attempt that.**
#298 (RBAC interface spec) and #299 (audit log hooks) — the issues that would
define what those hooks need to do — have no committed design yet; inventing
Protocol method shapes ahead of that design would be exactly the kind of
speculative building this repo has repeatedly avoided (see #921/#948 in
ROADMAP.md's "don't build ahead of a measured need" posture). **When #298 and
#299 land a design, amend this ADR** with the concrete hook shapes and mark
them in the freeze-scope table above — most likely as new optional-capability
Protocols (`RbacAware`, `Auditable`) per the extension mechanism this ADR
already establishes, not as additions to the 4 already-frozen core Protocols.

## Consequences

- **#469** (`Destination.fetch_existing()` Protocol refactor, ROADMAP v0.10)
  should land before v1.0 lands the freeze — it's a shape change to a
  frozen-scope Protocol area, cheaper to make now than as a v2.0-only
  breaking change later. ROADMAP.md already orders it this way.
- **#992**, the mechanical PR1 half of #300, is a prerequisite this ADR
  assumes is merged: it makes `@runtime_checkable` consistent across all 17
  Protocols (required for the extension mechanism above to work uniformly)
  and adds the `Raises:` documentation this ADR's breaking-change table
  leans on being accurate.
- **#304's deliverables** ("Update all Protocol docstrings with stability
  annotations", "Add Stability: Stable badges", "Publish stability policy in
  docs") should link back to this ADR as the policy source rather than
  re-deriving it at freeze time.

## Follow-up issues

1. Amend this ADR once #298/#299 land committed designs, adding concrete RBAC
   / audit optional-capability Protocol shapes to the freeze-scope table.
2. #304 (the actual v1.0 freeze) should reference this ADR directly in its
   "Publish stability policy in docs" deliverable rather than restating it.
3. Consider whether `ObjectClient` (`drt/state/_objectstore.py`) becomes a
   public plugin surface as part of #297 (third-party connector
   auto-discovery) — if so, it moves from "internal, not frozen" to a scoped
   freeze decision of its own at that point, not retroactively here.
