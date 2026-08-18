# ADR 0007 — Protocol stability policy

- **Status:** Accepted 2026-08-18.
- **Issues:** [#300](https://github.com/drt-hub/drt/issues/300) (this ADR's
  deliverable — review and freeze preparation), feeds
  [#304](https://github.com/drt-hub/drt/issues/304) (the v1.0 freeze itself).
- **Relates to:** [#992](https://github.com/drt-hub/drt/pull/992) (the
  mechanical half of #300 — `@runtime_checkable` consistency and `Raises:`
  documentation across all 17 Protocols). **This ADR assumes #992 is merged
  first** — until then, `SecretProvider`, `LimiterFactory`, and
  `WatermarkStorage` still lack `@runtime_checkable`, and this ADR's
  "16 public/frozen Protocols all `@runtime_checkable`" premise does not yet
  hold. Land #992 before (or in the same merge window as) this ADR.
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

A signature change also has to be judged from **both** sides of the
Protocol, not just the caller's: a `Protocol` has callers (drt's engine,
mostly) who need the old contract to keep holding, *and* structural
implementers (every connector, drt's own and third-party) who wrote their
method bodies against the old contract. A change that's safe for one side is
routinely unsafe for the other — e.g. widening a parameter's accepted type
(`x: int` → `x: int | str`) is safe for existing *callers* (anyone already
passing an `int` still works) but breaks every existing *implementer*,
because the engine may now actually call their method with a `str` their
method body was never written to handle — that's a real `AttributeError` at
runtime, not just a type-checker complaint. Narrowing a return type has the
mirror problem on the implementer side (an implementation returning the
wider original type is no longer conformant). Given that, this ADR does not
attempt a change-by-change compatibility table — the earlier draft of this
document had one, and Codex review correctly flagged two of its rows as
unsafe once implementers are accounted for, not just callers.

**The rule instead: treat any change to an already-shipped Protocol
method's signature — parameters or return type, narrowing or widening — as
breaking.** For any of the 17 Protocols:

| Change | Breaking? |
|---|---|
| Add a required method to an existing Protocol | **Yes — no default-method escape hatch exists** |
| Add an optional method (with `...` body but callers use `getattr`/`hasattr`) | Yes in practice — nothing in Python enforces "optional" on a `Protocol` method the way a `@property` with a default would on a class |
| Remove or rename a method | Yes |
| Any change to an existing method's parameter types or return type (narrowing or widening, in either direction) | **Yes — default assumption; see reasoning above.** Don't try to reason out a case-by-case exception without checking both the caller side (`drt/engine/`) and every current implementer |
| Change documented `Raises:` behavior (e.g. a method that never raised starts raising, or vice versa) | Yes — treat it the same as a signature change |
| Add a new, `@runtime_checkable`, optional-capability Protocol (`FooCapable`) checked via `isinstance()`, never touching an existing Protocol's method set | **No** — this is the sanctioned extension path, see below |
| Docstring-only changes that don't alter documented behavior (clarifying wording, adding examples) | No |

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
usable as an extension point at all. Once #992 merges, all 17 Protocols have
it.

## Deprecation workflow

Two different mechanisms are needed here, and conflating them was an error
in an earlier draft of this ADR (caught in Codex review): a **concrete class
alias** and a **Protocol method deprecation** are not interchangeable.

**Concrete class/name aliases already have working precedent** —
`StateManager = LocalStateManager` (`drt/state/manager.py:165`),
`HistoryManager = LocalHistoryManager` (`drt/state/history.py:180`),
`DlqStore = LocalDlqStore` (`drt/state/dlq.py:311`). These are real
assignments to a concrete, instantiable class; the alias works because
`LocalStateManager` has actual method bodies that run. Keep using this
pattern for renaming concrete classes.

**This does not transfer to Protocol methods, and must not be imitated
there.** A `Protocol` method's body (even a non-`...` one) never executes
for a structural implementer — implementers provide their own bodies
entirely. Adding a wrapper method to a `Protocol` that calls another
`Protocol` method does nothing for existing implementers, who don't have
either method's logic inherited from anywhere. And adding the *new* method
name to the *same* `Protocol` immediately requires every existing
implementer to define it too (see the breaking-change table above) — there
is no gradual-adoption path within a single Protocol.

For a Protocol-level rename or signature change (post-v1.0, following
`VERSIONING.md`'s deprecation cycle, which #304 also points back to):

1. The old Protocol is left completely unchanged. The new shape ships as a
   **separate, new, `@runtime_checkable` Protocol** (`FooV2`, or better, a
   named optional-capability Protocol describing what's actually new) —
   the same extension mechanism as any other new capability, not a special
   case for deprecations.
2. Callers structurally check for the new Protocol first
   (`isinstance(x, FooV2)`), falling back to the old required one — this
   adapter logic lives on the caller side (`drt/engine/`), not on either
   Protocol.
3. The old Protocol's docstring gets a `Deprecated since vX.Y — use FooV2
   instead.` line. CHANGELOG entry under `## [Unreleased]` with a
   `[DEPRECATED]` tag, per `VERSIONING.md`'s existing Step 1.
4. **Removal requires both conditions VERSIONING.md already sets, not just
   one:** the deprecated Protocol must have been announced for at least 2
   minor releases (`VERSIONING.md`'s existing minimum floor), **and**
   removing it only actually happens at the next MAJOR version once v1.0's
   freeze is in effect — `VERSIONING.md`'s pre-1.0 notice explicitly stops
   applying its "can remove after 2 minor releases in a MINOR bump" language
   to Protocols the moment v1.0 ships. A method deprecated in v1.1 is not
   removal-eligible in v1.3; it's removal-eligible at v2.0, provided v1.3 or
   later has already passed. This corrects an earlier draft of this ADR,
   which stated only the 2-minor-version floor and left the MAJOR-bump
   requirement implicit.

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

`Source.test_connection(config) -> bool` (caller checks the return; connection
failures are caught and reported as `False`, though a cleanup-step failure in
some implementations can still propagate — see the Protocol docstring) and
`ConnectionTestable.test_connection(config) -> None` (raises on failure) share
a method name but have differently-shaped error-handling contracts.
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

- **#469** (originally scoped as a `Destination.fetch_existing()` refactor,
  ROADMAP v0.10) landed as a new, separate `QueryableDestination` Protocol
  in `drt/destinations/base.py` instead — the issue's original design
  predates this ADR's extension mechanism, and `Destination.load()` itself
  was never touched. This is the mechanism working as intended: new
  capability (`get_table_name` / `execute_test_query`, replacing the old
  `_QUERYABLE_TYPES` config-class isinstance tuple in
  `drt/destinations/query.py`) shipped without changing any already-shipped
  Protocol's method set, so it carried no breaking-change risk and needed no
  freeze-timing urgency the way an actual `Destination` shape change would
  have.
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
