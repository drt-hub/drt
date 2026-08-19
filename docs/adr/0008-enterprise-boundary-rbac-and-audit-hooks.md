# ADR 0008 — Enterprise boundary: permission checks and audit-log hooks

- **Status:** Accepted 2026-08-19.
- **Issues:** [#298](https://github.com/drt-hub/drt/issues/298) (RBAC interface,
  design only), [#299](https://github.com/drt-hub/drt/issues/299) (audit log
  hooks, design only).
- **Relates to:** [ADR 0007](0007-protocol-stability-policy.md) — this ADR is
  ADR 0007's own named follow-up ("amend once #298/#299 land committed
  designs"); [#297](https://github.com/drt-hub/drt/issues/297) (plugin system)
  — not a dependency of this work, but the natural future upgrade for the
  registration gap this ADR knowingly leaves open (see Consequences).
- **Implementation:** `drt/security/base.py` (new package), `drt/observability/audit.py`
  (new module), plus hook calls at the CLI sites named under Decision 3.
  Design + seam only, per both issues' explicit "design only, not
  implementation" scope — no enforcement logic, no event shipping.

## Context

drt's monetization plan splits an OSS core from a closed Enterprise product
(`project_startup_vision` in this project's own working notes; see
ROADMAP.md's v0.10 "Enterprise Boundary & Ecosystem" theme). Two Enterprise
features were named without an interface: RBAC ("who can run/edit/view which
syncs") and audit logging (SOC2/HIPAA compliance trails). Both issues ask for
design only — the OSS core gets the right seam; Enterprise, a separate
package, implements behavior behind it.

This repo's own `CLAUDE.md` says **"Do not add RBAC or multi-tenancy — small
team / personal use."** That line predates the Enterprise-split plan and
describes the OSS core's own posture correctly — this ADR does not add RBAC
*enforcement* to OSS. It adds a Protocol and a no-op default, the same shape
already used for `SecretProvider` and every optional-capability Protocol in
`drt/destinations/base.py`. Flagged to the user before this ADR was written;
confirmed as an intentional, scoped exception for Enterprise-boundary design
work, not a contradiction to resolve silently.

## Decision 1 — Audit events: reuse `SyncObserver`, don't duplicate it

#299 proposes five event types: `sync_started`, `sync_completed`,
`sync_failed`, `config_changed`, `secret_accessed`. Checked each against
`drt/engine/observer.py`'s existing `SyncObserver` Protocol
(`on_sync_started`, `on_sync_completed`, `on_sync_ended`, `on_records_failed`,
`on_warning`, `on_watermark_resolved`, `on_interrupted`) — a Protocol whose
own module docstring already names it "the engine's event surface" and the
seam OTel (#527) and the error formatter (#544) hook into, with
`CompositeObserver` already fanning events out to multiple observers.

**Three of the five events are already `SyncObserver` events wearing a
different name — inventing a second Protocol for them would duplicate an
existing seam rather than extend it:**

- `sync_started` → `on_sync_started`
- `sync_completed` → `on_sync_completed`
- `sync_failed` → not a separate callback; an Enterprise `SyncObserver`
  implementation derives it from `on_sync_completed(result)` by checking
  `result.failed > 0` / `result.errors`, the same signal every other
  consumer of `SyncResult` already reads. Adding a dedicated
  `on_sync_failed` would be a required-method addition to an
  already-shipped Protocol — breaking under ADR 0007 — for a distinction
  `on_sync_completed`'s payload already carries.

**Two have no `SyncObserver` home, because they aren't sync-lifecycle
events:**

- `config_changed` — nothing fires when a sync config is edited; drt has no
  runtime config-mutation path today (`drt/mcp/tools/` has no config-writing
  tool). The nearest honest trigger is config *load*, not config *diff*:
  `drt/config/parser.py`'s `load_syncs`/`load_project`, fired once per CLI
  invocation that reads project YAML. An Enterprise audit trail pairs this
  with an external config-revision signal (git SHA) it supplies itself —
  drt has no stored "previous config" to diff against, and inventing one
  is out of scope for a design-only issue.
- `secret_accessed` — fires inside secret resolution
  (`drt/config/secret_providers/base.py`'s `resolve_provider_uri`), a
  different subsystem from the engine entirely. Never logs the resolved
  value — only the scheme and path (`aws-sm://prod/drt/snowflake`, not
  what it resolves to).

**These two get a new, narrow Protocol** — `AuditLogger.log_event(event)` —
scoped to exactly the two events that need it, not a five-event Protocol
that would duplicate `SyncObserver` for the other three.

## Decision 2 — RBAC: a new `PermissionChecker` Protocol

`PermissionChecker` has no existing seam to reuse — nothing in the engine or
CLI today asks "is this allowed." New Protocol:

```python
class PermissionAction(str, Enum):
    RUN = "run"
    EDIT = "edit"
    VIEW = "view"

@runtime_checkable
class PermissionChecker(Protocol):
    def check(
        self, action: PermissionAction, sync_name: str | None, *, principal: str | None = None
    ) -> None:
        """Raise PermissionDeniedError if `principal` may not `action` on `sync_name`.

        `sync_name=None` for project-wide actions (e.g. `drt run` with no
        --select). `principal=None` when the caller has no identity concept
        (the OSS default, and any CLI invocation before an Enterprise
        identity layer resolves one) — a checker MUST treat an absent
        principal as "unauthenticated", not "trusted", though the OSS
        no-op checker permits it regardless, matching #298's stated OSS
        default.
        """
        ...
```

Matches #298's exact three-verb model ("who can run/edit/view which
syncs") rather than inventing a richer permission language — a design-only
issue is the wrong place to speculate about scopes/roles Enterprise hasn't
specified.

## Decision 3 — Hook points, named concretely

Both Protocols need actual call sites, not just a definition, per both
issues' "Hook points in CLI and engine where auth checks are injected"
scope line. Wired at one representative site per verb/event rather than
exhaustively at all 19 CLI commands — the pattern is what's being proven
here, not blanket coverage, and extending it to every command is
mechanical follow-up once Enterprise actually exists to consume it:

| Hook | File | Call |
|---|---|---|
| `action="run"` | `drt/cli/commands/run.py`, inside `_run_one` before the sync executes | `get_permission_checker().check(PermissionAction.RUN, sync.name)` |
| `action="edit"` | `drt/cli/commands/state.py`, inside the `reset` command before `state_mgr.reset(...)` | `get_permission_checker().check(PermissionAction.EDIT, sync_name)` |
| `action="view"` | `drt/cli/commands/status.py`, inside `status` before rendering | `get_permission_checker().check(PermissionAction.VIEW, None)` |
| `sync_started`/`sync_completed` | `drt/cli/commands/run.py`'s `_build_observer` | any registered extra observer is appended to the existing `observers` list — no new call, `SyncObserver` already fires these |
| `config_changed` | `drt/config/parser.py`, end of `load_syncs`/`load_project` | `get_audit_logger().log_event(AuditEvent("config_changed", ...))` |
| `secret_accessed` | `drt/config/secret_providers/base.py`'s `resolve_provider_uri`, after a successful fetch | `get_audit_logger().log_event(AuditEvent("secret_accessed", ...))` |

Every hook is behaviorally inert under the OSS defaults
(`AllowAllPermissionChecker`, `NoOpAuditLogger`) — this ADR changes no
observable CLI behavior, only adds call sites a registered Enterprise
implementation would observe.

## Decision 4 — Registration: mirror `SecretProvider.register()`, not a new mechanism

`drt/config/secret_providers/base.py` already has the pattern this needs: a
module-level `_registry`, a `register()` function raising on duplicate
registration, built-ins registering themselves at package `__init__.py`
import time. Reused verbatim, not reinvented:

- `drt/security/base.py`: `register_permission_checker(checker)` /
  `get_permission_checker()` — single active checker (unlike secret
  providers' per-scheme map, there's exactly one policy per process),
  defaulting to `AllowAllPermissionChecker()`.
- `drt/observability/audit.py`: `register_audit_logger(logger)` /
  `get_audit_logger()` — same shape, defaulting to `NoOpAuditLogger()`.
- `drt/cli/commands/run.py`'s `_build_observer`: a new
  `register_extra_observer(observer)` / `_extra_observers()` pair in
  `drt/engine/observer.py`, appended to the `observers` list alongside the
  existing `LoggingObserver`/`StatePersistingObserver`/`DlqObserver`.

## Consequences

**Who calls `register_*` is deliberately left open, and that's an existing
gap, not a new one.** Nothing in drt-core today auto-discovers third-party
`SecretProvider`s either — a third-party provider module's `register()` call
only runs if something imports that module first, and drt-core has no
mechanism for that beyond the operator's own wrapper script explicitly
importing it before invoking the CLI. This ADR's three new `register_*`
functions have the identical shape and the identical gap. **#297 (entry-point
plugin discovery) is the eventual fix for all four registries at once**
(sources, destinations, secret providers, and now permission-checkers /
audit-loggers / extra observers) — not a blocker for this ADR, since
`SecretProvider` already shipped and is useful today without it.

**RBAC does not become enforced anywhere by landing this ADR.** The default
checker permits everything; `CLAUDE.md`'s "no RBAC" line remains true of the
OSS product's actual behavior. Only a separately-installed Enterprise
package that calls `register_permission_checker()` changes that.

## Freeze-scope addition to ADR 0007

Both new Protocols join ADR 0007's freeze-scope table as of this ADR,
public and frozen at v1.0 alongside the other 17:

| Protocol | File | Freeze scope at v1.0 |
|---|---|---|
| `PermissionChecker` | `drt/security/base.py` | Public, frozen — the Enterprise RBAC extension point |
| `AuditLogger` | `drt/observability/audit.py` | Public, frozen — scoped to `config_changed`/`secret_accessed` only; sync-lifecycle audit events go through the already-frozen `SyncObserver` instead |

ADR 0007's "RBAC / audit hooks — explicitly deferred" section is superseded
by this ADR for the interface shape; that section's caution about not
inventing hook shapes ahead of #298/#299's design remains correct in
hindsight — this ADR exists because those designs landed.

## Follow-up issues

1. Wire `PermissionChecker.check()` into the remaining CLI commands beyond
   the three representative sites above, once an Enterprise consumer
   exists to justify the coverage.
2. `secret_accessed`'s event payload needs a concrete `AuditEvent` field
   list decided at implementation time — this ADR fixes the event's
   *existence* and *trigger point*, not its full schema.
3. Revisit registration ergonomics (`register_*` requiring an explicit
   operator-side import) once #297 lands.
