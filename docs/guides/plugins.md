# Third-Party Plugins (#297)

drt discovers third-party packages via standard Python [entry points](https://packaging.python.org/en/latest/specifications/entry-points/) — the same mechanism pytest, SQLAlchemy, and Flask extensions use. `pip install` your package, and drt picks it up automatically; no import needed in your `drt_project.yml` or anywhere else.

Run `drt plugins list` at any time to see what drt has discovered.

## What works today

Four extension points are fully usable end to end: a plugin registered this way is live the moment its package is installed, with no other configuration.

| Entry-point group | Extends | Registration function |
|---|---|---|
| `drt.secret_providers` | New secret-backend URI scheme (like `aws-sm://`, `vault://`) | `drt.config.secret_providers.base.register(scheme, provider)` |
| `drt.permission_checkers` | Who can run/edit/view which syncs (ADR 0008) | `drt.security.register_permission_checker(checker)` |
| `drt.audit_loggers` | `config_changed` / `secret_accessed` audit events (ADR 0008) | `drt.observability.register_audit_logger(logger)` |
| `drt.observers` | Extra `SyncObserver` callbacks (`on_sync_started`, `on_sync_completed`, ...) | `drt.engine.observer.register_extra_observer(observer)` |

### Example: a third-party audit logger

```python
# my_package/__init__.py
def register() -> None:
    from drt.observability import register_audit_logger
    from .audit import MyAuditLogger

    register_audit_logger(MyAuditLogger())
```

```toml
# my_package/pyproject.toml
[project.entry-points."drt.audit_loggers"]
my_audit_logger = "my_package:register"
```

That's the whole contract: **the entry point's value is a zero-argument callable, loaded and invoked once at drt CLI startup.** The callable performs its own registration as a side effect — it is not itself the `AuditLogger`/`PermissionChecker`/etc. instance. This mirrors how drt's built-in connectors self-register in `drt/connectors/registry.py`.

A broken plugin's exception is caught, logged, and reported by `drt plugins list` (`Status: error: ...`) rather than crashing the CLI — one bad third-party package can't take down unrelated commands.

## Connectors: `drt.sources` / `drt.destinations`

`drt.sources` and `drt.destinations` entry points are discovered, their registration callables are invoked (so `register_source()` / `register_destination()` in `drt/connectors/registry.py` runs), and **the registered type can be named in a sync YAML like any built-in**:

```yaml
# syncs/leads.yml — `salesforce_premium` comes from a third-party package
name: leads_to_salesforce
model: ref('qualified_leads')
destination:
  type: salesforce_premium
  instance_url: https://acme.my.salesforce.com
  api_key_env: SF_PREMIUM_KEY
```

Until #997 this failed `drt validate`: `SyncConfig.destination` and `load_profile()` both checked `type` against a closed, hand-enumerated set of built-ins *before* the connector registry was consulted, so a connector could register itself and still be permanently unreachable. [ADR 0009](../adr/0009-plugin-config-union-blocker.md) records that blocker and how it was closed.

Two things worth knowing when you build one:

- **Your config fields are carried, not validated.** drt-core does not know your schema, so a plugin destination's fields are accepted as-is and handed to your implementation. A typo in one of *your* fields is kept rather than rejected — validate what you need inside your connector. (A typo in the `type` itself is still rejected: an unregistered type is indistinguishable from a misspelled built-in, and is reported as one.)
- **`retry` and `rate_limit` still work.** Both are understood generically, so `resolve_retry()` and the rate-limiter registry apply to your connector the same way they do to a built-in.

A source plugin's profile class is constructed directly from the `profiles.yml` mapping, so its fields should match the keys operators will write.

## `drt plugins list`

```
$ drt plugins list
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
┃ Group                ┃ Name          ┃ Package         ┃ Version ┃ Author ┃ Status ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
│ drt.audit_loggers    │ my_logger     │ my-package      │ 0.1.0   │ ?      │ loaded │
└──────────────────────┴───────────────┴─────────────────┴─────────┴────────┴────────┘
```

`--format json` emits the same data as machine-readable JSON, including a `usable_in_sync_yaml` boolean per entry (`false` for `drt.sources` / `drt.destinations`, `true` for the other four groups).
