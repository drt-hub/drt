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

## What's discovered but not yet usable: `drt.sources` / `drt.destinations`

`drt.sources` and `drt.destinations` entry points are discovered and their registration callables *are* invoked (so `register_source()` / `register_destination()` in `drt/connectors/registry.py` runs, and the type is queryable via `get_source()` / `get_destination()`). `drt plugins list` shows these as `registered (not yet usable in sync YAML — see ADR 0009)`.

**A sync YAML naming a third-party `type` will fail `drt validate` today.** `SyncConfig.destination` and `load_profile()` both validate the `type` field against a closed, hand-enumerated set of built-in types *before* the connector registry is ever consulted — registering a connector doesn't add it to that set. This is a real architectural gap, not a bug in the plugin loader; see [ADR 0009](../adr/0009-plugin-config-union-blocker.md) for the full explanation and the candidate designs for closing it.

If you're building a third-party connector today:

- The registration and discovery machinery above is ready for you to build against now — write and ship your `drt.destinations` entry point, and it will "just work" the moment the config-union gap closes, with no changes needed on your side.
- Until then, in-tree contribution (see [Building a Destination Connector](building-a-destination.md)) is the only way to make a new connector reachable from a sync YAML.

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
