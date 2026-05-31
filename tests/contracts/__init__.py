"""Universal contract tests for ``drt`` destinations.

A "contract" here is an invariant that **every** destination should
satisfy regardless of its underlying protocol (HTTP webhook vs SQL DB
vs file write). Contract tests are parametrised across destinations
rather than written per-connector, so adding a new destination
auto-extends the assertion surface as soon as it joins the parameter
list.

This package starts with HTTP destinations + the empty-batch contract
(#364 / Step 1 of FakeSource follow-up). Future contracts to add:

- ``dry_run`` no-op (no external calls when ``sync_options.dry_run``)
- Idempotency for upsert-capable destinations (same record × 2 = 1)
- Row-level error surfacing (malformed record lands in ``row_errors``)

Extend by appending to the parameter list in each test module — the
list of (destination_class, config_factory) tuples is the registry.
"""
