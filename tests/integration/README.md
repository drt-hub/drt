# Integration tests

End-to-end tests that drive the full reverse-ETL pipeline: real Source →
engine → real Destination process. Unlike the unit suite, these tests do
not mock `drt.sources.*` or `drt.destinations.*` internals — both ends run
their real I/O paths (a real `duckdb.connect()`, a real HTTP server, etc.).

## Why this exists

Unit tests are good at catching logic regressions inside a single module,
but they cannot catch breakage at the seams: a Source whose row dict shape
silently drifts, a destination that stops honoring `batch_size`, a resolver
that returns SQL the Source cannot parse. The integration suite is the
regression net for the seams.

## The harness pattern

Every Source E2E test follows the same three-part shape:

1. **Seed a real Source** — a fixture creates the actual database / file
   / API state the Source will read from.
2. **Run `engine.run_sync`** with that Source and a destination wired to a
   controllable receiver (today: `pytest-httpserver` as a REST endpoint).
3. **Assert on both sides** — what the engine reports (`SyncResult`) and
   what the receiver actually saw.

The destination side is the same across every Source. The fixture is the
only thing that swaps.

## Adding a new Source E2E

To add `test_postgres_e2e.py`, `test_mysql_e2e.py`, `test_snowflake_e2e.py`,
etc.:

1. Add a `<source>_with_users` fixture in `conftest.py` that:
   - Creates a real database (use `tmp_path` for file-backed engines;
     consider `testcontainers` or a docker-compose service for
     server-backed engines)
   - Seeds a `users (id, name, email)` table with three rows so the
     E2E test bodies stay copy-paste compatible
   - Returns `(source, profile)` — the model field on `SyncConfig` will
     be `ref('users')`, which the resolver turns into the right dialect's
     `SELECT * FROM users`
2. Copy `test_duckdb_e2e.py` and rename the import + fixture name. The
   destination-side helpers (`_dest_config`, `_sync`) and the four
   canonical assertions (full pipeline / empty result / value flow /
   connection test) carry over unchanged.
3. If the Source driver is an optional extra (e.g. `pip install
   drt-core[postgres]`), gate the fixture with
   `pytest.importorskip("<driver_module>")` so dev environments without
   the extra installed still pass test collection.
4. Add the driver to the CI install line in `.github/workflows/ci.yml`
   so the test actually runs in CI rather than skipping silently.

## What does NOT belong here

- Pure logic tests on engine/resolver/state internals — those go in
  `tests/unit/`
- Tests that need `FakeSource` for speed and don't need a real Source —
  use the existing `FakeSource` fixture in this `conftest.py`
- Tests against a third-party live API (HubSpot, Salesforce, etc.) —
  those should use recorded fixtures or a stub server, not the live API
