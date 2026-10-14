# Review before you write: DuckDB → local CRM

This is the 60–90 second `drt plan` / `drt apply` story in a reproducible local
project. A DuckDB “warehouse” proposes CRM enrichment, a Postgres 16 container
stands in for the CRM, and no cloud account or cloud credential is involved.

Docker is the only prerequisite besides Python/pip. From the repository root,
install the local project with the two connector extras:

```bash
pip install -e ".[duckdb,postgres]"
```

## Five-minute path

```bash
cd examples/review-demo
cp .env.example .env
./run-demo.sh --reset
```

The script starts Postgres on host port `55432`, waits for its healthcheck,
seeds both databases, plans and applies the safe change, then plans an unsafe
mirror and proves that apply refuses it. It does not edit `~/.drt`: each drt
command gets an example-local `HOME`, whose `.drt/profiles.yml` is copied from
`profiles.yml.example`. `run-demo.sh` derives `REVIEW_DEMO_PG_DSN` from the
Postgres user, password, database, and `POSTGRES_HOST_PORT` in `.env`, so changing
only the host port is sufficient.

The deterministic seed (fixed RNG seed `20261013`) always produces:

- 1,500 warehouse companies and 1,512 CRM companies.
- **450 rows to fill**: 150 `NULL`, 150 empty strings, 150 spaces-only strings.
- **300 conflicting rep-entered values kept** by `write_policy: fill_empty`.
  Another 750 populated values already match the inference and are no-ops.
- **12 CRM-only rows** absent from the warehouse and left untouched.
- The deliberate 25-row mirror mistake would delete **1,487 rows** (98.35% of
  the CRM); its 5% delete guard refuses apply.

Run one scene at a time with `setup`, `plan`, `apply`, or `danger`:

```bash
./run-demo.sh --reset setup
./run-demo.sh plan
./run-demo.sh apply
./run-demo.sh danger
```

`drt plan --detailed-exitcode` uses 0 for no changes, 2 for changes, and 1 for
an error/unavailable plan. The danger scene prints code 2 for the successfully
computed change set, then code 1 when guarded apply refuses the write. See the
[plan/apply guide](../../docs/guides/plan.md#exit-codes) for the contract.

## What each file does

| File | Role |
|---|---|
| `docker-compose.yml` / `.env.example` | Postgres 16 CRM, non-default port, local credentials, healthcheck |
| `seed.py` | Rebuilds deterministic DuckDB and Postgres fixtures; also verifies the post-apply invariants |
| `drt_project.yml` / `profiles.yml.example` | Self-contained DuckDB source profile without touching the user's profile |
| `syncs/fill_industry.yml` | Safe fill-only enrichment with zero-create and zero-delete guards |
| `syncs/dangerous_mirror.yml` | Deliberately unsafe 25-row mirror whose delete-percentage guard trips |
| `run-demo.sh` | Idempotent full run plus scene-level commands and `--reset` |
| `demo.tape` | VHS script that renders `demo.mp4` and `demo.gif` |
| `AGENT_PROMPTS.md` | Copy-paste Codex/Claude prompts for schema, validation, planning, approval, and apply |

### Why the safe sync says `match_policy: upsert`

The intended business setting is `match_policy: update_only`, but current
`drt plan` deliberately returns “plan unavailable” for `update_only` and
`create_only` because it cannot yet model their skipped rows. This example does
not change `drt/`; it uses `max_creates: 0` so plan/apply fails closed if any
create appears. Replace that line with `match_policy: update_only` when planner
support lands. The limitation and apply's extraction race are documented in
the [plan guide](../../docs/guides/plan.md).

### Partial-upsert schema gotcha

`fill_industry` selects only `company_id` and `industry`. With PostgreSQL's
`INSERT ... ON CONFLICT` upsert, the proposed insert row is checked before the
conflict update runs. Destination columns omitted by a partial-column upsert
therefore need to be nullable or have defaults; this demo deliberately makes
`domain` and `name` nullable. A native `match_policy: update_only` UPDATE would
not hit this INSERT-time constraint, but the planner limitation above is why
the executable demo currently uses guarded `upsert`.

## Regenerate the video

Install [VHS v0.11](https://github.com/charmbracelet/vhs) and its runtime
dependencies `ttyd` and `ffmpeg`, make sure Docker is running, then:

```bash
cp .env.example .env
vhs demo.tape
```

The tape is 1280×720 with a 19px font and writes both `demo.mp4` and
`demo.gif`. Those generated files are ignored.

## CI and agents

For a pull-request plan artifact followed by a merge-time apply, use the
[CI review loop](../../docs/guides/plan.md#review-loop-in-ci-github-actions).
For an MCP-driven review with a named human approver, start with
[AGENT_PROMPTS.md](./AGENT_PROMPTS.md). The same plan/apply implementation backs
the CLI and MCP paths.

## Cleanup

```bash
docker compose down -v
```

The Compose project is explicitly named `drt-review-demo`, so this command and
the script's reset path target only this demo's container, network, and volume
rather than inheriting a possibly-colliding parent-directory name.

Or run `./run-demo.sh --reset setup` to discard and immediately rebuild all
local demo state.
