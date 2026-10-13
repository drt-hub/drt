#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_HOME="$DEMO_DIR/.demo-home"
STAGE="all"
RESET=0

usage() {
  echo "Usage: ./run-demo.sh [--reset] [all|setup|plan|apply|danger|mcp-env]"
}

for argument in "$@"; do
  case "$argument" in
    --reset) RESET=1 ;;
    all|setup|plan|apply|danger|mcp-env) STAGE="$argument" ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

if (( RESET )) && [[ "$STAGE" != "all" && "$STAGE" != "setup" ]]; then
  echo "--reset is only valid with the all or setup stage." >&2
  usage >&2
  exit 2
fi

cd "$DEMO_DIR"

if [[ ! -f .env ]]; then
  echo "Missing .env. Run: cp .env.example .env" >&2
  exit 1
fi

set -a
# The user-local copy is the documented input.
# shellcheck disable=SC1091
source .env
set +a

export REVIEW_DEMO_PG_DSN="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_HOST_PORT}/${POSTGRES_DB}"
export DRT_PLAN_KEY="review-demo-fixed-local-key"
export COMPOSE_PROJECT_NAME="drt-review-demo"

drt_local() {
  HOME="$DEMO_HOME" command drt "$@"
}

reset_demo() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$DEMO_DIR/warehouse.duckdb" "$DEMO_DIR/plan.json" \
    "$DEMO_DIR/dangerous-plan.json"
  rm -rf "$DEMO_HOME" "$DEMO_DIR/.drt" "$DEMO_DIR/target"
  echo "Reset local demo state."
}

wait_for_crm() {
  # `up --wait` blocks until the service's healthcheck reports healthy. (`ps --status`
  # has no "healthy" value, so polling it can never succeed.)
  if ! docker compose up -d --wait --wait-timeout 120 crm; then
    echo "Postgres did not become healthy; inspect: docker compose logs crm" >&2
    exit 1
  fi
}

setup_demo() {
  mkdir -p "$DEMO_HOME/.drt"
  cp profiles.yml.example "$DEMO_HOME/.drt/profiles.yml"
  wait_for_crm
  python seed.py
  echo
  echo "Ready: DuckDB warehouse + local Postgres CRM (no cloud credentials)."
}

plan_fill() {
  echo "=== REVIEW: fill empty CRM industries ==="
  drt_local validate
  drt_local plan fill_industry --out plan.json
  python -c 'import json; s=json.load(open("plan.json"))["summary"]; print("Review checkpoint: {update} updates, {create} creates, {delete} deletes, {kept_values} conflicting rep values kept".format(**s))'
}

apply_fill() {
  echo "=== APPROVE: apply exactly the reviewed plan ==="
  drt_local apply plan.json --auto-approve --approved-by "review-demo operator"
  python seed.py --verify
}

show_danger() {
  echo "=== GUARD: catch an unsafe mirror ==="
  set +e
  drt_local plan dangerous_mirror --out dangerous-plan.json --detailed-exitcode
  local plan_exit=$?
  set -e
  echo "drt plan exit code: $plan_exit (2 means changes were found)"

  set +e
  drt_local apply dangerous-plan.json --auto-approve --approved-by "review-demo operator"
  local apply_exit=$?
  set -e
  echo "drt apply exit code: $apply_exit (1 means the guard refused the write)"
  if [[ "$plan_exit" -ne 2 || "$apply_exit" -ne 1 ]]; then
    echo "Unexpected plan/apply exit code." >&2
    exit 1
  fi
  python seed.py --verify
}

show_mcp_env() {
  python - "$DEMO_DIR" "$DEMO_HOME" "$REVIEW_DEMO_PG_DSN" "$DRT_PLAN_KEY" <<'PY'
import json
import shlex
import sys

demo_dir, demo_home, postgres_dsn, plan_key = sys.argv[1:]
direct = (
    f"cd {shlex.quote(demo_dir)} && set -a && source .env && set +a && "
    f"export REVIEW_DEMO_PG_DSN={shlex.quote(postgres_dsn)} && "
    f"HOME={shlex.quote(demo_home)} DRT_PLAN_KEY={shlex.quote(plan_key)} drt mcp run"
)
config = {
    "mcpServers": {
        "drt-review-demo": {
            "type": "stdio",
            "command": "/bin/bash",
            "args": ["-c", f"cd {shlex.quote(demo_dir)} && exec drt mcp run"],
            "env": {
                "HOME": demo_home,
                "REVIEW_DEMO_PG_DSN": postgres_dsn,
                "DRT_PLAN_KEY": plan_key,
            },
        }
    }
}
print("Direct stdio server launch (run after ./run-demo.sh setup):")
print(direct)
print("\nReady-to-paste Claude Code .mcp.json entry:")
print(json.dumps(config, indent=2))
PY
}

if (( RESET )); then
  reset_demo
fi

case "$STAGE" in
  setup) setup_demo ;;
  plan) plan_fill ;;
  apply) apply_fill ;;
  danger) show_danger ;;
  mcp-env) show_mcp_env ;;
  all)
    setup_demo
    echo
    plan_fill
    echo
    apply_fill
    echo
    show_danger
    ;;
esac
