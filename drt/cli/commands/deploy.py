"""`drt deploy github-actions` — scaffold a scheduled sync workflow (#785).

Generates ``.github/workflows/drt-sync.yml`` wired to the official
``drt-hub/drt-action``, with connector extras inferred from the project's
profiles + sync definitions and every required secret enumerated as
``${{ secrets.NAME }}`` — the part users otherwise transcribe by hand from
connector docs. Prior art: ``dlt deploy github-action``.

The scanner reads YAML *raw* (no ``${VAR}`` expansion), so scaffolding works
in a fresh checkout where none of the runtime env vars are set yet.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import typer
import yaml

from drt.cli._app import app
from drt.cli.output import console, print_error

deploy_app = typer.Typer(
    name="deploy",
    help="Scaffold CI/CD deployment files for this project.",
    no_args_is_help=True,
)
app.add_typer(deploy_app)


# drt-core extras required per connector ``type`` (sources and destinations
# share names where both exist). Types not listed here ship in base
# drt-core. Guarded against connector-registry / pyproject drift by
# tests/unit/test_cli_deploy.py.
_TYPE_TO_EXTRA: dict[str, str] = {
    "azure_blob": "azure",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "deltalake": "deltalake",
    "duckdb": "duckdb",
    "gcs": "gcs",
    "google_sheets": "sheets",
    "iceberg": "iceberg",
    "mysql": "mysql",
    "parquet": "parquet",
    "postgres": "postgres",
    "redshift": "redshift",
    "s3": "s3",
    "snowflake": "snowflake",
    "sqlserver": "sqlserver",
}

_ENV_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_DEFAULT_OUTPUT = Path(".github/workflows/drt-sync.yml")
_DEFAULT_PLAN_OUTPUT = Path(".github/workflows/drt-plan.yml")
_DEFAULT_APPLY_OUTPUT = Path(".github/workflows/drt-apply.yml")
_PLAN_KEY_SECRET = "DRT_PLAN_KEY"
_PATHS = ('"syncs/**"', '"drt_project.yml"', '"profiles.yml"')


def _collect_env_refs(node: Any, envs: set[str]) -> None:
    """Recursively collect the *values* of ``*_env`` keys (env var names)."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str) and key.endswith("_env") and isinstance(value, str):
                envs.add(value)
            _collect_env_refs(value, envs)
    elif isinstance(node, list):
        for item in node:
            _collect_env_refs(item, envs)


def _scan_project(project_dir: Path) -> tuple[set[str], set[str], bool]:
    """Return (env var names, connector types, repo profiles.yml present)."""
    envs: set[str] = set()
    types: set[str] = set()

    sync_files = sorted((project_dir / "syncs").glob("*.yml")) + sorted(
        (project_dir / "syncs").glob("*.yaml")
    )
    texts: list[str] = []
    for path in sync_files:
        text = path.read_text(encoding="utf-8")
        texts.append(text)
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError:
            continue  # drt validate owns YAML errors; the scaffolder skips
        if not isinstance(data, dict):
            continue
        destination = data.get("destination")
        if isinstance(destination, dict) and isinstance(destination.get("type"), str):
            types.add(destination["type"])
        _collect_env_refs(data, envs)

    project_file = project_dir / "drt_project.yml"
    if project_file.exists():
        texts.append(project_file.read_text(encoding="utf-8"))

    # drt-action stages a repo-committed profiles.yml (its `profiles-file`
    # input, default "profiles.yml") to ~/.drt — scan it for the source
    # connector type + credential env vars.
    profiles_file = project_dir / "profiles.yml"
    has_profiles = profiles_file.exists()
    if has_profiles:
        text = profiles_file.read_text(encoding="utf-8")
        texts.append(text)
        try:
            profiles = yaml.safe_load(text)
        except yaml.YAMLError:
            profiles = None
        if isinstance(profiles, dict):
            entries = profiles.get("profiles", profiles)
            if isinstance(entries, dict):
                for entry in entries.values():
                    if isinstance(entry, dict):
                        if isinstance(entry.get("type"), str):
                            types.add(entry["type"])
                        _collect_env_refs(entry, envs)

    # ${VAR} placeholders anywhere in the raw YAML are runtime env vars too.
    for text in texts:
        envs.update(_ENV_PLACEHOLDER.findall(text))

    return envs, types, has_profiles


def _render_workflow(
    select: str,
    schedule: str | None,
    profile: str,
    extras: str,
    secrets: list[str],
) -> str:
    on_lines = ["on:", "  workflow_dispatch:"]
    if schedule:
        on_lines += ["  schedule:", f'    - cron: "{schedule}"']
    else:
        on_lines += [
            "  # Uncomment to run on a schedule (UTC):",
            "  # schedule:",
            '  #   - cron: "40 3 * * *"',
        ]

    with_lines = [f'          select: "{select}"']
    if extras:
        with_lines.append(f'          extras: "{extras}"')
    if profile:
        with_lines.append(f'          profile: "{profile}"')

    env_lines: list[str] = []
    if secrets:
        env_lines.append("        env:")
        env_lines += [f"          {name}: ${{{{ secrets.{name} }}}}" for name in secrets]

    lines = [
        "# Generated by `drt deploy github-actions` — review before committing.",
        "# Secrets below must exist in Settings → Secrets and variables → Actions.",
        "name: drt sync",
        "",
        *on_lines,
        "",
        "permissions:",
        "  contents: read",
        "",
        "jobs:",
        "  drt-sync:",
        "    runs-on: ubuntu-latest",
        "    steps:",
        "      - uses: actions/checkout@v4",
        "",
        "      - name: Run drt syncs",
        "        uses: drt-hub/drt-action@v1",
        "        with:",
        *with_lines,
        *env_lines,
        "",
    ]
    return "\n".join(lines)


def _env_block(secrets: list[str], indent: int) -> list[str]:
    pad = " " * indent
    return [f"{pad}env:", *[f"{pad}  {name}: ${{{{ secrets.{name} }}}}" for name in secrets]]


def _setup_steps(extras: str, has_profiles: bool) -> list[str]:
    spec = f"drt-core[{extras}]" if extras else "drt-core"
    lines = [
        "      - uses: actions/checkout@v4",
        "",
        "      - uses: actions/setup-python@v5",
        "        with:",
        '          python-version: "3.12"',
        "",
        "      - name: Install drt-core",
        f'        run: python -m pip install "{spec}"',
        "",
    ]
    if has_profiles:
        lines += [
            "      - name: Stage drt profile",
            "        run: |",
            "          mkdir -p ~/.drt",
            "          cp profiles.yml ~/.drt/profiles.yml",
            "",
        ]
    else:
        lines += [
            "      # No profiles.yml at the project root: commit one (with *_env references,",
            "      # never inline secrets) and copy it to ~/.drt/profiles.yml here.",
            "",
        ]
    return lines


_KEY_CHECK_SCRIPT = r"""if [ -z "$DRT_PLAN_KEY" ]; then
  echo "::error::The DRT_PLAN_KEY secret is not set (or empty). Create it with: openssl rand -hex 32 | gh secret set DRT_PLAN_KEY"
  exit 1
fi"""  # noqa: E501

_COMMENT_SCRIPT = r"""# GitHub caps a comment at 65,536 characters; cut on a character boundary.
python3 - <<'PY'
raw = open("plan-comment.md", "rb").read()[:60000]
open("comment.md", "w", encoding="utf-8").write(raw.decode("utf-8", "ignore"))
PY
# Only our own comment: anyone can post the marker, but only the bot's can be edited.
id=$(gh api "repos/$REPO/issues/$PR/comments" --paginate \
  --jq '[.[] | select(.user.login == "github-actions[bot]" and (.body | contains("<!-- drt-plan -->")))][0].id // empty')
if [ -n "$id" ]; then
  gh api -X PATCH "repos/$REPO/issues/comments/$id" -F body=@comment.md
else
  gh pr comment "$PR" --repo "$REPO" --body-file comment.md
fi"""  # noqa: E501

_FIND_PLAN_SCRIPT = r"""set -euo pipefail
pr=""
if [ -n "${PLAN_RUN_ID:-}" ]; then
  # A run id typed by hand is not trusted: it must be a successful `drt plan` run of a
  # pull request that was merged into main.
  run_id="$PLAN_RUN_ID"
  meta=$(gh run view "$run_id" --repo "$REPO" --json workflowName,event,conclusion,headSha \
    --jq '[.workflowName, .event, .conclusion, .headSha] | @tsv')
  IFS=$'\t' read -r wf event conclusion head <<< "$meta"
  if [ "$wf" != "drt plan" ] || [ "$event" != "pull_request" ] || [ "$conclusion" != "success" ]; then
    echo "::error::Run $run_id is not a successful pull-request run of the drt plan workflow ($wf / $event / $conclusion)."
    exit 1
  fi
  pr=$(gh api "repos/$REPO/commits/$head/pulls" \
    --jq '[.[] | select(.merged_at != null and .base.ref == "main")][0].number // empty')
  if [ -z "$pr" ]; then
    echo "::error::Run $run_id planned $head, which is not part of a pull request merged into main."
    exit 1
  fi
  approver="manual run by $ACTOR of plan run $run_id (PR #$pr)"
else
  pr=$(gh api "repos/$REPO/commits/$SHA/pulls" --jq '.[0].number // empty')
  if [ -z "$pr" ]; then
    echo "::error::$SHA changed a sync without a pull request, so no plan was reviewed and nothing was applied."
    exit 1
  fi
  head=$(gh pr view "$pr" --repo "$REPO" --json headRefOid --jq .headRefOid)
  run_id=$(gh run list --repo "$REPO" --workflow drt-plan.yml --commit "$head" \
    --status success --limit 1 --json databaseId --jq '.[0].databaseId // empty')
  if [ -z "$run_id" ]; then
    echo "::error::No successful drt plan run for PR #$pr ($head), so nothing was applied. Re-run the plan, then run this workflow with its run id."
    exit 1
  fi
  merger=$(gh pr view "$pr" --repo "$REPO" --json mergedBy --jq .mergedBy.login)
  approver="merge of PR #$pr by @$merger (plan run $run_id)"
fi
gh run download "$run_id" --repo "$REPO" --name drt-plans --dir plans
echo "run_id=$run_id" >> "$GITHUB_OUTPUT"
echo "approver=$approver" >> "$GITHUB_OUTPUT"
"""  # noqa: E501


def _script(text: str, indent: int) -> list[str]:
    pad = " " * indent
    return [f"{pad}{line}" if line else "" for line in text.rstrip("\n").split("\n")]


def _render_plan_workflow(
    extras: str, secrets: list[str], has_profiles: bool, redact_keys: bool
) -> str:
    redact = " --redact-keys" if redact_keys else ""
    lines = [
        "# Generated by `drt deploy github-actions --with-plan` — review before committing.",
        "# Plans every sync on a pull request and keeps one comment up to date.",
        "# Needs the secrets below (Settings → Secrets and variables → Actions), including",
        f"# {_PLAN_KEY_SECRET}, which must be the SAME value in drt-plan.yml and drt-apply.yml.",
        "name: drt plan",
        "",
        "on:",
        "  pull_request:",
        "    paths:",
        *[f"      - {p}" for p in _PATHS],
        "",
        "permissions:",
        "  contents: read",
        "  pull-requests: write",
        "",
        "jobs:",
        "  plan:",
        "    # A plan reads your warehouse with real credentials. Pull requests from forks",
        "    # get no secrets, so they are skipped. Do NOT switch this to pull_request_target",
        "    # to make them run: that would run untrusted code with your credentials.",
        "    if: github.event.pull_request.head.repo.full_name == github.repository",
        "    runs-on: ubuntu-latest",
        "    steps:",
        *_setup_steps(extras, has_profiles),
        "      - name: Check the plan key",
        "        env:",
        f"          DRT_PLAN_KEY: ${{{{ secrets.{_PLAN_KEY_SECRET} }}}}",
        "        run: |",
        *_script(_KEY_CHECK_SCRIPT, 10),
        "",
        "      - name: Plan every sync",
        *_env_block(secrets, 8),
        "        run: |",
        f"          drt plan --all --out-dir plans --output markdown{redact} > plan-comment.md",
        "",
        "      - uses: actions/upload-artifact@v4",
        "        with:",
        "          name: drt-plans",
        "          path: plans/",
        "          retention-days: 14",
        "          if-no-files-found: ignore",
        "",
        "      - name: Post or update the plan comment",
        "        if: always() && hashFiles('plan-comment.md') != ''",
        "        env:",
        "          GH_TOKEN: ${{ github.token }}",
        "          REPO: ${{ github.repository }}",
        "          PR: ${{ github.event.pull_request.number }}",
        "        run: |",
        *_script(_COMMENT_SCRIPT, 10),
        "",
    ]
    return "\n".join(lines)


def _render_apply_workflow(
    extras: str, secrets: list[str], has_profiles: bool, max_age: str
) -> str:
    lines = [
        "# Generated by `drt deploy github-actions --with-plan` — review before committing.",
        "# On merge, applies the plan that was reviewed on the pull request, but only if",
        "# recomputing it still gives the same change set: `drt apply` writes NOTHING when the",
        "# world no longer matches, so a stale plan makes this job fail. It is a guard in front of",
        "# a normal run, not a transaction: the sync then runs and writes what it extracts.",
        "name: drt apply",
        "",
        "on:",
        "  push:",
        "    branches: [main]",
        "    paths:",
        *[f"      - {p}" for p in _PATHS],
        "  workflow_dispatch:",
        "    inputs:",
        "      plan_run_id:",
        '        description: "Run id of a drt plan run to apply (default: the merged PR)"',
        "        required: false",
        "",
        "permissions:",
        "  contents: read",
        "  pull-requests: read",
        "  actions: read",
        "",
        "# Never two applies at once, never cancel one that is writing, and queue the rest:",
        "# without `queue: max` GitHub keeps ONE pending run and silently cancels the older",
        "# ones, so a PR merged in a burst would never be applied.",
        "concurrency:",
        "  group: drt-apply",
        "  cancel-in-progress: false",
        "  queue: max",
        "",
        "jobs:",
        "  apply:",
        "    # Never apply from another branch, even if someone dispatches this workflow there.",
        "    if: github.ref == 'refs/heads/main'",
        "    runs-on: ubuntu-latest",
        "    # For an approval gate before writing, uncomment and add required reviewers to",
        "    # this environment (Settings → Environments):",
        "    # environment: production",
        "    steps:",
        *_setup_steps(extras, has_profiles),
        "      - name: Check the plan key",
        "        env:",
        f"          DRT_PLAN_KEY: ${{{{ secrets.{_PLAN_KEY_SECRET} }}}}",
        "        run: |",
        *_script(_KEY_CHECK_SCRIPT, 10),
        "",
        "      - name: Find the reviewed plan",
        "        id: plan",
        "        env:",
        "          GH_TOKEN: ${{ github.token }}",
        "          REPO: ${{ github.repository }}",
        "          SHA: ${{ github.sha }}",
        "          ACTOR: ${{ github.actor }}",
        "          PLAN_RUN_ID: ${{ inputs.plan_run_id }}",
        "        run: |",
        *_script(_FIND_PLAN_SCRIPT, 10),
        "",
        "      - name: Apply the reviewed plans",
        "        if: steps.plan.outputs.run_id != ''",
        *_env_block(secrets, 8),
        "          APPROVER: ${{ steps.plan.outputs.approver }}",
        "        run: |",
        f'          drt apply plans --auto-approve --max-age {max_age} --approved-by "$APPROVER"',
        "",
        "      - uses: actions/upload-artifact@v4",
        "        if: always()",
        "        with:",
        "          name: drt-apply-results",
        "          path: target/drt/run_results.json",
        "          if-no-files-found: ignore",
        "",
    ]
    return "\n".join(lines)


@deploy_app.command(name="github-actions")
def deploy_github_actions(
    schedule: str = typer.Option(
        None,
        "--schedule",
        "-s",
        help='Cron schedule (UTC), e.g. "40 3 * * *". Omit for manual dispatch only.',
    ),
    select: str = typer.Option("*", "--select", help="Sync selector passed to drt-action."),
    profile: str = typer.Option(
        "", "--profile", "-p", help="Profile name passed to drt-action (empty = project default)."
    ),
    extras: str = typer.Option(
        None,
        "--extras",
        help="Override the inferred drt-core extras (comma-separated).",
    ),
    with_plan: bool = typer.Option(
        False,
        "--with-plan",
        help=(
            "Scaffold the review loop instead of a scheduled sync: a pull-request workflow "
            "that comments `drt plan`, and a merge workflow that runs `drt apply` on the plan "
            "that was reviewed."
        ),
    ),
    plan_output: Path = typer.Option(
        _DEFAULT_PLAN_OUTPUT, "--plan-output", help="With --with-plan: the plan workflow path."
    ),
    apply_output: Path = typer.Option(
        _DEFAULT_APPLY_OUTPUT, "--apply-output", help="With --with-plan: the apply workflow path."
    ),
    max_age: str = typer.Option(
        "7d",
        "--max-age",
        help="With --with-plan: how old a reviewed plan may be when it is applied.",
    ),
    redact_keys: bool = typer.Option(
        False,
        "--redact-keys",
        help="With --with-plan: hash key values in the PR comment and the plan artifact.",
    ),
    output: Path = typer.Option(
        _DEFAULT_OUTPUT, "--output", "-o", help="Workflow file path to write."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the workflow instead of writing it."
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing workflow file."),
) -> None:
    """Scaffold a GitHub Actions workflow that runs this project's syncs."""
    project_dir = Path(".")
    if not (project_dir / "drt_project.yml").exists():
        print_error(
            "No drt_project.yml found in the current directory. "
            "Run this from your drt project root (or `drt init` first)."
        )
        raise typer.Exit(code=1)

    if schedule is not None and len(schedule.split()) != 5:
        print_error(
            f'"{schedule}" does not look like a 5-field cron expression (e.g. "40 3 * * *").'
        )
        raise typer.Exit(code=1)

    envs, types, has_profiles = _scan_project(project_dir)
    inferred_extras = ",".join(sorted({_TYPE_TO_EXTRA[t] for t in types if t in _TYPE_TO_EXTRA}))
    effective_extras = extras if extras is not None else inferred_extras
    secrets = sorted(envs)

    if with_plan:
        _scaffold_plan_loop(
            effective_extras,
            sorted(envs | {_PLAN_KEY_SECRET}),
            has_profiles,
            redact_keys,
            max_age,
            plan_output,
            apply_output,
            dry_run,
            force,
        )
        return

    content = _render_workflow(select, schedule, profile, effective_extras, secrets)

    if dry_run:
        console.print(content)
        return

    if output.exists() and not force:
        print_error(f"{output} already exists. Re-run with --force to overwrite.")
        raise typer.Exit(code=1)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")

    console.print(f"[green]✓ Wrote {output}[/green]")
    console.print("\n[bold]Next steps:[/bold]")
    step = 1
    if not has_profiles:
        console.print(
            f"  {step}. Commit a profiles.yml at the project root — drt-action stages it to "
            "~/.drt/profiles.yml (use *_env references, never inline secrets)."
        )
        step += 1
    if secrets:
        console.print(
            f"  {step}. Add these {len(secrets)} secret(s) in "
            "Settings → Secrets and variables → Actions:"
        )
        for name in secrets:
            console.print(f"       gh secret set {name}")
        step += 1
    console.print(f"  {step}. Commit the workflow and push — then run it from the Actions tab.")
    if not schedule:
        console.print(
            "     (No --schedule given: the workflow is manual-dispatch only; "
            "uncomment the cron block to schedule it.)"
        )


def _scaffold_plan_loop(
    extras: str,
    secrets: list[str],
    has_profiles: bool,
    redact_keys: bool,
    max_age: str,
    plan_output: Path,
    apply_output: Path,
    dry_run: bool,
    force: bool,
) -> None:
    """Write the pull-request plan workflow and the merge apply workflow (#1219)."""
    if not re.fullmatch(r"\d+[smhd]", max_age):
        print_error(f'--max-age "{max_age}" must be a number and a unit: 30m, 24h, 7d.')
        raise typer.Exit(code=1)

    plan_text = _render_plan_workflow(extras, secrets, has_profiles, redact_keys)
    apply_text = _render_apply_workflow(extras, secrets, has_profiles, max_age)

    if dry_run:
        # typer.echo, not rich: `[main]` and long lines must come out exactly as written.
        typer.echo(f"# {plan_output}\n{plan_text}\n# {apply_output}\n{apply_text}")
        return

    for path in (plan_output, apply_output):
        if path.exists() and not force:
            print_error(f"{path} already exists. Re-run with --force to overwrite.")
            raise typer.Exit(code=1)
    for path, text in ((plan_output, plan_text), (apply_output, apply_text)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        console.print(f"[green]✓ Wrote {path}[/green]")

    console.print("\n[bold]Next steps:[/bold]")
    step = 1
    if not has_profiles:
        console.print(
            f"  {step}. Commit a profiles.yml at the project root (use *_env references, never "
            "inline secrets); the workflows copy it to ~/.drt/profiles.yml."
        )
        step += 1
    console.print(
        f"  {step}. Create the plan key — the SAME value must be in both workflows, or apply "
        "refuses the plan:"
    )
    console.print(f"       openssl rand -hex 32 | gh secret set {_PLAN_KEY_SECRET}")
    step += 1
    others = [name for name in secrets if name != _PLAN_KEY_SECRET]
    if others:
        console.print(f"  {step}. Add these {len(others)} secret(s):")
        for name in others:
            console.print(f"       gh secret set {name}")
        step += 1
    console.print(
        f"  {step}. Commit both workflows. Open a pull request that changes a sync: the plan "
        "appears as a comment; merging applies the reviewed plan if it still matches."
    )
    console.print(
        "     Pull requests from forks are skipped (no secrets, by design). For an approval "
        "gate before writing, use a protected `environment:` in drt-apply.yml."
    )
