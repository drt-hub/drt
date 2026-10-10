"""Tests for `drt deploy github-actions` (#785).

The scaffolder scans the project raw (no ${VAR} expansion), infers drt-core
extras from connector types, and enumerates every required secret into the
generated workflow's env block.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from drt.cli.commands.deploy import _TYPE_TO_EXTRA
from drt.cli.main import app

runner = CliRunner()


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Minimal project: snowflake profile, hubspot sync with ${VAR} + *_env."""
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    syncs = tmp_path / "syncs"
    syncs.mkdir()
    (syncs / "users_to_hubspot.yml").write_text(
        """
name: users_to_hubspot
model: ref('users')
destination:
  type: hubspot
  object_type: contacts
  token_env: HUBSPOT_TOKEN
sync:
  mode: full
""".lstrip()
    )
    (syncs / "events_to_rest.yml").write_text(
        """
name: events_to_rest
model: ref('events')
destination:
  type: rest_api
  url: "https://${API_HOST}/v1/events"
  auth:
    type: bearer
    token_env: EVENTS_API_TOKEN
sync:
  mode: full
""".lstrip()
    )
    (tmp_path / "profiles.yml").write_text(
        """
default:
  type: snowflake
  account: acme-xy12345
  user: DRT_SERVICE
  password_env: SNOWFLAKE_PASSWORD
  database: ANALYTICS
  warehouse: WH
""".lstrip()
    )
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_deploy_writes_workflow_with_secrets_and_extras(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--schedule", "40 3 * * *"])

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()

    assert 'cron: "40 3 * * *"' in workflow
    assert "uses: drt-hub/drt-action@v1" in workflow
    assert 'extras: "snowflake"' in workflow  # inferred from profiles.yml
    # Every *_env value and ${VAR} placeholder becomes a wired secret:
    for name in ("HUBSPOT_TOKEN", "EVENTS_API_TOKEN", "SNOWFLAKE_PASSWORD", "API_HOST"):
        assert f"{name}: ${{{{ secrets.{name} }}}}" in workflow
    # Checklist names the secrets for copy-paste:
    assert "gh secret set HUBSPOT_TOKEN" in result.output


def test_deploy_without_schedule_is_dispatch_only(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()
    assert "workflow_dispatch:" in workflow
    assert '- cron: "40 3 * * *"' not in workflow.replace('#   - cron: "40 3 * * *"', "")


def test_deploy_rejects_malformed_cron(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--schedule", "hourly"])
    assert result.exit_code == 1
    assert "5-field cron" in result.output


def test_deploy_dry_run_prints_without_writing(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "drt-hub/drt-action@v1" in result.output
    assert not (project / ".github/workflows/drt-sync.yml").exists()


def test_deploy_refuses_overwrite_without_force(project: Path) -> None:
    first = runner.invoke(app, ["deploy", "github-actions"])
    assert first.exit_code == 0

    second = runner.invoke(app, ["deploy", "github-actions"])
    assert second.exit_code == 1
    assert "--force" in second.output

    forced = runner.invoke(app, ["deploy", "github-actions", "--force"])
    assert forced.exit_code == 0


def test_deploy_extras_override_and_options(project: Path) -> None:
    result = runner.invoke(
        app,
        [
            "deploy",
            "github-actions",
            "--extras",
            "postgres,bigquery",
            "--select",
            "tag:nightly",
            "--profile",
            "prd",
        ],
    )

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()
    assert 'extras: "postgres,bigquery"' in workflow
    assert 'select: "tag:nightly"' in workflow
    assert 'profile: "prd"' in workflow


def test_deploy_warns_when_repo_profiles_missing(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (project / "profiles.yml").unlink()

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output
    assert "profiles.yml" in result.output  # checklist tells the user to commit one


def test_deploy_outside_project_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["deploy", "github-actions"])
    assert result.exit_code == 1
    assert "drt_project.yml" in result.output


def test_extras_mapping_matches_registered_connectors() -> None:
    """Drift guard: every mapped type must be a registered connector type."""
    from drt.connectors import registry

    registered = set(registry._source_registry) | set(registry._destination_registry)
    unknown = set(_TYPE_TO_EXTRA) - registered
    assert not unknown, f"_TYPE_TO_EXTRA references unregistered connector types: {unknown}"


def test_extras_mapping_matches_pyproject_extras() -> None:
    """Drift guard: every mapped extra must exist in pyproject optional-dependencies.

    tomllib is stdlib only from Python 3.11 — skipped on 3.10, where the CI
    matrix still runs this guard on the three newer interpreters.
    """
    tomllib = pytest.importorskip("tomllib")

    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    declared = set(data["project"]["optional-dependencies"])
    unknown = set(_TYPE_TO_EXTRA.values()) - declared
    assert not unknown, f"_TYPE_TO_EXTRA references undeclared extras: {unknown}"


# ---------------------------------------------------------------------------
# --with-plan: the pull-request review loop (#1219)
# ---------------------------------------------------------------------------


def _scaffold(*args: str) -> tuple[dict, dict, str, str]:
    import yaml

    result = runner.invoke(app, ["deploy", "github-actions", "--with-plan", *args])
    assert result.exit_code == 0, result.output
    plan_text = Path(".github/workflows/drt-plan.yml").read_text()
    apply_text = Path(".github/workflows/drt-apply.yml").read_text()
    return yaml.safe_load(plan_text), yaml.safe_load(apply_text), plan_text, apply_text


def _on(workflow: dict) -> dict:
    # PyYAML reads the bare key `on` as the boolean True.
    return workflow.get("on") or workflow[True]  # type: ignore[no-any-return]


def test_with_plan_writes_two_valid_workflows(project: Path) -> None:
    plan, apply_, _, _ = _scaffold()

    assert plan["name"] == "drt plan" and apply_["name"] == "drt apply"
    assert "pull_request" in _on(plan)
    assert _on(apply_)["push"]["branches"] == ["main"]
    assert "workflow_dispatch" in _on(apply_)


def test_plan_workflow_skips_fork_prs_and_never_uses_pull_request_target(project: Path) -> None:
    plan, _, plan_text, apply_text = _scaffold()

    job = plan["jobs"]["plan"]
    assert job["if"] == "github.event.pull_request.head.repo.full_name == github.repository"
    assert "pull_request_target:" not in plan_text and "pull_request_target:" not in apply_text
    assert plan["permissions"] == {"contents": "read", "pull-requests": "write"}


def test_plan_workflow_plans_everything_and_keeps_one_sticky_comment(project: Path) -> None:
    _, _, plan_text, _ = _scaffold()

    assert "drt plan --all --out-dir plans --output markdown" in plan_text
    assert "<!-- drt-plan -->" in plan_text
    assert "[:60000]" in plan_text  # GitHub's comment size limit, cut on a character boundary
    assert '.user.login == "github-actions[bot]"' in plan_text  # only our own comment
    assert "name: drt-plans" in plan_text


def test_both_workflows_carry_the_plan_key_and_the_project_secrets(project: Path) -> None:
    plan, apply_, plan_text, apply_text = _scaffold()

    for text in (plan_text, apply_text):
        assert "DRT_PLAN_KEY: ${{ secrets.DRT_PLAN_KEY }}" in text
        assert "HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}" in text
        assert "SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}" in text
    assert 'pip install "drt-core[snowflake]"' in plan_text


def test_apply_workflow_serialises_and_finds_the_reviewed_plan(project: Path) -> None:
    _, apply_, _, apply_text = _scaffold()

    assert apply_["concurrency"] == {
        "group": "drt-apply",
        "cancel-in-progress": False,
        "queue": "max",  # without it GitHub keeps one pending run and cancels the rest
    }
    assert apply_["permissions"]["actions"] == "read"
    assert "--workflow drt-plan.yml" in apply_text
    assert "gh run download" in apply_text
    assert 'drt apply plans --auto-approve --max-age 7d --approved-by "$APPROVER"' in apply_text
    # Nothing is applied unless a reviewed plan was found.
    assert "steps.plan.outputs.run_id != ''" in apply_text


def test_redact_keys_and_max_age_thread_through(project: Path) -> None:
    _, _, plan_text, apply_text = _scaffold("--redact-keys", "--max-age", "48h")

    assert "--output markdown --redact-keys > plan-comment.md" in plan_text
    assert "--max-age 48h" in apply_text


def test_with_plan_rejects_a_bad_max_age_and_existing_files(project: Path) -> None:
    bad = runner.invoke(app, ["deploy", "github-actions", "--with-plan", "--max-age", "soon"])
    assert bad.exit_code == 1 and "--max-age" in bad.output

    assert runner.invoke(app, ["deploy", "github-actions", "--with-plan"]).exit_code == 0
    again = runner.invoke(app, ["deploy", "github-actions", "--with-plan"])
    assert again.exit_code == 1 and "already exists" in again.output
    assert runner.invoke(app, ["deploy", "github-actions", "--with-plan", "--force"]).exit_code == 0


def test_with_plan_dry_run_prints_both_files_verbatim(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--with-plan", "--dry-run"])

    assert result.exit_code == 0
    assert "branches: [main]" in result.output  # rich markup would have eaten "[main]"
    assert "# .github/workflows/drt-plan.yml" in result.output
    assert not Path(".github/workflows/drt-plan.yml").exists()


def test_next_steps_tell_the_user_to_create_the_shared_plan_key(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--with-plan"])

    assert "openssl rand -hex 32 | gh secret set DRT_PLAN_KEY" in result.output
    assert "gh secret set HUBSPOT_TOKEN" in result.output
    assert "forks" in result.output


def test_with_plan_without_a_profiles_file_says_how_to_stage_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    (tmp_path / "syncs").mkdir()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["deploy", "github-actions", "--with-plan"])

    assert result.exit_code == 0, result.output
    assert "Commit a profiles.yml" in result.output
    assert (
        "No profiles.yml at the project root" in Path(".github/workflows/drt-plan.yml").read_text()
    )
    assert "gh secret set DRT_PLAN_KEY" in result.output


def test_with_plan_stages_the_profile_when_one_is_committed(project: Path) -> None:
    _scaffold()

    assert (
        "cp profiles.yml ~/.drt/profiles.yml" in Path(".github/workflows/drt-plan.yml").read_text()
    )


def test_a_missing_reviewed_plan_fails_the_apply_job_instead_of_passing(project: Path) -> None:
    _, _, _, apply_text = _scaffold()

    # Every refusal path is red, never a quiet success: no key, an unverifiable run id,
    # an unmerged PR, no pull request, no successful plan run.
    assert apply_text.count("::error::") == 5
    assert "::warning::" not in apply_text and "::notice::" not in apply_text
    assert apply_text.count("exit 1") >= 2


def test_the_comment_step_runs_even_when_planning_failed(project: Path) -> None:
    plan, _, _, _ = _scaffold()

    comment = next(s for s in plan["jobs"]["plan"]["steps"] if "comment" in s.get("name", ""))
    assert comment["if"].startswith("always()")


def test_the_generated_comment_truncation_never_splits_a_character(
    project: Path, tmp_path: Path
) -> None:
    import re
    import subprocess
    import sys

    _, _, plan_text, _ = _scaffold()
    snippet = re.search(r"python3 - <<'PY'\n(.*?)\n\s*PY\n", plan_text, re.S)
    assert snippet is not None
    code = "\n".join(line.strip() for line in snippet.group(1).splitlines())
    work = tmp_path / "work"
    work.mkdir()
    (work / "plan-comment.md").write_bytes("\u3042".encode() * 30000)  # 3 bytes each, 90,000 bytes

    subprocess.run([sys.executable, "-c", code], cwd=work, check=True)

    out = (work / "comment.md").read_bytes()
    assert len(out) <= 60000 and len(out) % 3 == 0
    out.decode("utf-8")  # raises if a character was cut in half


def test_the_apply_job_only_runs_on_main_and_checks_the_plan_key_first(project: Path) -> None:
    plan, apply_, plan_text, apply_text = _scaffold()

    assert apply_["jobs"]["apply"]["if"] == "github.ref == 'refs/heads/main'"
    for workflow, job in ((plan, "plan"), (apply_, "apply")):
        steps = [s.get("name", "") for s in workflow["jobs"][job]["steps"]]
        key_check = steps.index("Check the plan key")
        assert key_check < max(
            steps.index(n) for n in steps if "Plan every" in n or "Find the" in n
        )
    assert 'if [ -z "$DRT_PLAN_KEY" ]' in plan_text and 'if [ -z "$DRT_PLAN_KEY" ]' in apply_text


def test_the_generated_text_does_not_promise_a_transaction(project: Path) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--with-plan"])
    apply_text = Path(".github/workflows/drt-apply.yml").read_text()

    assert "not a transaction" in apply_text
    assert "exactly what was reviewed" not in apply_text + result.output


def _fake_gh(tmp_path: Path, **answers: str) -> Path:
    """A `gh` that answers from environment variables, so the generated bash can run offline."""
    script = tmp_path / "bin" / "gh"
    script.parent.mkdir(exist_ok=True)
    script.write_text(
        """#!/usr/bin/env bash
case "$1 $2" in
  "run view") printf '%s' "$FAKE_RUN_META" ;;
  "run list") printf '%s' "$FAKE_RUN_LIST" ;;
  "run download") mkdir -p plans ;;
  "pr view")
    case "$*" in
      *headRefOid*) printf '%s' "$FAKE_PR_HEAD" ;;
      *) printf '%s' "$FAKE_MERGER" ;;
    esac ;;
  api*)
    case "$*" in
      *"$FAKE_META_SHA"*) printf '%s' "$FAKE_PR_BY_META" ;;
      *) printf '%s' "$FAKE_PR_BY_PUSH" ;;
    esac ;;
esac
"""
    )
    script.chmod(0o755)
    return script


def _run_find_plan(tmp_path: Path, env_extra: dict[str, str]) -> tuple[int, str, str]:
    import os
    import subprocess

    from drt.cli.commands.deploy import _FIND_PLAN_SCRIPT

    _fake_gh(tmp_path)
    out = tmp_path / "gh_output"
    out.write_text("")
    env = {
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "REPO": "o/r",
        "SHA": "mergesha",
        "ACTOR": "alice",
        "GITHUB_OUTPUT": str(out),
        "FAKE_META_SHA": "headsha",
        **env_extra,
    }
    done = subprocess.run(
        ["bash", "-c", _FIND_PLAN_SCRIPT],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    return done.returncode, done.stdout + done.stderr, out.read_text()


def test_a_hand_typed_run_id_must_be_a_successful_pr_plan_run_of_a_merged_pr(
    tmp_path: Path,
) -> None:
    good = "drt plan\tpull_request\tsuccess\theadsha"
    base = {"PLAN_RUN_ID": "42", "FAKE_PR_BY_META": "7"}

    ok = _run_find_plan(tmp_path, {**base, "FAKE_RUN_META": good})
    assert ok[0] == 0 and "run_id=42" in ok[2]
    assert "manual run by alice of plan run 42 (PR #7)" in ok[2]

    for bad in (
        "drt apply\tpull_request\tsuccess\theadsha",  # a different workflow
        "drt plan\tworkflow_dispatch\tsuccess\theadsha",  # not a pull-request run
        "drt plan\tpull_request\tfailure\theadsha",  # a failed plan
    ):
        code, text, written = _run_find_plan(tmp_path, {**base, "FAKE_RUN_META": bad})
        assert code == 1 and "not a successful pull-request run" in text and written == ""


def test_a_hand_typed_run_id_for_an_unmerged_branch_is_refused(tmp_path: Path) -> None:
    code, text, written = _run_find_plan(
        tmp_path,
        {
            "PLAN_RUN_ID": "42",
            "FAKE_RUN_META": "drt plan\tpull_request\tsuccess\theadsha",
            "FAKE_PR_BY_META": "",  # no merged pull request contains that commit
        },
    )

    assert code == 1 and "not part of a pull request merged into main" in text
    assert written == ""


def test_the_push_path_applies_only_the_merged_prs_reviewed_plan(tmp_path: Path) -> None:
    ok = _run_find_plan(
        tmp_path,
        {
            "FAKE_PR_BY_PUSH": "9",
            "FAKE_PR_HEAD": "headsha",
            "FAKE_RUN_LIST": "555",
            "FAKE_MERGER": "bob",
        },
    )
    assert ok[0] == 0 and "run_id=555" in ok[2] and "merge of PR #9 by @bob" in ok[2]

    no_pr = _run_find_plan(tmp_path, {"FAKE_PR_BY_PUSH": ""})
    assert no_pr[0] == 1 and "without a pull request" in no_pr[1]

    no_run = _run_find_plan(
        tmp_path, {"FAKE_PR_BY_PUSH": "9", "FAKE_PR_HEAD": "headsha", "FAKE_RUN_LIST": ""}
    )
    assert no_run[0] == 1 and "No successful drt plan run" in no_run[1]
