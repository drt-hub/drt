"""Deployment scaffolds for scheduled drt syncs (#785, #1225).

``github-actions`` generates a workflow wired to the official
``drt-hub/drt-action``. ``cloud-run-job`` generates a container, Cloud Run
Job, and Cloud Scheduler command. ``k8s-cronjob`` generates a hardened
Kubernetes CronJob. Connector extras are inferred from the project's profiles
and sync definitions, and required secret *names* are enumerated without ever
reading or emitting secret values.

The scanner reads YAML *raw* (no ``${VAR}`` expansion), so scaffolding works
in a fresh checkout where none of the runtime env vars are set yet.
"""

from __future__ import annotations

import os
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
    "sqs": "sqs",
}

# Secret-provider URIs (#782) in project YAML need their provider SDK in the image.
_URI_EXTRAS = {"gcp-sm": "gcp-secrets", "aws-sm": "aws-secrets", "vault": "vault"}
_SECRET_URI = re.compile(r"\b(gcp-sm|aws-sm|vault)://")
_ENV_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PROFILE_NAME = re.compile(r"[A-Za-z0-9_.-]+")
_IMAGE_REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*(?:[:/@][A-Za-z0-9][A-Za-z0-9._-]*)*")
# DNS-1123 subdomain: dot-separated labels of at most 63 characters.
_K8S_LABEL = re.compile(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?")

_DEFAULT_OUTPUT = Path(".github/workflows/drt-sync.yml")
_DEFAULT_PLAN_OUTPUT = Path(".github/workflows/drt-plan.yml")
_DEFAULT_APPLY_OUTPUT = Path(".github/workflows/drt-apply.yml")
_DEFAULT_CLOUD_RUN_OUTPUT = Path("deploy/cloud-run-job")
_DEFAULT_K8S_OUTPUT = Path("deploy/k8s-cronjob.yml")
_PLAN_KEY_SECRET = "DRT_PLAN_KEY"
_PATHS = ('"syncs/**"', '"drt_project.yml"', '"profiles.yml"')

_CRON_FIELD = r"[A-Za-z0-9*/,-]+"
_CRON_EXPRESSION = re.compile(rf"{_CRON_FIELD}(?: {_CRON_FIELD}){{4}}")
_SELECT_PATTERN = re.compile(r"[A-Za-z0-9_./:*?\[\]-]+")
_SELECT_METHODS = {"tag", "destination"}
_EXTRAS = re.compile(r"[A-Za-z0-9_-]+(?:,[A-Za-z0-9_-]+)*")
# A PEP 440 release version (1.2.1, 1.3.0rc1, 1.2.1.post1); nothing shell- or pip-special.
_DRT_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+)*(?:(?:a|b|rc|\.post|\.dev)[0-9]+)?")


def _collect_env_refs(node: Any, envs: set[str]) -> None:
    """Recursively collect the *values* of ``*_env`` keys (env var names)."""
    if isinstance(node, dict):
        for key, value in node.items():
            if (
                isinstance(key, str)
                and key.lower().endswith("_env")  # Delta/Iceberg profiles use `*_ENV`
                and isinstance(value, str)
                and _ENV_NAME.fullmatch(value)  # a name only: it lands in generated YAML/shell
            ):
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
        project_text = project_file.read_text(encoding="utf-8")
        texts.append(project_text)
        try:
            project_data = yaml.safe_load(project_text)
        except yaml.YAMLError:
            project_data = None
        if isinstance(project_data, dict):
            _collect_env_refs(project_data, envs)  # e.g. state credentials (*_env)
        state = project_data.get("state") if isinstance(project_data, dict) else None
        backend = state.get("backend") if isinstance(state, dict) else None
        if backend in {"gcs", "s3"}:
            types.add(backend)  # the remote state backend needs its storage extra too

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
        types.update(_SECRET_URI.findall(text))  # provider SDKs, mapped by _project_details

    return envs, types, has_profiles


_CRON_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
_MONTH_NAMES = {
    m: i for i, m in enumerate(("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC").split(), 1)
}
_WEEKDAY_NAMES = {d: i for i, d in enumerate(("SUN MON TUE WED THU FRI SAT").split())}
# Names are only valid in the month (4th) and weekday (5th) fields.
_CRON_FIELD_NAMES: tuple[dict[str, int], ...] = ({}, {}, {}, _MONTH_NAMES, _WEEKDAY_NAMES)


def _cron_field_is_valid(field: str, low: int, high: int, names: dict[str, int]) -> bool:
    for part in field.split(","):
        base, slash, step = part.partition("/")
        if slash and (not step.isdigit() or int(step) < 1):
            return False
        if base == "*":
            continue
        bounds = base.split("-")
        if len(bounds) > 2:
            return False
        numbers: list[int] = []
        for token in bounds:
            value = int(token) if token.isdigit() else names.get(token.upper())
            if value is None:
                return False
            numbers.append(value)
        if any(not low <= n <= high for n in numbers):
            return False
        if len(numbers) == 2 and numbers[0] > numbers[1]:
            return False
    return True


_SECRET_KEY = re.compile(
    r"(?:^|_|-)(?:password|passwd|secret|token|api[_-]?key|private[_-]key|access[_-]key"
    r"|secret[_-]key|authorization|webhook_url)$",
    re.IGNORECASE,
)
# `Bearer ${TOKEN}` is a reference wrapped in an auth scheme, not an embedded credential.
_SCHEME_ONLY = re.compile(r"\s*(?:bearer|basic|token|apikey|api-key)?\s*", re.IGNORECASE)
# A value that is only a ${VAR} placeholder is a reference, not a literal.
_ONLY_PLACEHOLDER = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}")
# Subtrees that hold data or documentation, not connection credentials.
_NOT_CREDENTIALS = frozenset({"mask", "unit_tests", "tests", "field_mappings", "description"})


# user:password@host inside a URL is a credential wherever it appears.
_URL_CREDENTIALS = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@")


def _is_literal_secret(key: str, value: Any, parent: dict[Any, Any]) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        value = str(value)  # an unquoted numeric password or token is still a literal
    if not isinstance(value, str) or not value or key.lower().endswith("_env"):
        return False
    if _URL_CREDENTIALS.search(value) and "${" not in value:
        return True
    if _ONLY_PLACEHOLDER.fullmatch(value) or _SECRET_URI.match(value):
        return False
    if "${" in value and _SCHEME_ONLY.fullmatch(re.sub(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}", "", value)):
        return False
    if _SECRET_KEY.search(key):
        return True
    # `auth: {type: api_key, value: ...}` — the credential sits under a neutral key.
    if key == "value" and parent.get("type") in {"api_key", "apikey", "header"}:
        return True
    # An alert channel's URL is its credential (`alerts.on_*: [{type: webhook, url: ...}]`).
    return key == "url" and parent.get("type") in {"webhook", "slack"}


def _walk_inline_secrets(node: Any, path: str, found: list[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{path}.{key}" if path else str(key)
            if key in _NOT_CREDENTIALS:
                continue
            if isinstance(key, str) and _is_literal_secret(key, value, node):
                found.append(here)
            _walk_inline_secrets(value, here, found)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            _walk_inline_secrets(item, f"{path}[{index}]", found)


def _find_inline_secrets(project_dir: Path) -> list[str]:
    """``file: key.path`` of every literal credential-looking value (never the value)."""
    files = [
        *sorted((project_dir / "syncs").glob("*.yml")),
        *sorted((project_dir / "syncs").glob("*.yaml")),
        project_dir / "drt_project.yml",
        project_dir / "profiles.yml",
    ]
    found: list[str] = []
    for path in files:
        if not path.is_file():
            continue
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            continue
        keys: list[str] = []
        _walk_inline_secrets(data, "", keys)
        found += [f"{path.relative_to(project_dir)}: {key}" for key in keys]
    return found


def _validate_schedule(schedule: str) -> None:
    """Accept a valid five-field cron expression that is safe to embed in artifacts."""
    if not _CRON_EXPRESSION.fullmatch(schedule):
        print_error(
            f'"{schedule}" does not look like a safe 5-field cron expression '
            '(e.g. "40 3 * * *"). Use letters, digits, *, /, comma, and hyphen only.'
        )
        raise typer.Exit(code=1)
    fields = schedule.split(" ")
    if not all(
        _cron_field_is_valid(f, lo, hi, names)
        for f, (lo, hi), names in zip(fields, _CRON_RANGES, _CRON_FIELD_NAMES, strict=True)
    ):
        print_error(
            f'"{schedule}" is not a valid cron expression (minute 0-59, hour 0-23, '
            "day 1-31, month 1-12 or JAN-DEC, weekday 0-7 or SUN-SAT)."
        )
        raise typer.Exit(code=1)


def _validate_select(select: str) -> None:
    """Validate one selector token from drt's public selection grammar.

    Generated manifests pass arguments as arrays, but rejecting quotes,
    whitespace, and control characters as well keeps regenerate comments and
    the Cloud Scheduler shell artifact safe by construction.
    """
    if select in {"*", "all"}:
        return

    method: str | None = None
    pattern = select
    if ":" in select:
        method, pattern = select.split(":", 1)
        if method not in _SELECT_METHODS:
            pattern = ""

    if not pattern or not _SELECT_PATTERN.fullmatch(pattern):
        methods = ", ".join(f"{name}:<pattern>" for name in sorted(_SELECT_METHODS))
        print_error(
            f'--select "{select}" is not a safe selector. Use *, all, a sync name/glob, '
            f"{methods}; quotes and whitespace are not allowed. "
            "(`state:modified` / `state:new` need a baseline manifest via `drt run --state`, "
            "which a scheduled scaffold does not carry.)"
        )
        raise typer.Exit(code=1)


def _validate_drt_version(version: str | None) -> None:
    """Reject anything but a plain release version before it reaches a Docker RUN line."""
    if version is not None and not _DRT_VERSION.fullmatch(version):
        print_error(f'--drt-version "{version}" must be a release version such as 1.2.1.')
        raise typer.Exit(1)


def _validate_extras(extras: str) -> None:
    """Validate a comma-separated extras list before placing it in a Docker RUN."""
    if extras and not _EXTRAS.fullmatch(extras):
        print_error(f'--extras "{extras}" must be a comma-separated list of package extra names.')
        raise typer.Exit(code=1)


def _project_details(project_dir: Path, extras: str | None) -> tuple[list[str], str, bool]:
    """Scan a project and resolve sorted secrets plus its drt-core install spec."""
    envs, types, has_profiles = _scan_project(project_dir)
    inferred = ",".join(
        sorted(
            {_TYPE_TO_EXTRA[t] for t in types if t in _TYPE_TO_EXTRA}
            | {_URI_EXTRAS[t] for t in types if t in _URI_EXTRAS}
        )
    )
    effective = extras if extras is not None else inferred
    _validate_extras(effective)
    return sorted(envs), effective, has_profiles


def _require_project() -> Path:
    project_dir = Path(".")
    if not (project_dir / "drt_project.yml").exists():
        print_error(
            "No drt_project.yml found in the current directory. "
            "Run this from your drt project root (or `drt init` first)."
        )
        raise typer.Exit(code=1)
    return project_dir


def _yaml_artifact(command: str, document: dict[str, Any]) -> str:
    """Render deterministic YAML with a regeneration header."""
    body = yaml.safe_dump(document, sort_keys=False, default_flow_style=False)
    header = f"# Generated by `{command}`.\n# Regenerate with the same command plus `--force`."
    return f"{header}\n{body}"


def _check_target_is_safe(path: Path, project_dir: Path = Path(".")) -> None:
    """Refuse a target outside the project or reached through a symlink.

    ``Path.exists()`` and ``write_text()`` both follow symlinks, so a symlinked
    scaffold directory or file would redirect (or, with ``--force``, overwrite)
    something outside the repository. Checked lexically from the project root.
    """
    root = Path(os.path.abspath(project_dir))
    target = Path(os.path.abspath(path))
    try:
        relative = target.relative_to(root)
    except ValueError:
        print_error(f"{path} is outside the project directory; refusing to write there.")
        raise typer.Exit(code=1) from None
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            print_error(f"{current} is a symlink; refusing to write through it.")
            raise typer.Exit(code=1)


def _write_scaffold_files(items: list[tuple[Path, str, int]], *, force: bool) -> None:
    """Write several scaffold files, never opening an existing target.

    Each file is written to a private, randomly named temporary file in its own
    directory (``O_EXCL | O_NOFOLLOW``) and only then moved into place:

    * an existing FIFO, hard link or read-only file is *replaced*, never opened, so a
      FIFO cannot hang the command and a hard-linked file elsewhere is never modified;
    * a failure while staging removes the temporaries and changes nothing;
    * without ``--force`` the final step is ``os.link``, which fails if the target
      appeared in the meantime, so a concurrently created file is never clobbered.

    The final renames are not atomic *across* files. They are renames inside directories
    that were just created or written to, so a failure there is very unlikely; if it
    happens the error names the files already updated, and they are project files kept
    in Git (``git restore`` brings them back). No rollback machinery on purpose: it
    would be far more code than the problem deserves.
    """
    staged: list[tuple[Path, Path]] = []
    published: list[Path] = []
    try:
        for target, content, mode in items:
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                print_error(f"Cannot create {target.parent}: {exc.strerror or exc}.")
                raise typer.Exit(code=1) from exc
            tmp = target.with_name(f".{target.name}.{os.urandom(6).hex()}.tmp")
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(tmp, flags, mode)
            except OSError as exc:
                print_error(f"Cannot write {target}: {exc.strerror or exc}.")
                raise typer.Exit(code=1) from exc
            staged.append((tmp, target))
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                if hasattr(os, "fchmod"):  # POSIX; Windows has no descriptor-level chmod
                    os.fchmod(handle.fileno(), mode)
                handle.write(content)
            if not hasattr(os, "fchmod"):
                tmp.chmod(mode)
        for tmp, target in staged:
            try:
                if force:
                    os.replace(tmp, target)
                else:
                    os.link(tmp, target)  # fails if the target now exists
                    tmp.unlink()
            except FileExistsError:
                print_error(f"{target} already exists. Re-run with --force to overwrite.")
                raise typer.Exit(code=1) from None
            except OSError as exc:
                done = ", ".join(str(p) for p in published) or "none"
                print_error(
                    f"Cannot write {target}: {exc.strerror or exc}. "
                    f"Already written before the failure: {done}."
                )
                raise typer.Exit(code=1) from exc
            published.append(target)
    finally:
        for tmp, _ in staged:
            tmp.unlink(missing_ok=True)


def _check_outputs(paths: list[Path], force: bool) -> None:
    """Refuse the whole scaffold before writing any of several files."""
    for path in paths:
        _check_target_is_safe(path)
    for path in paths:
        if path.is_dir():
            print_error(f"{path} is a directory; refusing to replace it.")
            raise typer.Exit(code=1)
        if path.exists() and not force:
            print_error(f"{path} already exists. Re-run with --force to overwrite.")
            raise typer.Exit(code=1)


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
        "# Regenerate with the same command plus `--force`.",
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
        "# Regenerate both files with the same command plus `--force`.",
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
        "# Regenerate both files with the same command plus `--force`.",
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


def _render_dockerfile(extras: str, has_profiles: bool, drt_version: str | None = None) -> str:
    spec = f"drt-core[{extras}]" if extras else "drt-core"
    if drt_version:
        spec += f"=={drt_version}"
    profile_lines = (
        [
            "# profiles.yml is copied as-is: it must hold *_env references, never literal secrets.",
            "COPY profiles.yml /home/drt/.drt/profiles.yml",
        ]
        if has_profiles
        else [
            "# No profiles.yml was found. Add one (with *_env references, never values) and a",
            "# `COPY profiles.yml /home/drt/.drt/profiles.yml` line before building.",
        ]
    )
    return "\n".join(
        [
            "# Generated by `drt deploy cloud-run-job`.",
            "# Regenerate with the same command plus `--force`.",
            "# Build from the project root: docker build -f deploy/cloud-run-job/Dockerfile .",
            "FROM python:3.12-slim",
            "",
            "WORKDIR /app",
            f'RUN python -m pip install --no-cache-dir "{spec}"',
            "# Only the project definition is copied. Never `COPY . /app`: that would bake",
            "# .env, .drt/ (local state, secrets.toml) and any inline credentials into the image.",
            "COPY drt_project.yml /app/drt_project.yml",
            "COPY syncs /app/syncs",
            *profile_lines,
            "RUN mkdir -p /home/drt/.drt /app/.drt /app/target \\",
            "    && chown -R 65532:65532 /home/drt /app/.drt /app/target",
            "",
            "ENV HOME=/home/drt",
            "USER 65532:65532",
            'ENTRYPOINT ["drt"]',
            'CMD ["run"]',
            "",
        ]
    )


def _render_cloud_run_job(select: str, secrets: list[str]) -> str:
    container: dict[str, Any] = {
        "image": "REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY/drt-sync:latest",
        "args": ["run", "--select", select],
        "resources": {"limits": {"cpu": "1", "memory": "512Mi"}},
    }
    if secrets:
        container["env"] = [
            {
                "name": name,
                # Cloud Run: `name` is the Secret Manager secret, `key` its version.
                "valueFrom": {"secretKeyRef": {"name": name, "key": "latest"}},
            }
            for name in secrets
        ]

    document = {
        "apiVersion": "run.googleapis.com/v1",
        "kind": "Job",
        "metadata": {"name": "drt-sync"},
        "spec": {
            "template": {
                "spec": {
                    "taskCount": 1,
                    "template": {
                        "spec": {
                            "serviceAccountName": "drt-job@PROJECT_ID.iam.gserviceaccount.com",
                            "containers": [container],
                            "maxRetries": 1,
                            "timeoutSeconds": "3600",
                        }
                    },
                }
            }
        },
    }
    return _yaml_artifact("drt deploy cloud-run-job", document)


def _render_cloud_scheduler(schedule: str) -> str:
    uri = (
        "https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${REGION}/jobs/drt-sync:run"
    )
    return f"""#!/bin/sh
# Generated by `drt deploy cloud-run-job`.
# Regenerate with the same command plus `--force`.
set -eu

: "${{PROJECT_ID:?Set PROJECT_ID to the Google Cloud project id}}"
: "${{REGION:=us-central1}}"
: "${{SCHEDULER_SERVICE_ACCOUNT:?Set SCHEDULER_SERVICE_ACCOUNT to its email}}"

gcloud scheduler jobs create http drt-sync \\
  --project="${{PROJECT_ID}}" \\
  --location="${{REGION}}" \\
  --schedule='{schedule}' \\
  --time-zone="Etc/UTC" \\
  --uri="{uri}" \\
  --http-method=POST \\
  --headers="Content-Type=application/json" \\
  --message-body="{{}}" \\
  --oauth-service-account-email="${{SCHEDULER_SERVICE_ACCOUNT}}"
"""


def _render_k8s_cronjob(
    schedule: str, select: str, image: str, secret_name: str, secrets: list[str]
) -> str:
    container: dict[str, Any] = {
        "name": "drt",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["drt"],
        "args": ["run", "--select", select],
        "resources": {
            "requests": {"cpu": "100m", "memory": "256Mi"},
            "limits": {"cpu": "1", "memory": "512Mi"},
        },
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [
            {"name": "drt-state", "mountPath": "/app/.drt"},
            {"name": "drt-target", "mountPath": "/app/target"},
            {"name": "tmp", "mountPath": "/tmp"},
        ],
    }
    if secrets:
        container["env"] = [
            {
                "name": name,
                "valueFrom": {"secretKeyRef": {"name": secret_name, "key": name}},
            }
            for name in secrets
        ]

    document = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "drt-sync"},
        "spec": {
            "schedule": schedule,
            "timeZone": "Etc/UTC",
            "concurrencyPolicy": "Forbid",
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 3,
            "jobTemplate": {
                "spec": {
                    "activeDeadlineSeconds": 3600,
                    "backoffLimit": 1,
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "securityContext": {
                                "runAsNonRoot": True,
                                "seccompProfile": {"type": "RuntimeDefault"},
                            },
                            "containers": [container],
                            "volumes": [
                                {"name": "drt-state", "emptyDir": {}},
                                {"name": "drt-target", "emptyDir": {}},
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        }
                    },
                }
            },
        },
    }
    return _yaml_artifact("drt deploy k8s-cronjob", document)


@deploy_app.command(name="cloud-run-job")
def deploy_cloud_run_job(
    schedule: str = typer.Option(
        ..., "--schedule", "-s", help='Required 5-field cron schedule, e.g. "40 3 * * *".'
    ),
    select: str = typer.Option("*", "--select", help="One sync selector passed to drt run."),
    extras: str = typer.Option(
        None, "--extras", help="Override the inferred drt-core extras (comma-separated)."
    ),
    drt_version: str = typer.Option(
        None,
        "--drt-version",
        help=(
            "Pin the image to drt-core==VERSION (e.g. 1.2.1) for reproducible builds. "
            "Without it the image installs the latest drt-core, like `deploy github-actions`."
        ),
    ),
    output_dir: Path = typer.Option(
        _DEFAULT_CLOUD_RUN_OUTPUT,
        "--output-dir",
        "-o",
        help="Directory for Dockerfile, job.yaml, and scheduler.sh.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print all scaffold files instead of writing them."
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite existing scaffold files."),
    allow_inline_secrets: bool = typer.Option(
        False,
        "--allow-inline-secrets",
        help=(
            "Build the image even though project YAML contains literal credentials "
            "(they would be baked into an image layer). Prefer *_env references."
        ),
    ),
) -> None:
    """Scaffold a containerized Cloud Run Job and Cloud Scheduler trigger."""
    project_dir = _require_project()
    _validate_schedule(schedule)
    _validate_select(select)
    _validate_drt_version(drt_version)
    inline = _find_inline_secrets(project_dir)
    if inline and not allow_inline_secrets:
        print_error(
            "The image would bake literal credentials from the project YAML into a layer:\n  "
            + "\n  ".join(inline[:10])
            + ("\n  ..." if len(inline) > 10 else "")
            + "\nMove them to *_env references (or a secret provider URI), or pass "
            "--allow-inline-secrets to accept that."
        )
        raise typer.Exit(code=1)
    secrets, effective_extras, has_profiles = _project_details(project_dir, extras)

    outputs = [
        (
            output_dir / "Dockerfile",
            _render_dockerfile(effective_extras, has_profiles, drt_version),
        ),
        (output_dir / "job.yaml", _render_cloud_run_job(select, secrets)),
        (output_dir / "scheduler.sh", _render_cloud_scheduler(schedule)),
    ]
    if dry_run:
        typer.echo("\n".join(f"# {path}\n{text}" for path, text in outputs))
        return

    _check_outputs([path for path, _ in outputs], force)
    _write_scaffold_files(
        [
            (path, content, 0o750 if path.name == "scheduler.sh" else 0o640)
            for path, content in outputs
        ],
        force=force,
    )
    for path, _ in outputs:
        console.print(f"[green]✓ Wrote {path}[/green]")

    console.print("\n[bold]Next steps:[/bold]")
    console.print("  1. Replace PROJECT_ID, REGION, REPOSITORY, and service-account placeholders.")
    if secrets:
        console.print(
            f"  2. Create these {len(secrets)} Secret Manager secret(s) and grant the job "
            "service account access:"
        )
        for name in secrets:
            console.print(f"       {name}")
    else:
        console.print("  2. No runtime environment secrets were found in the project YAML.")
    if not has_profiles:
        console.print(
            "  !  No profiles.yml was found: add one (with *_env references) and the matching "
            "COPY line in the Dockerfile, or the image cannot connect."
        )
    console.print("  3. Build and push the image, replace the job, then run scheduler.sh.")
    console.print(
        "     Use a remote state backend for incremental syncs on ephemeral Cloud Run tasks."
    )


@deploy_app.command(name="k8s-cronjob")
def deploy_k8s_cronjob(
    schedule: str = typer.Option(
        ..., "--schedule", "-s", help='Required 5-field cron schedule, e.g. "40 3 * * *".'
    ),
    select: str = typer.Option("*", "--select", help="One sync selector passed to drt run."),
    image: str = typer.Option(
        "YOUR_REGISTRY/drt-sync:latest", "--image", help="Container image for the CronJob."
    ),
    secret_name: str = typer.Option(
        "drt-secrets", "--secret-name", help="Kubernetes Secret containing runtime env vars."
    ),
    output: Path = typer.Option(
        _DEFAULT_K8S_OUTPUT, "--output", "-o", help="CronJob manifest path to write."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the manifest instead of writing it."
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing manifest."),
) -> None:
    """Scaffold a hardened Kubernetes CronJob for this project's syncs."""
    project_dir = _require_project()
    _validate_schedule(schedule)
    _validate_select(select)
    if not _IMAGE_REF.fullmatch(image):
        print_error(f'--image "{image}" must be a container image reference without whitespace.')
        raise typer.Exit(code=1)
    if len(secret_name) > 253 or not all(
        _K8S_LABEL.fullmatch(label) for label in secret_name.split(".")
    ):
        print_error(f'--secret-name "{secret_name}" must be a valid Kubernetes name (DNS-1123).')
        raise typer.Exit(code=1)
    secrets, _, _ = _project_details(project_dir, None)
    content = _render_k8s_cronjob(schedule, select, image, secret_name, secrets)

    if dry_run:
        typer.echo(content)
        return

    _check_outputs([output], force)
    _write_scaffold_files([(output, content, 0o640)], force=force)
    console.print(f"[green]✓ Wrote {output}[/green]")
    console.print("\n[bold]Next steps:[/bold]")
    console.print(f"  1. Build and push {image}, with this project and its connector extras.")
    if secrets:
        console.print(
            f"  2. Create Kubernetes Secret {secret_name} with these keys (values are not "
            "written to the manifest):"
        )
        for name in secrets:
            console.print(f"       {name}")
    else:
        console.print("  2. No runtime environment secrets were found in the project YAML.")
    console.print(f"  3. Review and apply it: kubectl apply -f {output}")
    console.print("     Use durable remote state; emptyDir state disappears with each Job pod.")


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
    project_dir = _require_project()
    if schedule is not None:
        _validate_schedule(schedule)
    _validate_select(select)
    if profile and not _PROFILE_NAME.fullmatch(profile):
        print_error(f'--profile "{profile}" must be a profile name (letters, digits, _ . -).')
        raise typer.Exit(code=1)
    secrets, effective_extras, has_profiles = _project_details(project_dir, extras)

    if with_plan:
        _scaffold_plan_loop(
            effective_extras,
            sorted(set(secrets) | {_PLAN_KEY_SECRET}),
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

    _check_outputs([output], force)
    _write_scaffold_files([(output, content, 0o640)], force=force)

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

    if os.path.abspath(plan_output) == os.path.abspath(apply_output):
        print_error("--plan-output and --apply-output must be different files.")
        raise typer.Exit(code=1)
    _check_outputs([plan_output, apply_output], force)
    _write_scaffold_files(
        [(plan_output, plan_text, 0o640), (apply_output, apply_text, 0o640)], force=force
    )
    for path in (plan_output, apply_output):
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
