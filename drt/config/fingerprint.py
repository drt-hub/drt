"""Per-sync fingerprints for the ``state:modified`` selector (#772).

Answers "which syncs did this PR change?" so CI can run only those, instead of
dry-running the whole project or nothing. dbt's ``state:modified`` is the model.

**What is hashed: the sync file exactly as written on disk**, before ``${VAR}``
substitution and before ``var()`` rendering — plus the raw bytes of the
``syncs/models/<name>.sql`` a ``ref()`` points at, since editing the SQL is
editing the sync.

Two consequences, both deliberate:

*Secrets cannot enter the input.* The file holds ``${API_TOKEN}`` or
``token_env: NAME``, never a value, so there is no "which fields are secret?"
classification to maintain or get wrong. That is the same structural choice
every comparable tool makes — dbt keeps credentials in ``profiles.yml`` outside
the project (which is why its ``manifest.json`` is documented as safe to share
as a CI artifact), and Hightouch Git Sync / Census Git-backed Models keep them
in the vendor workspace, referenced by id. An earlier draft of this hashed the
*resolved* config and had to decide whether ``upsert_key`` was a secret
(``config/secrets.py``'s name-suffix heuristic says yes, wrongly, and redacting
it would have hidden a real change in write semantics). Hashing the file removes
the question rather than answering it carefully.

*Environment changes are invisible.* Changing ``API_TOKEN``'s value, or a
``DRT_VAR_*``, does not move the fingerprint. dbt has and documents the same
blind spot ("dbt is unable to identify that lineage ... because the ``var`` or
``env_var`` value has changed"). The upside is that a baseline generated in one
environment compares meaningfully against another, because file bytes do not
depend on the environment.

Not covered, and documented rather than silently missing: a change to
``drt_project.yml`` (project ``vars``, profile) affects every sync but no sync
file, so ``state:modified`` will not select anything. This is a per-sync-file
change detector, not a whole-project one.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml

from drt.config.parser import expand_env_vars, expand_sync_vars, project_vars
from drt.engine.resolver import parse_ref

# Long enough that collisions are not a practical concern, short enough to read
# in a manifest diff or a CI log line.
_DIGEST_CHARS = 16

# Keeps the two inputs from running together — without it, a yaml file ending in
# "x" plus SQL "y" would hash identically to one ending "xy" with empty SQL.
_SEPARATOR = b"\x00drt-model\x00"


def sync_fingerprints(project_dir: Path | str = Path(".")) -> dict[str, str]:
    """Map each sync's **resolved** name to a fingerprint of its definition.

    The name is resolved (env vars and ``var()`` expanded) so it lines up with
    ``SyncConfig.name`` at the call site; the *fingerprint* is computed from the
    unresolved bytes. Those two treatments are intentionally different: the key
    has to match what the rest of drt calls this sync, while the value must not
    depend on the environment it was computed in.

    Files that cannot be parsed are skipped rather than raising — ``drt
    validate`` is where a broken sync file gets reported, and a selector that
    explodes on an unrelated malformed file would be worse than one that cannot
    see it.
    """
    root = Path(project_dir)
    syncs_dir = root / "syncs"
    if not syncs_dir.exists():
        return {}

    variables = project_vars(root)
    fingerprints: dict[str, str] = {}

    # Same glob and ordering as parser.load_syncs, so the two agree on what a
    # project's syncs are.
    for path in sorted(syncs_dir.glob("*.yml")):
        raw = path.read_bytes()
        data = _parse(raw)
        if data is None:
            continue
        name = _resolved_name(data, variables)
        if name is None:
            continue
        fingerprints[name] = _digest(raw, _model_bytes(data, root))

    return fingerprints


def _parse(raw: bytes) -> dict[str, Any] | None:
    """Parse one sync file, or None if it is not a YAML mapping.

    The single parse point for the module: everything downstream takes the
    parsed mapping, so no helper has to re-check that it got one.
    """
    try:
        data = yaml.safe_load(raw)
    except Exception:  # noqa: BLE001 — malformed files are drt validate's job
        return None
    return data if isinstance(data, dict) else None


def _resolved_name(data: dict[str, Any], variables: dict[str, Any]) -> str | None:
    """The sync's ``name:`` with env vars and ``var()`` applied, or None.

    Expansion is applied to the **name value alone**, never to the whole
    document. Expanding the document would make an undefined ``${VAR}``
    *anywhere* in the file — a destination token env var not yet configured on
    this machine, most likely — throw away the sync entirely, so
    ``state:modified`` would silently not select it. A PR that adds a sync
    referencing a not-yet-provisioned secret is exactly a PR CI should be
    looking at, and dropping it is the quiet failure this whole feature exists
    to avoid. Isolating the name keeps an unresolvable field elsewhere from
    deciding whether the sync is visible at all.

    Only a sync with no ``name:`` is skipped here — reporting a broken sync is
    ``drt validate``'s job, and a selector that raised on an unrelated malformed
    file would be worse than one that cannot see it.
    """
    name = data.get("name")
    if not name:
        return None
    try:
        return str(expand_sync_vars(expand_env_vars(name), variables))
    except Exception:  # noqa: BLE001 — an unresolvable name still names a sync
        return str(name)


def _model_bytes(data: dict[str, Any], project_dir: Path) -> bytes:
    """Raw bytes of the ``.sql`` file a ``ref()`` resolves to, else empty.

    Read straight off disk rather than through
    :func:`drt.engine.resolver.resolve_model_ref`, which takes
    ``last_cursor_value`` and substitutes the **current watermark** into the SQL
    it returns. Fingerprinting that would move the hash after every incremental
    run, so ``state:modified`` would select every sync, every time.

    A missing file is not an error: ``ref()`` may name a dbt model or a bare
    warehouse table with no local SQL.
    """
    model = data.get("model")
    if not isinstance(model, str):
        return b""

    table = parse_ref(model)
    if table is None:
        return b""  # inline SQL or a table name — already covered by the yaml bytes
    sql_file = project_dir / "syncs" / "models" / f"{table}.sql"
    return sql_file.read_bytes() if sql_file.exists() else b""


def _digest(yaml_bytes: bytes, model_bytes: bytes) -> str:
    return hashlib.sha256(yaml_bytes + _SEPARATOR + model_bytes).hexdigest()[:_DIGEST_CHARS]
