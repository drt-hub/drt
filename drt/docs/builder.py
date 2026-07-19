"""Build a `Manifest` from a drt project on disk."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from drt import __version__
from drt.config.models import SyncConfig
from drt.config.parser import load_project, load_syncs_safe
from drt.docs.manifest import (
    SCHEMA_VERSION,
    Destination,
    Edge,
    Manifest,
    Project,
    Source,
    Sync,
    SyncField,
    SyncRun,
    SyncStateSnapshot,
)
from drt.state.dlq import DlqStore
from drt.state.history import HistoryEntry, HistoryManager
from drt.state.manager import StateManager, SyncState

_SLUG_RE = re.compile(r"[^A-Za-z0-9_]+")


def _slug(value: str) -> str:
    """Mermaid-safe node id."""
    return _SLUG_RE.sub("_", value).strip("_") or "node"


def _allocate_destination_ids(syncs: list[SyncConfig]) -> dict[str, str]:
    """Map each destination identity to a node id that never touches a
    sensitive value (#696): ``dest_<slug(describe_safe())>``, plus a
    deterministic ``_2``/``_3`` suffix when two *distinct* destinations share
    a safe label (two ``rest_api`` endpoints).

    Two review findings shaped this shape:

    - The first cut hashed the unredacted ``describe()``; a truncated hash of
      a low-entropy value (phone number, email with a known domain) is
      brute-forceable, so no function of the sensitive string may ship at all.
      Distinctness is therefore tracked by the full ``describe()`` **in
      memory only** — the returned dict is keyed by it, but only the values
      reach the output.
    - Suffixes were then allocated in sync-file encounter order, so renaming
      ``a.yml`` → ``z.yml`` silently swapped which endpoint owned
      ``dest_rest_api`` vs ``_2`` — a bookmarked page started showing the
      other destination's syncs (@Pawansingh3889). Suffixes now follow the
      lexicographically smallest *referencing sync name*: sync names are
      manifest-public (no ordering leak, unlike sorting by the secret string)
      and survive file renames — ids move only when the set of same-label
      destinations actually changes.

    Independent of ``--full-labels``, so switching label modes never rewires
    the graph or renames pages.
    """
    # identity -> (base slug, smallest referencing sync name)
    info: dict[str, tuple[str, str]] = {}
    for sync in syncs:
        dest = sync.destination
        identity = f"{dest.type}|{dest.describe()}"
        base = f"dest_{_slug(_safe_label(sync))}"
        prev = info.get(identity)
        if prev is None or sync.name < prev[1]:
            info[identity] = (base, sync.name)

    groups: dict[str, list[tuple[str, str]]] = {}  # base -> [(anchor sync, identity)]
    for identity, (base, anchor) in info.items():
        groups.setdefault(base, []).append((anchor, identity))

    ids: dict[str, str] = {}
    for base, members in groups.items():
        members.sort()  # by anchor sync name — public, rename-stable
        for i, (_anchor, identity) in enumerate(members):
            ids[identity] = base if i == 0 else f"{base}_{i + 1}"
    return ids


def _safe_label(sync: SyncConfig) -> str:
    """The docs-safe label — the only destination string allowed to ship."""
    dest = sync.destination
    safe = getattr(dest, "describe_safe", None)
    return safe() if callable(safe) else str(dest.type)


def _destination_label(sync: SyncConfig, full_labels: bool) -> str:
    """Docs label for a destination (#696): safe by default, verbatim on opt-in.

    ``describe_safe`` is duck-typed so configs that don't inherit
    :class:`DescribableConfig` still participate; anything without the method
    (e.g. a future connector added without thinking about docs exposure)
    falls back to its bare ``type`` — safe by default.
    """
    if full_labels:
        return sync.destination.describe()
    return _safe_label(sync)


def _destination_table(sync: SyncConfig) -> str | None:
    """SQL-shaped destinations expose `.table`; non-SQL ones do not."""
    table = getattr(sync.destination, "table", None)
    return str(table) if table else None


def _destination_lookup_tables(sync: SyncConfig) -> list[str]:
    """Tables referenced via `lookups` on the destination, if any."""
    lookups = getattr(sync.destination, "lookups", None)
    if not lookups:
        return []
    return [str(cfg.table) for cfg in lookups.values()]


def _table_aliases(table: str) -> tuple[str, ...]:
    aliases = [table]
    if "." in table:
        aliases.append(table.rsplit(".", maxsplit=1)[-1])
    return tuple(dict.fromkeys(aliases))


def _state_snapshot(state: SyncState, redact: bool) -> SyncStateSnapshot:
    """Rename internal SyncState fields to the public manifest schema.

    ``last_error`` passes through :func:`_redact_error_text` unless
    *redact* is off (``--full-labels``) — connector error strings routinely
    embed endpoints, DSNs, and addresses (#698 interlock with #696).
    """
    error = state.error
    if error is not None and redact:
        error = _redact_error_text(error)
    return SyncStateSnapshot(
        last_sync_at=state.last_run_at,
        last_cursor_value=state.last_cursor_value,
        rows_synced=state.records_synced,
        last_status=state.status,
        last_error=error,
    )


def _sync_runs(entries: list[HistoryEntry], redact: bool) -> tuple[SyncRun, ...]:
    """Map history entries (newest first) to the public ``runs`` shape (v2)."""
    runs: list[SyncRun] = []
    for e in entries:
        errors = tuple(_redact_error_text(t) for t in e.errors) if redact else tuple(e.errors)
        runs.append(
            SyncRun(
                started_at=e.started_at,
                completed_at=e.completed_at,
                duration_seconds=e.duration_seconds,
                status=e.status,
                records_synced=e.records_synced,
                records_failed=e.records_failed,
                errors=errors,
                cursor_value_used=e.cursor_value_used,
            )
        )
    return tuple(runs)


def _declared_fields(sync_cfg: SyncConfig) -> tuple[SyncField, ...]:
    """Write-side column facts declared on the sync (v2, #808 column lineage).

    Union of ``field_mappings`` targets and ``mask`` keys, keyed by the
    destination-side name (mask keys reference the post-rename name — see
    ``SyncOptions.mask``). Sorted by name for deterministic output. Declared
    facts only: a mask key that matches no mapping is still emitted verbatim,
    never validated here.
    """
    opts = sync_cfg.sync
    by_name: dict[str, SyncField] = {}
    for src, dst in (opts.field_mappings or {}).items():
        by_name[dst] = SyncField(name=dst, source_name=src)
    for field_name, spec in (opts.mask or {}).items():
        strategy = spec if isinstance(spec, str) else spec.strategy
        prev = by_name.get(field_name)
        by_name[field_name] = SyncField(
            name=field_name,
            source_name=prev.source_name if prev is not None else field_name,
            mask=strategy,
        )
    return tuple(sorted(by_name.values(), key=lambda f: f.name))


def build_manifest(
    project_dir: Path = Path("."),
    include_state: bool = False,
    full_labels: bool = False,
    history_depth: int = 10,
) -> Manifest:
    """Build a `Manifest` from sync YAMLs + project config under *project_dir*.

    Destination labels are **docs-safe by default** (#696): object identity
    (table, channel, sheet, bucket) stays, network locations and personal
    identifiers (URLs, hosts, phone numbers, emails) do not. Pass
    *full_labels=True* (CLI: ``--full-labels``) to restore verbatim
    ``describe()`` output for trusted/internal hosting. The same switch
    governs error text embedded in ``state`` / ``runs`` — safe mode passes it
    through :func:`_redact_error_text` (#698).

    When *include_state* is true, each sync additionally carries what is
    machine-local (schema v2, #698): the latest persisted state from
    ``.drt/state.json`` (``state`` block, omitted for never-run syncs), up to
    *history_depth* recent executions from ``.drt/history/`` (``runs``,
    newest first; 0 disables), and the current Dead Letter Queue depth
    (``dlq_depth``). Declared column facts (``fields``) are a function of the
    repo like the rest of the catalog, so they are always emitted.
    """
    project = load_project(project_dir)
    syncs_result = load_syncs_safe(project_dir)

    states: dict[str, SyncState] = {}
    dlq_depths: dict[str, int] = {}
    history: HistoryManager | None = None
    if include_state:
        states = StateManager(project_dir).get_all()
        dlq_depths = DlqStore(project_dir).all_depths()
        if history_depth > 0:
            history = HistoryManager(project_dir)

    # Source: project.profile is authoritative; type from inline ProjectConfig.source if present,
    # else "configured" because profiles.yml resolves the concrete type later.
    source_type = project.source.type if project.source else "configured"
    source = Source(name=project.profile, type=source_type)

    destinations: dict[str, Destination] = {}
    syncs: list[Sync] = []
    edges: list[Edge] = []
    dest_ids = _allocate_destination_ids(syncs_result.syncs)

    for sync_cfg in syncs_result.syncs:
        dest = sync_cfg.destination
        dest_id = dest_ids[f"{dest.type}|{dest.describe()}"]
        if dest_id not in destinations:
            destinations[dest_id] = Destination(
                name=dest_id,
                type=sync_cfg.destination.type,
                label=_destination_label(sync_cfg, full_labels),
            )

        sync_state = states.get(sync_cfg.name)
        syncs.append(
            Sync(
                name=sync_cfg.name,
                source=source.name,
                destination=dest_id,
                mode=sync_cfg.sync.mode,
                description=sync_cfg.description,
                tags=tuple(sync_cfg.tags),
                state=_state_snapshot(sync_state, redact=not full_labels) if sync_state else None,
                runs=_sync_runs(
                    history.read(sync_cfg.name, limit=history_depth), redact=not full_labels
                )
                if history is not None
                else (),
                fields=_declared_fields(sync_cfg),
                dlq_depth=dlq_depths.get(sync_cfg.name, 0) if include_state else None,
            )
        )

        edges.append(Edge(kind="source_to_sync", from_=source.name, to=sync_cfg.name))
        edges.append(Edge(kind="sync_to_destination", from_=sync_cfg.name, to=dest_id))

    # Heuristic lookup edges: if sync A's destination has lookups pointing at a table
    # that matches sync B's destination.table, draw a `B -> A` lookup edge (A depends on B).
    sync_by_dest_table: dict[str, str] = {}
    for sync_cfg in syncs_result.syncs:
        table = _destination_table(sync_cfg)
        if table:
            for alias in _table_aliases(table):
                sync_by_dest_table.setdefault(alias, sync_cfg.name)

    lookup_edges_seen: set[tuple[str, str]] = set()
    for sync_cfg in syncs_result.syncs:
        for lookup_table in _destination_lookup_tables(sync_cfg):
            producer = None
            for alias in _table_aliases(lookup_table):
                producer = sync_by_dest_table.get(alias)
                if producer:
                    break
            if not producer or producer == sync_cfg.name:
                continue
            edge_key = (producer, sync_cfg.name)
            if edge_key in lookup_edges_seen:
                continue
            lookup_edges_seen.add(edge_key)
            edges.append(Edge(kind="lookup", from_=producer, to=sync_cfg.name))

    return Manifest(
        schema_version=SCHEMA_VERSION,
        drt_version=__version__,
        generated_at=datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        project=Project(name=project.name, profile=project.profile),
        syncs=syncs,
        sources=[source],
        destinations=list(destinations.values()),
        edges=edges,
    )


# Free-text redaction for error strings embedded in the manifest (#698). The
# ``state.last_error`` and ``runs[].errors`` values come straight from
# connector exceptions, which routinely embed the very things #696 keeps out
# of labels: URLs/DSNs ("connection to postgres://user@db.internal:5432
# failed"), hosts, e-mail addresses, phone numbers, and key=value credential
# fragments. Free text has no key structure to anchor on, so this is a
# pattern sweep — deliberately over-eager ("user: 42" masks the 42), because
# for a hosted artifact over-redaction is a cosmetic bug and under-redaction
# is a leak. ``--full-labels`` bypasses it, same trust model as labels.
_ERROR_URL_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")
_ERROR_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b")
_ERROR_PHONE_RE = re.compile(r"\+\d[\d\s().-]{7,}\d")
_ERROR_KV_RE = re.compile(
    r"(?i)\b(password|passwd|passphrase|secret|token|api[_-]?key|access[_-]?key|"
    r"authorization|host(?:name)?|dsn|user(?:name)?|account|endpoint)\b"
    r"(\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|\S+)"
)
_ERROR_REDACTION = "« redacted »"


def _redact_error_text(text: str) -> str:
    """Mask URLs, e-mails, phone numbers, and credential-ish ``key=value``
    fragments in free-form error text. URLs go first so a ``dsn=scheme://…``
    loses the whole locator, not just the part after the key."""
    text = _ERROR_URL_RE.sub(_ERROR_REDACTION, text)
    text = _ERROR_EMAIL_RE.sub(_ERROR_REDACTION, text)
    text = _ERROR_PHONE_RE.sub(_ERROR_REDACTION, text)
    text = _ERROR_KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{_ERROR_REDACTION}", text)
    return text


# Inline value redaction for the raw-YAML docs tab (#696). The tab publishes the
# sync file verbatim, so an inlined credential / endpoint / PII value would land
# in a docs site that may be committed or hosted. We mask the value of any key
# whose name matches one of these (case-insensitive substring), covering
# secrets, connection endpoints, and PII. ``*_env`` keys are env-var *references*
# (drt's recommended pattern), never secrets, so they are left intact.
_SENSITIVE_KEY_RE = re.compile(
    r"(?i)(password|passwd|passphrase|secret|token|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|client[_-]?secret|credential|connection[_-]?string|dsn|"
    r"auth|webhook|url|endpoint|host|hostname|email|phone|number|sender|recipient|"
    r"bucket|container|path)"
)
# key: value line, allowing an optional "- " list-item prefix; value must be a
# scalar on the same line (block scalars / nested maps have no inline value).
_YAML_KV_RE = re.compile(r"^(\s*(?:-\s+)?)([A-Za-z0-9_.\-]+)(\s*:\s+)(\S.*)$")
_REDACTION = "'« redacted »'"
_MAX_YAML_BYTES = 64 * 1024  # a sync definition beyond this is pathological


def _redact_sensitive_yaml(raw: str) -> tuple[str, bool]:
    """Mask inline secret / endpoint / PII values, preserving the file's layout.

    Line-based so the "as written" formatting survives — only the scalar value
    after a sensitive ``key:`` is replaced. Keys ending in ``_env`` (env-var
    references) and block scalars / anchors / nested maps (no inline value) are
    left untouched. Returns ``(text, redacted_any)``.
    """
    redacted = False
    out: list[str] = []
    for line in raw.split("\n"):
        m = _YAML_KV_RE.match(line)
        if m:
            prefix, key, sep, value = m.groups()
            block_or_anchor = value[:1] in {"|", ">", "&", "*", "#"}
            if key.endswith("_env") or block_or_anchor or not _SENSITIVE_KEY_RE.search(key):
                out.append(line)
            else:
                out.append(f"{prefix}{key}{sep}{_REDACTION}")
                redacted = True
        else:
            out.append(line)
    return "\n".join(out), redacted


def collect_sync_yaml_texts(project_dir: Path = Path(".")) -> dict[str, tuple[str, str]]:
    """Best-effort map of sync name -> (relative path, raw YAML text).

    Reads ``<project_dir>/syncs/*.yml`` off disk so the docs site can show the
    sync definition as written (including ``model`` SQL, which manifest schema
    v1 does not carry). Purely presentational: the texts are NOT part of the
    manifest. Files that cannot be read or parsed, or that carry no ``name``,
    are silently skipped — the renderer falls back to its manifest-derived
    view for those syncs.

    Hardened for a docs artifact that may be committed / hosted:
    - **symlinks are not followed** — they could point outside ``syncs/`` while
      the code header still shows the in-project path;
    - **non-mapping YAML** (e.g. a top-level list) is skipped, not crashed on;
    - text is **capped** at 64 KiB with a truncation note;
    - inline **secrets / endpoints / PII are masked** (#696), with a leading
      note when anything was redacted.
    """
    import yaml

    texts: dict[str, tuple[str, str]] = {}
    syncs_dir = project_dir / "syncs"
    if not syncs_dir.is_dir():
        return texts
    for path in sorted(syncs_dir.glob("*.yml")):
        if path.is_symlink():
            # A symlink could read outside syncs/ under a masked in-project path.
            continue
        try:
            raw = path.read_text(encoding="utf-8")
            parsed = yaml.safe_load(raw)
        except (OSError, UnicodeDecodeError, yaml.YAMLError):
            continue
        if not isinstance(parsed, dict):
            # Valid YAML but not a mapping (list / scalar) — has no sync name and
            # would AttributeError on ``.get``; skip per the best-effort contract.
            continue
        name = parsed.get("name")
        if not (isinstance(name, str) and name):
            continue

        display = raw.rstrip("\n")
        encoded = display.encode("utf-8")
        if len(encoded) > _MAX_YAML_BYTES:
            display = encoded[:_MAX_YAML_BYTES].decode("utf-8", "ignore").rstrip()
            display += f"\n# … truncated by drt docs (file exceeds {_MAX_YAML_BYTES // 1024} KiB)"
        display, redacted = _redact_sensitive_yaml(display)
        if redacted:
            display = (
                "# drt docs: inline secrets / PII masked — prefer *_env references\n" + display
            )

        rel = path.relative_to(project_dir).as_posix()
        texts[name] = (rel, display)
    return texts
