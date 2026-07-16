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
    SyncStateSnapshot,
)
from drt.state.manager import StateManager, SyncState

_SLUG_RE = re.compile(r"[^A-Za-z0-9_]+")


def _slug(value: str) -> str:
    """Mermaid-safe node id."""
    return _SLUG_RE.sub("_", value).strip("_") or "node"


class _DestinationIds:
    """Allocate destination node ids that never touch a sensitive value (#696).

    The id becomes page filenames and manifest node names — it ships. The
    first #696 cut hashed the *unredacted* ``describe()`` string here, which
    review showed is brute-forceable for low-entropy inputs (a phone number
    was recovered from the truncated hash in under a minute; the partial
    redactions — kept country code, kept domain — shrink the preimage space
    further). Hashing is not redaction. So ids are now derived **only from
    the safe label**: ``dest_<slug(describe_safe())>``, with a deterministic
    ``_2``/``_3`` suffix when two distinct destinations share a safe label
    (two ``rest_api`` endpoints). Distinctness is tracked by the full
    ``describe()`` string **in memory only** — it never reaches the output.

    Deterministic because sync files are iterated in sorted order (#697), and
    independent of ``--full-labels`` so switching label modes never rewires
    the graph or renames pages.
    """

    def __init__(self) -> None:
        self._by_identity: dict[str, str] = {}  # full describe() -> id (never shipped)
        self._used: dict[str, int] = {}  # base slug -> allocation count

    def get(self, sync: SyncConfig) -> str:
        dest = sync.destination
        identity = f"{dest.type}|{dest.describe()}"
        existing = self._by_identity.get(identity)
        if existing is not None:
            return existing
        base = f"dest_{_slug(_safe_label(sync))}"
        n = self._used.get(base, 0) + 1
        self._used[base] = n
        allocated = base if n == 1 else f"{base}_{n}"
        self._by_identity[identity] = allocated
        return allocated


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


def _state_snapshot(state: SyncState) -> SyncStateSnapshot:
    """Rename internal SyncState fields to the public manifest schema (v1)."""
    return SyncStateSnapshot(
        last_sync_at=state.last_run_at,
        last_cursor_value=state.last_cursor_value,
        rows_synced=state.records_synced,
        last_status=state.status,
        last_error=state.error,
    )


def build_manifest(
    project_dir: Path = Path("."),
    include_state: bool = False,
    full_labels: bool = False,
) -> Manifest:
    """Build a `Manifest` from sync YAMLs + project config under *project_dir*.

    Destination labels are **docs-safe by default** (#696): object identity
    (table, channel, sheet, bucket) stays, network locations and personal
    identifiers (URLs, hosts, phone numbers, emails) do not. Pass
    *full_labels=True* (CLI: ``--full-labels``) to restore verbatim
    ``describe()`` output for trusted/internal hosting.

    When *include_state* is true, each sync's latest persisted state from
    ``.drt/state.json`` is attached as :class:`SyncStateSnapshot` under the
    sync's ``state`` field. Syncs that have never run are emitted with
    ``state=None`` (and the public schema omits the ``state`` block).
    """
    project = load_project(project_dir)
    syncs_result = load_syncs_safe(project_dir)

    states: dict[str, SyncState] = {}
    if include_state:
        states = StateManager(project_dir).get_all()

    # Source: project.profile is authoritative; type from inline ProjectConfig.source if present,
    # else "configured" because profiles.yml resolves the concrete type later.
    source_type = project.source.type if project.source else "configured"
    source = Source(name=project.profile, type=source_type)

    destinations: dict[str, Destination] = {}
    syncs: list[Sync] = []
    edges: list[Edge] = []
    dest_ids = _DestinationIds()

    for sync_cfg in syncs_result.syncs:
        dest_id = dest_ids.get(sync_cfg)
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
                state=_state_snapshot(sync_state) if sync_state else None,
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


# Inline value redaction for the raw-YAML docs tab (#696). The tab publishes the
# sync file verbatim, so an inlined credential / endpoint / PII value would land
# in a docs site that may be committed or hosted. We mask the value of any key
# whose name matches one of these (case-insensitive substring), covering
# secrets, connection endpoints, and PII. ``*_env`` keys are env-var *references*
# (drt's recommended pattern), never secrets, so they are left intact.
_SENSITIVE_KEY_RE = re.compile(
    r"(?i)(password|passwd|passphrase|secret|token|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|client[_-]?secret|credential|connection[_-]?string|dsn|"
    r"auth|webhook|url|endpoint|host|hostname|email|phone|number|sender|recipient)"
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
