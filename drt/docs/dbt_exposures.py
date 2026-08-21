"""Render drt syncs as dbt exposures (#781)."""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path

import yaml

from drt.docs._svg import _slug
from drt.docs.manifest import Manifest

_DBT_IDENTIFIER_UNSAFE_RE = re.compile(r"[^a-z0-9]+")


def _dbt_exposure_name(sync_name: str) -> str:
    """Return an ASCII dbt identifier while retaining drt's namespace.

    The ``drt_`` prefix guarantees that the identifier starts with a letter,
    even when the original sync name starts with a digit. The original name is
    preserved separately under ``meta.drt.sync``.
    """
    normalized = _DBT_IDENTIFIER_UNSAFE_RE.sub("_", sync_name.lower()).strip("_")
    return f"drt_{normalized or 'sync'}"


def _allocate_exposure_names(sync_names: Sequence[str]) -> dict[str, str]:
    """Allocate deterministic, unique names after dbt normalization."""
    allocated: dict[str, str] = {}
    used: set[str] = set()
    for sync_name in sorted(sync_names):
        base = _dbt_exposure_name(sync_name)
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        allocated[sync_name] = candidate
        used.add(candidate)
    return allocated


def _dbt_served_docs_dir(docs_output_dir: Path) -> Path | None:
    """Return *docs_output_dir* relative to dbt's served ``target/`` root."""
    project_dir = Path.cwd()
    output_dir = docs_output_dir if docs_output_dir.is_absolute() else project_dir / docs_output_dir
    # normpath is lexical: unlike Path.resolve(), it never consults the
    # filesystem, so URL output cannot vary with generated files or symlinks.
    output_dir = Path(os.path.normpath(output_dir))
    target_dir = Path(os.path.normpath(project_dir / "target"))
    try:
        return output_dir.relative_to(target_dir)
    except ValueError:
        # A directory outside target/ cannot be reached through dbt docs serve.
        return None


def _sync_page_urls(sync_names: Sequence[str], docs_output_dir: Path) -> dict[str, str]:
    """Compute deterministic dbt-served URLs for non-colliding HTML pages."""
    served_docs_dir = _dbt_served_docs_dir(docs_output_dir)
    if served_docs_dir is None:
        return {}

    slugs = {name: _slug(name) for name in sync_names}
    counts = Counter(slugs.values())
    # HTML generation rejects colliding slugs because either page would be
    # ambiguous. Exposure export is still useful, so degrade only those links
    # instead of letting the shared page-slug constraint abort the whole file.
    return {
        name: (served_docs_dir / "sync" / f"{slug}.html").as_posix()
        for name, slug in slugs.items()
        if counts[slug] == 1
    }


def render_dbt_exposures(
    manifest: Manifest,
    model_refs: Mapping[str, str],
    skipped_syncs: Mapping[str, str] | Sequence[str] = (),
    docs_output_dir: Path = Path("target/docs"),
) -> str:
    """Render a deterministic dbt ``exposures:`` YAML block.

    A sync is eligible only when the builder found an exact ``ref()`` model
    that is not shadowed by a local SQL override. Raw SQL is never inspected
    for heuristic lineage. Per-sync HTML URLs are computed relative to dbt's
    served ``target/`` root without probing the filesystem.
    """
    destinations = {destination.name: destination for destination in manifest.destinations}
    ordered_syncs = sorted(manifest.syncs, key=lambda sync: sync.name)
    sync_page_urls = _sync_page_urls([sync.name for sync in ordered_syncs], docs_output_dir)
    exposure_names = _allocate_exposure_names(
        [sync.name for sync in ordered_syncs if sync.name in model_refs]
    )
    exposures: list[dict[str, object]] = []

    for sync in ordered_syncs:
        model_ref = model_refs.get(sync.name)
        if model_ref is None:
            continue
        destination = destinations[sync.destination]
        exposure: dict[str, object] = {
            "name": exposure_names[sync.name],
            "type": "application",
            "maturity": "high",
            "owner": {"name": "drt"},
            "depends_on": [f"ref('{model_ref}')"],
            "description": (
                f"drt sync {sync.name} -> {destination.type} ({sync.mode}). Managed by drt."
            ),
        }
        if sync_page_url := sync_page_urls.get(sync.name):
            exposure["url"] = sync_page_url
        exposure["meta"] = {
            "drt": {
                "sync": sync.name,
                "destination": destination.type,
                "mode": sync.mode,
            }
        }
        exposures.append(exposure)

    rendered = yaml.safe_dump(
        {"exposures": exposures},
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    )
    skipped_reasons = (
        dict(skipped_syncs)
        if isinstance(skipped_syncs, Mapping)
        else {name: "raw_sql" for name in skipped_syncs}
    )
    notes_by_name = {
        name: (
            (
                f"# Skipped sync {json.dumps(name)}: local SQL override takes "
                "precedence over ref(...); dbt lineage would be inaccurate."
            )
            if reason == "local_override"
            else (
                f"# Skipped sync {json.dumps(name)}: model is not ref(...); "
                "raw-SQL lineage parsing is out of scope."
            )
        )
        for name, reason in skipped_reasons.items()
    }
    notes = [notes_by_name[name] for name in sorted(notes_by_name)]
    if notes:
        rendered += "\n" + "\n".join(notes) + "\n"
    return rendered
