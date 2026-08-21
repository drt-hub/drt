"""Render drt syncs as dbt exposures (#781)."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import yaml

from drt.docs._svg import _slug_map
from drt.docs.manifest import Manifest


def render_dbt_exposures(
    manifest: Manifest,
    model_refs: Mapping[str, str],
    skipped_syncs: Sequence[str] = (),
    docs_output_dir: Path = Path("target/docs"),
) -> str:
    """Render a deterministic dbt ``exposures:`` YAML block.

    A sync is eligible only when the builder found an exact ``ref()`` model.
    Raw SQL is never inspected for heuristic lineage. If a matching per-sync
    HTML page already exists under *docs_output_dir*, its stable relative path
    is included as the exposure URL.
    """
    destinations = {destination.name: destination for destination in manifest.destinations}
    ordered_syncs = sorted(manifest.syncs, key=lambda sync: sync.name)
    sync_slugs = _slug_map([sync.name for sync in ordered_syncs], "sync")
    exposures: list[dict[str, object]] = []

    for sync in ordered_syncs:
        model_ref = model_refs.get(sync.name)
        if model_ref is None:
            continue
        destination = destinations[sync.destination]
        exposure: dict[str, object] = {
            "name": f"drt_{sync.name}",
            "type": "application",
            "maturity": "high",
            "owner": {"name": "drt"},
            "depends_on": [f"ref('{model_ref}')"],
            "description": (
                f"drt sync {sync.name} -> {destination.type} ({sync.mode}). Managed by drt."
            ),
        }
        docs_page = docs_output_dir / "sync" / f"{sync_slugs[sync.name]}.html"
        if docs_page.is_file():
            try:
                # Keep committed output machine-independent and avoid leaking
                # an absolute home/workspace path through the dbt artifact.
                exposure["url"] = docs_page.relative_to(Path.cwd()).as_posix()
            except ValueError:
                if not docs_page.is_absolute():
                    exposure["url"] = docs_page.as_posix()
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
    notes = [
        f"# Skipped sync {json.dumps(name)}: model is not ref(...); "
        "raw-SQL lineage parsing is out of scope."
        for name in sorted(skipped_syncs)
    ]
    if notes:
        rendered += "\n" + "\n".join(notes) + "\n"
    return rendered
