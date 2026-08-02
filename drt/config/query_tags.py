"""Query tagging — cost-attribution payload shared by every connector (#768).

Every extract and destination-side query drt issues arrives at the warehouse
anonymously today: no BigQuery job label, no Snowflake ``QUERY_TAG``, no
Databricks session tag, not even a SQL comment. Warehouse admins can't answer
"what does reverse ETL cost us" without falling back to account-level spend
(see the #710/#738 smoke-account digest). This module builds one tag payload
— ``app`` / ``sync`` / ``run_id`` plus the project's ``query_tagging.extra``
— that every connector renders in whatever shape its warehouse understands:
a leading SQL comment (universal fallback, every dialect), a BigQuery job
label dict, a Snowflake session parameter, a Databricks ``query_tags`` kwarg.

Lives in ``drt/config/`` rather than ``drt/engine/`` so both directions can
import it without a cycle: ``engine/sync.py`` builds the payload, and the
BigQuery/Snowflake/Databricks *sources* (one layer below the engine) render
it into their native tagging call — a source importing from the engine
would run backwards against ``Config → Source → Engine → Destination``.

Pure, no I/O — safe to call from ``engine/sync.py``'s Rust-boundary.
"""

from __future__ import annotations

import re
import uuid

_COMMENT_UNSAFE = re.compile(r"\*/|[\r\n]")


def new_run_id() -> str:
    """A short identifier for one sync's single execution.

    Not the engine-wide ``run_id`` + metadata-columns concept tracked by
    #762 (unimplemented, targeted after this issue) — this is scoped
    narrowly to "distinguish this query from the last one this sync ran",
    which is all cost attribution needs. Lowercase hex only, so it never
    needs the BigQuery label normalization below.
    """
    return uuid.uuid4().hex[:12]


def build_query_tags(sync_name: str, run_id: str, extra: dict[str, str]) -> dict[str, str]:
    """The canonical, unnormalized tag payload for one sync run.

    ``extra`` is merged last so a project's own keys can override drt's
    defaults (e.g. supplying a custom ``app``) — same override direction as
    every other ``extra``-shaped config in this codebase.
    """
    return {"app": "drt", "sync": sync_name, "run_id": run_id, **extra}


def render_comment_header(tags: dict[str, str]) -> str:
    """``/* drt sync=... run_id=... key=val ... */`` — universal SQL fallback.

    Every dialect drt speaks (including the in-memory DuckDB the lakehouse
    sources register against) accepts a leading block comment ahead of a
    statement, so this needs no per-connector support to be useful.

    Values are stripped of ``*/`` and newlines before rendering: they come
    from project config (sync names, ``query_tagging.extra``) rather than
    request-time user input, so this is corruption-proofing against a stray
    character breaking the comment open into live SQL, not an injection
    defense against an adversarial value.
    """
    parts = " ".join(f"{k}={_COMMENT_UNSAFE.sub('', v)}" for k, v in tags.items())
    return f"/* drt {parts} */"


def normalize_bigquery_label(value: str, *, max_len: int = 63) -> str:
    """BigQuery job/table label constraint: lowercase ``[a-z0-9_-]``, <=63 chars.

    Everything outside that set collapses to ``-`` rather than being dropped,
    so ``"Users -> HubSpot"`` stays recognizable as ``users---hubspot``
    instead of losing word boundaries.
    """
    normalized = re.sub(r"[^a-z0-9_-]", "-", value.lower())
    return normalized[:max_len]
