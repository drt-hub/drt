"""Shared SVG/emission helpers for the docs site.

Extracted from ``drt.docs.html`` (#704) so the full-DAG emission (#701) and the
per-sync ego-graphs render nodes, badges, and arrow markers through one code
path. Everything here is pure string emission — deterministic for a given
input, all manifest-derived text HTML-escaped at the point of use.
"""

from __future__ import annotations

import re
from html import escape

_SLUG_RE = re.compile(r"[^A-Za-z0-9]+")


def _slug(value: str) -> str:
    return _SLUG_RE.sub("-", value).strip("-").lower() or "x"


def _slug_map(names: list[str], kind: str) -> dict[str, str]:
    """Map each name to a stable slug, failing fast on a collision.

    ``_slug`` collapses runs of non-alphanumerics, so punctuation-only-distinct
    names (``a_b`` vs ``a__b``) can slugify to the same file — which would
    silently overwrite a page and break cross-links. Detect that and raise.
    """
    out: dict[str, str] = {}
    seen: dict[str, str] = {}
    for name in names:
        slug = _slug(name)
        if slug in seen and seen[slug] != name:
            raise ValueError(
                f"Two {kind} names slugify to the same page {slug!r}: "
                f"{seen[slug]!r} and {name!r}. Rename one so their pages don't collide."
            )
        seen[slug] = name
        out[name] = slug
    return out


# Connector badges — brand-color initials (decision: docs design pass 1).
# Curated map for known types; anything else falls back to a neutral brand
# badge with the first two letters, so new plugins render without changes.
# Keys must match the registered connector ``type`` (see
# ``drt/connectors/registry.py``) — a guard test pins this so a typo'd key
# (e.g. the old ``salesforce`` vs the registered ``salesforce_bulk``) can't
# silently fall through to the neutral badge.
_BADGES: dict[str, tuple[str, str, str]] = {
    # Warehouses / databases
    "bigquery": ("BQ", "#4285f4", "#ffffff"),
    "postgres": ("PG", "#336791", "#ffffff"),
    "redshift": ("RS", "#8c4fff", "#ffffff"),
    "duckdb": ("DK", "#fff100", "#3a3a00"),
    # NB: snowflake moved SF -> SN so the now-active salesforce_bulk keeps the
    # canonical "SF". Revert if design prefers SF for Snowflake instead.
    "snowflake": ("SN", "#29b5e8", "#062d3d"),
    "databricks": ("DX", "#ff3621", "#ffffff"),
    "clickhouse": ("CH", "#faff69", "#3a3d00"),
    "sqlite": ("SQ", "#0f80cc", "#ffffff"),
    "mysql": ("MY", "#00758f", "#ffffff"),
    "sqlserver": ("MS", "#a91d22", "#ffffff"),
    # Lakehouse / files / object stores
    "deltalake": ("DL", "#00add4", "#00303d"),
    "iceberg": ("IB", "#2c88d9", "#ffffff"),
    "parquet": ("PQ", "#50abf1", "#0a2740"),
    "file": ("FL", "#5a6068", "#ffffff"),
    "s3": ("S3", "#ff9900", "#3e2500"),
    "gcs": ("GC", "#4285f4", "#ffffff"),
    "azure_blob": ("AZ", "#0078d4", "#ffffff"),
    "elasticsearch": ("ES", "#00bfb3", "#00312e"),
    # Messaging / collaboration
    "slack": ("SL", "#4a154b", "#ffffff"),
    "discord": ("DC", "#5865f2", "#ffffff"),
    "teams": ("TM", "#6264a7", "#ffffff"),
    "email_smtp": ("EM", "#5a6068", "#ffffff"),
    "twilio": ("TW", "#f22f46", "#ffffff"),
    "sendgrid": ("SG", "#1a82e2", "#ffffff"),
    # CRM / marketing / product analytics
    "hubspot": ("HS", "#ff7a59", "#3e1c00"),
    "salesforce_bulk": ("SF", "#00a1e0", "#ffffff"),
    "intercom": ("IC", "#1f8ded", "#ffffff"),
    "klaviyo": ("KL", "#232426", "#ffffff"),
    "airtable": ("AT", "#fcb400", "#3d2c00"),
    "notion": ("NO", "#191919", "#ffffff"),
    "amplitude": ("AM", "#1f6fff", "#ffffff"),
    "mixpanel": ("MX", "#7856ff", "#ffffff"),
    "google_ads": ("GA", "#4285f4", "#ffffff"),
    "google_sheets": ("GS", "#0f9d58", "#ffffff"),
    # Issue trackers / other
    "jira": ("JR", "#0052cc", "#ffffff"),
    "linear": ("LN", "#5e6ad2", "#ffffff"),
    "github_actions": ("GH", "#2088ff", "#ffffff"),
    "zendesk": ("ZD", "#03363d", "#ffffff"),
    "staged_upload": ("SU", "#5a6068", "#ffffff"),
    "rest_api": ("API", "#5a6068", "#ffffff"),
}

# Status dot/word pairs (never color-alone) — token names from STYLE_CSS.
_STATUS_VARS: dict[str, str] = {
    "success": "--success",
    "partial": "--warning",
    "failed": "--error",
}


def _badge(conn_type: str) -> tuple[str, str, str]:
    known = _BADGES.get(conn_type)
    if known:
        return known
    initials = (conn_type[:2] or "??").upper()
    return (initials, "#7c3aed", "#ffffff")


def _badge_svg(conn_type: str, x: int, y: int) -> str:
    """A 26x26 brand-initial badge at (x, y). All text escaped."""
    initials, bg, fg = _badge(conn_type)
    fs = 9 if len(initials) > 2 else 10
    return (
        f'<rect x="{x}" y="{y}" width="26" height="26" rx="7" fill="{bg}"/>'
        f'<text x="{x + 13}" y="{y + 17}" font-size="{fs}" font-weight="700" '
        f'class="mono" fill="{fg}" text-anchor="middle">{escape(initials)}</text>'
    )


def _clip(value: str, limit: int = 24) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _marker_defs(prefix: str, size: int = 6) -> str:
    """Arrowhead ``<defs>`` for forward (``{prefix}-arr``) and lookup
    (``{prefix}-arr-lk``) edges, colored by the ``--edge`` / ``--edge-lookup``
    tokens. ``prefix`` keeps marker ids unique per SVG."""
    return (
        "<defs>"
        f'<marker id="{prefix}-arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="{size}" '
        f'markerHeight="{size}" orient="auto-start-reverse">'
        '<path d="M0,0.6 L7.4,4 L0,7.4 Z" fill="var(--edge)"/></marker>'
        f'<marker id="{prefix}-arr-lk" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="{size}" '
        f'markerHeight="{size}" orient="auto-start-reverse">'
        '<path d="M0,0.6 L7.4,4 L0,7.4 Z" fill="var(--edge-lookup)"/></marker>'
        "</defs>"
    )


def _node_card(
    x: int,
    y: int,
    w: int,
    conn_type: str,
    name: str,
    sub: str,
    href: str | None,
    *,
    code: bool = False,
    status: str | None = None,
) -> str:
    """One 54px-high node card. ``code=True`` renders the drt-managed sync style
    (violet accent + doc icon); otherwise a connector card with a brand badge.
    ``status`` adds a right-aligned dot + word pair colored by the status tokens.
    """
    accent = (
        f'<rect x="{x}" y="{y}" width="3" height="54" rx="1.5" fill="var(--brand-600)"/>'
        if code
        else ""
    )
    stroke = "var(--zone-drt-line)" if code else "var(--line)"
    if code:
        icon = (
            f'<g transform="translate({x + 13},{y + 14})">'
            '<path d="M0,1.5 a1.5,1.5 0 0 1 1.5,-1.5 h6 l4,4 v11 a1.5,1.5 0 0 1 -1.5,1.5 '
            'h-8.5 a1.5,1.5 0 0 1 -1.5,-1.5 z" fill="none" stroke="var(--brand-600)" '
            'stroke-width="1.4" stroke-linejoin="round"/>'
            '<path d="M7.5,0 v4 h4" fill="none" stroke="var(--brand-600)" '
            'stroke-width="1.4" stroke-linejoin="round"/>'
            '<path d="M3,9 h6 M3,12 h4" stroke="#a78bfa" stroke-width="1.2" '
            'stroke-linecap="round"/></g>'
        )
        tx = x + 34
    else:
        icon = _badge_svg(conn_type, x + 12, y + 14)
        tx = x + 48
    name = _clip(name)
    sub = _clip(sub, 16 if status else 30)
    status_svg = ""
    if status:
        var = _STATUS_VARS.get(status, "--muted")
        sx = x + w - 62
        status_svg = (
            f'<circle cx="{sx}" cy="{y + 36.5}" r="3.5" fill="var({var})"/>'
            f'<text x="{sx + 8}" y="{y + 40}" font-size="10" font-weight="500" '
            f'fill="var({var})">{escape(_clip(status, 8))}</text>'
        )
    body = (
        f'<rect x="{x}" y="{y}" width="{w}" height="54" rx="8" '
        f'fill="var(--surface)" stroke="{stroke}"/>'
        f"{accent}{icon}"
        f'<text x="{tx}" y="{y + 23}" font-size="12.5" font-weight="600" '
        f'class="mono" fill="var(--fg)">{escape(name)}</text>'
        f'<text x="{tx}" y="{y + 40}" font-size="10" letter-spacing="0.6" '
        f'fill="var(--muted)">{escape(sub.upper())}</text>'
        f"{status_svg}"
    )
    if href:
        return f'<a href="{escape(href, quote=True)}">{body}</a>'
    return body
