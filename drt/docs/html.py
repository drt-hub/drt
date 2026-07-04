"""Render a :class:`Manifest` into a multi-file static HTML site (P3 of #499).

``render_html`` writes a self-contained ``target/docs/`` tree — one HTML page
per view (overview, DAG, and each sync/source/destination) plus vendored
``assets/``. Per the ADR (#500): no runtime fetch, each page inlines its data
subset in a ``<script type="application/json" id="drt-data">`` block, so the
site works opened directly via ``file://`` with no CORS issues. Mermaid renders
the DAG via CDN; YAML is syntax-highlighted at build time with Pygments.
"""

from __future__ import annotations

import json
import re
import shutil
from collections import Counter
from pathlib import Path

import yaml
from jinja2 import DictLoader, Environment
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import YamlLexer

from html import escape

from drt.docs._html_assets import APP_JS, STYLE_CSS
from drt.docs.manifest import Manifest, Sync
from drt.docs.mermaid import render_mermaid

_SLUG_RE = re.compile(r"[^A-Za-z0-9]+")


def _slug(value: str) -> str:
    return _SLUG_RE.sub("-", value).strip("-").lower() or "x"


# Connector badges — brand-color initials (decision: docs design pass 1).
# Curated map for known types; anything else falls back to a neutral brand
# badge with the first two letters, so new plugins render without changes.
_BADGES: dict[str, tuple[str, str, str]] = {
    "bigquery": ("BQ", "#4285f4", "#ffffff"),
    "postgres": ("PG", "#336791", "#ffffff"),
    "duckdb": ("DK", "#fff100", "#3a3a00"),
    "snowflake": ("SF", "#29b5e8", "#062d3d"),
    "databricks": ("DX", "#ff3621", "#ffffff"),
    "clickhouse": ("CH", "#faff69", "#3a3d00"),
    "sqlite": ("SQ", "#0f80cc", "#ffffff"),
    "mysql": ("MY", "#00758f", "#ffffff"),
    "elasticsearch": ("ES", "#00bfb3", "#00312e"),
    "s3": ("S3", "#ff9900", "#3e2500"),
    "gcs": ("GC", "#4285f4", "#ffffff"),
    "azure_blob": ("AZ", "#0078d4", "#ffffff"),
    "slack": ("SL", "#4a154b", "#ffffff"),
    "discord": ("DC", "#5865f2", "#ffffff"),
    "hubspot": ("HS", "#ff7a59", "#3e1c00"),
    "salesforce": ("SF", "#00a1e0", "#ffffff"),
    "airtable": ("AT", "#fcb400", "#3d2c00"),
    "klaviyo": ("KL", "#232426", "#ffffff"),
    "rest_api": ("API", "#5a6068", "#ffffff"),
    "webhook": ("WH", "#5a6068", "#ffffff"),
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


def _node_card(x: int, y: int, w: int, conn_type: str, name: str, sub: str,
               href: str | None, *, code: bool = False) -> str:
    """One ego-graph node card. `code=True` renders the drt-managed sync style."""
    accent = ('<rect x="%d" y="%d" width="3" height="54" rx="1.5" fill="var(--brand-600)"/>'
              % (x, y)) if code else ""
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
    sub = _clip(sub, 30)
    body = (
        f'<rect x="{x}" y="{y}" width="{w}" height="54" rx="8" '
        f'fill="var(--surface)" stroke="{stroke}"/>'
        f"{accent}{icon}"
        f'<text x="{tx}" y="{y + 23}" font-size="12.5" font-weight="600" '
        f'class="mono" fill="var(--fg)">{escape(name)}</text>'
        f'<text x="{tx}" y="{y + 40}" font-size="10" letter-spacing="0.6" '
        f'fill="var(--muted)">{escape(sub.upper())}</text>'
    )
    if href:
        return f'<a href="{escape(href, quote=True)}">{body}</a>'
    return body


def _ego_svg(sync: Sync, manifest: Manifest, sync_slugs: dict[str, str],
             source_slugs: dict[str, str], dest_slugs: dict[str, str]) -> str:
    """Static ego-graph for one sync: reads on the left, writes on the right.

    Deterministic single-fan layout (no crossing possible), emitted as inline
    themeable SVG. Names are escaped here — the template injects with |safe.
    """
    dest_by_id = {d.name: d for d in manifest.destinations}
    src_type = {s.name: s.type for s in manifest.sources}
    sync_by_name = {s.name: s for s in manifest.syncs}

    upstream = [e.from_ for e in manifest.edges if e.kind == "lookup" and e.to == sync.name]
    downstream = [e.to for e in manifest.edges if e.kind == "lookup" and e.from_ == sync.name]

    w_card, x_l, x_m, x_r = 230, 10, 330, 650
    rows = max(len(upstream), len(downstream))
    height = 84 + 84 * rows
    parts: list[str] = [
        f'<svg viewBox="0 0 890 {height}" width="890" height="{height}" role="img" '
        f'aria-label="Lineage for {escape(sync.name, quote=True)}">',
        '<defs>'
        '<marker id="ego-arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" '
        'markerHeight="6" orient="auto-start-reverse">'
        '<path d="M0,0.6 L7.4,4 L0,7.4 Z" fill="var(--edge)"/></marker>'
        '<marker id="ego-arr-lk" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" '
        'markerHeight="6" orient="auto-start-reverse">'
        '<path d="M0,0.6 L7.4,4 L0,7.4 Z" fill="var(--edge-lookup)"/></marker>'
        "</defs>",
    ]

    # main row: source → sync → destination
    y0 = 16
    parts.append(_node_card(x_l, y0, w_card, src_type.get(sync.source, "configured"),
                            sync.source, src_type.get(sync.source, "source"),
                            f"../source/{source_slugs.get(sync.source, _slug(sync.source))}.html"))
    parts.append(_node_card(x_m, y0, w_card, "", sync.name, sync.mode, None, code=True))
    dest = dest_by_id.get(sync.destination)
    dest_label = dest.label if dest else sync.destination
    dest_type = dest.type if dest else "destination"
    parts.append(_node_card(x_r, y0, w_card, dest_type, dest_label, dest_type,
                            f"../destination/{dest_slugs.get(sync.destination, _slug(sync.destination))}.html"))
    parts.append(f'<g fill="none" stroke="var(--edge)" stroke-width="1.5" marker-end="url(#ego-arr)">'
                 f'<line x1="{x_l + w_card}" y1="{y0 + 27}" x2="{x_m - 2}" y2="{y0 + 27}"/>'
                 f'<line x1="{x_m + w_card}" y1="{y0 + 27}" x2="{x_r - 2}" y2="{y0 + 27}"/></g>')

    # upstream lookups: destination tables this sync reads, below the source column
    for i, producer in enumerate(upstream):
        y = y0 + 84 * (i + 1)
        p_sync = sync_by_name.get(producer)
        p_dest = dest_by_id.get(p_sync.destination) if p_sync else None
        label = p_dest.label if p_dest else producer
        p_type = p_dest.type if p_dest else "table"
        href = (f"../destination/{dest_slugs.get(p_sync.destination, _slug(producer))}.html"
                if p_sync else None)
        parts.append(_node_card(x_l, y, w_card, p_type, label, f"lookup · via {producer}", href))
        port_x = x_m + 30 + i * 44
        parts.append(
            f'<path d="M{x_l + w_card},{y + 27} C{x_l + w_card + 70},{y + 27} '
            f'{port_x},{y - 10} {port_x},{y0 + 58}" fill="none" '
            f'stroke="var(--edge-lookup)" stroke-width="1.5" stroke-dasharray="5 4" '
            f'marker-end="url(#ego-arr-lk)"/>'
            f'<circle cx="{port_x}" cy="{y0 + 54}" r="2.5" fill="var(--edge-lookup)"/>')

    # downstream consumers: syncs that look up this sync's destination, below it
    for i, consumer in enumerate(downstream):
        y = y0 + 84 * (i + 1)
        c_sync = sync_by_name.get(consumer)
        parts.append(_node_card(x_r, y, w_card, "", consumer,
                                (c_sync.mode if c_sync else "sync") + " · lookup",
                                f"../sync/{sync_slugs.get(consumer, _slug(consumer))}.html", code=True))
        parts.append(
            f'<path d="M{x_r + 30},{y0 + 70} C{x_r - 30},{y0 + 100} '
            f'{x_r - 30},{y + 27} {x_r - 2},{y + 27}" fill="none" '
            f'stroke="var(--edge-lookup)" stroke-width="1.5" stroke-dasharray="5 4" '
            f'marker-end="url(#ego-arr-lk)"/>')

    parts.append("</svg>")
    return "".join(parts)


def _sync_yaml(sync: Sync) -> str:
    """A readable YAML view of the sync's catalog entry (manifest schema v1).

    The manifest does not carry the original sync's ``model`` SQL (it is not part
    of schema v1), so this renders the fields the catalog knows about.
    """
    doc: dict[str, object] = {
        "name": sync.name,
        "source": sync.source,
        "destination": sync.destination,
        "mode": sync.mode,
    }
    if sync.description:
        doc["description"] = sync.description
    if sync.tags:
        doc["tags"] = list(sync.tags)
    return yaml.safe_dump(doc, sort_keys=False, default_flow_style=False).rstrip()


_BASE = """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ page_title }} · drt docs</title>
<link rel="stylesheet" href="{{ root }}assets/style.css">
<link rel="stylesheet" href="{{ root }}assets/pygments-default.css">
{% block head %}{% endblock %}
</head>
<body>
<header class="topbar">
  <span class="brand"><span class="logo">d</span>drt docs</span>
  <span class="project">{{ project_name }}<br><span class="ver">drt {{ drt_version }}</span></span>
  <nav class="topnav">
    <a class="navbtn {{ 'active' if active=='overview' }}"
       href="{{ root }}index.html">Overview</a>
    <a class="navbtn {{ 'active' if active=='dag' }}" href="{{ root }}dag.html">DAG</a>
  </nav>
  <span class="search"><input id="drt-search" type="search" placeholder="Filter…" autocomplete="off"></span>
</header>
<div class="shell">
  <aside class="sidebar">
    <details class="group" data-group="syncs" open>
      <summary>Syncs <span class="count">{{ nav.syncs|length }}</span></summary>
      <ul>{% for s in nav.syncs %}<li><a href="{{ root }}sync/{{ s.slug }}.html"
        {{ 'class=current' if s.slug==current_slug and active=='sync' }}>{{ s.name }}</a></li>
      {% endfor %}</ul>
    </details>
    <details class="group" data-group="sources" open>
      <summary>Sources <span class="count">{{ nav.sources|length }}</span></summary>
      <ul>{% for s in nav.sources %}<li><a href="{{ root }}source/{{ s.slug }}.html"
        {{ 'class=current' if s.slug==current_slug and active=='source' }}>{{ s.name }}</a></li>
      {% endfor %}</ul>
    </details>
    <details class="group" data-group="destinations" open>
      <summary>Destinations <span class="count">{{ nav.destinations|length }}</span></summary>
      <ul>{% for d in nav.destinations %}<li><a href="{{ root }}destination/{{ d.slug }}.html"
        {{ 'class=current' if d.slug==current_slug and active=='destination' }}>{{ d.label }}</a></li>
      {% endfor %}</ul>
    </details>
    {% if nav.tags %}
    <details class="group" data-group="tags" open>
      <summary>Tags <span class="count">{{ nav.tags|length }}</span></summary>
      <ul>{% for t in nav.tags %}<li><a href="{{ root }}tag/{{ t.slug }}.html"
        {{ 'class=current' if t.slug==current_slug and active=='tag' }}>#{{ t.name }} <span class="count">{{ t.count }}</span></a></li>
      {% endfor %}</ul>
    </details>
    {% endif %}
  </aside>
  <main class="main">
    {% block main %}{% endblock %}
    {# no timestamp here — pages stay byte-identical across regens (#697); generated_at lives in manifest.json #}
    <div class="footer">drt docs · static · drt {{ drt_version }} · manifest schema v{{ schema_version }}</div>
  </main>
</div>
<script type="application/json"
  id="drt-data">{{ data_json|safe }}</script>
<script src="{{ root }}assets/app.js"></script>
{% block scripts %}{% endblock %}
</body>
</html>
"""

_INDEX = """\
{% extends "base" %}
{% block main %}
<div class="eyebrow">Overview</div>
<h1>{{ project_name }}</h1>
<p class="lede">drt {{ drt_version }} &middot; profile <code>{{ profile }}</code> &middot; manifest schema v{{ schema_version }}</p>
<div class="cards">
  <div class="card"><div class="num">{{ nav.syncs|length }}</div><div class="lbl">Syncs</div></div>
  <div class="card"><div class="num">{{ nav.sources|length }}</div><div class="lbl">Sources</div></div>
  <div class="card"><div class="num">{{ nav.destinations|length }}</div><div class="lbl">Destinations</div></div>
</div>
<div class="two-col">
  <div>
    <h2>Source types</h2>
    {% set max_src = source_type_counts.values()|max|default(1) %}
    {% for type, count in source_type_counts.items() %}
    <div class="bar-row">
      <div class="bar-row__label">{{ type }}</div>
      <div class="bar-row__bar-bg"><div class="bar-row__bar-fill" style="width:{{ (count*100/max_src)|round(0,'floor') }}%"></div></div>
      <div class="bar-row__count">{{ count }}</div>
    </div>
    {% endfor %}
  </div>
  <div>
    <h2>Destination types</h2>
    {% set max_dst = destination_type_counts.values()|max|default(1) %}
    {% for type, count in destination_type_counts.items() %}
    <div class="bar-row">
      <div class="bar-row__label">{{ type }}</div>
      <div class="bar-row__bar-bg"><div class="bar-row__bar-fill" style="width:{{ (count*100/max_dst)|round(0,'floor') }}%"></div></div>
      <div class="bar-row__count">{{ count }}</div>
    </div>
    {% endfor %}
  </div>
</div>
<h2>Recent runs</h2>
{% if recent_runs %}
<table>
  <tr><th>Time (UTC)</th><th>Sync</th><th>Status</th><th class="right">Rows</th></tr>
  {% for run in recent_runs %}
  <tr>
    <td class="font-mono">{{ run.last_sync_at }}</td>
    <td><a href="sync/{{ run.slug }}.html">{{ run.name }}</a></td>
    <td>{% if run.last_status=='success' %}<span class="status-success">&#10003; success</span>
      {%- elif run.last_status=='partial' %}<span class="status-partial">&#9888; partial</span>
      {%- elif run.last_status=='failed' %}<span class="status-failed">&#10007; failed</span>
      {%- else %}{{ run.last_status }}{% endif %}</td>
    <td class="right">{{ run.rows_synced }}</td>
  </tr>
  {% endfor %}
</table>
{% else %}
<div class="empty">No run history yet &mdash; run <code>drt run</code> to populate state.</div>
{% endif %}
{% endblock %}
"""

_DAG = """\
{% extends "base" %}
{% block main %}
<div class="eyebrow">Lineage</div>
<h1>DAG</h1>
<p class="lede">Source → sync → destination lineage. Dashed edges are destination lookups.</p>
<pre class="mermaid">{{ mermaid }}</pre>
{% endblock %}
{% block scripts %}
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>
  var dark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  mermaid.initialize({ startOnLoad: true, theme: dark ? "dark" : "default" });
</script>
{% endblock %}
"""

_SYNC = """\
{% extends "base" %}
{% block main %}
<div class="crumb"><a href="../index.html">Syncs</a> / {{ sync.name }}</div>
<h1>{{ sync.name }}</h1>
{% if sync.description %}<p class="lede">{{ sync.description }}</p>{% endif %}
{% if sync.tags %}<p>{% for t in sync.tags %}<span class="chip">{{ t }}</span> {% endfor %}</p>{% endif %}
<div class="two-col">
  <div class="kpi">
    <div class="kpi__label">Last run</div>
    {% if state %}
    <div>{{ state.last_sync_at }} &middot; <span class="status-{{ state.last_status }}">{{ state.last_status }}</span></div>
    <div class="font-mono">{{ state.rows_synced }} rows</div>
    {% else %}<div class="empty" style="padding:8px">No runs yet</div>{% endif %}
  </div>
  <div class="kpi">
    <div class="kpi__label">Configuration</div>
    <div>source: <code>{{ sync.source }}</code> &middot; mode: <code>{{ sync.mode }}</code></div>
    <div class="font-mono">&rarr; {{ destination_label }}</div>
  </div>
</div>

<div class="tabs" role="tablist">
  <button class="tab-btn active" data-tab="yaml">YAML</button>
  <button class="tab-btn" data-tab="lineage">Lineage</button>
  <button class="tab-btn" data-tab="state">State</button>
</div>

<div class="tab-panel active" data-tab="yaml">
  <h2>Definition</h2>
  {{ yaml_html|safe }}
</div>

<div class="tab-panel" data-tab="lineage">
  <h2>Lineage</h2>
  <div class="ego">{{ ego_svg|safe }}</div>
  <p class="lede" style="font-size:12.5px;margin-top:10px">
    Reads <a href="../source/{{ source_slug }}.html">{{ sync.source }}</a>
    {%- for up in upstream %} + <a href="../destination/{{ up.slug }}.html">{{ up.label }}</a> (lookup){% endfor %} &middot;
    writes <a href="../destination/{{ destination_slug }}.html">{{ destination_label }}</a>
    {%- if downstream %} &middot; read by {% for d in downstream %}<a href="../sync/{{ d.slug }}.html">{{ d.name }}</a>{{ ", " if not loop.last }}{% endfor %}{% endif %}.
  </p>
</div>

<div class="tab-panel" data-tab="state">
  <h2>State</h2>
  {% if state %}
  <table>
    <tr><td>last_sync_at</td><td class="font-mono">{{ state.last_sync_at }}</td></tr>
    <tr><td>last_cursor_value</td><td class="font-mono">{{ state.last_cursor_value or "—" }}</td></tr>
    <tr><td>rows_synced</td><td class="font-mono">{{ state.rows_synced }}</td></tr>
    <tr><td>last_status</td><td class="font-mono status-{{ state.last_status }}">{{ state.last_status }}</td></tr>
    <tr><td>last_error</td><td class="font-mono">{{ state.last_error or "—" }}</td></tr>
  </table>
  {% else %}
  <div class="empty">No state recorded yet. Run <code>drt run --select {{ sync.name }}</code> to populate.</div>
  {% endif %}
</div>
{% endblock %}
"""

_STATUS_TD = (
    '<td>{% if s.status %}'
    '<span class="dot" style="background:var(--{{ s.status_var }})"></span>'
    '<span class="status-{{ s.status }}">{{ s.status }}</span>'
    '{% else %}<span class="font-mono">—</span>{% endif %}</td>'
)

_SOURCE = """\
{% extends "base" %}
{% block main %}
<div class="crumb"><a href="../index.html">Sources</a> / {{ source.name }}</div>
<h1><span class="badge" style="background:{{ badge.bg }};color:{{ badge.fg }}">{{ badge.initials }}</span> {{ source.name }}</h1>
<p class="lede">{{ source.type }} &middot; external &middot; read by {{ syncs|length }} sync{{ 's' if syncs|length != 1 }}</p>
<h2>Used by</h2>
<table>
  <tr><th>Sync</th><th>Destination</th><th>Mode</th><th>Last status</th></tr>
  {% for s in syncs %}
  <tr><td><a href="../sync/{{ s.slug }}.html">{{ s.name }}</a></td><td>{{ s.destination_label }}</td>
  <td><span class="mode">{{ s.mode }}</span></td>""" + _STATUS_TD + """</tr>
  {% endfor %}
</table>
{% endblock %}
"""

_DESTINATION = """\
{% extends "base" %}
{% block main %}
<div class="crumb"><a href="../index.html">Destinations</a> / {{ destination.label }}</div>
<h1><span class="badge" style="background:{{ badge.bg }};color:{{ badge.fg }}">{{ badge.initials }}</span> {{ destination.label }}</h1>
<p class="lede">{{ destination.type }} &middot; external &middot; fed by {{ syncs|length }} sync{{ 's' if syncs|length != 1 }}</p>
<h2>Fed by</h2>
<table>
  <tr><th>Sync</th><th>Source</th><th>Mode</th><th>Last status</th></tr>
  {% for s in syncs %}
  <tr><td><a href="../sync/{{ s.slug }}.html">{{ s.name }}</a></td><td>{{ s.source }}</td>
  <td><span class="mode">{{ s.mode }}</span></td>""" + _STATUS_TD + """</tr>
  {% endfor %}
</table>
{% endblock %}
"""

_TAG = """\
{% extends "base" %}
{% block main %}
<div class="crumb"><a href="../index.html">Tags</a> / #{{ tag }}</div>
<h1><span class="chip">#{{ tag }}</span></h1>
<p class="lede">{{ syncs|length }} tagged sync{{ 's' if syncs|length != 1 }}</p>
<table>
  <tr><th>Sync</th><th>Destination</th><th>Mode</th><th>Last status</th></tr>
  {% for s in syncs %}
  <tr><td><a href="../sync/{{ s.slug }}.html">{{ s.name }}</a></td><td>{{ s.destination_label }}</td>
  <td><span class="mode">{{ s.mode }}</span></td>""" + _STATUS_TD + """</tr>
  {% endfor %}
</table>
{% endblock %}
"""


def render_html(manifest: Manifest, output_dir: Path) -> list[Path]:
    """Render *manifest* into a multi-file static site under *output_dir*.

    Returns the list of files written. The output is self-contained and
    portable: open ``index.html`` directly (``file://``) or host the directory
    on any static server.
    """
    env = Environment(
        loader=DictLoader(
            {
                "base": _BASE,
                "index": _INDEX,
                "dag": _DAG,
                "sync": _SYNC,
                "source": _SOURCE,
                "destination": _DESTINATION,
                "tag": _TAG,
            }
        ),
        autoescape=True,
    )

    _status_vars = {"success": "success", "partial": "warning", "failed": "error"}

    def _status_fields(s: Sync) -> dict:
        if s.state is None:
            return {"status": None, "status_var": None}
        st = s.state.last_status
        return {"status": st, "status_var": _status_vars.get(st, "muted")}

    def _badge_dict(conn_type: str) -> dict:
        initials, bg, fg = _badge(conn_type)
        return {"initials": initials, "bg": bg, "fg": fg}

    # Stable slugs for filenames + cross-links.
    sync_slugs = {s.name: _slug(s.name) for s in manifest.syncs}
    source_slugs = {s.name: _slug(s.name) for s in manifest.sources}
    dest_slugs = {d.name: _slug(d.name) for d in manifest.destinations}
    dest_by_id = {d.name: d for d in manifest.destinations}

    tag_syncs: dict[str, list[Sync]] = {}
    for s in manifest.syncs:
        for t in s.tags:
            tag_syncs.setdefault(t, []).append(s)
    tag_slugs = {t: _slug(t) for t in tag_syncs}

    nav = {
        "syncs": [{"name": s.name, "slug": sync_slugs[s.name]} for s in manifest.syncs],
        "sources": [{"name": s.name, "slug": source_slugs[s.name]} for s in manifest.sources],
        "destinations": [
            {"label": d.label, "slug": dest_slugs[d.name]} for d in manifest.destinations
        ],
        "tags": [
            {"name": t, "slug": tag_slugs[t], "count": len(tag_syncs[t])}
            for t in sorted(tag_syncs)
        ],
    }

    common = {
        "project_name": manifest.project.name if manifest.project else "drt project",
        "drt_version": manifest.drt_version,
        "generated_at": manifest.generated_at,
        "schema_version": manifest.schema_version,
        "profile": manifest.project.profile if manifest.project else "",
        "nav": nav,
    }

    written: list[Path] = []

    # Clear any previous build so a removed sync/source/destination doesn't
    # leave an orphan page behind.
    if output_dir.exists():
        shutil.rmtree(output_dir)

    def write(rel: str, html: str) -> None:
        path = output_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html, encoding="utf-8")
        written.append(path)

    output_dir.mkdir(parents=True, exist_ok=True)

    # assets/
    write("assets/style.css", STYLE_CSS)
    write("assets/app.js", APP_JS)
    write("assets/pygments-default.css", HtmlFormatter().get_style_defs(".highlight"))

    # manifest.json — still emitted (P2 artifact for external tools).
    write("manifest.json", json.dumps(manifest.to_dict(), indent=2))

    def dumps(view: dict[str, object]) -> str:
        # Escape <, >, & to their \u00xx forms so an embedded "</script>"
        # (or stray markup) can't break out of the inline JSON block. Stays
        # valid JSON for JSON.parse().
        return (
            json.dumps(view, separators=(",", ":"))
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
            .replace("&", "\\u0026")
        )

    # index.html — overview with type-distribution bars + recent runs.
    # Bars count *syncs* by the type of their source / destination (how many
    # syncs read from bigquery, write to postgres, …) — more telling than the
    # bare node count, and matches the ADR #500 mockup.
    src_type = {s.name: s.type for s in manifest.sources}
    dst_type = {d.name: d.type for d in manifest.destinations}
    source_type_counts = dict(
        Counter(src_type.get(s.source, s.source) for s in manifest.syncs)
    )
    destination_type_counts = dict(
        Counter(dst_type.get(s.destination, s.destination) for s in manifest.syncs)
    )
    runs_with_state = sorted(
        (s for s in manifest.syncs if s.state is not None),
        key=lambda s: (s.state.last_sync_at if s.state else "") or "",
        reverse=True,
    )
    recent_runs = [
        {
            "name": s.name,
            "slug": sync_slugs[s.name],
            "last_sync_at": s.state.last_sync_at if s.state else "",
            "last_status": s.state.last_status if s.state else "",
            "rows_synced": s.state.rows_synced if s.state else 0,
        }
        for s in runs_with_state[:10]
    ]
    write(
        "index.html",
        env.get_template("index").render(
            page_title="Overview",
            active="overview",
            root="",
            current_slug="",
            source_type_counts=source_type_counts,
            destination_type_counts=destination_type_counts,
            recent_runs=recent_runs,
            data_json=dumps({"project": common["project_name"], "counts": {
                "syncs": len(manifest.syncs),
                "sources": len(manifest.sources),
                "destinations": len(manifest.destinations),
            }, "nav": nav}),
            **common,
        ),
    )

    # dag.html
    write(
        "dag.html",
        env.get_template("dag").render(
            page_title="DAG",
            active="dag",
            root="",
            current_slug="",
            mermaid=render_mermaid(manifest),
            data_json=dumps({"nav": nav}),
            **common,
        ),
    )

    # per-sync pages
    formatter = HtmlFormatter(cssclass="highlight")
    sync_by_name = {s.name: s for s in manifest.syncs}
    for s in manifest.syncs:
        yaml_text = _sync_yaml(s)
        yaml_html = highlight(yaml_text, YamlLexer(), formatter)
        dest = dest_by_id.get(s.destination)
        state = None
        if s.state is not None:
            state = {
                "last_status": s.state.last_status,
                "last_sync_at": s.state.last_sync_at,
                "rows_synced": s.state.rows_synced,
                "last_cursor_value": s.state.last_cursor_value,
                "last_error": s.state.last_error,
            }
        upstream = []
        for e in manifest.edges:
            if e.kind == "lookup" and e.to == s.name:
                p_sync = sync_by_name.get(e.from_)
                p_dest = dest_by_id.get(p_sync.destination) if p_sync else None
                upstream.append({
                    "label": p_dest.label if p_dest else e.from_,
                    "slug": dest_slugs.get(p_sync.destination, _slug(e.from_))
                    if p_sync else _slug(e.from_),
                })
        downstream = [
            {"name": e.to, "slug": sync_slugs.get(e.to, _slug(e.to))}
            for e in manifest.edges
            if e.kind == "lookup" and e.from_ == s.name
        ]
        write(
            f"sync/{sync_slugs[s.name]}.html",
            env.get_template("sync").render(
                page_title=s.name,
                active="sync",
                root="../",
                current_slug=sync_slugs[s.name],
                sync=s,
                source_slug=source_slugs.get(s.source, _slug(s.source)),
                destination_slug=dest_slugs.get(s.destination, _slug(s.destination)),
                destination_label=dest.label if dest else s.destination,
                yaml_html=yaml_html,
                state=state,
                ego_svg=_ego_svg(s, manifest, sync_slugs, source_slugs, dest_slugs),
                upstream=upstream,
                downstream=downstream,
                data_json=dumps({"sync": s.name, "nav": nav}),
                **common,
            ),
        )

    # per-source pages
    for src in manifest.sources:
        used_by = [
            {
                "name": s.name,
                "slug": sync_slugs[s.name],
                "destination_label": dest_by_id[s.destination].label
                if s.destination in dest_by_id
                else s.destination,
                "mode": s.mode,
                **_status_fields(s),
            }
            for s in manifest.syncs
            if s.source == src.name
        ]
        write(
            f"source/{source_slugs[src.name]}.html",
            env.get_template("source").render(
                page_title=src.name,
                active="source",
                root="../",
                current_slug=source_slugs[src.name],
                source=src,
                badge=_badge_dict(src.type),
                syncs=used_by,
                data_json=dumps({"source": src.name, "nav": nav}),
                **common,
            ),
        )

    # per-destination pages
    for d in manifest.destinations:
        fed_by = [
            {
                "name": s.name,
                "slug": sync_slugs[s.name],
                "source": s.source,
                "mode": s.mode,
                **_status_fields(s),
            }
            for s in manifest.syncs
            if s.destination == d.name
        ]
        write(
            f"destination/{dest_slugs[d.name]}.html",
            env.get_template("destination").render(
                page_title=d.label,
                active="destination",
                root="../",
                current_slug=dest_slugs[d.name],
                destination=d,
                badge=_badge_dict(d.type),
                syncs=fed_by,
                data_json=dumps({"destination": d.name, "nav": nav}),
                **common,
            ),
        )

    # per-tag pages
    for t in sorted(tag_syncs):
        tagged = [
            {
                "name": s.name,
                "slug": sync_slugs[s.name],
                "destination_label": dest_by_id[s.destination].label
                if s.destination in dest_by_id
                else s.destination,
                "mode": s.mode,
                **_status_fields(s),
            }
            for s in tag_syncs[t]
        ]
        write(
            f"tag/{tag_slugs[t]}.html",
            env.get_template("tag").render(
                page_title=f"#{t}",
                active="tag",
                root="../",
                current_slug=tag_slugs[t],
                tag=t,
                syncs=tagged,
                data_json=dumps({"tag": t, "nav": nav}),
                **common,
            ),
        )

    return written
