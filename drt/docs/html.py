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
from pathlib import Path

import yaml
from jinja2 import DictLoader, Environment, select_autoescape
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import YamlLexer

from drt.docs._html_assets import APP_JS, STYLE_CSS
from drt.docs.manifest import Manifest, Sync
from drt.docs.mermaid import render_mermaid

_SLUG_RE = re.compile(r"[^A-Za-z0-9]+")


def _slug(value: str) -> str:
    return _SLUG_RE.sub("-", value).strip("-").lower() or "x"


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
  </aside>
  <main class="main">
    {% block main %}{% endblock %}
    <div class="footer">Generated {{ generated_at }} · drt {{ drt_version }} · schema v{{ schema_version }}</div>
  </main>
</div>
<script type="application/json"
  id="drt-data">{{ data_json }}</script>
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
<p class="lede">Sync catalog generated from the drt project manifest.</p>
<div class="cards">
  <div class="card"><div class="num">{{ nav.syncs|length }}</div><div class="lbl">Syncs</div></div>
  <div class="card"><div class="num">{{ nav.sources|length }}</div><div class="lbl">Sources</div></div>
  <div class="card"><div class="num">{{ nav.destinations|length }}</div><div class="lbl">Destinations</div></div>
</div>
<h2>Syncs</h2>
<table>
  <tr><th>Name</th><th>Source</th><th>Destination</th><th>Mode</th></tr>
  {% for s in syncs %}
  <tr>
    <td><a href="sync/{{ s.slug }}.html">{{ s.name }}</a></td>
    <td>{{ s.source }}</td><td>{{ s.destination_label }}</td>
    <td><span class="mode">{{ s.mode }}</span></td>
  </tr>
  {% endfor %}
</table>
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
<div class="eyebrow">Sync</div>
<h1>{{ sync.name }}</h1>
{% if sync.description %}<p class="lede">{{ sync.description }}</p>{% endif %}
<dl class="kv">
  <dt>Source</dt><dd><a href="../source/{{ source_slug }}.html">{{ sync.source }}</a></dd>
  <dt>Destination</dt><dd><a href="../destination/{{ destination_slug }}.html">{{ destination_label }}</a></dd>
  <dt>Mode</dt><dd><span class="mode">{{ sync.mode }}</span></dd>
  {% if sync.tags %}<dt>Tags</dt><dd>{% for t in sync.tags %}<span class="chip">{{ t }}</span> {% endfor %}</dd>{% endif %}
</dl>
{% if state %}
<h2>Last run</h2>
<dl class="kv">
  <dt>Status</dt><dd class="status-{{ state.last_status }}">{{ state.last_status }}</dd>
  <dt>At</dt><dd>{{ state.last_sync_at }}</dd>
  <dt>Rows synced</dt><dd>{{ state.rows_synced }}</dd>
  {% if state.last_error %}<dt>Error</dt><dd>{{ state.last_error }}</dd>{% endif %}
</dl>
{% endif %}
<h2>Definition</h2>
{{ yaml_html }}
{% endblock %}
"""

_SOURCE = """\
{% extends "base" %}
{% block main %}
<div class="eyebrow">Source</div>
<h1>{{ source.name }}</h1>
<dl class="kv"><dt>Type</dt><dd>{{ source.type }}</dd></dl>
<h2>Used by</h2>
<table>
  <tr><th>Sync</th><th>Destination</th><th>Mode</th></tr>
  {% for s in syncs %}
  <tr><td><a href="../sync/{{ s.slug }}.html">{{ s.name }}</a></td><td>{{ s.destination_label }}</td><td><span class="mode">{{ s.mode }}</span></td></tr>
  {% endfor %}
</table>
{% endblock %}
"""

_DESTINATION = """\
{% extends "base" %}
{% block main %}
<div class="eyebrow">Destination</div>
<h1>{{ destination.label }}</h1>
<dl class="kv"><dt>Type</dt><dd>{{ destination.type }}</dd></dl>
<h2>Fed by</h2>
<table>
  <tr><th>Sync</th><th>Source</th><th>Mode</th></tr>
  {% for s in syncs %}
  <tr><td><a href="../sync/{{ s.slug }}.html">{{ s.name }}</a></td><td>{{ s.source }}</td><td><span class="mode">{{ s.mode }}</span></td></tr>
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
            }
        ),
        autoescape=select_autoescape(["html"]),
    )

    # Stable slugs for filenames + cross-links.
    sync_slugs = {s.name: _slug(s.name) for s in manifest.syncs}
    source_slugs = {s.name: _slug(s.name) for s in manifest.sources}
    dest_slugs = {d.name: _slug(d.name) for d in manifest.destinations}
    dest_by_id = {d.name: d for d in manifest.destinations}

    nav = {
        "syncs": [{"name": s.name, "slug": sync_slugs[s.name]} for s in manifest.syncs],
        "sources": [{"name": s.name, "slug": source_slugs[s.name]} for s in manifest.sources],
        "destinations": [
            {"label": d.label, "slug": dest_slugs[d.name]} for d in manifest.destinations
        ],
    }

    common = {
        "project_name": manifest.project.name if manifest.project else "drt project",
        "drt_version": manifest.drt_version,
        "generated_at": manifest.generated_at,
        "schema_version": manifest.schema_version,
        "nav": nav,
    }

    written: list[Path] = []

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
        return json.dumps(view, separators=(",", ":"))

    # index.html
    index_syncs = [
        {
            "name": s.name,
            "slug": sync_slugs[s.name],
            "source": s.source,
            "destination_label": dest_by_id[s.destination].label
            if s.destination in dest_by_id
            else s.destination,
            "mode": s.mode,
        }
        for s in manifest.syncs
    ]
    write(
        "index.html",
        env.get_template("index").render(
            page_title="Overview",
            active="overview",
            root="",
            current_slug="",
            syncs=index_syncs,
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
                "last_error": s.state.last_error,
            }
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
                syncs=used_by,
                data_json=dumps({"source": src.name, "nav": nav}),
                **common,
            ),
        )

    # per-destination pages
    for d in manifest.destinations:
        fed_by = [
            {"name": s.name, "slug": sync_slugs[s.name], "source": s.source, "mode": s.mode}
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
                syncs=fed_by,
                data_json=dumps({"destination": d.name, "nav": nav}),
                **common,
            ),
        )

    return written
