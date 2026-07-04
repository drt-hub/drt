"""Vendored static assets for the HTML docs site (P3 of #499).

Hand-written CSS + JS written verbatim into ``<output>/assets/`` by
``drt.docs.html.render_html``. No Tailwind/JS framework at runtime — the
CSS carries the design tokens from the ADR mockup
(``docs/design/drt-docs-prototype.html``); the brand violet is ``#7c3aed``.
Theme is light/dark via ``prefers-color-scheme`` (no JS toggle).
"""

from __future__ import annotations

STYLE_CSS = """\
/* drt docs — vendored, no runtime framework. Tokens from the ADR mockup. */
:root {
  --brand-50:#f5f3ff; --brand-100:#ede9fe; --brand-200:#ddd6fe;
  --brand-500:#8b5cf6; --brand-600:#7c3aed; --brand-700:#6d28d9; --brand-900:#1e1b4b;
  --ink-50:#f7f8fa; --ink-100:#eef0f3; --ink-200:#e3e6eb; --ink-500:#5a6068;
  --ink-700:#262b33; --ink-800:#1a1d22; --ink-900:#0f1115;
  --bg:#ffffff; --fg:var(--ink-800); --muted:var(--ink-500);
  --line:var(--ink-200); --surface:#ffffff; --chip:var(--ink-100);
  --radius:8px;
  --mono:ui-monospace,SFMono-Regular,Menlo,Monaco,monospace;
  --sans:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg:var(--ink-900); --fg:var(--ink-50); --muted:#c4c7cc;
    --line:var(--ink-700); --surface:var(--ink-800); --chip:var(--ink-700);
  }
}
* { box-sizing:border-box; }
html,body { margin:0; padding:0; }
body { background:var(--bg); color:var(--fg); font-family:var(--sans); font-size:14px; line-height:1.55; }
a { color:var(--brand-700); text-decoration:none; }
a:hover { text-decoration:underline; }
code,pre,.mono { font-family:var(--mono); }

/* top bar */
.topbar {
  display:flex; align-items:center; gap:16px; padding:10px 16px;
  border-bottom:1px solid var(--line); position:sticky; top:0; background:var(--bg); z-index:10;
}
.brand { display:flex; align-items:center; gap:10px; font-weight:700; }
.brand .logo { width:26px; height:26px; border-radius:6px; background:var(--brand-600); display:inline-grid; place-items:center; color:#fff; font-size:13px; }
.project { color:var(--muted); border-left:1px solid var(--line); padding-left:12px; font-size:13px; }
.project .ver { font-size:11px; }
.topnav { display:flex; gap:4px; margin-left:8px; }
.navbtn { padding:4px 10px; border-radius:6px; color:var(--muted); }
.navbtn:hover { background:var(--brand-50); color:var(--brand-700); text-decoration:none; }
.navbtn.active { background:var(--brand-600); color:#fff; }
@media (prefers-color-scheme: dark) {
  .navbtn:hover { background:var(--brand-900); color:var(--brand-200); }
}
.search { margin-left:auto; }
.search input {
  border:1px solid var(--line); background:var(--surface); color:var(--fg);
  border-radius:6px; padding:5px 10px; font-size:13px; width:240px;
}

/* layout */
.shell { display:flex; }
.sidebar {
  width:256px; flex-shrink:0; border-right:1px solid var(--line); padding:16px;
  height:calc(100vh - 53px); overflow-y:auto; position:sticky; top:53px;
}
.group { margin-bottom:14px; }
.group > summary, .group-title {
  font-weight:600; color:var(--fg); display:flex; justify-content:space-between;
  cursor:pointer; list-style:none; padding:2px 0;
}
.group > summary::-webkit-details-marker { display:none; }
.group .count { color:var(--muted); font-weight:400; }
.group ul { list-style:none; margin:6px 0 0; padding:0 0 0 8px; }
.group li { padding:2px 0; }
.group a { color:var(--fg); display:block; padding:2px 4px; border-radius:4px; }
.group a:hover { background:var(--chip); text-decoration:none; }
.group a.current { color:var(--brand-700); font-weight:500; }

/* main */
.main { flex:1; padding:28px 32px; max-width:980px; }
.eyebrow { font-size:12px; color:var(--muted); margin-bottom:2px; }
h1 { font-size:28px; font-weight:700; margin:0 0 4px; letter-spacing:-0.02em; }
h2 { font-size:14px; font-weight:600; text-transform:uppercase; letter-spacing:0.04em; color:var(--muted); margin:28px 0 12px; }
.lede { color:var(--muted); margin:0 0 22px; }

.cards { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:24px; }
.card { border:1px solid var(--line); border-radius:var(--radius); padding:16px; }
.card .num { font-size:28px; font-weight:700; }
.card .lbl { font-size:13px; color:var(--muted); }

table { width:100%; border-collapse:collapse; font-size:13px; }
th,td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); vertical-align:top; }
th { color:var(--muted); font-weight:600; }
.chip { display:inline-block; padding:1px 8px; border-radius:999px; background:var(--chip); font-size:12px; }
.mode { font-family:var(--mono); font-size:12px; }
.kv { display:grid; grid-template-columns:160px 1fr; gap:6px 14px; font-size:13px; margin:0 0 8px; }
.kv dt { color:var(--muted); }
.kv dd { margin:0; }
.status-success { color:#0a7d33; } .status-failed { color:#b3261e; }
@media (prefers-color-scheme: dark) {
  .status-success { color:#4ade80; } .status-failed { color:#f87171; }
}

pre.code { background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); padding:14px; overflow-x:auto; font-size:12.5px; }
.mermaid { background:var(--surface); border:1px solid var(--line); border-radius:var(--radius); padding:16px; }

.footer { color:var(--muted); font-size:12px; margin-top:36px; padding-top:14px; border-top:1px solid var(--line); }

@media (max-width:860px) {
  .cards { grid-template-columns:1fr; }
  .sidebar { display:none; }
  .topnav { display:none; }
}
"""

APP_JS = """\
// drt docs — tiny runtime: sidebar <details> persistence + client-side search.
// No framework; reads the inlined #drt-data JSON. Safe on file://.
(function () {
  "use strict";
  var KEY = "drt-docs-open-groups";

  // Persist which sidebar groups are open across pages.
  function restoreGroups() {
    var saved;
    try { saved = JSON.parse(localStorage.getItem(KEY) || "{}"); } catch (e) { saved = {}; }
    document.querySelectorAll("details.group").forEach(function (d) {
      var id = d.getAttribute("data-group");
      if (id && saved.hasOwnProperty(id)) { d.open = !!saved[id]; }
      d.addEventListener("toggle", function () {
        try {
          var cur = JSON.parse(localStorage.getItem(KEY) || "{}");
          cur[id] = d.open;
          localStorage.setItem(KEY, JSON.stringify(cur));
        } catch (e) { /* ignore */ }
      });
    });
  }

  // Filter sidebar links by text.
  function wireSearch() {
    var input = document.getElementById("drt-search");
    if (!input) return;
    input.addEventListener("input", function () {
      var q = input.value.trim().toLowerCase();
      document.querySelectorAll(".sidebar li").forEach(function (li) {
        var a = li.querySelector("a");
        var hit = !q || (a && a.textContent.toLowerCase().indexOf(q) !== -1);
        li.style.display = hit ? "" : "none";
      });
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    restoreGroups();
    wireSearch();
  });
})();
"""
