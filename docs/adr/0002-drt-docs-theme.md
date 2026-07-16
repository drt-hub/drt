# ADR 0002 — `drt docs` visual design: tokens, layout chrome, lineage language

- **Status:** Accepted (implemented; shipped across v0.7.11 and the #702 design train)
- **Issue:** [#500](https://github.com/drt-hub/drt/issues/500), under epic [#499](https://github.com/drt-hub/drt/issues/499); follow-ups tracked in [#702](https://github.com/drt-hub/drt/issues/702)
- **Implementation:** `drt/docs/_html_assets.py` (tokens + CSS), `drt/docs/dag.py` / `_svg.py` (lineage)
- **Design artifacts:** `docs/design/drt-docs-prototype.html` (chrome + palette), `docs/design/drt-docs-lineage-mock.html` (lineage language), screenshots alongside

## Context

`drt docs generate --format html` (#677) produces a static site that must work
**hosted or opened via `file://`**, with no CDN, no runtime framework, and no
build step. The audience is the same as dbt docs': engineers checking "what
syncs exist, what feeds what, what ran last". Design decisions were made
against ASCII mockups on #500, then two HTML prototypes, and refined through
the #702 train (#704 tokens/tabs/badges · #751 empty states · #752 code chrome
& mobile · #753 a11y/finishing · #796 static SVG DAG).

## Decision

### Layout chrome: dbt-docs-shaped

Top bar (brand · project · nav · search) + collapsible left sidebar
(syncs / sources / destinations / tags) + right detail panel. All five views
share this chrome; only the panel content changes. This is the layout data
engineers already know how to read — familiarity is a feature, not a lack of
imagination.

### Design tokens: one `:root` block, nowhere else

Every color, radius, and font stack is a CSS custom property declared in a
single `:root` block in `_html_assets.py`, with one `prefers-color-scheme:
dark` override block beside it. **New tokens are added there, never mid-file**
— the block is the palette's single source of truth, and the dark theme stays
complete because it overrides the same names it can see.

- **Brand:** violet scale around `--brand-600: #7c3aed` (50→900). Used for
  nav-active, links, the "managed by drt" zone, and lookup edges.
- **Neutrals:** `--ink-*` scale (50→900) feeding semantic surface/text tokens
  (`--bg`, `--fg`, `--muted`, `--line`, `--surface`, `--chip`).
- **Status:** `--success` / `--warning` / `--error`, always rendered as
  **dot + word pairs — never color alone** (color-blind safety is a hard rule,
  not a preference).
- **Type & shape:** system font stacks only (`ui-sans-serif`, `ui-monospace` —
  no webfonts, keeps `file://` and offline honest), 14px base, `--radius: 8px`.

### Theming: `prefers-color-scheme`, no JS toggle

Light/dark follows the OS. A manual toggle needs JS and persisted preference —
both against the site's no-runtime baseline. Revisit only if the interactivity
layer (#702) lands and users ask.

### Lineage visual language (settled on #677/#702)

The DAG page renders the language of `drt-docs-lineage-mock.html`:

- **Ownership zone bands**, left to right: `SOURCES — external · read` |
  `SYNCS — managed by drt` (brand-tinted band + pill) | `DESTINATIONS —
  external · write`. The zones state drt's contract at a glance: drt owns the
  middle column and only *touches* the outer two.
- **Forward edges** as bezier curves with arrowheads; **lookup back-edges** as
  dashed brand-colored "subway lanes" routed over the top — visually secondary,
  because they are derived hints, not data flow.
- **Node cards** (fixed 54px height) with connector **brand badges**, each
  linked to its detail page; the same card component serves the per-sync ego
  lineage on sync pages.
- **Detail levels:** L0 project DAG and L1 ego lineage shipped; L2
  column-level lineage is deferred until manifest v2 carries
  `field_mappings`/`mask` (ADR 0001, consequences).

### Static SVG, not a rendering library

The DAG is an inline SVG emitted from a hand-rolled deterministic layout engine
(#713/#796), replacing runtime Mermaid. Reasons, in order: **byte-identical
output** (same manifest, same bytes — #697's diffability contract extends to
the diagram), **`file://` safety** (no CDN script tag), and **themeability**
(the SVG references the same CSS tokens, so dark mode is free). The trade —
maintaining our own layout code — was accepted because docs-shaped DAGs are
narrow: three ranks, modest fan-out, no cycles.

### Accessibility & robustness floor (#753)

Skip link, `:focus-visible` rings, tab ARIA, print styles, and a search empty
state are part of the theme, not optional polish. Anything interactive added
later (#702's ego-view/hover layer) must degrade to plain links when JS is
unavailable.

## Consequences

- The site has **zero runtime dependencies**; the whole theme is vendored CSS
  plus inline SVG. Framework churn cannot break old docs.
- Visual regression is guarded by the byte-identical test rather than
  screenshot diffing; a Playwright screenshot lane remains a future option
  (#702) if the CSS surface grows.
- The single `:root` contract makes palette changes one-block diffs — and
  makes it obvious in review when someone tries to introduce a color anywhere
  else.
- Scale mode (compact density for large projects) and the interactivity layer
  remain open on #702 and must work within these tokens and the no-JS
  baseline.
