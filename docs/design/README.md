# `docs/design/` — design artifacts

This directory holds **design references** for in-flight features. These files
are not part of the runtime shipping surface; they document intent so that
implementation work can converge against a shared visual / behavioral target.

## Files

- [`drt-docs-prototype.html`](./drt-docs-prototype.html) — interactive HTML
  prototype of `drt docs generate --format html` (Phase 3 of epic
  [#499](https://github.com/drt-hub/drt/issues/499)). Open in a browser to
  preview the five planned views (Overview / DAG / Sync detail / Source detail
  / Destination detail), the violet brand palette extracted from the drt logo,
  Mermaid DAG rendering, and `prefers-color-scheme` dark mode.

  Designed against ADR [#500](https://github.com/drt-hub/drt/issues/500). When
  P3 implementation lands (Jinja templates + CSS in `drt/docs/templates/` and
  `drt/docs/assets/`), this prototype is the visual reference.

## Conventions

- Files here are **frozen snapshots** — once a feature ships, the implementation
  becomes the source of truth and prototypes here are kept for historical
  context (linked from PR descriptions / ADRs), not maintained.
- New design artifacts should land alongside the ADR / epic issue that motivates
  them, with a one-line entry above.
