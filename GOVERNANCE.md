[English](./GOVERNANCE.md) | [日本語](./GOVERNANCE.ja.md)

# Governance

drt is an open-source project. This document explains how decisions are made, who has what permissions, and how contributors grow into trusted roles.

## Roles

drt has three roles. Each is a step on a ladder — anyone can grow from one to the next through their contributions.

### Contributor

**Anyone with at least one merged PR.**

Permissions:
- Open issues, discussions, and pull requests
- Comment, review, and propose changes

Recognition:
- Listed in the project's contributor graph
- Acknowledged in release notes when their changes ship

### Triage Collaborator

**Trusted contributors who help manage the project's day-to-day flow.** Cannot push code directly — that protects both the contributor and the project.

Permissions:
- Triage issues (label, assign, close)
- Manage pull requests (request reviews, label, close)
- Help review code (approval is non-binding for merge)

Criteria for invitation (any path qualifies):
- **5+ merged PRs** with consistent quality, OR
- **One major contribution completed end-to-end** (e.g. a new connector or integration), AND
- Active in the **last 30 days**, AND
- Demonstrates **constructive communication** in issues and PRs

Process:
- A current Owner proposes the invitation publicly in a Discussion
- The candidate is invited with **GitHub Triage permission**
- Invitation is an **offer, not a decree** — the contributor can decline or step down anytime

### Owner

**Project maintainers with full administrative rights.**

Permissions:
- Merge pull requests
- Push to protected branches (rare — squash-merge is the norm)
- Manage repository settings, secrets, and releases
- Invite/remove collaborators

Current Owners:
- [@masukai](https://github.com/masukai) — project lead
- [@yodakanohoshi](https://github.com/yodakanohoshi) — co-maintainer

**Owner role is intentionally kept small** (currently 2). Adding an Owner requires unanimous agreement from all current Owners and is reserved for contributors who have demonstrated long-term commitment as a Triage Collaborator.

## How Decisions Are Made

drt follows a **lazy consensus** model:

- **Small changes** (bug fixes, small features, docs): merge after one Owner approval
- **Medium changes** (new connectors, new CLI commands): one Owner approval + 24h for objection window
- **Large changes** (Protocol changes, breaking API, governance changes): require explicit agreement from both Owners + a public Discussion thread

When in doubt, open a Discussion before opening a PR.

## Roadmap & Source of Truth

The roadmap is tracked in **[GitHub Milestones](https://github.com/drt-hub/drt/milestones)** — that is the single source of truth. README sections about roadmap link to the milestones rather than restating them, to avoid drift.

## Code of Conduct

We follow the [Contributor Covenant](https://www.contributor-covenant.org/). Be kind, be constructive. Owners are responsible for enforcement.

## Recognition

Beyond role progression, drt recognizes contributors in several ways:

- **Release notes** — every merged PR is credited
- **Monthly Contributor Spotlight** — see [#295](https://github.com/drt-hub/drt/issues/295)
- **all-contributors** — README contributor list (planned)
- **Conference / blog mentions** — when speaking about drt, contributors are credited

## Stepping Down

Roles are voluntary. If a Triage Collaborator or Owner needs to step back:

- Open a PR removing yourself from this document, or
- Send a note to the Owners

No explanation needed. Re-joining later is welcome.

## Changing this Document

Governance changes (criteria, role definitions, processes) require:

1. A Discussion thread proposing the change
2. Agreement from both Owners
3. A PR updating this document

This intentionally has more friction than code changes — governance stability matters.
