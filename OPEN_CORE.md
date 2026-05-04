# Open Core Model

drt follows an **open core model**. This document explains what's always free, what defines the enterprise boundary, and how we make these decisions.

## What's Always Free

Everything core to reverse ETL workflows is in open source:

- **All connectors** — BigQuery, Slack, REST APIs, Google Sheets, HubSpot, and more. No connector is behind a paywall.
- **Sync engine** — Core orchestration, batching, rate limits, retry logic, cursor management. Free to run anywhere.
- **CLI** — Full feature parity. `drt init`, `drt run`, `drt validate` work the same in OSS and enterprise.
- **MCP server** — LLM integration via Model Context Protocol. Enables AI-native workflows without restriction.

**If it ships in drt-core, it's free forever.**

## Enterprise Boundary

These features are designed for enterprise deployments but are **not shipped in drt-core**:

- **Role-Based Access Control (RBAC)** — permission boundaries, team isolation
- **Audit logging** — who ran what sync, when, with what results
- **Plugin system** — extend drt with custom connectors or transformations
- **Cloud push** — managed hosting, zero-ops deployment

We provide **interfaces** for these in the OSS repo (see below). Third parties can implement against them. drt itself ships a reference implementation in the enterprise product only.

## What "Interface in OSS" Means

Some features have interfaces (abstract classes, Protocol definitions, config schemas) in the OSS codebase, but **no implementation**. This allows:

- Third-party vendors to build compatible extensions
- Community members to fork and self-implement if needed
- Clear extensibility points without bloat

**Example:** The `PluginRegistry` interface exists in drt-core. You can build a plugin. The official plugin system (management UI, hosted plugin marketplace) is enterprise-only.

## How We Decide

The boundary between free and enterprise is guided by:

1. **Developer experience first** — if it's table-stakes for using drt, it's free
2. **Scale and ops** — features that matter mainly at scale (RBAC, audit logging) live in enterprise
3. **Community feedback** — we listen. If the community needs something, we reconsider
4. **Sustainability** — we must fund the project to keep both drt-core and enterprise moving

This model is inspired by Airbyte, Meltano, and other successful open core projects.

See [GOVERNANCE.md](./GOVERNANCE.md) for how decisions are made and who has input.

## FAQ

### Will X ever be free?

**It depends.** If the feature is core to reverse ETL workflows (like connectors or the CLI), it stays free. If it's a deployments/ops/governance feature (like RBAC or audit logging), it's likely enterprise. We're transparent about the boundary and listen to the community.

Open a [Discussion](https://github.com/drt-hub/drt/discussions) to propose moving a feature to OSS. We take it seriously.

### Can I self-implement RBAC or audit logging?

**Yes.** The interfaces are in drt-core. Fork the code, build your own implementation, and contribute back if you'd like. No vendor lock-in.

We also welcome community forks and distributions. If you want to ship drt with your own extensions, go for it.

### What about pricing or licensing terms?

Out of scope for this document. See the main [README](./README.md) or contact the team for enterprise questions.

### How does this compare to Airbyte/Meltano/other tools?

It's the same idea. Some things are free and always will be (core data movement). Some things are enterprise (deployment, governance, SaaS convenience). Both matter. Both are sustainable.

### Where can I ask more questions?

- **Technical questions:** [GitHub Discussions](https://github.com/drt-hub/drt/discussions)
- **Governance questions:** See [GOVERNANCE.md](./GOVERNANCE.md)
- **Enterprise/licensing:** Reach out to the team
