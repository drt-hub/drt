# Security Policy

## Supported Versions

### drt-core

| Version | Supported |
|---------|-----------|
| 0.7.x   | ✅        |
| 0.6.x   | ✅        |
| < 0.6   | ❌        |

### dagster-drt

| Version | Supported |
|---------|-----------|
| 0.3.x   | ✅        |
| 0.2.x   | ✅        |

## Supply-Chain Scanning

The repository runs the following automated scans:

| Scan          | Trigger                          | Workflow                                |
|---------------|----------------------------------|-----------------------------------------|
| Dependabot    | Weekly                           | (GitHub-native, no workflow file)       |
| CodeQL        | PR + push to `main` + weekly     | `.github/workflows/codeql.yml`          |
| `pip-audit`   | PR + push to `main` + weekly     | `.github/workflows/ci.yml` (`test` job) |
| CycloneDX SBOM| On release tag (`v*` / `dagster-drt-v*`) | `.github/workflows/publish-*.yml` |

`pip-audit` runs against the OSV vulnerability database and fails CI on any
known advisory. False positives can be triaged via `--ignore-vuln <GHSA-id>`
flags in `.github/workflows/ci.yml`; any entry added there must be documented
in this file with a link to the upstream advisory and a justification.

**Current allow-list entries:** none.

A CycloneDX-format SBOM (`*-sbom.cdx.json`) is generated and attached to each
GitHub Release as an asset. Downstream consumers can use it to feed their own
vulnerability tooling (Grype, Trivy, Dependency-Track, etc.).

## Branch Protection

The `main` branch enforces the following rules to mitigate supply chain attacks (e.g., [GlassWorm/ForceMemo](https://socket.dev/blog/glassworm-forcememo-github-supply-chain-attack)):

- **Signed commits required** — prevents commit author spoofing via force-push
- **Force-push and branch deletion disabled**
- **PR reviews required** — at least 1 approval; stale reviews are dismissed on new pushes
- **Status checks required** — CI must pass before merge

> **Note:** `enforce_admins` is currently disabled to keep the solo-maintainer workflow practical. This will be re-enabled when the project has multiple maintainers.

## Reporting a Vulnerability

Please **do not** open a public GitHub Issue for security vulnerabilities.

Report vulnerabilities by emailing **masukai9612kf@gmail.com**.

We will:
- Acknowledge receipt within **48 hours**
- Provide a fix or mitigation within **7 days** for critical issues
- Credit the reporter in the release notes (unless anonymity is requested)
