# Security Policy

## Supported Versions

### drt-core

| Version | Supported |
|---------|-----------|
| 0.5.x   | ✅        |
| 0.4.x   | ✅        |
| < 0.4   | ❌        |

### dagster-drt

| Version | Supported |
|---------|-----------|
| 0.2.x   | ✅        |
| 0.1.x   | ✅        |

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
