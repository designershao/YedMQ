# Security Policy

## Supported versions

Until YedMQ reaches 1.0, security fixes are targeted at:

- `main`
- The most recent tagged release, if one exists

Older commits, branches, and pre-release snapshots may not receive fixes.

## Reporting a vulnerability

Please do not open a public GitHub issue for an undisclosed security vulnerability.

Preferred process:

1. Use GitHub's private vulnerability reporting flow for this repository if the "Report a vulnerability" option is available.
2. If private reporting is not available yet, use the contact channels listed in the project README to request a private reporting path without posting exploit details publicly.

For non-sensitive security improvements, documentation corrections, or hardening suggestions, a normal public issue is fine.

## What to include in a report

Please include as much of the following as you can:

- Affected version, tag, or commit SHA
- The component involved
- The impact and expected attacker capabilities
- Step-by-step reproduction details
- A proof of concept, if safe to share privately
- Any suggested mitigation or fix

## Response expectations

The maintainers will try to:

- Acknowledge new reports within 7 days
- Confirm whether the issue is in scope and reproducible
- Keep the reporter informed as triage and remediation progress
- Credit the reporter after disclosure, if the reporter wants to be credited

Response times may vary depending on maintainer availability and the complexity of the issue.

## Disclosure policy

Please allow time for investigation and a fix before public disclosure.

After a fix is available, maintainers may publish a changelog entry, advisory, or other public disclosure describing the issue and the affected versions.

## Project-specific notes

- YedMQ is still under active development and is not yet recommended for production use.
- The example configuration is intentionally locked down by default. Review authentication, authorization, TLS, and management API settings before running outside a local development environment.
- Files under `broker/tests/certs` are test fixtures for automated tests only. Never reuse test certificates or private keys in real deployments.
