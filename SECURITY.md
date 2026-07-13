# Security Policy

## Project Status

KoldStore is under active development and is **not production-ready**.

Security, recovery, backup/restore, schema evolution, compaction, and failure handling are still being hardened. Do not use KoldStore for sensitive, regulated, financial, or business-critical data without independently reviewing the risks.

Known limitations are documented in [docs/limitations.md](docs/limitations.md).

## Supported Versions

Only the latest code on the `main` branch and the latest published release receive security fixes.

| Version        | Supported   |
| -------------- | ----------- |
| Latest release | Yes         |
| `main` branch  | Best effort |
| Older releases | No          |

Because the project is still in beta, fixes may require upgrading to a newer release and may include breaking changes.

## Reporting a Vulnerability

Please **do not open a public GitHub issue** for a suspected vulnerability.

Use GitHub's private vulnerability reporting:

1. Open the repository's **Security** tab.
2. Select **Report a vulnerability**.
3. Include enough information for us to reproduce and assess the issue.

If private vulnerability reporting is unavailable, contact the repository maintainer privately through GitHub before sharing technical details publicly.

Please include:

* A clear description of the vulnerability
* Affected KoldStore and PostgreSQL versions
* Steps or a minimal test case to reproduce it
* Expected and actual behavior
* Potential impact
* Any suggested mitigation
* Whether the issue is already public or known to others

Do not include real credentials, access keys, private data, or production database contents.

## What to Report

Examples of relevant security issues include:

* Unauthorized access to hot or cold rows
* Queries returning rows from the wrong table, tenant, or scope
* SQL injection or unsafe identifier handling
* Privilege escalation through extension functions
* Unsafe filesystem or object-storage path handling
* Exposure of storage credentials or secrets
* Corruption or deletion of data outside the managed table
* Incorrect enforcement of PostgreSQL permissions
* Memory-safety issues in PostgreSQL integration or custom scan code
* Crash-recovery behavior that exposes inconsistent or unintended data

General bugs, performance problems, and feature requests should be reported through normal GitHub issues unless they have a security impact.

## Response Expectations

KoldStore is currently maintained on a best-effort basis.

We will try to:

* Acknowledge a private report promptly
* Confirm whether the issue can be reproduced
* Share the expected next step when possible
* Credit the reporter in the fix or release notes, unless anonymity is requested

We cannot currently guarantee a specific response time, remediation deadline, bounty, or long-term support window.

## Disclosure

Please allow reasonable time for investigation and a fix before publishing vulnerability details.

Once a fix is available, the project may publish a GitHub Security Advisory describing the affected versions, impact, and upgrade instructions.
