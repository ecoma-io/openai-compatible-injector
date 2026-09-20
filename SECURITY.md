# Security Policy

## Reporting a vulnerability

**Do not open a public issue.** A public report of a credential leak is
itself a disclosure of the credential.

- Preferred: [a private security advisory](https://github.com/ecoma-io/openai-compatible-injector/security/advisories/new)
  (the repository's _Security_ tab → _Report a vulnerability_).
- Or email **john.itvn@gmail.com** with: a description of the issue, a
  reproduction or proof of concept, and your assessment of the impact.

Please never paste real credentials or `Authorization` values into any
report — a PoC that needs them should hold placeholders.

## What counts as a vulnerability here

openai-compatible-injector forwards requests between clients and upstream
model providers: every request passes through it with the client's
`Authorization` header attached, and every model mapping carries an upstream
endpoint the operator chose. Two defect classes therefore count as security
vulnerabilities even when the underlying mechanism is an ordinary bug:

- **An `Authorization` header, upstream endpoint, or injection prompt
  reaching logs, error text, or a forwarded response.** The documented
  contract forbids each of these (see README "Safety and credentials"); a
  redaction that misses a format fails in the quiet direction — the report
  is a leak, not a typo.
- **A path that lets a request reach an upstream the operator did not map.** A
  request for an unmapped model name must fail locally with 404
  `model_not_found`; it must never be forwarded. A config reload that
  silently rebinds a model to a different endpoint without a validated file
  is likewise in scope.

Supply-chain defects in the CI itself (an unpinned action, an unpinned
container image, a workflow interpolating attacker-reachable input into a
shell command) are also in scope; `.github/semgrep/` pins the classes this
repository treats as vulnerabilities in its own automation.

Everything else — a miscounted header, a wrong status code, a stream chunk
that arrives a second late — is an ordinary bug, and the
[public tracker](https://github.com/ecoma-io/openai-compatible-injector/issues)
is the right place for it.

## Supported versions

This project is pre-1.0. Security fixes are applied to the `main` branch
and released in the next version; there is no long-term support branch and
no backport policy for older tags.

## Service levels

Stated honestly for a single-maintainer project:

- **Acknowledgement** within 48 hours of a report.
- **Fix published** within 14 days of a confirmed report — "published"
  meaning a tagged release, not an unmerged commit.
- The **disclosure date** is agreed with the reporter; credit in the
  release notes unless anonymity is preferred.
