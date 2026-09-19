"""Free-text redaction for error strings and other free-form values that may
embed secrets/PII (#698, extracted from ``drt/docs/builder.py`` in #778 so
``drt run``'s ``run_results.json`` artifact could reuse it as a second,
best-effort, defense-in-depth pass on argv -- see ``run.py``'s
``_redact_argv``).

Connector exceptions routinely embed URLs/DSNs ("connection to
postgres://user@db.internal:5432 failed"), hosts, e-mail addresses, phone
numbers, and key=value credential fragments. Free text has no key structure
to anchor on, so this is a pattern sweep — deliberately over-eager ("user: 42"
masks the 42), because for an artifact that leaves the process (a hosted docs
site) over-redaction is a cosmetic bug and under-redaction is a leak.

This is a heuristic keyword sweep, not a guarantee: a #778 review round found
it has no fixed point (unquoted multi-word values, quoted dict-repr keys,
comma-embedded values, compound identifiers like ``client_secret`` where
``secret`` doesn't start on a word boundary each defeated an attempted fix in
turn). ``run.py`` does not rely on this to scrub known-risky free text
(a raw exception's ``str(e)``, a diff-preview failure's raw message) out of
``run_results.json`` -- those fields are dropped from the artifact outright
(``_sanitize_entry_for_artifact``) rather than redacted. This module stays a
narrower, best-effort second layer for the docs manifest's hosted-artifact
redaction (its original job) and for incidental argv content outside the one
CLI option (``--vars``) known to carry arbitrary values, which is redacted
by whole-value replacement instead of by pattern.
"""

from __future__ import annotations

import re

_URL_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b")
_PHONE_RE = re.compile(r"\+\d[\d\s().-]{7,}\d")
_KV_RE = re.compile(
    r"(?i)\b(password|passwd|passphrase|secret|token|api[_-]?key|access[_-]?key|"
    r"authorization|host(?:name)?|dsn|user(?:name)?|account|endpoint)\b"
    r"(\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|\S+)"
)
REDACTED = "« redacted »"


def redact_error_text(text: str) -> str:
    """Mask URLs, e-mails, phone numbers, and credential-ish ``key=value``
    fragments in free-form text. URLs go first so a ``dsn=scheme://…`` loses
    the whole locator, not just the part after the key."""
    text = _URL_RE.sub(REDACTED, text)
    text = _EMAIL_RE.sub(REDACTED, text)
    text = _PHONE_RE.sub(REDACTED, text)
    text = _KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", text)
    return text
