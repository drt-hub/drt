"""Free-text redaction for error strings and other free-form values that may
embed secrets/PII (originally #698, extracted from ``drt/docs/builder.py`` in
#778 for a second consumer — ``drt run``'s ``run_results.json`` artifact).

Connector exceptions routinely embed URLs/DSNs ("connection to
postgres://user@db.internal:5432 failed"), hosts, e-mail addresses, phone
numbers, and key=value credential fragments. Free text has no key structure
to anchor on, so this is a pattern sweep — deliberately over-eager ("user: 42"
masks the 42), because for an artifact that leaves the process (a hosted docs
site, a CI-uploaded run artifact) over-redaction is a cosmetic bug and
under-redaction is a leak.
"""

from __future__ import annotations

import re

_URL_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b")
_PHONE_RE = re.compile(r"\+\d[\d\s().-]{7,}\d")
_KV_RE = re.compile(
    r"(?i)\b(password|passwd|passphrase|secret|token|api[_-]?key|access[_-]?key|"
    r"authorization|host(?:name)?|dsn|user(?:name)?|account|endpoint)\b"
    # An optional quote right after the keyword matches a Python/JSON dict
    # repr's own quoted key (`{'Authorization': 'Bearer ...'}`) -- without
    # it the quote sits between the keyword and the "=:" separator and the
    # whole match fails, leaving the credential untouched (#778 review).
    r"(['\"]?\s*[=:]\s*)"
    # Unquoted values run across a few spaces (not just to the first one) so
    # an "Authorization: Bearer <token>"-shaped value redacts as one unit --
    # a single \S+ used to stop at "Bearer" and leave the actual credential
    # sitting right after it. Not bounded by punctuation like ,/;/) --
    # a real secret can itself contain one ("password=abc,def" is one value,
    # not "abc" plus unrelated trailing text), and under-redaction is a
    # leak while over-redacting an adjacent field is only cosmetic (#778
    # review). Bounded to 5 extra space/tab-separated words, and never
    # crosses a newline, so it can't run on into an unrelated later
    # paragraph of a long multi-line exception message.
    r"(\"[^\"]*\"|'[^']*'|\S+(?:[ \t]+\S+){0,5})"
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
