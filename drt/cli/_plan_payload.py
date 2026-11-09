"""In-memory age payloads for exact plan replay (#1221, ADR 0014)."""

from __future__ import annotations

import hashlib
import json
import os
import re
from base64 import b64decode, b64encode
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

PLAN_RECIPIENTS_ENV_VAR = "DRT_PLAN_RECIPIENTS"
DEFAULT_MAX_PAYLOAD_ROWS = 10_000
# Fixed alongside the row limit: large enough for ordinary review batches, small
# enough to keep a CI artifact and its in-memory decrypt bounded.
MAX_PAYLOAD_BYTES = 10 * 1024 * 1024
PAYLOAD_SCHEMA_VERSION = 1


class PlanPayloadError(ValueError):
    """An exact-replay payload cannot be created or trusted."""


def payload_path_for(plan_path: Path) -> Path:
    return Path(f"{plan_path}.payload")


def resolve_recipients(cli_recipients: list[str] | None) -> list[str]:
    """CLI recipients win; otherwise read comma/whitespace-separated env values."""
    if cli_recipients:
        return [value.strip() for value in cli_recipients if value.strip()]
    raw = os.environ.get(PLAN_RECIPIENTS_ENV_VAR, "")
    return [value for value in re.split(r"[\s,]+", raw.strip()) if value]


_TYPE_KEY = "__drt_exact_replay_type__"


def _encode_value(value: Any) -> Any:
    """JSON-safe value that preserves common warehouse-driver scalar types."""
    if isinstance(value, dict):
        if _TYPE_KEY in value:
            return {
                _TYPE_KEY: "dict",
                "value": [[key, _encode_value(item)] for key, item in value.items()],
            }
        return {key: _encode_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_encode_value(item) for item in value]
    if isinstance(value, tuple):
        return {_TYPE_KEY: "tuple", "value": [_encode_value(item) for item in value]}
    if isinstance(value, datetime):
        return {_TYPE_KEY: "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {_TYPE_KEY: "date", "value": value.isoformat()}
    if isinstance(value, time):
        return {_TYPE_KEY: "time", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {_TYPE_KEY: "decimal", "value": str(value)}
    if isinstance(value, UUID):
        return {_TYPE_KEY: "uuid", "value": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {_TYPE_KEY: "bytes", "value": b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise PlanPayloadError(f"exact replay cannot serialize a value of type {type(value).__name__}")


def _decode_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    kind = value.get(_TYPE_KEY)
    if kind is None:
        return {key: _decode_value(item) for key, item in value.items()}
    if set(value) != {_TYPE_KEY, "value"}:
        raise PlanPayloadError("The decrypted exact replay payload has a malformed typed value.")
    raw = value["value"]
    try:
        if kind == "dict" and isinstance(raw, list):
            return {key: _decode_value(item) for key, item in raw}
        if kind == "tuple" and isinstance(raw, list):
            return tuple(_decode_value(item) for item in raw)
        if kind == "datetime" and isinstance(raw, str):
            return datetime.fromisoformat(raw)
        if kind == "date" and isinstance(raw, str):
            return date.fromisoformat(raw)
        if kind == "time" and isinstance(raw, str):
            return time.fromisoformat(raw)
        if kind == "decimal" and isinstance(raw, str):
            return Decimal(raw)
        if kind == "uuid" and isinstance(raw, str):
            return UUID(raw)
        if kind == "bytes" and isinstance(raw, str):
            return b64decode(raw, validate=True)
    except (ValueError, TypeError) as exc:
        raise PlanPayloadError(
            "The decrypted exact replay payload has a malformed typed value."
        ) from exc
    raise PlanPayloadError("The decrypted exact replay payload has an unsupported typed value.")


def serialize_rows(rows: list[dict[str, Any]]) -> bytes:
    """Canonical, type-preserving JSON encoded only in memory."""
    return json.dumps(
        {"schema_version": PAYLOAD_SCHEMA_VERSION, "rows": _encode_value(rows)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def encrypt_rows(rows: list[dict[str, Any]], recipients: list[str]) -> tuple[bytes, dict[str, Any]]:
    """Encrypt rows to public age recipients and return ciphertext + sealed metadata."""
    if not recipients:
        raise PlanPayloadError(
            f"--include-values requires at least one --recipient or {PLAN_RECIPIENTS_ENV_VAR}."
        )
    plaintext = serialize_rows(rows)
    if len(plaintext) > MAX_PAYLOAD_BYTES:
        raise PlanPayloadError(
            f"exact replay payload is {len(plaintext)} bytes; the byte cap is "
            f"{MAX_PAYLOAD_BYTES} bytes"
        )
    try:
        import pyrage
    except ModuleNotFoundError as exc:
        raise ImportError(
            "Exact replay payloads require the optional encryption extra. "
            "Install it with: pip install 'drt-core[encryption]'"
        ) from exc
    parsed = []
    for value in recipients:
        try:
            parsed.append(pyrage.x25519.Recipient.from_str(value))
        except (pyrage.RecipientError, ValueError) as exc:
            if value.lstrip().startswith("AGE-SECRET-KEY-"):
                raise PlanPayloadError(
                    "Invalid age X25519 recipient: that is a private identity, "
                    "pass the public key (age1...)."
                ) from exc
            # Recipient input can be secret material accidentally pasted into
            # the public-key option. Never reflect any rejected value into
            # console output, logs, or CI artifacts.
            raise PlanPayloadError("Invalid age X25519 recipient.") from exc
    try:
        ciphertext: bytes = pyrage.encrypt(plaintext, parsed)
    except pyrage.EncryptError as exc:
        raise PlanPayloadError("Unable to encrypt the exact replay payload with age.") from exc
    metadata = {
        "sha256": "sha256:" + hashlib.sha256(ciphertext).hexdigest(),
        "rows": len(rows),
        "bytes": len(ciphertext),
    }
    return ciphertext, metadata


def decrypt_rows(ciphertext: bytes, expected: dict[str, Any]) -> list[dict[str, Any]]:
    """Authenticate sidecar bytes, decrypt with DRT_SECRETS_KEY, and validate rows."""
    actual_hash = "sha256:" + hashlib.sha256(ciphertext).hexdigest()
    if actual_hash != expected.get("sha256"):
        raise PlanPayloadError(
            "The exact replay sidecar sha256 does not match the sealed plan "
            "(the sidecar was edited or swapped)."
        )
    if len(ciphertext) != expected.get("bytes"):
        raise PlanPayloadError(
            "The exact replay sidecar byte count does not match the sealed plan."
        )

    from drt.config.encryption import decrypt_secrets

    try:
        plaintext = decrypt_secrets(ciphertext)
    except (ImportError, ValueError) as exc:
        raise PlanPayloadError(str(exc).replace("secrets.toml.enc", "the plan payload")) from exc
    try:
        document = json.loads(plaintext)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlanPayloadError("The decrypted exact replay payload is not valid JSON.") from exc
    if not isinstance(document, dict) or document.get("schema_version") != PAYLOAD_SCHEMA_VERSION:
        raise PlanPayloadError("The decrypted exact replay payload has an unsupported schema.")
    rows = _decode_value(document.get("rows"))
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise PlanPayloadError("The decrypted exact replay payload has malformed rows.")
    if len(rows) != expected.get("rows"):
        raise PlanPayloadError(
            "The decrypted exact replay row count does not match the sealed plan."
        )
    return rows
