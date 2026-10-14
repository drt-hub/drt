"""Pure canary-rollout parsing, bucketing, health, and compatibility logic."""

from __future__ import annotations

import hashlib
import unicodedata
from collections.abc import Callable, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID


class CanaryWaves(list[int]):
    """Validated cumulative wave bounds in integer hundredths of a percent."""


def normalize_canary_waves(waves: Sequence[Decimal | float | int]) -> CanaryWaves:
    if not waves:
        raise ValueError("canary waves must contain at least one percentage")
    if any(isinstance(wave, bool) or not isinstance(wave, Decimal | int | float) for wave in waves):
        raise ValueError("canary waves must contain only numbers (not booleans or strings)")
    decimals = [Decimal(str(wave)) for wave in waves]
    if any(not wave.is_finite() or not 0 < wave <= 100 for wave in decimals):
        raise ValueError("canary waves must each be greater than 0 and at most 100")
    scaled = [wave * 100 for wave in decimals]
    if any(wave != wave.to_integral_value() for wave in scaled):
        raise ValueError("canary waves must be multiples of 0.01 (the bucket resolution)")
    normalized = [int(wave) for wave in scaled]
    if any(left >= right for left, right in zip(normalized, normalized[1:], strict=False)):
        raise ValueError("canary waves must be strictly increasing without duplicates")
    if normalized[-1] != 10_000:
        normalized.append(10_000)
    return CanaryWaves(normalized)


def parse_canary_waves(text: str) -> CanaryWaves:
    parts = [part.strip() for part in text.split(",")]
    if not parts or any(not part for part in parts):
        raise ValueError("canary waves must be a comma-separated list of numbers")
    try:
        values = [Decimal(part) for part in parts]
    except Exception as exc:
        raise ValueError("canary waves must contain only numbers") from exc
    return normalize_canary_waves(values)


def _decimal_key_text(number: Decimal) -> str:
    if number.is_zero():
        return "0"
    parts = number.as_tuple()
    digits = list(parts.digits)
    exponent = int(parts.exponent)
    while digits and digits[-1] == 0:
        digits.pop()
        exponent += 1
    coefficient = "".join(str(digit) for digit in digits) or "0"
    return f"{'-' if parts.sign else ''}{coefficient}e{exponent}"


def canonical_key_value(value: Any) -> bytes:
    if value is None:
        raise ValueError("Canary key components cannot be null.")
    if isinstance(value, bool | int | float | Decimal):
        number = Decimal(int(value)) if isinstance(value, bool) else Decimal(str(value))
        if not number.is_finite():
            raise ValueError(f"Unsupported non-finite canary key number: {value!r}")
        return f"number:{_decimal_key_text(number)}".encode()
    if isinstance(value, str):
        return b"string:" + unicodedata.normalize("NFC", value).encode("utf-8")
    if isinstance(value, datetime):
        aware = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        return b"datetime:" + aware.astimezone(timezone.utc).isoformat().encode()
    if isinstance(value, date):
        return b"date:" + value.isoformat().encode()
    if isinstance(value, bytes):
        return b"bytes:" + value.hex().encode()
    if isinstance(value, UUID):
        return b"uuid:" + value.hex.encode()
    raise ValueError(
        "Unsupported canary key type "
        f"{type(value).__name__}; use bool/number, string, date/datetime, UUID, or bytes."
    )


def canary_bucket(record: dict[str, Any], upsert_key: list[str] | None = None) -> int:
    if not upsert_key:
        raise ValueError("Canary apply requires destination.upsert_key for stable wave keys.")
    encoded: list[bytes] = []
    for column in upsert_key:
        if column not in record:
            raise ValueError(
                f"Canary key column {column!r} is missing after transforms; missing is not null."
            )
        token = canonical_key_value(record[column])
        encoded.append(len(token).to_bytes(8, "big") + token)
    return int.from_bytes(hashlib.sha256(b"".join(encoded)).digest(), "big") % 10_000


def canary_filter(
    lower_hundredths: int,
    upper_hundredths: int,
    upsert_key: list[str] | None,
) -> Callable[[dict[str, Any]], bool]:
    """Select an exclusive bucket range from already-transformed records."""

    def selected(record: dict[str, Any]) -> bool:
        return lower_hundredths <= canary_bucket(record, upsert_key) < upper_hundredths

    return selected


def canary_health(
    entry: dict[str, Any], *, had_error: bool, on_error: str, max_error_pct: float
) -> tuple[int, int, int, float, bool]:
    row_error_value = entry.pop("_row_error_count", 0)
    row_errors = int(row_error_value) if isinstance(row_error_value, int | float) else 0
    ok_value = entry.get("rows_synced", 0)
    failed_value = entry.get("rows_failed", 0)
    rows_value = entry.get("rows_extracted", 0)
    ok = int(ok_value) if isinstance(ok_value, int | float) else 0
    destination_failed = int(failed_value) if isinstance(failed_value, int | float) else 0
    rows = int(rows_value) if isinstance(rows_value, int | float) else 0
    error_rows = max(destination_failed, row_errors)
    rate = 0.0 if rows == 0 else error_rows * 100.0 / rows
    fatal = on_error == "fail" and (had_error or destination_failed > 0)
    return rows, ok, error_rows, rate, not fatal and rate <= max_error_pct


def compatibility_problem(sync: Any, destination: Any) -> tuple[list[str] | None, str | None]:
    """Return the stable key or a pure compatibility refusal."""
    from drt.destinations.base import KeyedUpsertCapable, StagedDestination

    if isinstance(destination, StagedDestination):
        return None, "Canary apply supports only non-staged per-row destinations."
    upsert_key = getattr(sync.destination, "upsert_key", None)
    if sync.sync.mode not in {"full", "upsert"}:
        return None, "Canary apply supports only non-incremental sync.mode: full or upsert."
    if sync.sync.incremental_strategy == "diff":
        return None, "Canary apply does not support incremental_strategy: diff."
    if not upsert_key:
        return None, "Canary apply requires destination.upsert_key for stable wave keys."
    destination_name = getattr(sync.destination, "type", type(destination).__name__)
    if not isinstance(destination, KeyedUpsertCapable) or not destination.supports_keyed_upsert(
        sync.destination
    ):
        return None, (
            f"Canary apply requires a keyed-upsert destination; {destination_name!r} does not "
            "provably overwrite the same upsert_key row."
        )
    metadata_targets = (
        set(sync.sync.metadata_columns.model_dump(exclude_none=True).values())
        if sync.sync.metadata_columns is not None
        else set()
    )
    metadata_keys = set(upsert_key) & metadata_targets
    if metadata_keys:
        return None, (
            "Canary apply cannot use an upsert_key produced by sync.metadata_columns: "
            f"{sorted(metadata_keys)}."
        )
    if sync.sync.match_policy != "upsert":
        return None, "Canary apply requires sync.match_policy: upsert."
    masked_keys = []
    for key in upsert_key:
        rule = (sync.sync.mask or {}).get(key)
        strategy = rule if isinstance(rule, str) else getattr(rule, "strategy", None)
        if strategy not in {None, "hash"}:
            masked_keys.append(key)
    if masked_keys:
        return None, (
            "Canary apply requires injective key handling; upsert_key columns cannot use "
            f"redact/truncate masking: {sorted(masked_keys)}."
        )
    lookup_outputs = {
        (sync.sync.field_mappings or {}).get(name, name)
        for name in (getattr(sync.destination, "lookups", None) or {})
    }
    unstable_keys = set(upsert_key) & lookup_outputs
    if unstable_keys:
        return None, (
            "Canary apply cannot use an upsert_key produced by a destination lookup: "
            f"{sorted(unstable_keys)}."
        )
    return list(upsert_key), None
