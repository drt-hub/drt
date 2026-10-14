"""Private, type-preserving snapshot spool for canary apply."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID


class SpoolError(ValueError):
    """The snapshot could not be written or safely replayed."""


def _encode(value: Any) -> dict[str, Any]:
    if value is None:
        return {"t": "null"}
    if isinstance(value, bool):
        return {"t": "bool", "v": value}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    if isinstance(value, float):
        return {"t": "float", "v": value.hex()}
    if isinstance(value, Decimal):
        parts = value.as_tuple()
        return {
            "t": "decimal",
            "s": parts.sign,
            "d": list(parts.digits),
            "e": parts.exponent,
        }
    if isinstance(value, datetime):
        return {"t": "datetime", "v": value.isoformat(), "f": value.fold}
    if isinstance(value, date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, time):
        return {"t": "time", "v": value.isoformat(), "f": value.fold}
    if isinstance(value, UUID):
        return {"t": "uuid", "v": value.hex}
    if isinstance(value, bytes):
        return {"t": "bytes", "v": value.hex()}
    if isinstance(value, bytearray):
        return {"t": "bytearray", "v": bytes(value).hex()}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    if isinstance(value, list):
        return {"t": "list", "v": [_encode(item) for item in value]}
    if isinstance(value, tuple):
        return {"t": "tuple", "v": [_encode(item) for item in value]}
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise SpoolError("Canary snapshot records and nested objects require string keys.")
        return {"t": "dict", "v": [[key, _encode(item)] for key, item in value.items()]}
    raise SpoolError(
        f"Canary snapshot cannot preserve values of type {type(value).__name__}; "
        "remove the rollout or cast the source value to a supported scalar/container type."
    )


def _decode(value: Any) -> Any:
    if not isinstance(value, dict) or not isinstance(value.get("t"), str):
        raise SpoolError("Canary snapshot contains an invalid type-tagged value.")
    tag = value["t"]
    if tag == "null":
        return None
    if tag in {"bool", "str"}:
        return value["v"]
    if tag == "int":
        return int(value["v"])
    if tag == "float":
        return float.fromhex(value["v"])
    if tag == "decimal":
        return Decimal((int(value["s"]), tuple(value["d"]), value["e"]))
    if tag == "datetime":
        return datetime.fromisoformat(value["v"]).replace(fold=int(value.get("f", 0)))
    if tag == "date":
        return date.fromisoformat(value["v"])
    if tag == "time":
        return time.fromisoformat(value["v"]).replace(fold=int(value.get("f", 0)))
    if tag == "uuid":
        return UUID(hex=value["v"])
    if tag == "bytes":
        return bytes.fromhex(value["v"])
    if tag == "bytearray":
        return bytearray.fromhex(value["v"])
    if tag in {"list", "tuple"}:
        items = [_decode(item) for item in value["v"]]
        return items if tag == "list" else tuple(items)
    if tag == "dict":
        return {key: _decode(item) for key, item in value["v"]}
    raise SpoolError(f"Canary snapshot contains unknown type tag {tag!r}.")


class SnapshotSpoolWriter:
    """A row-filter callback that snapshots every raw extracted record."""

    def __init__(
        self,
        path: Path,
        *,
        max_rows: int,
        max_bytes: int,
        bucket_of: Callable[[dict[str, Any]], int],
    ) -> None:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
        self._fd = os.open(path, flags, 0o600)
        os.fchmod(self._fd, 0o600)
        self._handle = os.fdopen(self._fd, "wb")
        self.path = path
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.bucket_of = bucket_of
        self.rows = 0
        self.bytes = 0
        self.buckets = [0] * 10_000
        self._digest = hashlib.sha256()
        self.closed = False

    def append(self, record: dict[str, Any]) -> bool:
        bucket = self.bucket_of(record)
        line = (
            json.dumps(_encode(record), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + b"\n"
        )
        next_rows = self.rows + 1
        next_bytes = self.bytes + len(line)
        if next_rows > self.max_rows:
            raise SpoolError(
                f"Canary snapshot exceeded rollout.max_spool_rows ({self.max_rows:,}); "
                "nothing was written. Raise the limit deliberately or remove the rollout."
            )
        if next_bytes > self.max_bytes:
            raise SpoolError(
                f"Canary snapshot exceeded rollout.max_spool_bytes ({self.max_bytes:,}); "
                "nothing was written. Raise the limit deliberately or remove the rollout."
            )
        self._handle.write(line)
        self._digest.update(line)
        self.rows = next_rows
        self.bytes = next_bytes
        self.buckets[bucket] += 1
        return True

    def close(self) -> tuple[int, int, str]:
        if not self.closed:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()
            self.closed = True
        return self.rows, self.bytes, self._digest.hexdigest()

    def abort(self) -> None:
        if not self.closed:
            self._handle.close()
            self.closed = True


def verify_spool(path: Path, expected_sha256: str) -> None:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise SpoolError("Canary snapshot is unavailable; refusing the next wave.") from exc
    if digest.hexdigest() != expected_sha256:
        raise SpoolError("Canary snapshot hash changed; refusing the next wave.")


class SnapshotSpoolSource:
    """Source adapter that replays only the immutable local snapshot."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def extract(
        self, query: str, config: Any, *, query_tags: dict[str, str] | None = None
    ) -> Iterator[dict[str, Any]]:
        del query, config, query_tags
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                decoded = _decode(json.loads(line))
                if not isinstance(decoded, dict):
                    raise SpoolError("Canary snapshot row is not an object.")
                yield decoded
