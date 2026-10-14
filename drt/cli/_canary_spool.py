"""Private, type-preserving snapshot spool for canary apply."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import struct
from collections.abc import Callable, Iterator
from datetime import date, datetime, time, timedelta, timezone
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
        return {"t": "float", "v": struct.pack(">d", value).hex()}
    if isinstance(value, Decimal):
        parts = value.as_tuple()
        return {
            "t": "decimal",
            "s": parts.sign,
            "d": list(parts.digits),
            "e": parts.exponent,
        }
    if isinstance(value, datetime):
        offset = value.utcoffset()
        if offset is None:
            return {"t": "datetime", "v": value.isoformat(), "o": None, "f": value.fold}
        return {
            "t": "datetime",
            "v": value.astimezone(timezone.utc).isoformat(),
            "o": int(offset.total_seconds()),
            "f": value.fold,
        }
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
        raw = bytes.fromhex(value["v"])
        if len(raw) != 8:
            raise SpoolError("Canary snapshot contains an invalid IEEE-754 float.")
        return struct.unpack(">d", raw)[0]
    if tag == "decimal":
        return Decimal((int(value["s"]), tuple(value["d"]), value["e"]))
    if tag == "datetime":
        parsed = datetime.fromisoformat(value["v"])
        offset = value.get("o")
        if offset is None:
            return parsed.replace(fold=int(value.get("f", 0)))
        fixed = timezone(timedelta(seconds=int(offset)))
        return parsed.astimezone(fixed).replace(fold=int(value.get("f", 0)))
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
    """A record-sink callback that snapshots every transformed record."""

    def __init__(
        self,
        path: Path,
        *,
        max_rows: int,
        max_bytes: int,
        bucket_of: Callable[[dict[str, Any]], int],
    ) -> None:
        flags = (
            os.O_CREAT
            | os.O_EXCL
            | os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        self._fd = os.open(path, flags, 0o600)
        try:
            os.fchmod(self._fd, 0o600)
            _validate_spool_fd(self._fd)
        except BaseException:
            os.close(self._fd)
            self._fd = -1
            path.unlink(missing_ok=True)
            raise
        self._handle = os.fdopen(os.dup(self._fd), "wb")
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
            replay_fd = open_spool(self.path)
            created = os.fstat(self._fd)
            reopened = os.fstat(replay_fd)
            if (created.st_dev, created.st_ino) != (reopened.st_dev, reopened.st_ino):
                os.close(replay_fd)
                raise SpoolError("Canary snapshot path changed while it was being sealed.")
            os.close(self._fd)
            self._fd = replay_fd
        return self.rows, self.bytes, self._digest.hexdigest()

    def abort(self) -> None:
        if not self.closed:
            self._handle.close()
            self.closed = True

    @property
    def fd(self) -> int:
        return self._fd

    def dispose(self) -> None:
        self.abort()
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def unlink_owned(self) -> bool:
        """Unlink only when the pathname still names this open file."""
        if self._fd < 0:
            return False
        try:
            opened = os.fstat(self._fd)
            named = self.path.lstat()
        except FileNotFoundError:
            return True
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            return False
        self.path.unlink()
        return True


def _validate_spool_fd(fd: int) -> None:
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode):
        raise SpoolError("Canary snapshot must be a regular file.")
    if info.st_uid != os.getuid():
        raise SpoolError("Canary snapshot must be owned by the current user.")
    if info.st_mode & 0o077:
        raise SpoolError("Canary snapshot permissions must not allow group or other access.")
    if info.st_nlink != 1:
        raise SpoolError("Canary snapshot must have exactly one hard link.")


def open_spool(path: Path) -> int:
    """Open and validate an existing spool without following a symlink."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise SpoolError("Canary snapshot is unavailable or unsafe.") from exc
    try:
        _validate_spool_fd(fd)
    except BaseException:
        os.close(fd)
        raise
    return fd


def verify_spool(fd: int, expected_sha256: str) -> None:
    digest = hashlib.sha256()
    try:
        _validate_spool_fd(fd)
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(fd), "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise SpoolError("Canary snapshot is unavailable; refusing the next wave.") from exc
    if digest.hexdigest() != expected_sha256:
        raise SpoolError("Canary snapshot hash changed; refusing the next wave.")


class SnapshotSpoolSource:
    """Source adapter that replays only the immutable local snapshot."""

    def __init__(self, fd: int) -> None:
        self.fd = fd

    def test_connection(self, config: Any) -> bool:
        del config
        return True

    def extract(
        self, query: str, config: Any, *, query_tags: dict[str, str] | None = None
    ) -> Iterator[dict[str, Any]]:
        del query, config, query_tags
        _validate_spool_fd(self.fd)
        os.lseek(self.fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(self.fd), "r", encoding="utf-8") as handle:
            for line in handle:
                decoded = _decode(json.loads(line))
                if not isinstance(decoded, dict):
                    raise SpoolError("Canary snapshot row is not an object.")
                yield decoded
