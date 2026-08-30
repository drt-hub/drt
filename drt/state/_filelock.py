"""Instance-agnostic OS locks for local state read-modify-write cycles.

These advisory locks coordinate separate drt processes; each store's existing
``threading.Lock`` still coordinates threads sharing one store instance. Both
are needed. As with advisory ``flock`` generally, locking may be unreliable on
NFS or other network filesystems whose server or mount configuration does not
preserve the host OS's locking semantics.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

if os.name == "nt":
    import msvcrt
else:
    import fcntl

# The Windows CRT's ``_locking`` (which ``msvcrt.locking`` wraps) does not
# block indefinitely like POSIX ``flock`` — ``LK_LOCK`` retries internally for
# about 10 seconds and then raises ``OSError`` if the region is still locked.
# A held lock outliving that (a large DLQ/history rewrite, a slow disk, a
# suspended holder) would otherwise surface as a dropped write or a crash
# instead of the blocking wait callers rely on, so acquisition below retries
# across that ceiling itself rather than trusting a single ``LK_LOCK`` call.
_WINDOWS_RETRY_DELAY_SECONDS = 0.05


def _acquire_windows(lock_file: Any) -> None:
    while True:
        lock_file.seek(0)
        try:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
            return
        except OSError:
            time.sleep(_WINDOWS_RETRY_DELAY_SECONDS)


def _release_windows(lock_file: Any) -> None:
    lock_file.seek(0)
    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]


@contextmanager
def advisory_lock(path: Path) -> Iterator[None]:
    """Hold a blocking exclusive lock on the sidecar ``<path>.lock`` file."""
    lock_path = Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    # a+b creates the sidecar before locking and permits the one-byte Windows
    # locking convention. The sidecar deliberately remains after release.
    with lock_path.open("a+b") as lock_file:
        if os.name == "nt":
            lock_file.seek(0)
            if not lock_file.read(1):
                lock_file.write(b"\0")
                lock_file.flush()
            _acquire_windows(lock_file)
        else:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

        try:
            yield
        finally:
            if os.name == "nt":
                _release_windows(lock_file)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
