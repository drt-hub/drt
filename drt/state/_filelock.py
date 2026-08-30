"""Instance-agnostic OS locks for local state read-modify-write cycles.

These advisory locks coordinate separate drt processes; each store's existing
``threading.Lock`` still coordinates threads sharing one store instance. Both
are needed. As with advisory ``flock`` generally, locking may be unreliable on
NFS or other network filesystems whose server or mount configuration does not
preserve the host OS's locking semantics.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl


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
            lock_file.seek(0)
            getattr(msvcrt, "locking")(lock_file.fileno(), getattr(msvcrt, "LK_LOCK"), 1)
        else:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

        try:
            yield
        finally:
            if os.name == "nt":
                lock_file.seek(0)
                getattr(msvcrt, "locking")(lock_file.fileno(), getattr(msvcrt, "LK_UNLCK"), 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
