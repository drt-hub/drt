"""Run identifiers — first-class correlation IDs for one execution (#762).

Two distinct scopes share this one generator:

* **invocation `run_id`** — one per ``drt run`` / ``drt test`` process. The
  CLI generates it once (``drt/cli/commands/run.py``) and threads it through
  every sync in that invocation, so all of them can be correlated back to
  "the run that happened at 03:40 UTC".
* **``sync_run_id``** — one per *sync execution*, generated inside
  :func:`drt.engine.sync.run_sync` itself so every caller gets one, including
  library callers that never pass an invocation ``run_id`` at all.

Neither is the query-tagging ``run_id`` in :mod:`drt.config.query_tags` —
that one is scoped narrowly to "distinguish this query from the last one this
sync ran" for cost-attribution SQL comments, predates this module, and stays
separate on purpose (see its own docstring).

Plain ``uuid4`` rather than ``uuid7``: sortability isn't a real requirement
for any of the six correlation surfaces this threads through (history, DLQ,
alerts, OTel, ``--output json``, structured logs all key by ``sync_name`` +
timestamp already), and ``uuid.uuid7`` isn't in the stdlib on any Python drt
still supports (3.10-3.13) — pulling in a dependency to sort IDs nobody sorts
would be exactly the "heavy dependency in core" CLAUDE.md rules out.
"""

from __future__ import annotations

import uuid


def new_run_id() -> str:
    """A fresh globally-unique identifier for one run or one sync execution."""
    return str(uuid.uuid4())
