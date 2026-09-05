"""Shared constants for the warehouse-managed table primitive (#960, ADR 0005 step 3).

Dialect-agnostic pieces for ``ManagedTableCapable`` implementations
(``drt/sources/base.py``). The actual DDL (CREATE SCHEMA / CREATE TABLE /
DROP TABLE) lives in each source, using its own driver and quoting helpers —
same split as tracked mirror's ``destinations/_mirror_state.py``.

Naming convention across dialects (documented once, here, so a future
Snowflake/Databricks/BigQuery implementation doesn't drift): each source
profile's own field for the managed-schema name is called ``managed_schema``
(``PostgresProfile.managed_schema``, ``drt/config/profiles.py``) — never
reused from an existing ``schema``-named field (Snowflake/Databricks already
have one, meaning their query-execution default schema, a different concept
this deliberately avoids colliding with).
"""

from __future__ import annotations

#: Default schema name for drt's own bookkeeping tables, when a profile
#: does not override it. Never ``public`` (or a dialect's equivalent
#: catch-all default) — see ``PostgresProfile.managed_schema``'s docstring
#: for the competitive research (RudderStack/Segment/Hightouch all use a
#: dedicated schema) behind this choice.
DEFAULT_MANAGED_SCHEMA = "_drt"
