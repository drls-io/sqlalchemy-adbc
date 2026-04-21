"""PostgreSQL type decorators — ADBC Arrow codec → SQLAlchemy values.

ADBC's Arrow-over-the-wire codec returns PostgreSQL-specific types as
generic Arrow primitives rather than the Python objects SQLAlchemy's
psycopg2 dialect returns. This module provides ``TypeDecorator``
subclasses that round-trip the common PG types through ADBC:

- JSONB / JSON    → dict/list (json.loads / json.dumps)
- UUID            → uuid.UUID (when as_uuid=True), else str
- INET / CIDR /
  MACADDR / MACADDR8 → str (preserved as text — no stdlib type exists
                      for these that most users expect to round-trip)

Arrays (``INTEGER[]``, ``TEXT[]``, ...) already round-trip correctly
through ADBC → pyarrow → Python list, so we don't need a custom
decorator. ``hstore`` and range types are deferred — they're less
common and have non-trivial parsing requirements; follow-up PRs can
add them without changing this file's layout.

The ``pg_ischema_names`` dict registers these types under their PG
catalog names so ``get_columns`` reflection (Issue #1) hands them
back instead of ``NullType``. See :mod:`sqlalchemy_adbc.reflection`
for the generic type map; the PG dialect merges this one on top.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import types as sa_types


class JSONB(sa_types.TypeDecorator[Any]):
    """JSONB → ``dict`` / ``list`` round-trip.

    ADBC returns JSONB as a UTF-8 string via the TEXT Arrow type. We
    parse on result (``process_result_value``) and serialize on bind
    (``process_bind_param``). ``None`` passes through unchanged so
    nullable columns stay nullable.
    """

    impl = sa_types.Text
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        return json.dumps(value)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            # Some drivers may pre-parse; accept it rather than re-encoding
            return value
        return json.loads(value)


class JSON(JSONB):
    """``json`` is the non-indexed variant of ``jsonb`` in PG. From an
    application perspective they are interchangeable; only the
    storage/indexing profile differs. Sharing the decoder keeps
    behavior consistent.

    SQLAlchemy requires ``cache_ok`` on every ``TypeDecorator``
    subclass (the attribute does not inherit), so we redeclare it.
    """

    cache_ok = True


class UUID(sa_types.TypeDecorator[Any]):
    """PG ``UUID`` column → ``uuid.UUID`` (when ``as_uuid=True``, the
    default) or ``str``.

    Matches psycopg2's dialect behavior: by default, values come back
    as ``uuid.UUID`` instances; pass ``as_uuid=False`` for string
    round-tripping (useful when the target library can't cope with
    ``uuid.UUID`` — e.g. some JSON serializers).
    """

    impl = sa_types.String(length=36)
    cache_ok = True

    def __init__(self, as_uuid: bool = True) -> None:
        super().__init__()
        self.as_uuid = as_uuid

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return str(value)
        # Validate on bind — catches typos before they become
        # "invalid input syntax for type uuid" at the DB.
        return str(uuid.UUID(str(value)))

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None
        if self.as_uuid and not isinstance(value, uuid.UUID):
            return uuid.UUID(str(value))
        return value


class INET(sa_types.TypeDecorator[Any]):
    """IPv4/IPv6 address — returned as ``str`` (e.g. ``"192.168.1.1"``,
    ``"2001:db8::/32"``). Python's ``ipaddress`` module could own this
    but psycopg2's dialect also returns ``str`` by default; matching
    that behavior keeps code that moves between drivers portable."""

    impl = sa_types.String(length=45)
    cache_ok = True


class CIDR(INET):
    """Network address in CIDR notation — same runtime shape as INET.

    ``cache_ok`` must be redeclared (it doesn't inherit across
    TypeDecorator subclasses).
    """

    cache_ok = True


class MACADDR(sa_types.TypeDecorator[Any]):
    """MAC address (EUI-48) — returned as ``str``."""

    impl = sa_types.String(length=17)
    cache_ok = True


# ── Reflection registry ──────────────────────────────────────────────
#
# When the PG dialect reflects a column whose ``xdbc_type_name`` matches
# one of these keys, the generic type mapper (reflection.py) defers to
# this dict so the column's runtime type is the decorator above rather
# than a plain ``Text`` / ``String`` / ``NullType``.

pg_ischema_names: dict[str, type[sa_types.TypeEngine[Any]]] = {
    "JSONB": JSONB,
    "JSON": JSON,
    "UUID": UUID,
    "INET": INET,
    "CIDR": CIDR,
    "MACADDR": MACADDR,
    "MACADDR8": MACADDR,
}
