"""Decode PostgreSQL binary send format for range, multirange and hstore.

The ADBC PostgreSQL driver returns these columns as ``arrow.opaque`` values
whose binary storage is PG's binary *send* format (what ``COPY ... (FORMAT
binary)`` and binary-mode ``SELECT`` emit). The typed path in
:mod:`sqlalchemy_adbc.postgresql_ranges` sidesteps this by casting to text
in SQL; raw SQL (``pd.read_sql("SELECT period FROM t")``, SQL Lab) has no
type to attach a cast to, so the cursor decodes the bytes here, keyed by the
driver's per-column type metadata (:func:`decoders_for_schema`).

Formats (PostgreSQL 16 ``rangetypes.c``, ``multirangetypes.c``,
``contrib/hstore``):

- range: one flags byte (``0x01`` empty, ``0x02`` lower inclusive, ``0x04``
  upper inclusive, ``0x08`` lower unbounded, ``0x10`` upper unbounded),
  then ``int32 length + element`` for each bound that is present;
- multirange: ``int32 count`` then ``int32 length + range`` per range;
- hstore: ``int32 count`` then ``int32 keylen, key, int32 vallen, val`` per
  pair, ``vallen == -1`` meaning NULL;
- elements: int4/int8 big-endian; date = int32 days since 2000-01-01;
  timestamp/timestamptz = int64 microseconds since 2000-01-01 (UTC for
  timestamptz); numeric = int16 ndigits, weight, sign, dscale then base-10000
  digits. PG's ``infinity`` sentinels (int32/int64 extremes) decode to an
  unbounded side, matching the text codec policy.

Decoded objects are the same types the typed path yields: SQLAlchemy's
``Range`` / ``MultiRange`` and ``dict[str, str | None]``.
"""

from __future__ import annotations

import datetime as dt
import re
import struct
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from sqlalchemy.dialects.postgresql import MultiRange, Range

Decoder = Callable[[bytes], Any]

_EPOCH = dt.datetime(2000, 1, 1)
_EPOCH_DATE = dt.date(2000, 1, 1)
_EPOCH_UTC = _EPOCH.replace(tzinfo=dt.UTC)
_INT32_INF = {2**31 - 1, -(2**31)}
_INT64_INF = {2**63 - 1, -(2**63)}

_RANGE_EMPTY = 0x01
_RANGE_LB_INC = 0x02
_RANGE_UB_INC = 0x04
_RANGE_LB_INF = 0x08
_RANGE_UB_INF = 0x10

_NUMERIC_NEG = 0x4000
_NUMERIC_NAN = 0xC000
_NUMERIC_PINF = 0xD000
_NUMERIC_NINF = 0xF000


def _bad(what: str, data: bytes) -> ValueError:
    return ValueError(f"truncated or malformed PostgreSQL binary {what}: {data!r}")


def _int32_at(data: bytes, pos: int, what: str) -> int:
    if pos + 4 > len(data):
        raise _bad(what, data)
    return struct.unpack_from(">i", data, pos)[0]


# ── Elements ───────────────────────────────────────────────────────────


def decode_int4(data: bytes) -> int:
    return struct.unpack(">i", data)[0]


def decode_int8(data: bytes) -> int:
    return struct.unpack(">q", data)[0]


def decode_date(data: bytes) -> dt.date | None:
    days = struct.unpack(">i", data)[0]
    if days in _INT32_INF:
        return None
    return _EPOCH_DATE + dt.timedelta(days=days)


def decode_timestamp(data: bytes) -> dt.datetime | None:
    micros = struct.unpack(">q", data)[0]
    if micros in _INT64_INF:
        return None
    return _EPOCH + dt.timedelta(microseconds=micros)


def decode_timestamptz(data: bytes) -> dt.datetime | None:
    micros = struct.unpack(">q", data)[0]
    if micros in _INT64_INF:
        return None
    return _EPOCH_UTC + dt.timedelta(microseconds=micros)


def decode_numeric(data: bytes) -> Decimal:
    ndigits, weight, sign, dscale = struct.unpack_from(">hhHh", data, 0)
    if sign == _NUMERIC_NAN:
        return Decimal("NaN")
    if sign == _NUMERIC_PINF:
        return Decimal("Infinity")
    if sign == _NUMERIC_NINF:
        return Decimal("-Infinity")
    digits = struct.unpack_from(f">{ndigits}h", data, 8)
    # Each base-10000 digit d_i has weight 10000**(weight - i); as a Decimal
    # that is int(digits) * 10**(4 * (weight - ndigits + 1)).
    magnitude = 0
    for digit in digits:
        magnitude = magnitude * 10000 + digit
    exponent = 4 * (weight - ndigits + 1) if ndigits else 0
    value = Decimal(magnitude).scaleb(exponent)
    if sign == _NUMERIC_NEG:
        value = -value
    # PG tracks the display scale separately from the stored digits
    # ('1.50' is one digit, dscale 2); quantize to reproduce it.
    return value.quantize(Decimal(1).scaleb(-dscale))


# ── Containers ─────────────────────────────────────────────────────────


def decode_range(data: bytes, element: Decoder) -> Range[Any]:
    if not data:
        raise _bad("range", data)
    flags = data[0]
    if flags & _RANGE_EMPTY:
        return Range(empty=True)
    pos = 1
    lower = upper = None
    if not flags & _RANGE_LB_INF:
        length = _int32_at(data, pos, "range")
        pos += 4
        if pos + length > len(data):
            raise _bad("range", data)
        lower = element(data[pos : pos + length])
        pos += length
    if not flags & _RANGE_UB_INF:
        length = _int32_at(data, pos, "range")
        pos += 4
        if pos + length > len(data):
            raise _bad("range", data)
        upper = element(data[pos : pos + length])
        pos += length
    bounds = ("[" if flags & _RANGE_LB_INC else "(") + ("]" if flags & _RANGE_UB_INC else ")")
    return Range(lower, upper, bounds=bounds)  # type: ignore[arg-type]


def decode_multirange(data: bytes, element: Decoder) -> MultiRange[Any]:
    count = _int32_at(data, 0, "multirange")
    pos = 4
    ranges: list[Range[Any]] = []
    for _ in range(count):
        length = _int32_at(data, pos, "multirange")
        pos += 4
        if pos + length > len(data):
            raise _bad("multirange", data)
        ranges.append(decode_range(data[pos : pos + length], element))
        pos += length
    return MultiRange(ranges)


def decode_hstore(data: bytes) -> dict[str, str | None]:
    count = _int32_at(data, 0, "hstore")
    pos = 4
    out: dict[str, str | None] = {}
    for _ in range(count):
        klen = _int32_at(data, pos, "hstore")
        pos += 4
        if klen < 0 or pos + klen > len(data):
            raise _bad("hstore", data)
        key = data[pos : pos + klen].decode()
        pos += klen
        vlen = _int32_at(data, pos, "hstore")
        pos += 4
        if vlen == -1:
            out[key] = None
            continue
        if pos + vlen > len(data):
            raise _bad("hstore", data)
        out[key] = data[pos : pos + vlen].decode()
        pos += vlen
    return out


def _range_of(element: Decoder) -> Decoder:
    return lambda data: decode_range(data, element)


def _multirange_of(element: Decoder) -> Decoder:
    return lambda data: decode_multirange(data, element)


_ELEMENTS: dict[str, Decoder] = {
    "int4": decode_int4,
    "int8": decode_int8,
    "num": decode_numeric,
    "ts": decode_timestamp,
    "tstz": decode_timestamptz,
    "date": decode_date,
}

DECODERS_BY_TYPNAME: dict[str, Decoder] = {
    **{f"{prefix}range": _range_of(elem) for prefix, elem in _ELEMENTS.items()},
    **{f"{prefix}multirange": _multirange_of(elem) for prefix, elem in _ELEMENTS.items()},
    "hstore": decode_hstore,
}
"""``pg_type.typname`` → decoder for every family the raw-SQL path decodes."""


# ── Decoder selection ──────────────────────────────────────────────────

BUILTIN_RANGE_OIDS: dict[int, str] = {
    3904: "int4range",
    3906: "numrange",
    3908: "tsrange",
    3910: "tstzrange",
    3912: "daterange",
    3926: "int8range",
    4451: "int4multirange",
    4532: "nummultirange",
    4533: "tsmultirange",
    4534: "tstzmultirange",
    4535: "datemultirange",
    4536: "int8multirange",
}
"""``pg_type.oid`` of the built-in range and multirange types (fixed across
PG versions; ``hstore`` is an extension and has a per-database OID)."""

_METADATA_KEYS = (b"POSTGRESQL:type", b"ADBC:postgresql:typname")
_UNNAMED = re.compile(r"^unnamed<oid:(\d+)>$")


def _typname_of(field: Any, hstore_oid: int | None) -> str | None:
    """The PG type name the driver recorded for an Arrow field, if any.

    The driver names the types it knows (``int4multirange``, ``inet``) and
    reports the rest as ``unnamed<oid:N>``; the built-in range OIDs are
    fixed and hstore's is looked up once per connection.
    """
    metadata = field.metadata or {}
    raw = next((metadata[key] for key in _METADATA_KEYS if key in metadata), None)
    if raw is None:
        return None
    name = raw.decode() if isinstance(raw, bytes) else str(raw)
    match = _UNNAMED.match(name)
    if match is None:
        return name
    oid = int(match.group(1))
    if hstore_oid is not None and oid == hstore_oid:
        return "hstore"
    return BUILTIN_RANGE_OIDS.get(oid)


def decoders_for_schema(schema: Any, hstore_oid: int | None) -> list[Decoder | None]:
    """One decoder (or None) per column of an Arrow result schema.

    Columns the raw-SQL path leaves alone get None; the cursor then passes
    their ``to_pylist()`` values through untouched.
    """
    out: list[Decoder | None] = []
    for field in schema:
        name = _typname_of(field, hstore_oid)
        out.append(DECODERS_BY_TYPNAME.get(name) if name is not None else None)
    return out
