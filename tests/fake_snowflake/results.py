"""Snowflake type model and result encoding for the fake endpoint.

The ADBC Snowflake driver builds its Arrow schema from the ``rowtype`` list
in a query response and then transforms each column by its Snowflake type:
``fixed`` integers become ``decimal128(precision, scale)`` (a decimal128
column passes through), ``time`` is an int64 scaled by ``10**scale``,
``timestamp_*`` is either a scaled int64 or a struct of ``epoch``/``fraction``
(plus ``timezone`` for TIMESTAMP_TZ, as offset minutes + 1440) and the rest
are passed through as sent. A ``text`` column whose ``length`` is 0 is taken
to be a UUID, so text lengths are never 0 here.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import io
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pyarrow as pa

MAX_TEXT = 16777216
MAX_BINARY = 8388608

# Declared type name → (canonical name, default precision/scale/length).
_SYNONYMS: dict[str, str] = {
    "INT": "NUMBER",
    "INTEGER": "NUMBER",
    "BIGINT": "NUMBER",
    "SMALLINT": "NUMBER",
    "TINYINT": "NUMBER",
    "BYTEINT": "NUMBER",
    "NUMBER": "NUMBER",
    "DECIMAL": "NUMBER",
    "NUMERIC": "NUMBER",
    "FLOAT": "FLOAT",
    "FLOAT4": "FLOAT",
    "FLOAT8": "FLOAT",
    "DOUBLE": "FLOAT",
    "DOUBLE PRECISION": "FLOAT",
    "REAL": "FLOAT",
    "VARCHAR": "TEXT",
    "CHAR": "TEXT",
    "CHARACTER": "TEXT",
    "NCHAR": "TEXT",
    "STRING": "TEXT",
    "TEXT": "TEXT",
    "NVARCHAR": "TEXT",
    "NVARCHAR2": "TEXT",
    "CHAR VARYING": "TEXT",
    "NCHAR VARYING": "TEXT",
    "BINARY": "BINARY",
    "VARBINARY": "BINARY",
    "BINARY BLOB": "BINARY",  # the fake's own spelling, see sqlengine.sqlite_declared
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "TIMESTAMP_NTZ",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ",
    "VARIANT": "VARIANT",
    "OBJECT": "OBJECT",
    "ARRAY": "ARRAY",
}
_TIMESTAMPS = ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ")
_ROWTYPE_NAMES = {"NUMBER": "fixed", "FLOAT": "real"}
_LOGICAL_NAMES = {"NUMBER": "FIXED", "FLOAT": "REAL"}

_TYPE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z_ ]*?)\s*(?:\(\s*([^)]*)\))?\s*$")


@dataclass(frozen=True)
class SnowflakeType:
    name: str
    precision: int | None = None
    scale: int | None = None
    length: int | None = None

    @property
    def rowtype_type(self) -> str:
        return _ROWTYPE_NAMES.get(self.name, self.name.lower())

    @property
    def logical_type(self) -> str:
        return _LOGICAL_NAMES.get(self.name, self.name)

    def sqlite_declared(self) -> str:
        """``declared()`` with SQLite's storage affinity steered: BLOB for BINARY.

        SQLite derives affinity from the type name; the canonical Snowflake
        spellings land on TEXT/REAL/NUMERIC as needed, except BINARY, whose
        hex text would be coerced to a number under NUMERIC affinity.
        """
        if self.name == "BINARY":
            return f"BINARY BLOB({self.length})"
        return self.declared()

    def declared(self) -> str:
        if self.name == "NUMBER":
            return f"NUMBER({self.precision},{self.scale})"
        if self.name == "TEXT":
            return f"VARCHAR({self.length})"
        if self.name == "BINARY":
            return f"BINARY({self.length})"
        if self.name == "TIME" or self.name in _TIMESTAMPS:
            return f"{self.name}({self.scale})"
        return self.name

    def xdbc(self) -> dict[str, Any]:
        """The ``information_schema.columns`` derived fields of GetObjects."""
        size: int | None = None
        if self.name == "TEXT":
            size = self.length
        elif self.name == "NUMBER":
            size = self.precision
        return {
            "xdbc_type_name": self.name,
            "xdbc_column_size": size,
            "xdbc_char_octet_length": self.length if self.name == "TEXT" else None,
            "xdbc_decimal_digits": self.scale if self.name == "NUMBER" else None,
            "xdbc_num_prec_radix": 10 if self.name in ("NUMBER", "FLOAT") else None,
            "xdbc_datetime_sub": self.scale
            if self.name == "TIME" or self.name in _TIMESTAMPS
            else None,
        }


def parse_type(declared: str) -> SnowflakeType:
    """Normalise a declared type (``NUMBER(10,2)``, ``int``, ``varchar``) like Snowflake does."""
    m = _TYPE_RE.match(declared or "")
    if not m:
        return SnowflakeType("TEXT", length=MAX_TEXT)
    base = " ".join(m.group(1).upper().split())
    args = [a.strip() for a in (m.group(2) or "").split(",") if a.strip()]
    nums = [int(a) for a in args if a.isdigit()]
    name = _SYNONYMS.get(base)
    if name is None:
        return SnowflakeType("TEXT", length=MAX_TEXT)
    if name == "NUMBER":
        precision = nums[0] if nums else 38
        scale = nums[1] if len(nums) > 1 else 0
        return SnowflakeType(name, precision, scale)
    if name == "TEXT":
        default = 1 if base in ("CHAR", "CHARACTER", "NCHAR") else MAX_TEXT
        return SnowflakeType(name, length=nums[0] if nums else default)
    if name == "BINARY":
        return SnowflakeType(name, length=nums[0] if nums else MAX_BINARY)
    if name == "TIME" or name in _TIMESTAMPS:
        return SnowflakeType(name, scale=nums[0] if nums else 9)
    return SnowflakeType(name)


def infer_type(values: list[Any]) -> SnowflakeType:
    """Type for a result column with no declaration, from its first non-null value."""
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return SnowflakeType("BOOLEAN")
        if isinstance(v, int):
            return SnowflakeType("NUMBER", 38, 0)
        if isinstance(v, float):
            return SnowflakeType("FLOAT")
        if isinstance(v, (bytes, bytearray, memoryview)):
            return SnowflakeType("BINARY", length=MAX_BINARY)
        break
    return SnowflakeType("TEXT", length=MAX_TEXT)


# ── value coercion ───────────────────────────────────────────────────

_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?\s*(Z|[+-]\d{2}:?\d{2})?$"
)
_TIME_RE = re.compile(r"^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$")


def _parse_timestamp(value: Any) -> tuple[int, int, int | None]:
    """→ (epoch seconds, nanosecond fraction, utc offset minutes or None)."""
    if isinstance(value, dt.datetime):
        d = value
        offset = d.utcoffset()
        nanos = d.microsecond * 1000
    else:
        m = _TS_RE.match(str(value).strip())
        if not m:
            raise ValueError(f"not a timestamp: {value!r}")
        d = dt.datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}")
        frac = m.group(3) or ""
        nanos = int(frac.ljust(9, "0")) if frac else 0
        tz = m.group(4)
        offset = None
        if tz == "Z":
            offset = dt.timedelta(0)
        elif tz:
            sign = -1 if tz[0] == "-" else 1
            hh, mm = int(tz[1:3]), int(tz[-2:])
            offset = sign * dt.timedelta(hours=hh, minutes=mm)
    if offset is None:
        epoch = int(d.replace(tzinfo=dt.timezone.utc).timestamp())
        return epoch, nanos, None
    epoch = int(d.replace(tzinfo=dt.timezone(offset)).timestamp())
    return epoch, nanos, int(offset.total_seconds() // 60)


def _parse_time_ns(value: Any) -> int:
    if isinstance(value, dt.time):
        return (
            (value.hour * 60 + value.minute) * 60 + value.second
        ) * 10**9 + value.microsecond * 1000
    m = _TIME_RE.match(str(value).strip())
    if not m:
        raise ValueError(f"not a time: {value!r}")
    h, mi, s = (int(m.group(i)) for i in (1, 2, 3))
    frac = (m.group(4) or "").ljust(9, "0")
    return ((h * 60 + mi) * 60 + s) * 10**9 + (int(frac) if frac else 0)


def _to_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("true", "t", "1", "yes", "y")
    return bool(value)


def _to_bytes(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return bytes.fromhex(str(value))


def _to_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value).strip()[:10])


def _significant_digits(value: float | int) -> int:
    mantissa = repr(value).split("e")[0]
    return len("".join(ch for ch in mantissa if ch.isdigit()).lstrip("0"))


def _to_decimal(value: Any, scale: int) -> Decimal:
    # SQLite's NUMERIC affinity turns decimal text into a double (and an
    # integral double back into an int64), so a NUMBER beyond 15 significant
    # digits is already wrong by the time it is read. Refuse rather than
    # return a plausible-looking but mangled Decimal; whole NUMBER(p,0)
    # values are exact as long as they stayed int64.
    suspicious = isinstance(value, float) or (
        isinstance(value, int) and not isinstance(value, bool) and scale > 0
    )
    if suspicious and _significant_digits(value) >= 16:
        raise ValueError(
            f"NUMBER value {value!r} exceeds the precision SQLite stored it with; "
            "the fake cannot represent more than 15 significant digits in a scaled "
            "NUMBER or values outside int64"
        )
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    with decimal.localcontext() as ctx:
        ctx.prec = 38
        return d.quantize(Decimal(1).scaleb(-scale))


def _arrow_column(t: SnowflakeType, values: list[Any]) -> pa.Array:
    name = t.name
    if name == "NUMBER":
        scale = t.scale or 0
        precision = t.precision or 38
        return pa.array(
            [None if v is None else _to_decimal(v, scale) for v in values],
            type=pa.decimal128(precision, scale),
        )
    if name == "FLOAT":
        return pa.array([None if v is None else float(v) for v in values], type=pa.float64())
    if name == "BOOLEAN":
        return pa.array([None if v is None else _to_bool(v) for v in values], type=pa.bool_())
    if name == "BINARY":
        return pa.array([None if v is None else _to_bytes(v) for v in values], type=pa.binary())
    if name == "DATE":
        return pa.array([None if v is None else _to_date(v) for v in values], type=pa.date32())
    if name == "TIME":
        return pa.array([None if v is None else _parse_time_ns(v) for v in values], type=pa.int64())
    if name in _TIMESTAMPS:
        fields = [pa.field("epoch", pa.int64()), pa.field("fraction", pa.int32())]
        if name == "TIMESTAMP_TZ":
            fields.append(pa.field("timezone", pa.int32()))
        rows: list[dict[str, int] | None] = []
        for v in values:
            if v is None:
                rows.append(None)
                continue
            epoch, nanos, offset = _parse_timestamp(v)
            row = {"epoch": epoch, "fraction": nanos}
            if name == "TIMESTAMP_TZ":
                row["timezone"] = (offset or 0) + 1440
            rows.append(row)
        return pa.array(rows, type=pa.struct(fields))
    # TEXT, VARIANT, OBJECT, ARRAY and anything else: utf8.
    return pa.array([None if v is None else str(v) for v in values], type=pa.string())


# ── rowtype + rowset ─────────────────────────────────────────────────


def rowtype_entry(name: str, t: SnowflakeType, nullable: bool = True) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": name,
        "database": "",
        "schema": "",
        "table": "",
        "type": t.rowtype_type,
        "length": None,
        "byteLength": None,
        "precision": None,
        "scale": None,
        "nullable": nullable,
        "collation": None,
    }
    if t.name == "NUMBER":
        entry["precision"] = t.precision
        entry["scale"] = t.scale
    elif t.name in ("TEXT", "BINARY"):
        entry["length"] = t.length
        entry["byteLength"] = t.length
    elif t.name in ("VARIANT", "OBJECT", "ARRAY"):
        entry["length"] = MAX_TEXT
        entry["byteLength"] = MAX_TEXT
    elif t.name == "TIME" or t.name in _TIMESTAMPS:
        entry["precision"] = 0
        entry["scale"] = t.scale
    return entry


def _field_metadata(t: SnowflakeType) -> dict[str, str]:
    md = {"logicalType": t.logical_type, "finalType": "T"}
    if t.name == "NUMBER":
        md["precision"] = str(t.precision)
        md["scale"] = str(t.scale)
    elif t.name in ("TEXT", "BINARY"):
        md["charLength"] = str(t.length)
        md["byteLength"] = str(t.length)
    elif t.name in ("VARIANT", "OBJECT", "ARRAY"):
        md["charLength"] = str(MAX_TEXT)
        md["byteLength"] = str(MAX_TEXT)
    elif t.name == "TIME" or t.name in _TIMESTAMPS:
        md["scale"] = str(t.scale)
        md["precision"] = "0"
    return md


def encode_rowset(
    columns: list[tuple[str, SnowflakeType, list[Any]]],
    *,
    nullable: list[bool] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Typed columns → (``rowtype`` list, base64 Arrow IPC stream)."""
    flags = nullable or [True] * len(columns)
    rowtype = [
        rowtype_entry(name, t, flag) for (name, t, _), flag in zip(columns, flags, strict=True)
    ]
    fields = []
    arrays = []
    for name, t, values in columns:
        arr = _arrow_column(t, values)
        fields.append(pa.field(name, arr.type, nullable=True, metadata=_field_metadata(t)))
        arrays.append(arr)
    schema = pa.schema(fields)
    batch = pa.record_batch(arrays, schema=schema)
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
    return rowtype, base64.b64encode(sink.getvalue()).decode()
