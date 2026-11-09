"""No-database tests for PostgreSQL binary send-format decoding (issue #59).

Byte strings were captured from PostgreSQL 16 through adbc-driver-postgresql
1.13.0 on 2026-11-09; see the design note in .claude/superpowers/specs.
"""

from __future__ import annotations

import datetime as dt
import struct
from decimal import Decimal

import pytest
from sqlalchemy.dialects.postgresql import MultiRange, Range

from sqlalchemy_adbc import postgresql_binary as pgb

UTC = dt.UTC

# ── Task 1: element, range, multirange and hstore decoders ─────────────

INT4RANGE_CASES = [
    (
        b"\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\n",
        Range(1, 10, bounds="[)"),
    ),
    (b"\x08\x00\x00\x00\x04\x00\x00\x00\x05", Range(None, 5, bounds="()")),
    (b"\x12\x00\x00\x00\x04\x00\x00\x00\x03", Range(3, None, bounds="[)")),
    (b"\x01", Range(empty=True)),
]


@pytest.mark.parametrize("data,expected", INT4RANGE_CASES)
def test_decode_int4range(data, expected):
    assert pgb.decode_range(data, pgb.decode_int4) == expected


def test_decode_int8range():
    data = (
        b"\x02\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01"
        b"\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00d"
    )
    assert pgb.decode_range(data, pgb.decode_int8) == Range(1, 100, bounds="[)")


NUMRANGE_CASES = [
    (
        b"\x06\x00\x00\x00\x0c\x00\x02\x00\x00\x00\x00\x00\x01\x00\x01\x13\x88"
        b"\x00\x00\x00\x0c\x00\x02\x00\x00\x00\x00\x00\x01\x00\x02\x13\x88",
        Range(Decimal("1.5"), Decimal("2.5"), bounds="[]"),
    ),
    (
        b"\x02\x00\x00\x00\x0c\x00\x02\x00\x00@\x00\x00\x01\x00\x01\x13\x88"
        b"\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00",
        Range(Decimal("-1.5"), Decimal("0"), bounds="[)"),
    ),
    (
        b"\x02\x00\x00\x00\n\x00\x01\xff\xff\x00\x00\x00\x03\x00\n"
        b"\x00\x00\x00\n\x00\x01\xff\xff\x00\x00\x00\x01\x03\xe8",
        Range(Decimal("0.001"), Decimal("0.1"), bounds="[)"),
    ),
    (
        b"\x06\x00\x00\x00\x14\x00\x06\x00\x02\x00\x00\x00\t\x00\x01\t)\x1a\x85\x04\xd2\x16.#("
        b"\x00\x00\x00\n\x00\x01\x00\x02\x00\x00\x00\x00\x00d",
        Range(Decimal("123456789.123456789"), Decimal("10000000000"), bounds="[]"),
    ),
    (
        # '[1.50,2)': the declared scale survives (Decimal('1.50'), not '1.5')
        b"\x02\x00\x00\x00\x0c\x00\x02\x00\x00\x00\x00\x00\x02\x00\x01\x13\x88"
        b"\x00\x00\x00\n\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02",
        Range(Decimal("1.50"), Decimal("2"), bounds="[)"),
    ),
]


@pytest.mark.parametrize("data,expected", NUMRANGE_CASES)
def test_decode_numrange(data, expected):
    got = pgb.decode_range(data, pgb.decode_numeric)
    assert got == expected
    assert str(got.lower) == str(expected.lower) and str(got.upper) == str(expected.upper)


def test_decode_numeric_nan():
    data = b"\x00\x00\x00\x00\xc0\x00\x00\x00"
    assert pgb.decode_numeric(data).is_nan()


def _numeric_bytes(digits: list[int], weight: int, dscale: int, negative: bool = False) -> bytes:
    """Build PG's binary numeric: base-10000 digits, most significant first."""
    sign = 0x4000 if negative else 0
    return struct.pack(">hhHh", len(digits), weight, sign, dscale) + struct.pack(
        f">{len(digits)}h", *digits
    )


def test_decode_numeric_beyond_28_significant_digits_is_exact():
    # Decimal's default context has 28 digits of precision; PG numerics go
    # far beyond that and the typed path returns them exactly.
    data = _numeric_bytes([12, 3456, 7890, 1234, 5678, 9012, 3456, 7890], weight=7, dscale=0)
    assert pgb.decode_numeric(data) == Decimal("123456789012345678901234567890")
    data = _numeric_bytes([1, 1234, 5678, 9012, 3456, 7890, 1234, 5678, 9000], weight=0, dscale=30)
    assert str(pgb.decode_numeric(data)) == "1.123456789012345678901234567890"


def test_decode_numeric_zero_with_scale_and_negative_weight():
    assert str(pgb.decode_numeric(_numeric_bytes([], weight=0, dscale=2))) == "0.00"
    assert pgb.decode_numeric(_numeric_bytes([5], weight=-2, dscale=8)) == Decimal("0.00000005")
    assert pgb.decode_numeric(_numeric_bytes([5], weight=-2, dscale=8, negative=True)) == Decimal(
        "-0.00000005"
    )


def test_decode_daterange_infinity_is_unbounded():
    data = b'\x02\x00\x00\x00\x04\x00\x00">\x00\x00\x00\x04\x7f\xff\xff\xff'
    assert pgb.decode_range(data, pgb.decode_date) == Range(dt.date(2024, 1, 1), None, bounds="[)")


def test_decode_tsrange_negative_infinity_is_unbounded():
    data = (
        b"\x00\x00\x00\x00\x08\x80\x00\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x08\x00\x02\xb0\xd5\xd4\xe9@\x00"
    )
    assert pgb.decode_range(data, pgb.decode_timestamp) == Range(
        None, dt.datetime(2024, 1, 1), bounds="()"
    )


def test_decode_tsrange_microseconds():
    data = (
        b"\x02\x00\x00\x00\x08\x00\x02\xb0\xd5\xd4\xe9@\x00"
        b"\x00\x00\x00\x08\x00\x02\xb0\xf4~\xa6\xc6\x08"
    )
    assert pgb.decode_range(data, pgb.decode_timestamp) == Range(
        dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2, 12, 34, 56, 789000), bounds="[)"
    )


def test_decode_tstzrange_is_utc_aware():
    data = (
        b"\x02\x00\x00\x00\x08\x00\x02\xb0\xd5\xd4\xe9@\x00"
        b"\x00\x00\x00\x08\x00\x02\xb0\xe9\xf2\xc0\xa0\x00"
    )
    got = pgb.decode_range(data, pgb.decode_timestamptz)
    assert got == Range(
        dt.datetime(2024, 1, 1, tzinfo=UTC), dt.datetime(2024, 1, 2, tzinfo=UTC), bounds="[)"
    )
    assert got.lower.tzinfo is not None


def test_decode_int4multirange():
    data = (
        b"\x00\x00\x00\x02"
        b"\x00\x00\x00\x11\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x03"
        b"\x00\x00\x00\x11\x02\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\x04\x00\x00\x00\x07"
    )
    got = pgb.decode_multirange(data, pgb.decode_int4)
    assert isinstance(got, MultiRange)
    assert got == [Range(1, 3, bounds="[)"), Range(5, 7, bounds="[)")]


def test_decode_empty_multirange():
    got = pgb.decode_multirange(b"\x00\x00\x00\x00", pgb.decode_int4)
    assert isinstance(got, MultiRange) and got == []


def test_decode_nummultirange():
    data = (
        b"\x00\x00\x00\x02"
        b"\x00\x00\x00\x1f\x02\x00\x00\x00\x0c\x00\x02\x00\x00\x00\x00\x00\x01\x00\x01\x13\x88"
        b"\x00\x00\x00\n\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02"
        b"\x00\x00\x00\x1d\x06\x00\x00\x00\n\x00\x01\x00\x00\x00\x00\x00\x00\x00\x03"
        b"\x00\x00\x00\n\x00\x01\x00\x00\x00\x00\x00\x00\x00\x04"
    )
    assert pgb.decode_multirange(data, pgb.decode_numeric) == [
        Range(Decimal("1.5"), Decimal("2"), bounds="[)"),
        Range(Decimal("3"), Decimal("4"), bounds="[]"),
    ]


HSTORE_CASES = [
    (
        b"\x00\x00\x00\x02\x00\x00\x00\x01a\x00\x00\x00\x011\x00\x00\x00\x01b\xff\xff\xff\xff",
        {"a": "1", "b": None},
    ),
    (b"\x00\x00\x00\x00", {}),
    (
        b"\x00\x00\x00\x02\x00\x00\x00\x01e\x00\x00\x00\x00\x00\x00\x00\x03a b\x00\x00\x00\x03c,d",
        {"e": "", "a b": "c,d"},
    ),
]


@pytest.mark.parametrize("data,expected", HSTORE_CASES)
def test_decode_hstore(data, expected):
    assert pgb.decode_hstore(data) == expected


def test_decoders_by_typname_covers_every_family():
    names = {
        "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange",
        "int4multirange", "int8multirange", "nummultirange", "tsmultirange",
        "tstzmultirange", "datemultirange", "hstore",
    }  # fmt: skip
    assert set(pgb.DECODERS_BY_TYPNAME) == names
    assert pgb.DECODERS_BY_TYPNAME["daterange"](
        b'\x02\x00\x00\x00\x04\x00\x00">\x00\x00\x00\x04\x7f\xff\xff\xff'
    ) == Range(dt.date(2024, 1, 1), None, bounds="[)")
    assert pgb.DECODERS_BY_TYPNAME["hstore"](b"\x00\x00\x00\x00") == {}


def test_truncated_input_raises_value_error():
    with pytest.raises(ValueError):
        pgb.decode_range(b"\x02\x00\x00\x00\x04\x00", pgb.decode_int4)
    with pytest.raises(ValueError):
        pgb.decode_hstore(b"\x00\x00\x00\x01\x00\x00\x00\x05ab")


# ── Task 2: decoder selection from Arrow field metadata ────────────────

import pyarrow as pa  # noqa: E402

HSTORE_BYTES = b"\x00\x00\x00\x00"
INT4RANGE_BYTES = b"\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\n"


def _field(name: str, typname: str | None, key: str = "POSTGRESQL:type") -> pa.Field:
    meta = {key: typname} if typname is not None else None
    return pa.field(name, pa.binary(), metadata=meta)


def test_named_multirange_field_gets_multirange_decoder():
    schema = pa.schema([_field("mr", "int4multirange")])
    (dec,) = pgb.decoders_for_schema(schema, hstore_oid=None)
    assert dec is not None
    assert dec(b"\x00\x00\x00\x00") == []


def test_builtin_range_oid_resolves_without_lookup():
    schema = pa.schema([_field("r", "unnamed<oid:3904>")])
    (dec,) = pgb.decoders_for_schema(schema, hstore_oid=None)
    assert dec is not None and dec(INT4RANGE_BYTES) == Range(1, 10, bounds="[)")


def test_every_builtin_oid_is_mapped():
    expected = {
        3904: "int4range", 3906: "numrange", 3908: "tsrange", 3910: "tstzrange",
        3912: "daterange", 3926: "int8range", 4451: "int4multirange",
        4532: "nummultirange", 4533: "tsmultirange", 4534: "tstzmultirange",
        4535: "datemultirange", 4536: "int8multirange",
    }  # fmt: skip
    assert pgb.BUILTIN_RANGE_OIDS == expected


def test_hstore_oid_from_connection_enables_hstore_decoder():
    schema = pa.schema([_field("h", "unnamed<oid:16390>")])
    (dec,) = pgb.decoders_for_schema(schema, hstore_oid=16390)
    assert dec is not None and dec(HSTORE_BYTES) == {}


def test_unknown_oid_without_hstore_stays_undecoded():
    schema = pa.schema([_field("h", "unnamed<oid:16390>")])
    assert pgb.decoders_for_schema(schema, hstore_oid=None) == [None]
    assert pgb.decoders_for_schema(schema, hstore_oid=99999) == [None]


def test_named_hstore_field_decodes_regardless_of_oid():
    schema = pa.schema([_field("h", "hstore")])
    (dec,) = pgb.decoders_for_schema(schema, hstore_oid=None)
    assert dec is not None and dec(HSTORE_BYTES) == {}


def test_plain_and_metadata_free_fields_get_no_decoder():
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), metadata={"POSTGRESQL:type": "int4"}),
            pa.field("txt", pa.string()),
            _field("ip", "inet"),
        ]
    )
    assert pgb.decoders_for_schema(schema, hstore_oid=None) == [None, None, None]


def test_adbc_typname_key_is_honoured_when_postgresql_type_absent():
    schema = pa.schema([_field("r", "unnamed<oid:3926>", key="ADBC:postgresql:typname")])
    (dec,) = pgb.decoders_for_schema(schema, hstore_oid=None)
    assert dec is not None


def test_decoder_list_is_positional():
    schema = pa.schema(
        [pa.field("a", pa.int32()), _field("r", "daterange"), pa.field("b", pa.string())]
    )
    decs = pgb.decoders_for_schema(schema, hstore_oid=None)
    assert [d is not None for d in decs] == [False, True, False]


# ── Task 3: hstore OID per connection and dialect wiring ─────────────────

from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect  # noqa: E402


class _FakeCursor:
    def __init__(self, rows, error=None):
        self._rows, self._error = rows, error

    def execute(self, sql, *a):
        assert "hstore" in sql and "pg_extension" in sql  # tied to the extension's schema
        if self._error:
            raise self._error

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, rows, error=None):
        self._cursor = _FakeCursor(rows, error)
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def rollback(self):
        self.rollbacks += 1


def test_on_connect_records_hstore_oid_per_connection():
    dialect = ADBCPostgreSQLDialect()
    hook = dialect.on_connect()
    assert hook is not None
    conn = _FakeConnection([(16390,)])
    hook(conn)
    assert pgb.hstore_oid_for(conn) == 16390


def test_on_connect_without_hstore_records_none():
    hook = ADBCPostgreSQLDialect().on_connect()
    conn = _FakeConnection([])
    hook(conn)
    assert pgb.hstore_oid_for(conn) is None


def test_on_connect_query_failure_is_tolerated_and_rolled_back():
    hook = ADBCPostgreSQLDialect().on_connect()
    conn = _FakeConnection([], error=RuntimeError("permission denied"))
    hook(conn)  # must not raise
    assert pgb.hstore_oid_for(conn) is None
    # ADBC runs with autocommit off: a failed SELECT leaves libpq in an
    # aborted transaction and the user's first statement would fail with
    # "cannot start transaction: the connection is in an error state".
    assert conn.rollbacks == 1


def test_on_connect_success_does_not_roll_back():
    hook = ADBCPostgreSQLDialect().on_connect()
    conn = _FakeConnection([(16390,)])
    hook(conn)
    assert conn.rollbacks == 0


def test_unknown_connection_has_no_hstore_oid():
    assert pgb.hstore_oid_for(object()) is None


def test_pg_dialect_cursor_decodes_opaque_range_columns():
    from sqlalchemy_adbc.base import ColumnwiseCursor

    dialect = ADBCPostgreSQLDialect()
    ctx = dialect.execution_ctx_cls.__new__(dialect.execution_ctx_cls)
    ctx.dialect = dialect
    schema = pa.schema(
        [pa.field("r", pa.binary(), metadata={"POSTGRESQL:type": "unnamed<oid:3904>"})]
    )
    batch = pa.RecordBatch.from_arrays(
        [pa.array([INT4RANGE_BYTES, None], pa.binary())], schema=schema
    )

    class _Cur:
        arraysize = 1

        def execute(self, *a):
            pass

        def fetch_record_batch(self):
            return pa.RecordBatchReader.from_batches(schema, [batch])

    class _DBAPIConn:
        dbapi_connection = _FakeConnection([])

        def cursor(self):
            return _Cur()

    ctx._dbapi_connection = _DBAPIConn()
    cur = ctx.create_default_cursor()
    assert isinstance(cur, ColumnwiseCursor)
    cur.execute("SELECT r FROM t")
    assert cur.fetchall() == [(Range(1, 10, bounds="[)"),), (None,)]
