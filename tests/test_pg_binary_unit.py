"""No-database tests for PostgreSQL binary send-format decoding (issue #59).

Byte strings were captured from PostgreSQL 16 through adbc-driver-postgresql
1.13.0 on 2026-11-09; see the design note in .claude/superpowers/specs.
"""

from __future__ import annotations

import datetime as dt
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
