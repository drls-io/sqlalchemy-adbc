"""No-database tests for PostgreSQL range/multirange/hstore support (issue #11).

Server strings below were captured from PostgreSQL 16 through the ADBC
driver on 2026-11-07; see the design note in .claude/superpowers/specs.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect


def test_pg_type_compiler_renders_pg_type_names():
    d = ADBCPostgreSQLDialect()
    t = sa.Table(
        "t",
        sa.MetaData(),
        sa.Column("r", pg.INT4RANGE),
        sa.Column("h", pg.HSTORE),
        sa.Column("j", pg.JSONB),
        sa.Column("ts", sa.DateTime()),
    )
    ddl = str(sa.schema.CreateTable(t).compile(dialect=d))
    assert "r INT4RANGE" in ddl and "h HSTORE" in ddl and "j JSONB" in ddl
    assert "ts TIMESTAMP WITHOUT TIME ZONE" in ddl  # generic compiler said DATETIME


# ── Task 2: element codecs and single-range text ─────────────────────

import datetime as dt  # noqa: E402
from decimal import Decimal  # noqa: E402

import pytest  # noqa: E402
from sqlalchemy.dialects.postgresql import Range  # noqa: E402

from sqlalchemy_adbc.postgresql_ranges import (  # noqa: E402
    DATE_CODEC,
    INT_CODEC,
    NUMERIC_CODEC,
    TEXT_CODEC,
    TIMESTAMP_CODEC,
    TIMESTAMPTZ_CODEC,
    format_range,
    parse_range,
)

UTC = dt.timezone.utc


@pytest.mark.parametrize(
    "text,expected",
    [
        ("[1,10)", Range(1, 10, bounds="[)")),
        ("(,6)", Range(None, 6, bounds="()")),
        ("empty", Range(empty=True)),
    ],
)
def test_parse_int4range(text, expected):
    assert parse_range(text, INT_CODEC) == expected


def test_parse_numrange_and_fully_unbounded():
    assert parse_range("[1.5,2.75]", NUMERIC_CODEC) == Range(
        Decimal("1.5"), Decimal("2.75"), bounds="[]"
    )
    assert parse_range("(,)", NUMERIC_CODEC) == Range(None, None, bounds="()")


def test_parse_daterange_open_upper():
    assert parse_range("[2024-01-01,)", DATE_CODEC) == Range(dt.date(2024, 1, 1), None, bounds="[)")


def test_parse_tstzrange_quoted_and_minus_infinity():
    r = parse_range('["2024-01-01 00:00:00+00","2024-02-01 00:00:00+00")', TIMESTAMPTZ_CODEC)
    assert r == Range(
        dt.datetime(2024, 1, 1, tzinfo=UTC), dt.datetime(2024, 2, 1, tzinfo=UTC), bounds="[)"
    )
    assert parse_range('(-infinity,"2024-02-01 00:00:00+00"]', TIMESTAMPTZ_CODEC) == Range(
        None, dt.datetime(2024, 2, 1, tzinfo=UTC), bounds="(]"
    )


def test_parse_tstz_fractional_seconds_and_offsets():  # Review Focus 1, 2
    assert TIMESTAMPTZ_CODEC.parse("2024-01-01 00:00:00.123456+00") == dt.datetime(
        2024, 1, 1, 0, 0, 0, 123456, tzinfo=UTC
    )
    assert TIMESTAMPTZ_CODEC.parse("2024-01-01 08:00:00-08").utcoffset() == dt.timedelta(hours=-8)
    assert TIMESTAMPTZ_CODEC.parse("2024-01-01 05:30:00+05:30").utcoffset() == dt.timedelta(
        hours=5, minutes=30
    )


def test_parse_tsrange_is_naive():
    r = parse_range('["2024-01-01 03:04:05","2024-01-02 00:00:00")', TIMESTAMP_CODEC)
    assert r.lower == dt.datetime(2024, 1, 1, 3, 4, 5) and r.lower.tzinfo is None


def test_quoted_elements_unescape():  # Review Focus 3
    r = parse_range('["a,b","c""d\\\\e"]', TEXT_CODEC)
    assert (r.lower, r.upper) == ("a,b", 'c"d\\e')


@pytest.mark.parametrize("bad", ["", "[1,10", "1,10)", "{1,10}", "[1;10)", "<1,10)"])
def test_malformed_raises_valueerror_naming_input(bad):
    with pytest.raises(ValueError, match=r"range"):
        parse_range(bad, INT_CODEC)


def test_format_range_roundtrips_server_strings():
    for text, codec in [
        ("[1,10)", INT_CODEC),
        ("(,6)", INT_CODEC),
        ("empty", INT_CODEC),
        ("[1.5,2.75]", NUMERIC_CODEC),
        ("(,)", NUMERIC_CODEC),
        ("[2024-01-01,)", DATE_CODEC),
    ]:
        assert format_range(parse_range(text, codec), codec) == text


def test_format_timestamps_are_quoted():
    r = Range(dt.datetime(2024, 1, 1, tzinfo=UTC), None, bounds="[)")
    assert format_range(r, TIMESTAMPTZ_CODEC) == '["2024-01-01 00:00:00+00:00",)'


def test_format_rejects_bad_bounds_and_naive_tstz():  # Review Focus 4
    with pytest.raises(ValueError, match="bounds"):
        format_range(Range(1, 2, bounds="[["), INT_CODEC)
    with pytest.raises(TypeError, match="aware"):
        format_range(Range(dt.datetime(2024, 1, 1), None, bounds="[)"), TIMESTAMPTZ_CODEC)
