"""Snowflake parameter binding (issue #11, finding B).

adbc-driver-snowflake 1.11 binds only int / str / float / bool. Snowflake
itself implicitly converts string parameters into DATE, TIMESTAMP_*,
NUMBER and BINARY columns, so the dialect renders the other Python types
as strings. A parameter that is None in every row of a batch infers as an
Arrow ``null`` column, which the driver also rejects, so the dialect hands
the cursor an explicitly typed record batch instead.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy import types as sa_types

from sqlalchemy_adbc.snowflake import ADBCSnowflakeDialect, to_record_batch

d = ADBCSnowflakeDialect()


def _bind(sa_type, value):
    proc = d.type_descriptor(sa_type).bind_processor(d)
    return value if proc is None else proc(value)


def test_date_binds_as_iso_string():
    assert _bind(sa_types.Date(), dt.date(2024, 1, 2)) == "2024-01-02"
    assert _bind(sa_types.Date(), None) is None


def test_datetime_binds_as_iso_string_with_space():
    assert _bind(sa_types.DateTime(), dt.datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02 03:04:05"
    aware = dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.UTC)
    assert _bind(sa_types.DateTime(timezone=True), aware) == "2024-01-02 03:04:05.123456+00:00"


def test_time_binds_as_iso_string():
    assert _bind(sa_types.Time(), dt.time(3, 4, 5)) == "03:04:05"


def test_decimal_binds_as_string_but_int_and_float_pass_through():
    assert _bind(sa_types.Numeric(10, 2), Decimal("12.34")) == "12.34"
    assert _bind(sa_types.Numeric(38, 0), 2**53 + 1) == 2**53 + 1
    assert _bind(sa_types.Float(), 1.5) == 1.5


def test_binary_binds_as_hex_string():
    assert _bind(sa_types.LargeBinary(), b"\x01\xff") == "01ff"
    assert _bind(sa_types.LargeBinary(), None) is None


def test_record_batch_types_all_null_columns_as_string():
    rb = to_record_batch([(1, None, "x"), (2, None, None)])
    assert rb.num_rows == 2
    assert rb.schema.types == [pa.int64(), pa.string(), pa.string()]
    assert rb.column(1).null_count == 2 and rb.column(2).to_pylist() == ["x", None]


def test_record_batch_single_row_with_none():
    rb = to_record_batch([(None,)])
    assert rb.num_rows == 1 and rb.schema.types == [pa.string()]


def test_record_batch_empty_params_is_none():
    assert to_record_batch([]) is None
    assert to_record_batch([()]) is None


def test_dialect_passes_arrow_batch_to_cursor():
    seen = {}

    class FakeCursor:
        def execute(self, statement, parameters=None):
            seen["execute"] = parameters

        def executemany(self, statement, parameters):
            seen["executemany"] = parameters

    d.do_execute(FakeCursor(), "INSERT INTO t VALUES (?, ?)", (1, None), None)
    assert isinstance(seen["execute"], pa.RecordBatch) and seen["execute"].schema.types == [
        pa.int64(),
        pa.string(),
    ]
    d.do_executemany(FakeCursor(), "INSERT INTO t VALUES (?, ?)", [(1, None), (2, "b")], None)
    assert isinstance(seen["executemany"], pa.RecordBatch) and seen["executemany"].num_rows == 2
    assert seen["executemany"].schema.types == [pa.int64(), pa.string()]
    d.do_execute(FakeCursor(), "SELECT 1", (), None)
    assert seen["execute"] is None


def test_snowflake_has_native_booleans():
    # Without this SQLAlchemy renders booleans as 0/1, and Snowflake rejects
    # ``WHERE "OK" IS 0`` (found live by the e2e suite). Snowflake has a
    # BOOLEAN type with true/false literals.
    assert d.supports_native_boolean is True
    t = sa.table("t", sa.column("ok", sa_types.Boolean))
    sql = str(sa.select(t).where(t.c.ok == sa.true()).compile(dialect=d))
    assert sql.endswith("WHERE t.ok = true")
    sql = str(sa.select(t).where(~t.c.ok).compile(dialect=d))
    assert sql.endswith("WHERE NOT t.ok")
