"""Snowflake type registry — reflection of Snowflake-specific type names.

The ADBC Snowflake driver reports clean ``xdbc_type_name`` values plus
``xdbc_column_size`` / ``xdbc_decimal_digits``. Without a registry the
generic map returns ``NullType`` for NUMBER, TIMESTAMP_*, VARIANT, OBJECT
and ARRAY (issue #11, finding A). Unit tests here need no database; the
live checks live in ``test_snowflake_live.py``.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import types as sa_types

from sqlalchemy_adbc.reflection import adbc_type_to_sqla, columns_from_table
from sqlalchemy_adbc.snowflake import ADBCSnowflakeDialect
from sqlalchemy_adbc.snowflake_types import (
    ARRAY,
    NUMBER,
    OBJECT,
    VARIANT,
    snowflake_ischema_names,
)


def _col(name, type_name, size=None, digits=None, nullable="YES"):
    return {
        "column_name": name,
        "xdbc_type_name": type_name,
        "xdbc_column_size": size,
        "xdbc_decimal_digits": digits,
        "xdbc_is_nullable": nullable,
    }


# Shape of the driver's GetObjects output for one table of each type
# (captured 2026-11-07 against Snowflake 10.40.102, adbc-driver-snowflake 1.11.0).
SNOWFLAKE_TABLE = {
    "table_name": "T",
    "table_type": "BASE TABLE",
    "table_columns": [
        _col("ID", "NUMBER", 38, 0, nullable="NO"),
        _col("N", "NUMBER", 10, 2),
        _col("NAME", "TEXT", 50),
        _col("SCORE", "FLOAT"),
        _col("ACTIVE", "BOOLEAN"),
        _col("D", "DATE"),
        _col("T", "TIME"),
        _col("TS", "TIMESTAMP_NTZ"),
        _col("TSZ", "TIMESTAMP_TZ"),
        _col("TSL", "TIMESTAMP_LTZ"),
        _col("V", "VARIANT"),
        _col("O", "OBJECT"),
        _col("A", "ARRAY"),
        _col("B", "BINARY"),
    ],
}


def test_registry_maps_every_snowflake_type_name():
    cols = columns_from_table(SNOWFLAKE_TABLE, ischema_names=snowflake_ischema_names)
    by_name = {c["name"]: c["type"] for c in cols}
    assert not any(isinstance(t, sa_types.NullType) for t in by_name.values()), by_name
    assert (
        isinstance(by_name["ID"], NUMBER)
        and by_name["ID"].precision == 38
        and by_name["ID"].scale == 0
    )
    assert isinstance(by_name["N"], NUMBER) and (by_name["N"].precision, by_name["N"].scale) == (
        10,
        2,
    )
    assert isinstance(by_name["NAME"], sa_types.String) and by_name["NAME"].length == 50
    assert isinstance(by_name["SCORE"], sa_types.Float)
    assert isinstance(by_name["ACTIVE"], sa_types.Boolean)
    assert isinstance(by_name["D"], sa_types.Date)
    assert isinstance(by_name["T"], sa_types.Time)
    assert isinstance(by_name["TS"], sa_types.DateTime) and by_name["TS"].timezone is False
    assert isinstance(by_name["TSZ"], sa_types.DateTime) and by_name["TSZ"].timezone is True
    assert isinstance(by_name["TSL"], sa_types.DateTime) and by_name["TSL"].timezone is True
    assert isinstance(by_name["V"], VARIANT)
    assert isinstance(by_name["O"], OBJECT)
    assert isinstance(by_name["A"], ARRAY)
    assert isinstance(by_name["B"], sa_types.LargeBinary)
    assert {c["name"]: c["nullable"] for c in cols}["ID"] is False


def test_number_scale_zero_returns_int_and_scaled_returns_decimal():
    whole = adbc_type_to_sqla(
        "NUMBER", ischema_names=snowflake_ischema_names, size=38, decimal_digits=0
    )
    scaled = adbc_type_to_sqla(
        "NUMBER", ischema_names=snowflake_ischema_names, size=10, decimal_digits=2
    )
    d = ADBCSnowflakeDialect()
    assert whole.process_result_value(Decimal("10"), d) == 10
    assert type(whole.process_result_value(Decimal("10"), d)) is int
    assert whole.process_result_value(None, d) is None
    assert scaled.process_result_value(Decimal("12.34"), d) == Decimal("12.34")
    assert whole.python_type is int and scaled.python_type is Decimal
    # Bind side must not go through Numeric's float conversion.
    proc = whole.bind_processor(d)
    assert proc is None or type(proc(2**53 + 1)) is int


def test_number_without_metadata_defaults_to_decimal():
    t = adbc_type_to_sqla("NUMBER", ischema_names=snowflake_ischema_names)
    assert isinstance(t, NUMBER) and t.scale is None
    assert t.process_result_value(Decimal("1.5"), ADBCSnowflakeDialect()) == Decimal("1.5")


def test_variant_object_array_decode_json_text():
    d = ADBCSnowflakeDialect()
    assert VARIANT().process_result_value('{"k": [1, 2, 3]}', d) == {"k": [1, 2, 3]}
    assert OBJECT().process_result_value('{"a": "b"}', d) == {"a": "b"}
    assert ARRAY().process_result_value("[1, 2]", d) == [1, 2]
    assert VARIANT().process_result_value(None, d) is None
    # Bind side serialises to JSON text; Snowflake parses it via PARSE_JSON.
    assert VARIANT().process_bind_param({"a": 1}, d) == '{"a": 1}'
    assert "PARSE_JSON" in str(sa.select(sa.literal({"a": 1}, VARIANT())).compile(dialect=d))


def test_generic_registry_entries_still_instantiate_bare():
    """Class entries (as in pg_ischema_names) must keep working next to factories."""
    from sqlalchemy_adbc.postgresql_types import pg_ischema_names

    t = adbc_type_to_sqla("JSONB", ischema_names=pg_ischema_names, size=None, decimal_digits=None)
    assert type(t).__name__ == "JSONB"


def test_snowflake_dialect_exposes_registry():
    assert ADBCSnowflakeDialect.ischema_names is snowflake_ischema_names


def test_timestamp_values_pass_through_untouched():
    d = ADBCSnowflakeDialect()
    tsz = adbc_type_to_sqla("TIMESTAMP_TZ", ischema_names=snowflake_ischema_names)
    aware = dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
    proc = tsz.result_processor(d, None)
    assert proc is None or proc(aware) == aware
