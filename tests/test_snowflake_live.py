"""Live Snowflake checks for the type registry. Opt-in.

Set ``DRLS_SNOWFLAKE_URL`` (e.g.
``snowflake+adbc://user@ORG-ACCT/DB/SCHEMA?warehouse=WH&authenticator=snowflake_jwt&privateKey=...``)
to run. Creates one uniquely named table and drops it afterwards.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import types as sa_types

from sqlalchemy_adbc.snowflake_types import ARRAY, NUMBER, OBJECT, VARIANT

URL = os.environ.get("DRLS_SNOWFLAKE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="Requires DRLS_SNOWFLAKE_URL")


@pytest.fixture(scope="module")
def engine():
    e = sa.create_engine(URL)
    yield e
    e.dispose()


@pytest.fixture
def table(engine):
    name = f"ADBC_TYPES_{uuid.uuid4().hex[:8].upper()}"
    with engine.begin() as c:
        c.exec_driver_sql(
            f'CREATE TABLE "{name}" ('
            '"ID" NUMBER(38,0) NOT NULL, "N" NUMBER(10,2), "NAME" VARCHAR(50), '
            '"SCORE" FLOAT, "ACTIVE" BOOLEAN, "D" DATE, "T" TIME, '
            '"TS" TIMESTAMP_NTZ, "TSZ" TIMESTAMP_TZ, "TSL" TIMESTAMP_LTZ, '
            '"V" VARIANT, "O" OBJECT, "A" ARRAY, "B" BINARY)'
        )
        c.exec_driver_sql(
            f'INSERT INTO "{name}" ("ID","N","NAME","SCORE","ACTIVE","D","T","TS","TSZ","TSL","V","O","A","B") '
            "SELECT 1, 12.34, 'alpha', 1.5, TRUE, '2024-01-02', '03:04:05', "
            "'2024-01-02 03:04:05', '2024-01-02 03:04:05+00:00', '2024-01-02 03:04:05+00:00', "
            "PARSE_JSON('{\"k\": [1, 2, 3]}'), OBJECT_CONSTRUCT('a', 'b'), ARRAY_CONSTRUCT(1, 2), TO_BINARY('0102', 'HEX')"
        )
    yield name
    with engine.begin() as c:
        c.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')


def test_reflected_types_are_not_nulltype(engine, table):
    cols = {c["name"]: c["type"] for c in sa.inspect(engine).get_columns(table)}
    assert not any(isinstance(t, sa_types.NullType) for t in cols.values()), {
        k: type(v).__name__ for k, v in cols.items()
    }
    assert isinstance(cols["ID"], NUMBER) and (cols["ID"].precision, cols["ID"].scale) == (38, 0)
    assert isinstance(cols["N"], NUMBER) and (cols["N"].precision, cols["N"].scale) == (10, 2)
    assert isinstance(cols["NAME"], sa_types.String) and cols["NAME"].length == 50
    assert isinstance(cols["TS"], sa_types.DateTime) and not cols["TS"].timezone
    assert isinstance(cols["TSZ"], sa_types.DateTime) and cols["TSZ"].timezone
    assert isinstance(cols["V"], VARIANT)
    assert isinstance(cols["O"], OBJECT)
    assert isinstance(cols["A"], ARRAY)


def test_autoloaded_table_reads_python_values(engine, table):
    t = sa.Table(table, sa.MetaData(), autoload_with=engine)
    with engine.connect() as c:
        row = c.execute(sa.select(t)).one()._mapping
    assert row["ID"] == 1 and type(row["ID"]) is int
    assert row["N"] == Decimal("12.34")
    assert row["NAME"] == "alpha"
    assert row["D"] == dt.date(2024, 1, 2)
    assert row["TS"] == dt.datetime(2024, 1, 2, 3, 4, 5)
    assert row["TSZ"].utcoffset() is not None
    assert row["V"] == {"k": [1, 2, 3]}
    assert row["O"] == {"a": "b"}
    assert row["A"] == [1, 2]
    assert row["B"] == b"\x01\x02"


def test_variant_binds_through_parse_json(engine, table):
    """Snowflake forbids PARSE_JSON(?) inside VALUES; from_select is the route."""
    t = sa.Table(table, sa.MetaData(), autoload_with=engine)
    payload = {"nested": {"x": [True, None]}}
    with engine.begin() as c:
        c.execute(
            t.insert().from_select(
                ["ID", "V"], sa.select(sa.literal(2, NUMBER(38, 0)), sa.literal(payload, VARIANT()))
            )
        )
        c.execute(t.update().where(t.c.ID == 2).values(O={"updated": 1}))
    with engine.connect() as c:
        row = c.execute(sa.select(t.c.ID, t.c.V, t.c.O).where(t.c.ID == 2)).one()
    assert row == (2, payload, {"updated": 1})
    assert type(row[0]) is int


def test_number_binds_whole_values_without_float_conversion(engine, table):
    t = sa.Table(table, sa.MetaData(), autoload_with=engine)
    big = 9007199254740993  # 2**53 + 1: survives only if never cast to float
    with engine.begin() as c:
        c.execute(t.insert().values(ID=big))
    with engine.connect() as c:
        assert c.execute(sa.select(t.c.ID).where(t.c.ID == big)).scalar_one() == big
