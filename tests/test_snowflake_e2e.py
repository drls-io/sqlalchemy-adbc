"""Snowflake dialect end to end, through the real ADBC driver.

Every test runs twice via the ``snowflake_engine`` fixture (``tests/conftest.py``):
against the in-process fake Snowflake endpoint (always), and against a real
account when ``DRLS_SNOWFLAKE_URL`` is set (e.g.
``snowflake+adbc://user@ORG-ACCT/DB/SCHEMA?warehouse=WH&authenticator=snowflake_jwt&privateKey=...``).
Each test creates uniquely named tables and drops them afterwards, so a
divergence between the fake and Snowflake shows up as a failure in one cell.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import types as sa_types

from sqlalchemy_adbc.snowflake_types import ARRAY, NUMBER, OBJECT, VARIANT


def _is_fake(engine: sa.Engine) -> bool:
    return engine.url.host == "FAKEACCT"


@pytest.fixture
def table(snowflake_engine):
    name = f"ADBC_TYPES_{uuid.uuid4().hex[:8].upper()}"
    with snowflake_engine.begin() as c:
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
    with snowflake_engine.begin() as c:
        c.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')


def test_reflected_types_are_not_nulltype(snowflake_engine, table):
    cols = {c["name"]: c["type"] for c in sa.inspect(snowflake_engine).get_columns(table)}
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


def test_autoloaded_table_reads_python_values(snowflake_engine, table):
    t = sa.Table(table, sa.MetaData(), autoload_with=snowflake_engine)
    with snowflake_engine.connect() as c:
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


def test_variant_binds_through_parse_json(snowflake_engine, table):
    """Snowflake forbids PARSE_JSON(?) inside VALUES; from_select is the route."""
    t = sa.Table(table, sa.MetaData(), autoload_with=snowflake_engine)
    payload = {"nested": {"x": [True, None]}}
    with snowflake_engine.begin() as c:
        c.execute(
            t.insert().from_select(
                ["ID", "V"], sa.select(sa.literal(2, NUMBER(38, 0)), sa.literal(payload, VARIANT()))
            )
        )
        c.execute(t.update().where(t.c.ID == 2).values(O={"updated": 1}))
    with snowflake_engine.connect() as c:
        row = c.execute(sa.select(t.c.ID, t.c.V, t.c.O).where(t.c.ID == 2)).one()
    assert row == (2, payload, {"updated": 1})
    assert type(row[0]) is int


def test_number_binds_whole_values_without_float_conversion(snowflake_engine, table):
    t = sa.Table(table, sa.MetaData(), autoload_with=snowflake_engine)
    big = 9007199254740993  # 2**53 + 1: survives only if never cast to float
    with snowflake_engine.begin() as c:
        c.execute(t.insert().values(ID=big))
    with snowflake_engine.connect() as c:
        assert c.execute(sa.select(t.c.ID).where(t.c.ID == big)).scalar_one() == big


def test_bind_every_python_type_in_one_row(snowflake_engine, table):
    """The exact cases that failed on the raw driver (finding B)."""
    t = sa.Table(table, sa.MetaData(), autoload_with=snowflake_engine)
    aware = dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.UTC)
    with snowflake_engine.begin() as c:
        c.execute(
            t.insert().values(
                ID=7,
                N=Decimal("12.34"),
                NAME=None,  # the only value for this column → typed null path
                SCORE=None,
                ACTIVE=True,
                D=dt.date(2024, 1, 2),
                T=dt.time(3, 4, 5),
                TS=dt.datetime(2024, 1, 2, 3, 4, 5),
                TSZ=aware,
                B=b"\x01\xff",
            )
        )
    with snowflake_engine.connect() as c:
        r = c.execute(sa.select(t).where(t.c.ID == 7)).one()._mapping
    assert r["N"] == Decimal("12.34") and r["NAME"] is None and r["SCORE"] is None
    assert r["ACTIVE"] is True and r["D"] == dt.date(2024, 1, 2) and r["T"] == dt.time(3, 4, 5)
    assert r["TS"] == dt.datetime(2024, 1, 2, 3, 4, 5)
    assert r["TSZ"] == aware
    assert r["B"] == b"\x01\xff"


def test_executemany_with_all_null_column(snowflake_engine, table):
    t = sa.Table(table, sa.MetaData(), autoload_with=snowflake_engine)
    with snowflake_engine.begin() as c:
        c.execute(
            t.insert(), [{"ID": 8, "NAME": None, "D": None}, {"ID": 9, "NAME": None, "D": None}]
        )
    with snowflake_engine.connect() as c:
        rows = c.execute(
            sa.select(t.c.ID, t.c.NAME, t.c.D).where(t.c.ID.in_([8, 9])).order_by(t.c.ID)
        ).all()
    assert rows == [(8, None, None), (9, None, None)]


def test_text_statement_with_date_and_none_params(snowflake_engine, table):
    with snowflake_engine.begin() as c:
        c.execute(
            sa.text(f'INSERT INTO "{table}" ("ID", "D", "NAME") VALUES (:i, :d, :n)'),
            {"i": 10, "d": "2024-05-06", "n": None},
        )
    with snowflake_engine.connect() as c:
        assert c.execute(
            sa.text(f'SELECT "D" FROM "{table}" WHERE "ID" = :i'), {"i": 10}
        ).scalar_one() == dt.date(2024, 5, 6)


def test_select_one_returns_int(snowflake_engine):
    with snowflake_engine.connect() as c:
        assert c.execute(sa.text("SELECT 1")).scalar() == 1


def test_crud_through_core(snowflake_engine):
    md = sa.MetaData()
    name = f"ADBC_CRUD_{uuid.uuid4().hex[:8].upper()}"
    t = sa.Table(
        name,
        md,
        sa.Column("ID", sa.Integer, primary_key=True),
        sa.Column("NAME", sa.String(20)),
        sa.Column("AMT", sa.Numeric(10, 2)),
        sa.Column("D", sa.Date),
        sa.Column("OK", sa.Boolean),
    )
    md.create_all(snowflake_engine)
    try:
        with snowflake_engine.begin() as c:
            c.execute(
                t.insert(),
                [
                    {
                        "ID": 1,
                        "NAME": "a",
                        "AMT": Decimal("1.50"),
                        "D": dt.date(2024, 1, 2),
                        "OK": True,
                    },
                    {"ID": 2, "NAME": None, "AMT": None, "D": None, "OK": False},
                ],
            )
            c.execute(t.update().where(t.c.ID == 2).values(NAME="b"))
            rows = c.execute(sa.select(t).order_by(t.c.ID)).all()
            assert rows[0][1:] == ("a", Decimal("1.50"), dt.date(2024, 1, 2), True)
            assert rows[1][1:] == ("b", None, None, False)
            stmt = sa.select(t.c.NAME).where(t.c.ID == sa.bindparam("p"))
            assert c.execute(stmt, {"p": 2}).scalar() == "b"
            c.execute(t.delete().where(~t.c.OK))
            assert c.execute(sa.select(sa.func.count()).select_from(t)).scalar() == 1
    finally:
        md.drop_all(snowflake_engine)
    assert not sa.inspect(snowflake_engine).has_table(name)


def test_executemany_reports_rowcount(snowflake_engine):
    md = sa.MetaData()
    name = f"ADBC_MANY_{uuid.uuid4().hex[:8].upper()}"
    t = sa.Table(name, md, sa.Column("ID", sa.Integer), sa.Column("V", sa.String(5)))
    md.create_all(snowflake_engine)
    try:
        with snowflake_engine.begin() as c:
            r = c.execute(t.insert(), [{"ID": i, "V": "x"} for i in range(3)])
            assert r.rowcount == 3
            assert c.execute(sa.select(sa.func.count()).select_from(t)).scalar() == 3
    finally:
        md.drop_all(snowflake_engine)


def test_lowercase_unquoted_names_round_trip_autoload(snowflake_engine):
    md = sa.MetaData()
    name = f"adbc_lower_{uuid.uuid4().hex[:8]}"
    t = sa.Table(
        name, md, sa.Column("id", sa.Integer, primary_key=True), sa.Column("txt", sa.String(5))
    )
    md.create_all(snowflake_engine)
    try:
        with snowflake_engine.begin() as c:
            c.execute(t.insert().values(id=1, txt="x"))
        insp = sa.inspect(snowflake_engine)
        assert name.upper() in insp.get_table_names()
        reflected = sa.Table(name.upper(), sa.MetaData(), autoload_with=snowflake_engine)
        assert [col.name for col in reflected.columns] == ["ID", "TXT"]
        assert list(reflected.primary_key.columns.keys()) == ["ID"]
        with snowflake_engine.connect() as c:
            assert c.execute(sa.select(reflected)).all() == [(1, "x")]
    finally:
        md.drop_all(snowflake_engine)


def test_constraints_reflect(snowflake_engine):
    sfx = uuid.uuid4().hex[:8].upper()
    parent, child = f"ADBC_P_{sfx}", f"ADBC_C_{sfx}"
    with snowflake_engine.begin() as c:
        c.exec_driver_sql(
            f'CREATE TABLE "{parent}" ("ID" NUMBER(38,0) NOT NULL PRIMARY KEY, "CODE" VARCHAR(10) UNIQUE)'
        )
        c.exec_driver_sql(
            f'CREATE TABLE "{child}" ("ID" NUMBER(38,0) PRIMARY KEY, '
            f'"PID" NUMBER(38,0) REFERENCES "{parent}" ("ID"))'
        )
    try:
        insp = sa.inspect(snowflake_engine)
        assert insp.get_pk_constraint(parent)["constrained_columns"] == ["ID"]
        assert [u["column_names"] for u in insp.get_unique_constraints(parent)] == [["CODE"]]
        [fk] = insp.get_foreign_keys(child)
        assert fk["constrained_columns"] == ["PID"]
        assert fk["referred_table"] == parent and fk["referred_columns"] == ["ID"]
        schemas = insp.get_schema_names()
        assert schemas
        if _is_fake(snowflake_engine):
            assert "PUBLIC" in schemas
        assert insp.has_table(parent) and not insp.has_table(f"NOPE_{sfx}")
    finally:
        with snowflake_engine.begin() as c:
            c.exec_driver_sql(f'DROP TABLE IF EXISTS "{child}"')
            c.exec_driver_sql(f'DROP TABLE IF EXISTS "{parent}"')


def test_view_reflects_as_view(snowflake_engine):
    sfx = uuid.uuid4().hex[:8].upper()
    table, view = f"ADBC_VT_{sfx}", f"ADBC_VV_{sfx}"
    with snowflake_engine.begin() as c:
        c.exec_driver_sql(f'CREATE TABLE "{table}" ("ID" NUMBER(38,0), "NAME" VARCHAR(10))')
        c.exec_driver_sql(f'CREATE VIEW "{view}" AS SELECT "ID", "NAME" FROM "{table}"')
    try:
        insp = sa.inspect(snowflake_engine)
        assert view in insp.get_view_names() and view not in insp.get_table_names()
        assert [col["name"] for col in insp.get_columns(view)] == ["ID", "NAME"]
    finally:
        with snowflake_engine.begin() as c:
            c.exec_driver_sql(f'DROP VIEW IF EXISTS "{view}"')
            c.exec_driver_sql(f'DROP TABLE IF EXISTS "{table}"')


def test_bad_sql_raises_dbapi_error(snowflake_engine):
    with snowflake_engine.connect() as c, pytest.raises(sa.exc.DBAPIError):
        c.exec_driver_sql("SELECT * FROM ADBC_DOES_NOT_EXIST_XYZ")
    # The connection is still usable afterwards.
    with snowflake_engine.connect() as c:
        assert c.execute(sa.text("SELECT 1")).scalar() == 1
