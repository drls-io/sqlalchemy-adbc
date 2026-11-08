"""BigQuery dialect compiles GoogleSQL and reflects BigQuery types (no driver)."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import types as sa_types

from sqlalchemy_adbc.bigquery import ADBCBigQueryDialect
from sqlalchemy_adbc.reflection import adbc_type_to_sqla

dialect = ADBCBigQueryDialect(paramstyle="qmark")


def sql(stmt) -> str:
    return " ".join(str(stmt.compile(dialect=dialect)).split())


def ddl(table: sa.Table) -> str:
    return " ".join(str(sa.schema.CreateTable(table).compile(dialect=dialect)).split())


def test_identifiers_quote_with_backticks():
    t = sa.Table(
        "select", sa.MetaData(), sa.Column("from", sa.Integer), sa.Column("MixedCase", sa.Integer)
    )
    # Plain lower-case names stay bare (as every SQLAlchemy dialect does); reserved
    # words and mixed case get backticks, never double quotes.
    assert sql(sa.select(t)) == "SELECT `select`.`from`, `select`.`MixedCase` FROM `select`"


@pytest.mark.parametrize(
    ("sa_type", "expected"),
    [
        (sa.String(50), "STRING(50)"),
        (sa.String, "STRING"),
        (sa.Text, "STRING"),
        (sa.Unicode(10), "STRING(10)"),
        (sa.CHAR(2), "STRING(2)"),
        (sa.Integer, "INT64"),
        (sa.BigInteger, "INT64"),
        (sa.SmallInteger, "INT64"),
        (sa.Float, "FLOAT64"),
        (sa.REAL, "FLOAT64"),
        (sa.Double, "FLOAT64"),
        (sa.Boolean, "BOOL"),
        (sa.Numeric, "NUMERIC"),
        (sa.Numeric(10, 2), "NUMERIC(10, 2)"),
        (sa.DECIMAL(5, 0), "NUMERIC(5, 0)"),
        (sa.LargeBinary, "BYTES"),
        (sa.BINARY(4), "BYTES"),
        (sa.Date, "DATE"),
        (sa.Time, "TIME"),
        (sa.DateTime, "DATETIME"),
        (sa.TIMESTAMP(timezone=True), "TIMESTAMP"),
        (sa.JSON, "JSON"),
        (sa.Uuid, "STRING"),
    ],
)
def test_type_compiler_emits_googlesql_types(sa_type, expected):
    assert dialect.type_compiler_instance.process(sa.types.to_instance(sa_type)) == expected


def test_primary_and_foreign_keys_are_not_enforced():
    md = sa.MetaData()
    parent = sa.Table("parent", md, sa.Column("id", sa.Integer, primary_key=True))
    child = sa.Table(
        "child",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("parent.id")),
    )
    assert "PRIMARY KEY (id) NOT ENFORCED" in ddl(parent)
    assert "FOREIGN KEY(parent_id) REFERENCES parent (id) NOT ENFORCED" in ddl(child)


def test_unique_constraints_are_omitted():
    t = sa.Table(
        "t",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("code", sa.String(8), unique=True),
    )
    assert ddl(t) == "CREATE TABLE t ( id INT64, code STRING(8) )"


def test_boolean_filters_use_native_booleans():
    t = sa.Table("t", sa.MetaData(), sa.Column("flag", sa.Boolean))
    assert sql(sa.select(t).where(t.c.flag)) == "SELECT t.flag FROM t WHERE t.flag"
    assert sql(sa.select(t).where(t.c.flag == sa.true())).endswith("WHERE t.flag = true")


def test_datetime_binds_are_cast_to_datetime():
    # A naive datetime arrives as an Arrow timestamp (a TIMESTAMP instant);
    # BigQuery will not store that in a DATETIME column without a cast.
    t = sa.Table("t", sa.MetaData(), sa.Column("dt", sa.DateTime), sa.Column("ts", sa.TIMESTAMP))
    assert sql(t.insert().values(dt=None, ts=None)) == (
        "INSERT INTO t (dt, ts) VALUES (CAST(? AS DATETIME), CAST(? AS TIMESTAMP))"
    )
    assert sql(sa.select(t.c.dt).where(t.c.dt == sa.bindparam("x"))).endswith(
        "WHERE t.dt = CAST(? AS DATETIME)"
    )


def test_dbapi_exposes_binary_for_largebinary_binds():
    pytest.importorskip("adbc_driver_bigquery")
    assert ADBCBigQueryDialect.import_dbapi().Binary is bytes


def test_multirow_insert_renders_one_values_clause():
    t = sa.Table("t", sa.MetaData(), sa.Column("id", sa.Integer), sa.Column("name", sa.String))
    stmt = t.insert().values([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    assert sql(stmt) == "INSERT INTO t (id, name) VALUES (?, ?), (?, ?)"


@pytest.mark.parametrize(
    ("type_name", "size", "digits", "expected"),
    [
        ("STRING", 0, 0, sa_types.String()),
        ("STRING", 50, 0, sa_types.String(50)),
        ("INTEGER", 0, 0, sa_types.BigInteger()),
        ("FLOAT", 0, 0, sa_types.Float()),
        ("BOOLEAN", 0, 0, sa_types.Boolean()),
        ("NUMERIC", 0, 0, sa_types.Numeric()),
        ("NUMERIC", 10, 2, sa_types.Numeric(10, 2)),
        ("BIGNUMERIC", 0, 0, sa_types.Numeric()),
        ("DATE", 0, 0, sa_types.Date()),
        ("TIME", 0, 0, sa_types.Time()),
        ("DATETIME", 0, 0, sa_types.DateTime()),
        ("TIMESTAMP", 0, 0, sa_types.TIMESTAMP(timezone=True)),
        ("BYTES", 0, 0, sa_types.LargeBinary()),
        ("JSON", 0, 0, sa_types.JSON()),
        ("GEOGRAPHY", 0, 0, sa_types.NullType()),
        ("RECORD", 0, 0, sa_types.NullType()),
    ],
)
def test_reflection_type_map(type_name, size, digits, expected):
    got = adbc_type_to_sqla(
        type_name, ischema_names=dialect.ischema_names, size=size, decimal_digits=digits
    )
    assert type(got) is type(expected)
    assert repr(got) == repr(expected)


def test_default_schema_is_the_url_dataset():
    url = sa.make_url("bigquery+adbc:///proj?dataset=ds")
    assert dialect.create_connect_args(url)[1]["db_kwargs"]["adbc.bigquery.sql.dataset_id"] == "ds"
    assert ADBCBigQueryDialect._dataset_of(url) == "ds"
