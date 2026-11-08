"""BigQuery dialect end to end, through the real ADBC driver.

Runs only when ``DRLS_BIGQUERY_URL`` is set, e.g.
``bigquery+adbc:///my-project?dataset=my_dataset&location=US`` with Application
Default Credentials (``gcloud auth application-default login``). Each test
creates uniquely named tables in that dataset and drops them afterwards.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.filterwarnings("ignore:Cannot disable autocommit")


@pytest.fixture
def types_table(bigquery_engine):
    md = sa.MetaData()
    t = sa.Table(
        f"adbc_types_{uuid.uuid4().hex[:8]}",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(50)),
        sa.Column("note", sa.Text),
        sa.Column("flag", sa.Boolean),
        sa.Column("amount", sa.Numeric(10, 2)),
        sa.Column("ratio", sa.Float),
        sa.Column("d", sa.Date),
        sa.Column("dt", sa.DateTime),
        sa.Column("ts", sa.TIMESTAMP(timezone=True)),
        sa.Column("blob", sa.LargeBinary),
        sa.Column("select", sa.Integer),
    )
    md.create_all(bigquery_engine)
    yield t
    md.drop_all(bigquery_engine)


ROW = {
    "id": 1,
    "name": "a",
    "note": "n",
    "flag": True,
    "amount": Decimal("1.50"),
    "ratio": 0.5,
    "d": dt.date(2026, 1, 2),
    "dt": dt.datetime(2026, 1, 2, 3, 4, 5),
    "ts": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
    "blob": b"\x00\x01",
    "select": 7,
}


def test_create_insert_select_round_trip(bigquery_engine, types_table):
    t = types_table
    with bigquery_engine.begin() as c:
        c.execute(t.insert(), [ROW, {**ROW, "id": 2, "flag": False, "name": "b"}])
        rows = c.execute(sa.select(t).order_by(t.c.id)).mappings().all()
    assert len(rows) == 2
    got = dict(rows[0])
    assert got["ts"] == ROW["ts"]
    assert got == ROW


def test_boolean_filter_reserved_column_in_list_and_limit(bigquery_engine, types_table):
    t = types_table
    with bigquery_engine.begin() as c:
        c.execute(t.insert(), [ROW, {**ROW, "id": 2, "flag": False, "select": 8}])
        assert c.execute(sa.select(t.c.id).where(t.c.flag)).scalars().all() == [1]
        assert c.execute(sa.select(t.c["select"]).order_by(t.c.id)).scalars().all() == [7, 8]
        assert c.execute(sa.select(t.c.id).where(t.c.id.in_([2, 3]))).scalars().all() == [2]
        assert c.execute(sa.select(t.c.id).order_by(t.c.id).limit(1).offset(1)).scalars().all() == [
            2
        ]


def test_update_and_delete(bigquery_engine, types_table):
    t = types_table
    with bigquery_engine.begin() as c:
        c.execute(t.insert(), [ROW])
        c.execute(t.update().where(t.c.id == 1).values(name="z"))
        assert c.execute(sa.select(t.c.name)).scalar() == "z"
        c.execute(t.delete().where(t.c.id == 1))
        assert c.execute(sa.select(sa.func.count()).select_from(t)).scalar() == 0


def test_reflection_types(bigquery_engine, types_table):
    reflected = sa.Table(types_table.name, sa.MetaData(), autoload_with=bigquery_engine)
    kinds = {c.name: type(c.type) for c in reflected.columns}
    assert kinds["id"] is sa.BigInteger
    assert kinds["name"] is sa.String and reflected.c.name.type.length == 50
    assert kinds["note"] is sa.String
    assert kinds["flag"] is sa.Boolean
    assert kinds["amount"] is sa.Numeric
    assert (reflected.c.amount.type.precision, reflected.c.amount.type.scale) == (10, 2)
    assert kinds["ratio"] is sa.Float
    assert kinds["d"] is sa.Date
    assert kinds["dt"] is sa.DateTime
    assert kinds["ts"] is sa.TIMESTAMP and reflected.c.ts.type.timezone
    assert kinds["blob"] is sa.LargeBinary
    assert not reflected.c.id.nullable
    insp = sa.inspect(bigquery_engine)
    assert types_table.name in insp.get_table_names()
