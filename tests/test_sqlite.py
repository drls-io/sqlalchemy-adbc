"""End-to-end smoke tests using adbc_driver_sqlite (installable in CI)."""

from __future__ import annotations

import pytest
import sqlalchemy
from sqlalchemy import text

pytest.importorskip("adbc_driver_sqlite")


def test_engine_created():
    engine = sqlalchemy.create_engine("sqlite+adbc:///:memory:")
    # `.name` is the backend identity (used by Alembic's DDL impl
    # registry); `.driver` is the wire path. Reporting name="sqlite"
    # lets Alembic autogenerate find the standard SQLite DDL impl.
    assert engine.dialect.name == "sqlite"
    assert engine.dialect.driver == "adbc"


def test_select_one():
    engine = sqlalchemy.create_engine("sqlite+adbc:///:memory:")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS n"))
        row = result.fetchone()
        assert row is not None
        assert row[0] == 1


def test_create_insert_select():
    engine = sqlalchemy.create_engine("sqlite+adbc:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
        result = conn.execute(text("SELECT COUNT(*) FROM t"))
        assert result.scalar() == 3

        result = conn.execute(text("SELECT name FROM t WHERE id = 2"))
        assert result.scalar() == "b"


def test_file_backed(tmp_path):
    db_path = tmp_path / "test.db"
    engine = sqlalchemy.create_engine(f"sqlite+adbc:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"))
        conn.execute(text("INSERT INTO kv VALUES ('hello', 'world')"))
    # Reconnect to confirm the file persisted and is readable.
    with engine.connect() as conn:
        result = conn.execute(text("SELECT v FROM kv WHERE k = 'hello'"))
        assert result.scalar() == "world"


def test_do_ping():
    engine = sqlalchemy.create_engine("sqlite+adbc:///:memory:")
    with engine.connect() as conn:
        # pool_pre_ping path exercises dialect.do_ping
        raw = conn.connection.dbapi_connection
        assert engine.dialect.do_ping(raw) is True


# ── SQLAlchemy's SQLite dialect over ADBC (compliance work) ─────────


def test_is_sqlalchemys_sqlite_dialect_over_adbc():
    from sqlalchemy.dialects.sqlite.base import SQLiteDialect

    from sqlalchemy_adbc.base import ADBCConnectionMixin
    from sqlalchemy_adbc.sqlite import ADBCSQLiteDialect

    d = ADBCSQLiteDialect()
    assert isinstance(d, SQLiteDialect) and isinstance(d, ADBCConnectionMixin)
    dbapi = ADBCSQLiteDialect.import_dbapi()
    assert dbapi.Binary is bytes
    assert isinstance(dbapi.sqlite_version_info, tuple) and dbapi.sqlite_version_info >= (3, 35)
    assert hasattr(dbapi, "connect") and hasattr(dbapi, "Error")
    assert d.postfetch_lastrowid is False and d.insert_returning is True


def test_memory_database_is_shared_across_connections():
    engine = sqlalchemy.create_engine("sqlite+adbc:///:memory:")
    with engine.begin() as c:
        c.exec_driver_sql("CREATE TABLE m (a INTEGER)")
        c.exec_driver_sql("INSERT INTO m VALUES (1)")
    with engine.connect() as c:
        assert c.exec_driver_sql("SELECT count(*) FROM m").scalar() == 1
    engine.dispose()


def test_temporal_binary_numeric_round_trip_as_python_types(tmp_path):
    import datetime as dt
    from decimal import Decimal

    engine = sqlalchemy.create_engine(f"sqlite+adbc:///{tmp_path}/t.db")
    t = sqlalchemy.Table(
        "t",
        sqlalchemy.MetaData(),
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("ts", sqlalchemy.DateTime),
        sqlalchemy.Column("d", sqlalchemy.Date),
        sqlalchemy.Column("tm", sqlalchemy.Time),
        sqlalchemy.Column("b", sqlalchemy.LargeBinary),
        sqlalchemy.Column("n", sqlalchemy.Numeric(10, 2)),
        sqlalchemy.Column("ok", sqlalchemy.Boolean),
    )
    t.metadata.create_all(engine)
    row = {
        "ts": dt.datetime(2012, 10, 15, 12, 57, 18, 39642),
        "d": dt.date(2012, 10, 15),
        "tm": dt.time(12, 57, 18),
        "b": b"\x00\x01",
        "n": Decimal("1.50"),
        "ok": True,
    }
    with engine.begin() as c:
        pk = c.execute(t.insert(), row).inserted_primary_key
        assert pk == (1,)
        assert c.execute(t.insert().returning(t.c.id), row).scalar() == 2
        c.execute(t.insert().values([{**row, "id": 10}, {**row, "id": 11}]))
        got = c.execute(sqlalchemy.select(t).where(t.c.id == 1)).one()._mapping
    assert {k: got[k] for k in row} == row
    assert sqlalchemy.inspect(engine).get_table_names() == ["t"]
    engine.dispose()


def test_not_null_reflects_on_every_driver_version(tmp_path):
    engine = sqlalchemy.create_engine(f"sqlite+adbc:///{tmp_path}/n.db")
    with engine.begin() as c:
        c.exec_driver_sql("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT NOT NULL, name TEXT)")
    cols = {col["name"]: col for col in sqlalchemy.inspect(engine).get_columns("u")}
    assert cols["email"]["nullable"] is False and cols["name"]["nullable"] is True
    engine.dispose()


def test_attached_schema_is_visible_to_reflection(tmp_path):
    engine = sqlalchemy.create_engine(f"sqlite+adbc:///{tmp_path}/main.db")
    with engine.connect() as c:
        c.exec_driver_sql(f'ATTACH DATABASE "{tmp_path}/other.db" AS other')
        c.exec_driver_sql("CREATE TABLE other.x (a INTEGER NOT NULL)")
        insp = sqlalchemy.inspect(c)
        assert insp.has_table("x", schema="other") and not insp.has_table("x")
        assert insp.get_table_names(schema="other") == ["x"]
        assert insp.get_schema_names() == ["main", "other"]
        assert insp.get_columns("x", schema="other")[0]["nullable"] is False
    engine.dispose()
