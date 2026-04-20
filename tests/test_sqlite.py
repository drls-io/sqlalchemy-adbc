"""End-to-end smoke tests using adbc_driver_sqlite (installable in CI)."""

from __future__ import annotations

import pytest
import sqlalchemy
from sqlalchemy import text

pytest.importorskip("adbc_driver_sqlite")


def test_engine_created():
    engine = sqlalchemy.create_engine("adbc+sqlite:///:memory:")
    assert engine.dialect.name == "adbc"
    assert engine.dialect.driver == "sqlite"


def test_select_one():
    engine = sqlalchemy.create_engine("adbc+sqlite:///:memory:")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS n"))
        row = result.fetchone()
        assert row is not None
        assert row[0] == 1


def test_create_insert_select():
    engine = sqlalchemy.create_engine("adbc+sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
        result = conn.execute(text("SELECT COUNT(*) FROM t"))
        assert result.scalar() == 3

        result = conn.execute(text("SELECT name FROM t WHERE id = 2"))
        assert result.scalar() == "b"


def test_file_backed(tmp_path):
    db_path = tmp_path / "test.db"
    engine = sqlalchemy.create_engine(f"adbc+sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"))
        conn.execute(text("INSERT INTO kv VALUES ('hello', 'world')"))
    # Reconnect to confirm the file persisted and is readable.
    with engine.connect() as conn:
        result = conn.execute(text("SELECT v FROM kv WHERE k = 'hello'"))
        assert result.scalar() == "world"


def test_do_ping():
    engine = sqlalchemy.create_engine("adbc+sqlite:///:memory:")
    with engine.connect() as conn:
        # pool_pre_ping path exercises dialect.do_ping
        raw = conn.connection.dbapi_connection
        assert engine.dialect.do_ping(raw) is True
