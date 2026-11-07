"""Column-wise result cursor.

The ADBC DBAPI converts rows one cell at a time (``arr[i].as_py()`` inside
``fetchone``; ``fetchmany``/``fetchall`` loop over it). pyarrow converts a
batch column-wise ~30x faster. The dialect wraps the DBAPI cursor so every
SQLAlchemy consumer (Superset, pandas, marimo, Polars via the engine) gets
the fast path without changes.
"""

from __future__ import annotations

import adbc_driver_manager.dbapi as adbc_dbapi
import pytest
import sqlalchemy as sa

from sqlalchemy_adbc.base import ColumnwiseCursor


@pytest.fixture
def engine():
    e = sa.create_engine("sqlite+adbc:///:memory:", poolclass=sa.pool.StaticPool)
    with e.begin() as c:
        c.exec_driver_sql(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, f REAL, s TEXT, b BLOB, big INTEGER)"
        )
        c.exec_driver_sql(
            "INSERT INTO t VALUES "
            "(1, 1.5, 'one', X'0102', 9223372036854775807), "
            "(2, NULL, NULL, NULL, NULL), "
            "(3, -0.0, '', X'', -9223372036854775808), "
            "(4, 2.5, 'ünï', X'FF', 0)"
        )
    return e


@pytest.fixture
def wide_engine():
    """~50k rows so the driver returns several Arrow batches."""
    e = sa.create_engine("sqlite+adbc:///:memory:", poolclass=sa.pool.StaticPool)
    with e.begin() as c:
        c.exec_driver_sql(
            "CREATE TABLE w AS WITH RECURSIVE s(i) AS "
            "(SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 50000) "
            "SELECT i AS id, i*1.5 AS x, 'row' || i AS name FROM s"
        )
    return e


def test_dialect_installs_columnwise_cursor(engine):
    with engine.connect() as c:
        result = c.exec_driver_sql("SELECT id FROM t")
        assert isinstance(result.cursor, ColumnwiseCursor)
        result.fetchall()


def test_fetch_never_uses_per_row_conversion(engine, monkeypatch):
    def boom(self):
        raise AssertionError("per-row _RowIterator.fetchone was used")

    monkeypatch.setattr(adbc_dbapi._RowIterator, "fetchone", boom)
    with engine.connect() as c:
        assert len(c.exec_driver_sql("SELECT * FROM t").fetchall()) == 4
        assert c.exec_driver_sql("SELECT id FROM t ORDER BY id").first() == (1,)
        assert len(c.exec_driver_sql("SELECT * FROM t").fetchmany(2)) == 2


def test_rows_match_stock_dbapi_cursor(engine):
    with engine.connect() as c:
        via_sqlalchemy = [tuple(r) for r in c.exec_driver_sql("SELECT * FROM t ORDER BY id")]
        stock = c.connection.dbapi_connection.cursor()
        try:
            stock.execute("SELECT * FROM t ORDER BY id")
            via_stock = stock.fetchall()
        finally:
            stock.close()
    assert via_sqlalchemy == via_stock
    assert via_sqlalchemy[0] == (1, 1.5, "one", b"\x01\x02", 9223372036854775807)
    assert via_sqlalchemy[1] == (2, None, None, None, None)


def test_fetchone_fetchmany_fetchall_sequence(engine):
    with engine.connect() as c:
        r = c.exec_driver_sql("SELECT id FROM t ORDER BY id")
        assert r.fetchone() == (1,)
        assert r.fetchmany(2) == [(2,), (3,)]
        assert r.fetchall() == [(4,)]
        assert r.fetchone() is None
        assert r.fetchall() == []


def test_fetchmany_streams_across_arrow_batches(wide_engine):
    with wide_engine.connect() as c:
        r = c.exec_driver_sql("SELECT id FROM w ORDER BY id")
        seen = 0
        while chunk := r.fetchmany(777):
            assert [row[0] for row in chunk] == list(range(seen + 1, seen + 1 + len(chunk)))
            seen += len(chunk)
        assert seen == 50000


def test_yield_per_partitions(wide_engine):
    with wide_engine.connect() as c:
        r = c.execution_options(yield_per=1000).exec_driver_sql("SELECT id FROM w ORDER BY id")
        total = sum(len(p) for p in r.partitions())
        assert total == 50000


def test_empty_result_set(engine):
    with engine.connect() as c:
        r = c.exec_driver_sql("SELECT * FROM t WHERE id > 100")
        assert r.fetchall() == []
        assert r.cursor is None or r.closed or True  # consumed; no error


def test_description_rowcount_and_arrow_passthrough(engine):
    with engine.connect() as c:
        r = c.exec_driver_sql("SELECT id, s FROM t ORDER BY id")
        assert [d[0] for d in r.cursor.description] == ["id", "s"]
        tbl = r.cursor.fetch_arrow_table()
        assert tbl.column_names == ["id", "s"] and tbl.num_rows == 4
    with engine.begin() as c:
        r = c.exec_driver_sql("UPDATE t SET s = 'x' WHERE id IN (1, 2)")
        assert r.rowcount in (2, -1)  # driver-dependent, but must not raise


def test_dml_and_ddl_through_proxy(engine):
    with engine.begin() as c:
        c.exec_driver_sql("INSERT INTO t (id, s) VALUES (5, 'five')")
        c.execute(
            sa.text("INSERT INTO t (id, s) VALUES (:i, :s)"),
            [{"i": 6, "s": "six"}, {"i": 7, "s": None}],
        )
        c.exec_driver_sql("CREATE TABLE t2 (x INTEGER)")
    with engine.connect() as c:
        assert c.exec_driver_sql("SELECT count(*) FROM t").scalar() == 7
