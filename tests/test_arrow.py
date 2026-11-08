"""``sqlalchemy_adbc.arrow`` on SQLite: Arrow in and out of a SQLAlchemy connection."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from sqlalchemy_adbc.arrow import fetch_arrow_table, ingest_arrow, open_arrow_batch_reader

TABLE = sa.Table(
    "t",
    sa.MetaData(),
    sa.Column("id", sa.Integer),
    sa.Column("name", sa.String(20)),
    sa.Column("amt", sa.Float),
)


def _engine(rows: int = 20) -> sa.Engine:
    # StaticPool: one in-memory database shared by every connection the test opens.
    e = sa.create_engine("sqlite+adbc:///:memory:", poolclass=StaticPool)
    TABLE.metadata.create_all(e)
    with e.begin() as c:
        c.execute(TABLE.insert(), [{"id": i, "name": f"n{i}", "amt": i / 2} for i in range(rows)])
    return e


@pytest.fixture
def engine() -> Iterator[sa.Engine]:
    e = _engine()
    yield e
    e.dispose()


def _count(c: sa.Connection) -> int:
    return c.execute(sa.select(sa.func.count()).select_from(TABLE)).scalar_one()


# ── fetching ─────────────────────────────────────────────────────────


def test_fetch_arrow_table_core_select_with_bindparam(engine: sa.Engine) -> None:
    with engine.connect() as c:
        stmt = sa.select(TABLE).where(TABLE.c.id < sa.bindparam("p")).order_by(TABLE.c.id)
        table = fetch_arrow_table(c, stmt, {"p": 10})
    assert isinstance(table, pa.Table) and table.num_rows == 10
    assert table.schema.names == ["id", "name", "amt"]
    assert table.schema.types == [pa.int64(), pa.string(), pa.float64()]
    assert table.column("name").to_pylist()[:3] == ["n0", "n1", "n2"]


def test_fetch_arrow_table_text_named_params(engine: sa.Engine) -> None:
    with engine.connect() as c:
        table = fetch_arrow_table(
            c, sa.text("SELECT id FROM t WHERE id < :p ORDER BY id"), {"p": 3}
        )
    assert table.column("id").to_pylist() == [0, 1, 2]


def test_fetch_arrow_table_plain_string_positional(engine: sa.Engine) -> None:
    with engine.connect() as c:
        table = fetch_arrow_table(c, "SELECT id FROM t WHERE id < ? ORDER BY id", [3])
        no_params = fetch_arrow_table(c, "SELECT count(*) AS n FROM t")
    assert table.column("id").to_pylist() == [0, 1, 2]
    assert no_params.column("n").to_pylist() == [20]


# ── streaming ────────────────────────────────────────────────────────


def test_open_arrow_batch_reader_streams_multiple_batches() -> None:
    e = _engine(rows=5000)
    try:
        with e.connect() as c, open_arrow_batch_reader(c, sa.select(TABLE)) as reader:
            assert isinstance(reader, pa.RecordBatchReader)
            assert reader.schema.names == ["id", "name", "amt"]
            batches = list(reader)
        assert len(batches) >= 2
        assert sum(b.num_rows for b in batches) == 5000
    finally:
        e.dispose()


def test_batch_reader_closes_cursor_on_error(engine: sa.Engine) -> None:
    with engine.connect() as c:
        with pytest.raises(RuntimeError, match="boom"):
            with open_arrow_batch_reader(c, sa.select(TABLE)) as reader:
                next(iter(reader))
                raise RuntimeError("boom")
        # The connection is still usable and no cursor is left open.
        assert fetch_arrow_table(c, sa.select(TABLE)).num_rows == 20
        assert _count(c) == 20


# ── ingest ───────────────────────────────────────────────────────────


def _data(*ids: int) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([f"x{i}" for i in ids], pa.string()),
            "amt": pa.array([float(i) for i in ids], pa.float64()),
        }
    )


def test_ingest_append_create_replace(engine: sa.Engine) -> None:
    with engine.begin() as c:
        assert ingest_arrow(c, "t", _data(100, 101)) == 2
        assert _count(c) == 22
        assert ingest_arrow(c, "t_new", _data(1, 2, 3), mode="create") == 3
        assert c.execute(sa.text("SELECT count(*) FROM t_new")).scalar_one() == 3
        assert ingest_arrow(c, "t_new", _data(7), mode="replace") == 1
        assert c.execute(sa.text("SELECT id FROM t_new")).scalars().all() == [7]
        assert ingest_arrow(c, "t_new2", _data(1), mode="create_append") == 1
        assert ingest_arrow(c, "t_new2", _data(2), mode="create_append") == 1
        assert c.execute(sa.text("SELECT count(*) FROM t_new2")).scalar_one() == 2


def test_rollback_undoes_ingest_and_commit_keeps_it(engine: sa.Engine) -> None:
    with engine.connect() as c:
        tx = c.begin()
        ingest_arrow(c, "t", _data(100))
        assert _count(c) == 21
        tx.rollback()
        assert _count(c) == 20
        c.rollback()  # the count above autobegan a transaction; end it before begin()
        with c.begin():
            ingest_arrow(c, "t", _data(100))
    with engine.connect() as c:
        assert _count(c) == 21


def test_helpers_autobegin_so_commit_persists(engine: sa.Engine) -> None:
    # No explicit begin(): the helper must start SQLAlchemy's transaction so
    # commit() has something to commit; otherwise the pool rolls it back.
    with engine.connect() as c:
        assert not c.in_transaction()
        ingest_arrow(c, "t", _data(100))
        assert c.in_transaction()
        c.commit()
    with engine.connect() as c:
        assert _count(c) == 21
        fetch_arrow_table(c, sa.select(TABLE))
        assert c.in_transaction()


def test_session_connection_is_accepted(engine: sa.Engine) -> None:
    with Session(engine) as session:
        conn = session.connection()
        ingest_arrow(conn, "t", _data(100))
        assert fetch_arrow_table(conn, sa.select(TABLE)).num_rows == 21
        session.rollback()
    with engine.connect() as c:
        assert _count(c) == 20


def test_type_errors_name_the_fix(engine: sa.Engine) -> None:
    with pytest.raises(TypeError, match="engine.connect\\(\\)"):
        fetch_arrow_table(engine, "SELECT 1")  # type: ignore[arg-type]
    with Session(engine) as session, pytest.raises(TypeError, match="session.connection\\(\\)"):
        fetch_arrow_table(session, "SELECT 1")  # type: ignore[arg-type]
    plain = sa.create_engine("sqlite://")
    try:
        with plain.connect() as c, pytest.raises(TypeError, match="ADBC"):
            fetch_arrow_table(c, "SELECT 1")
    finally:
        plain.dispose()


def test_polars_dataframe_ingest(engine: sa.Engine) -> None:
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame({"id": [100, 101], "name": ["p0", "p1"], "amt": [0.5, 1.5]})
    with engine.begin() as c:
        assert ingest_arrow(c, "t", frame) == 2
        assert c.execute(sa.select(TABLE.c.name).where(TABLE.c.id >= 100)).scalars().all() == [
            "p0",
            "p1",
        ]
        table = fetch_arrow_table(c, sa.select(TABLE).where(TABLE.c.id >= 100))
    assert pl.from_arrow(table).shape == (2, 3)
