"""Arrow helpers against PostgreSQL. Opt-in via ``DRLS_PG_URL`` (CI service container)."""

from __future__ import annotations

import datetime as dt
import os
import uuid
from collections.abc import Iterator

import pyarrow as pa
import pytest
import sqlalchemy as sa

from sqlalchemy_adbc.arrow import fetch_arrow_table, ingest_arrow, open_arrow_batch_reader

URL = os.environ.get("DRLS_PG_URL")
pytestmark = pytest.mark.skipif(not URL, reason="Requires DRLS_PG_URL")


@pytest.fixture(scope="module")
def engine() -> Iterator[sa.Engine]:
    e = sa.create_engine(URL or "")
    yield e
    e.dispose()


def _name() -> str:
    return f"adbc_arrow_{uuid.uuid4().hex[:8]}"


def _data(*ids: int) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "name": pa.array([f"x{i}" for i in ids], pa.string()),
            "amt": pa.array([float(i) for i in ids], pa.float64()),
            "d": pa.array([dt.date(2024, 1, 1 + (i % 28)) for i in ids], pa.date32()),
        }
    )


def test_fetch_with_date_bind_returns_date32(engine: sa.Engine) -> None:
    name = _name()
    t = sa.Table(
        name,
        sa.MetaData(),
        sa.Column("id", sa.BigInteger),
        sa.Column("d", sa.Date),
    )
    t.metadata.create_all(engine)
    try:
        with engine.begin() as c:
            c.execute(t.insert(), [{"id": i, "d": dt.date(2024, 1, 1 + i)} for i in range(5)])
            table = fetch_arrow_table(
                c,
                sa.select(t).where(t.c.d >= sa.bindparam("p")).order_by(t.c.id),
                {"p": dt.date(2024, 1, 3)},
            )
        assert table.num_rows == 3
        assert table.schema.field("d").type == pa.date32()
        assert table.column("d").to_pylist()[0] == dt.date(2024, 1, 3)
    finally:
        t.metadata.drop_all(engine)


def test_ingest_create_then_append_and_rollback(engine: sa.Engine) -> None:
    name = _name()
    try:
        with engine.begin() as c:
            assert ingest_arrow(c, name, _data(1, 2, 3), mode="create") == 3
            assert ingest_arrow(c, name, _data(4), mode="append") == 1
        with engine.connect() as c:
            tx = c.begin()
            ingest_arrow(c, name, _data(5, 6))
            assert fetch_arrow_table(c, f'SELECT count(*) AS n FROM "{name}"').column(
                "n"
            ).to_pylist() == [6]
            tx.rollback()
        with (
            engine.connect() as c,
            open_arrow_batch_reader(c, f'SELECT id FROM "{name}" ORDER BY id') as reader,
        ):
            assert reader.read_all().column("id").to_pylist() == [1, 2, 3, 4]
    finally:
        with engine.begin() as c:
            c.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')


def test_append_with_mismatched_schema_raises_driver_error(engine: sa.Engine) -> None:
    # PostgreSQL's ingest uses binary COPY: an int64 column cannot land in an
    # INTEGER column. The driver's error propagates unwrapped; its class differs
    # by driver version (ProgrammingError on 1.11, DataError on 1.13), so match
    # the DBAPI base class and the message.
    import adbc_driver_manager

    name = _name()
    t = sa.Table(name, sa.MetaData(), sa.Column("id", sa.Integer))
    t.metadata.create_all(engine)
    try:
        with engine.connect() as c:
            with pytest.raises(adbc_driver_manager.Error, match="binary data format"):
                ingest_arrow(c, name, pa.table({"id": pa.array([1], pa.int64())}))
            c.rollback()
            assert fetch_arrow_table(c, sa.select(sa.func.count()).select_from(t)).column(
                0
            ).to_pylist() == [0]
    finally:
        t.metadata.drop_all(engine)
