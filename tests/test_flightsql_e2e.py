"""Flight SQL dialect against a real Flight SQL server.

Skips without ``DRLS_FLIGHTSQL_URL``. CI points it at a GizmoSQL service
container (DuckDB backend); locally::

    docker run -d --name gizmosql -p 31337:31337 -e GIZMOSQL_USERNAME=ci \\
        -e GIZMOSQL_PASSWORD=ci-password -e TLS_ENABLED=0 -e PRINT_QUERIES=0 \\
        gizmodata/gizmosql:v1.42.2
    export DRLS_FLIGHTSQL_URL="flightsql+adbc://ci:ci-password@127.0.0.1:31337?auth=basic&tls=false"

Server-dependent behaviour (GizmoSQL reports no constraints or nullability
through GetObjects) is deliberately not asserted.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from decimal import Decimal

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.skipif(
    not os.environ.get("DRLS_FLIGHTSQL_URL"), reason="Requires DRLS_FLIGHTSQL_URL"
)


def test_select_one(flightsql_engine: sa.Engine) -> None:
    with flightsql_engine.connect() as c:
        assert c.execute(sa.text("SELECT 1")).scalar() == 1


def _url_with_password(url: str, password: str) -> str:
    u = sa.make_url(url)
    return u.set(password=password).render_as_string(hide_password=False)


def _table(name: str) -> sa.Table:
    return sa.Table(
        name,
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("big", sa.BigInteger),
        sa.Column("name", sa.String(20)),
        sa.Column("amt", sa.Numeric(10, 2)),
        sa.Column("f", sa.Float),
        sa.Column("ok", sa.Boolean),
        sa.Column("d", sa.Date),
        sa.Column("ts", sa.DateTime),
    )


ROW = {
    "id": 1,
    "big": 2**40,
    "name": "alpha",
    "amt": Decimal("12.34"),
    "f": 1.5,
    "ok": True,
    "d": dt.date(2024, 1, 2),
    "ts": dt.datetime(2024, 1, 2, 3, 4, 5),
}


def _name() -> str:
    return f"adbc_fs_{uuid.uuid4().hex[:8]}"


# ── auth ─────────────────────────────────────────────────────────────


def test_wrong_password_fails_at_connect(flightsql_url: str) -> None:
    e = sa.create_engine(_url_with_password(flightsql_url, "wrong"))
    try:
        with pytest.raises(sa.exc.DBAPIError, match="UNAUTHENTICATED|credentials"):
            e.connect()
    finally:
        e.dispose()


def test_bearer_token_from_handshake(flightsql_bearer_url: str) -> None:
    e = sa.create_engine(flightsql_bearer_url)
    try:
        with e.connect() as c:
            assert c.execute(sa.text("SELECT 1")).scalar() == 1
    finally:
        e.dispose()


def test_forged_bearer_is_rejected_on_first_query(flightsql_bearer_url: str) -> None:
    # Nothing is sent at connect() in bearer mode; the server rejects the
    # token on the first RPC.
    e = sa.create_engine(_url_with_password(flightsql_bearer_url, "forged"))
    try:
        with (
            e.connect() as c,
            pytest.raises(sa.exc.DBAPIError, match="[Tt]oken|UNAUTHENTICATED|verif"),
        ):
            c.execute(sa.text("SELECT 1")).scalar()
    finally:
        e.dispose()


# ── data ─────────────────────────────────────────────────────────────


def test_types_round_trip(flightsql_engine: sa.Engine) -> None:
    t = _table(_name())
    t.metadata.create_all(flightsql_engine)
    try:
        with flightsql_engine.begin() as c:
            c.execute(t.insert().values(**ROW))
            row = c.execute(sa.select(t)).one()
        assert row._mapping == ROW
        assert type(row.big) is int and type(row.amt) is Decimal
    finally:
        t.metadata.drop_all(flightsql_engine)


def test_executemany_parameters_update_delete(flightsql_engine: sa.Engine) -> None:
    t = _table(_name())
    t.metadata.create_all(flightsql_engine)
    try:
        with flightsql_engine.begin() as c:
            c.execute(
                t.insert(),
                [
                    {**ROW, "id": 1},
                    {**ROW, "id": 2, "name": None, "amt": None, "d": None, "ts": None},
                    {**ROW, "id": 3, "name": "gamma"},
                ],
            )
            stmt = sa.select(t.c.name).where(t.c.id == sa.bindparam("p"))
            assert c.execute(stmt, {"p": 3}).scalar() == "gamma"
            assert c.execute(stmt, {"p": 2}).scalar() is None
            assert c.execute(t.update().where(t.c.id == 2).values(name="beta")).rowcount == 1
            assert c.execute(t.delete().where(t.c.id == 1)).rowcount == 1
            assert c.execute(sa.select(sa.func.count()).select_from(t)).scalar() == 2
            assert c.execute(sa.select(t.c.name).order_by(t.c.id)).scalars().all() == [
                "beta",
                "gamma",
            ]
    finally:
        t.metadata.drop_all(flightsql_engine)


def test_rollback_discards_insert(flightsql_engine: sa.Engine) -> None:
    t = _table(_name())
    t.metadata.create_all(flightsql_engine)
    try:
        with flightsql_engine.connect() as c:
            tx = c.begin()
            c.execute(t.insert().values(**ROW))
            tx.rollback()
        with flightsql_engine.connect() as c:
            assert c.execute(sa.select(sa.func.count()).select_from(t)).scalar() == 0
    finally:
        t.metadata.drop_all(flightsql_engine)


def test_bad_sql_raises_dbapi_error(flightsql_engine: sa.Engine) -> None:
    with flightsql_engine.connect() as c:
        with pytest.raises(sa.exc.DBAPIError):
            c.exec_driver_sql("SELECT * FROM adbc_does_not_exist_xyz")
        c.rollback()
        assert c.execute(sa.text("SELECT 1")).scalar() == 1


# ── reflection ───────────────────────────────────────────────────────


def test_create_all_reflect_drop_all(flightsql_engine: sa.Engine) -> None:
    name = _name()
    t = _table(name)
    t.metadata.create_all(flightsql_engine)
    try:
        insp = sa.inspect(flightsql_engine)
        assert name in insp.get_table_names()
        assert insp.has_table(name)
        cols = {col["name"]: col["type"] for col in insp.get_columns(name)}
        assert isinstance(cols["id"], sa.types.Integer)
        assert isinstance(cols["big"], sa.types.Integer)  # BigInteger is an Integer
        assert isinstance(cols["name"], sa.types.String)
        assert isinstance(cols["amt"], sa.types.Numeric)
        assert isinstance(cols["f"], sa.types.Float)
        assert isinstance(cols["ok"], sa.types.Boolean)
        assert isinstance(cols["d"], sa.types.Date)
        assert isinstance(cols["ts"], sa.types.DateTime)
        reflected = sa.MetaData()
        reflected.reflect(bind=flightsql_engine, only=[name])
        assert [col.name for col in reflected.tables[name].columns] == list(ROW)
        auto = sa.Table(name, sa.MetaData(), autoload_with=flightsql_engine)
        assert [col.name for col in auto.columns] == list(ROW)
        assert "main" in insp.get_schema_names() or insp.get_schema_names()
    finally:
        t.metadata.drop_all(flightsql_engine)
    assert not sa.inspect(flightsql_engine).has_table(name)


# ── ecosystem ────────────────────────────────────────────────────────


def test_pandas_read_sql(flightsql_engine: sa.Engine) -> None:
    pd = pytest.importorskip("pandas")
    t = _table(_name())
    t.metadata.create_all(flightsql_engine)
    try:
        with flightsql_engine.begin() as c:
            c.execute(t.insert(), [{**ROW, "id": 1}, {**ROW, "id": 2, "name": "beta"}])
        frame = pd.read_sql(sa.select(t.c.id, t.c.name, t.c.amt).order_by(t.c.id), flightsql_engine)
        assert frame["id"].tolist() == [1, 2]
        assert frame["name"].tolist() == ["alpha", "beta"]
        # pandas may keep Decimals or coerce to float; the value is what matters.
        assert [float(v) for v in frame["amt"]] == [12.34, 12.34]
    finally:
        t.metadata.drop_all(flightsql_engine)
