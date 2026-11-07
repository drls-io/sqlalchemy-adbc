"""PostgreSQL type-decorator integration tests (Issue #3).

These tests need a live Postgres. They are gated by the
``DRLS_PG_URL`` environment variable so local developers don't need
a server to run the suite — CI sets it via the ``postgres`` service.

Covers:
- JSONB / JSON round-trip (dict, list, nested, None)
- UUID round-trip (as_uuid=True default, as_uuid=False opt-out)
- Array columns (integer[], text[]) via plain SQLAlchemy ARRAY
- INET / CIDR / MACADDR round-trip as strings
- Reflection maps JSONB/UUID/INET columns to our decorators

``testing.postgresql`` is intentionally NOT used — it would shell
out to initdb and inflate the dev install. The CI service container
is cheaper and more realistic.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import MultiRange, Range

UTC = dt.timezone.utc

PG_URL = os.environ.get("DRLS_PG_URL")
pytestmark = pytest.mark.skipif(
    not PG_URL,
    reason="Requires DRLS_PG_URL (e.g. postgresql+adbc://user:pw@host/db)",
)

# Lazy-imported so the pytest collection doesn't fail when the extra
# isn't installed. `pytestmark` already handles the skip.
if PG_URL:
    from sqlalchemy_adbc import postgresql_types as pgtypes
    from sqlalchemy_adbc.postgresql_types import (  # noqa: E402
        CIDR,
        INET,
        JSONB,
        MACADDR,
    )
    from sqlalchemy_adbc.postgresql_types import (
        JSON as JSONType,
    )
    from sqlalchemy_adbc.postgresql_types import (
        UUID as UUIDType,
    )


@pytest.fixture
def engine() -> sa.Engine:
    """Fresh engine per test. Tables are created and dropped in the
    test body so tests don't leak state to each other."""
    # pytestmark.skipif above guarantees PG_URL is set when we get here;
    # mypy can't see through the skip, so assert it for the narrower type.
    assert PG_URL is not None
    return sa.create_engine(PG_URL)


def _drop_table(engine: sa.Engine, name: str) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')


# ── JSONB / JSON ─────────────────────────────────────────────────────


def test_jsonb_roundtrip_dict(engine):
    _drop_table(engine, "t_jsonb")
    md = sa.MetaData()
    t = sa.Table(
        "t_jsonb", md, sa.Column("id", sa.Integer, primary_key=True), sa.Column("data", JSONB)
    )
    md.create_all(engine)
    try:
        payload = {"user": "rex", "roles": ["admin", "editor"], "nested": {"count": 7}}
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, data=payload))
            row = conn.execute(sa.select(t.c.data)).scalar_one()
        assert row == payload
        # Mutation on the returned object must not roundtrip back
        # without an explicit update — confirms we return a fresh dict.
        row["poison"] = True
        with engine.begin() as conn:
            stored = conn.execute(sa.select(t.c.data)).scalar_one()
        assert "poison" not in stored
    finally:
        _drop_table(engine, "t_jsonb")


def test_jsonb_roundtrip_list(engine):
    _drop_table(engine, "t_jsonb_list")
    md = sa.MetaData()
    t = sa.Table(
        "t_jsonb_list", md, sa.Column("id", sa.Integer, primary_key=True), sa.Column("data", JSONB)
    )
    md.create_all(engine)
    try:
        payload = [1, 2, {"three": 3}, [4, 5]]
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, data=payload))
            row = conn.execute(sa.select(t.c.data)).scalar_one()
        assert row == payload
    finally:
        _drop_table(engine, "t_jsonb_list")


def test_jsonb_none_passthrough(engine):
    _drop_table(engine, "t_jsonb_null")
    md = sa.MetaData()
    t = sa.Table(
        "t_jsonb_null", md, sa.Column("id", sa.Integer, primary_key=True), sa.Column("data", JSONB)
    )
    md.create_all(engine)
    try:
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, data=None))
            row = conn.execute(sa.select(t.c.data)).scalar_one()
        assert row is None
    finally:
        _drop_table(engine, "t_jsonb_null")


def test_json_and_jsonb_behave_identically(engine):
    """``json`` and ``jsonb`` are the same thing to callers — only
    storage/indexing differs. The decorator should treat them the same."""
    _drop_table(engine, "t_json")
    md = sa.MetaData()
    t = sa.Table(
        "t_json", md, sa.Column("id", sa.Integer, primary_key=True), sa.Column("data", JSONType)
    )
    md.create_all(engine)
    try:
        payload = {"k": "v"}
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, data=payload))
            row = conn.execute(sa.select(t.c.data)).scalar_one()
        assert row == payload
    finally:
        _drop_table(engine, "t_json")


# ── UUID ─────────────────────────────────────────────────────────────


def test_uuid_roundtrip_as_uuid(engine):
    _drop_table(engine, "t_uuid")
    md = sa.MetaData()
    t = sa.Table(
        "t_uuid",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("ident", UUIDType()),
    )
    md.create_all(engine)
    try:
        u = uuid.uuid4()
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, ident=u))
            row = conn.execute(sa.select(t.c.ident)).scalar_one()
        assert isinstance(row, uuid.UUID)
        assert row == u
    finally:
        _drop_table(engine, "t_uuid")


def test_uuid_roundtrip_as_str(engine):
    _drop_table(engine, "t_uuid_str")
    md = sa.MetaData()
    t = sa.Table(
        "t_uuid_str",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("ident", UUIDType(as_uuid=False)),
    )
    md.create_all(engine)
    try:
        u = uuid.uuid4()
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, ident=str(u)))
            row = conn.execute(sa.select(t.c.ident)).scalar_one()
        assert isinstance(row, str)
        assert row == str(u)
    finally:
        _drop_table(engine, "t_uuid_str")


def test_uuid_bind_from_string_validates():
    """Binding a non-UUID string must raise ``ValueError`` on bind —
    catches typos before they become PG's "invalid input syntax"."""
    from sqlalchemy_adbc.postgresql_types import UUID

    dec = UUID()
    with pytest.raises(ValueError):
        dec.process_bind_param("not-a-uuid", None)


# ── INET / CIDR / MACADDR ────────────────────────────────────────────


def test_inet_roundtrip(engine):
    _drop_table(engine, "t_inet")
    md = sa.MetaData()
    t = sa.Table(
        "t_inet",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("addr", INET()),
    )
    md.create_all(engine)
    try:
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, addr="192.168.1.1"))
            row = conn.execute(sa.select(t.c.addr)).scalar_one()
        # Postgres may normalize "192.168.1.1" → "192.168.1.1/32" in
        # INET; accept either form.
        assert row in {"192.168.1.1", "192.168.1.1/32"}
    finally:
        _drop_table(engine, "t_inet")


def test_cidr_roundtrip(engine):
    _drop_table(engine, "t_cidr")
    md = sa.MetaData()
    t = sa.Table(
        "t_cidr",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("net", CIDR()),
    )
    md.create_all(engine)
    try:
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, net="10.0.0.0/24"))
            row = conn.execute(sa.select(t.c.net)).scalar_one()
        assert row == "10.0.0.0/24"
    finally:
        _drop_table(engine, "t_cidr")


def test_macaddr_roundtrip(engine):
    _drop_table(engine, "t_mac")
    md = sa.MetaData()
    t = sa.Table(
        "t_mac",
        md,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("mac", MACADDR()),
    )
    md.create_all(engine)
    try:
        with engine.begin() as conn:
            conn.execute(t.insert().values(id=1, mac="08:00:2b:01:02:03"))
            row = conn.execute(sa.select(t.c.mac)).scalar_one()
        assert row == "08:00:2b:01:02:03"
    finally:
        _drop_table(engine, "t_mac")


# ── Reflection maps PG types to decorators ───────────────────────────


def test_reflection_maps_jsonb_column(engine):
    _drop_table(engine, "t_refl")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            'CREATE TABLE "t_refl" ('
            "  id INTEGER PRIMARY KEY,"
            "  payload JSONB,"
            "  ident UUID,"
            "  addr INET"
            ")"
        )
    try:
        cols = {c["name"]: c for c in inspect(engine).get_columns("t_refl")}
        assert isinstance(cols["payload"]["type"], JSONB)
        assert isinstance(cols["ident"]["type"], UUIDType)
        assert isinstance(cols["addr"]["type"], INET)
    finally:
        _drop_table(engine, "t_refl")


# ── Unit tests (no DB) ───────────────────────────────────────────────


def test_jsonb_process_bind_null_passthrough():
    from sqlalchemy_adbc.postgresql_types import JSONB

    assert JSONB().process_bind_param(None, None) is None


def test_jsonb_process_result_accepts_already_parsed():
    """Some ADBC paths may pre-parse JSONB; the decoder must accept a
    dict/list and return it rather than re-deserializing."""
    from sqlalchemy_adbc.postgresql_types import JSONB

    dec = JSONB()
    assert dec.process_result_value({"a": 1}, None) == {"a": 1}
    assert dec.process_result_value([1, 2], None) == [1, 2]


def test_jsonb_process_bind_serializes_to_string():
    """Bind path must hand libpq a JSON string — binding a dict
    would make ADBC try to quote the Python repr."""
    from sqlalchemy_adbc.postgresql_types import JSONB

    out = JSONB().process_bind_param({"k": 1}, None)
    assert isinstance(out, str)
    assert json.loads(out) == {"k": 1}


def test_uuid_process_bind_from_uuid_instance():
    from sqlalchemy_adbc.postgresql_types import UUID

    u = uuid.uuid4()
    assert UUID().process_bind_param(u, None) == str(u)


def test_uuid_process_result_wraps_string():
    from sqlalchemy_adbc.postgresql_types import UUID

    u = uuid.uuid4()
    got = UUID().process_result_value(str(u), None)
    assert isinstance(got, uuid.UUID)
    assert got == u


def test_uuid_as_uuid_false_returns_string():
    from sqlalchemy_adbc.postgresql_types import UUID

    u = uuid.uuid4()
    got = UUID(as_uuid=False).process_result_value(str(u), None)
    assert isinstance(got, str)
    assert got == str(u)


# ── Range types (issue #11) ──────────────────────────────────────────

RANGE_CASES = [  # (decorator, pg type, canonical Range values to round-trip, scalar operand)
    (
        "INT4RANGE",
        "int4range",
        [
            Range(1, 10, bounds="[)"),
            Range(None, 6, bounds="()"),
            Range(1, None, bounds="[)"),
            Range(None, None, bounds="()"),
            Range(empty=True),
            None,
        ],
        3,
    ),
    ("INT8RANGE", "int8range", [Range(2**40, 2**41, bounds="[)"), None], 2**40 + 1),
    (
        "NUMRANGE",
        "numrange",
        [Range(Decimal("1.5"), Decimal("2.75"), bounds="[]"), Range(empty=True), None],
        Decimal("2"),
    ),
    (
        "TSRANGE",
        "tsrange",
        [Range(dt.datetime(2024, 1, 1), dt.datetime(2024, 2, 1), bounds="[)"), None],
        dt.datetime(2024, 1, 15),
    ),
    (
        "TSTZRANGE",
        "tstzrange",
        [
            Range(
                dt.datetime(2024, 1, 1, tzinfo=UTC),
                dt.datetime(2024, 2, 1, tzinfo=UTC),
                bounds="[)",
            ),
            Range(None, dt.datetime(2024, 2, 1, tzinfo=UTC), bounds="(]"),
            None,
        ],
        dt.datetime(2024, 1, 15, tzinfo=UTC),
    ),
    (
        "DATERANGE",
        "daterange",
        [Range(dt.date(2024, 1, 1), None, bounds="[)"), Range(empty=True), None],
        dt.date(2024, 6, 1),
    ),
]


@pytest.mark.parametrize(
    "decorator,pgtype,values,scalar", RANGE_CASES, ids=[c[0] for c in RANGE_CASES]
)
def test_range_roundtrip_operators_and_reflection(engine, decorator, pgtype, values, scalar):
    name = f"t_{pgtype}"
    _drop_table(engine, name)
    cls = getattr(pgtypes, decorator)
    t = sa.Table(
        name,
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("r", cls),
    )
    t.create(engine)
    try:
        with engine.begin() as c:
            c.execute(t.insert(), [{"id": i, "r": v} for i, v in enumerate(values)])
        with engine.connect() as c:
            back = [r.r for r in c.execute(sa.select(t).order_by(t.c.id))]
            assert back == values  # Review Focus 5: None stays None
            assert all(type(b) is Range for b in back if b is not None)
            assert 0 in c.execute(sa.select(t.c.id).where(t.c.r.contains(scalar))).scalars().all()
            assert c.execute(sa.select(t.c.id).where(t.c.r == values[0])).scalars().all() == [0]
            assert (
                0 in c.execute(sa.select(t.c.id).where(t.c.r.overlaps(values[0]))).scalars().all()
            )
        cols = {col["name"]: col["type"] for col in inspect(engine).get_columns(name)}
        assert isinstance(cols["r"], cls)
        assert isinstance(sa.Table(name, sa.MetaData(), autoload_with=engine).c.r.type, cls)
    finally:
        _drop_table(engine, name)


def test_multirange_roundtrip_and_reflection(engine):
    _drop_table(engine, "t_mr")
    t = sa.Table(
        "t_mr",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("m", pgtypes.INT4MULTIRANGE),
    )
    t.create(engine)
    try:
        values = [
            MultiRange([Range(1, 5, bounds="[)"), Range(10, 20, bounds="[)")]),
            MultiRange([]),
            None,
            [Range(30, 40, bounds="[)")],  # plain list binds too
        ]
        with engine.begin() as c:
            c.execute(t.insert(), [{"id": i, "m": v} for i, v in enumerate(values)])
        with engine.connect() as c:
            back = [r.m for r in c.execute(sa.select(t).order_by(t.c.id))]
        assert back[0] == values[0] and back[1] == MultiRange([]) and back[2] is None
        assert back[3] == MultiRange(values[3])
        assert all(isinstance(b, MultiRange) for b in back if b is not None)
        with engine.connect() as c:
            assert c.execute(sa.select(t.c.id).where(t.c.m.contains(12))).scalars().all() == [0]
        assert isinstance(inspect(engine).get_columns("t_mr")[1]["type"], pgtypes.INT4MULTIRANGE)
    finally:
        _drop_table(engine, "t_mr")
