"""Reflection tests — ADBC ``get_objects`` → SQLAlchemy Inspector.

Tests run against an in-memory ADBC SQLite DB. SQLite is the only
driver currently exercised in CI because its DBAPI wheel is small and
self-contained; PG/Snowflake/BQ paths share the same reflection code
and are manually smoke-tested.
"""

from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect


def _make_engine():
    return sa.create_engine("adbc+sqlite:///:memory:")


def _seed(engine) -> None:
    """Create a small schema: 2 tables, 1 PK, 1 FK, 1 unique constraint."""
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE users ("
            "  id INTEGER PRIMARY KEY,"
            "  email TEXT NOT NULL UNIQUE,"
            "  name TEXT,"
            "  created_at TIMESTAMP"
            ")"
        )
        conn.exec_driver_sql(
            "CREATE TABLE posts ("
            "  id INTEGER PRIMARY KEY,"
            "  user_id INTEGER NOT NULL,"
            "  body TEXT,"
            "  FOREIGN KEY(user_id) REFERENCES users(id)"
            ")"
        )
        conn.exec_driver_sql("CREATE INDEX idx_posts_user ON posts(user_id)")


# ── get_table_names ──────────────────────────────────────────────────


def test_get_table_names_returns_user_tables():
    engine = _make_engine()
    _seed(engine)
    names = inspect(engine).get_table_names()
    assert set(names) >= {"users", "posts"}


def test_get_table_names_excludes_indexes():
    """SQLite exposes ``sqlite_autoindex_*`` as table_type='index'; they
    must not appear in the table list."""
    engine = _make_engine()
    _seed(engine)
    names = inspect(engine).get_table_names()
    assert not any(n.startswith("sqlite_autoindex") for n in names)
    assert "idx_posts_user" not in names


def test_get_table_names_empty_db():
    engine = _make_engine()
    assert inspect(engine).get_table_names() == []


# ── get_columns ──────────────────────────────────────────────────────


def test_get_columns_basic():
    engine = _make_engine()
    _seed(engine)
    cols = inspect(engine).get_columns("users")
    by_name = {c["name"]: c for c in cols}
    assert set(by_name) == {"id", "email", "name", "created_at"}


def test_get_columns_type_mapping():
    engine = _make_engine()
    _seed(engine)
    by_name = {c["name"]: c for c in inspect(engine).get_columns("users")}
    # SQLite reports INTEGER for id, TEXT for email/name
    assert isinstance(by_name["id"]["type"], sa.Integer)
    assert isinstance(by_name["email"]["type"], (sa.Text, sa.String))
    assert isinstance(by_name["created_at"]["type"], sa.DateTime)


def test_get_columns_unknown_table_returns_empty():
    engine = _make_engine()
    _seed(engine)
    assert inspect(engine).get_columns("does_not_exist") == []


# ── get_pk_constraint ────────────────────────────────────────────────


def test_get_pk_constraint():
    engine = _make_engine()
    _seed(engine)
    pk = inspect(engine).get_pk_constraint("users")
    assert pk["constrained_columns"] == ["id"]


def test_get_pk_no_pk():
    engine = _make_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE flat (a INTEGER, b TEXT)")
    pk = inspect(engine).get_pk_constraint("flat")
    assert pk["constrained_columns"] == []


# ── get_foreign_keys ─────────────────────────────────────────────────


def test_get_foreign_keys():
    engine = _make_engine()
    _seed(engine)
    fks = inspect(engine).get_foreign_keys("posts")
    assert len(fks) == 1
    fk = fks[0]
    assert fk["constrained_columns"] == ["user_id"]
    assert fk["referred_table"] == "users"
    assert fk["referred_columns"] == ["id"]


def test_get_foreign_keys_empty():
    engine = _make_engine()
    _seed(engine)
    assert inspect(engine).get_foreign_keys("users") == []


# ── get_unique_constraints ───────────────────────────────────────────


def test_get_unique_constraints():
    """SQLite reports UNIQUE at the column level; it may or may not
    surface as a named constraint in ADBC's GetObjects output. We just
    assert the call doesn't error and returns a list."""
    engine = _make_engine()
    _seed(engine)
    result = inspect(engine).get_unique_constraints("users")
    assert isinstance(result, list)


# ── get_indexes (SQLite native via PRAGMA) ───────────────────────────


def test_get_indexes_finds_user_index():
    """``CREATE INDEX idx_posts_user ON posts(user_id)`` → one entry."""
    engine = _make_engine()
    _seed(engine)
    indexes = inspect(engine).get_indexes("posts")
    names = {i["name"] for i in indexes}
    assert "idx_posts_user" in names
    by_name = {i["name"]: i for i in indexes}
    assert by_name["idx_posts_user"]["column_names"] == ["user_id"]
    assert by_name["idx_posts_user"]["unique"] is False


def test_get_indexes_excludes_sqlite_autoindex():
    """``sqlite_autoindex_<table>_<N>`` entries back PK / UNIQUE
    constraints and are already surfaced via get_pk_constraint and
    get_unique_constraints — they must not appear in get_indexes."""
    engine = _make_engine()
    _seed(engine)
    indexes = inspect(engine).get_indexes("users")
    assert all(not i["name"].startswith("sqlite_autoindex_") for i in indexes)


def test_get_indexes_unique_index_included():
    """User-created ``CREATE UNIQUE INDEX`` stays visible with unique=True.
    This is the SQLAlchemy convention — unique indexes appear in both
    get_indexes (as unique=True) and get_unique_constraints (as a
    distinct constraint entry)."""
    engine = _make_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (a INTEGER, b INTEGER)")
        conn.exec_driver_sql("CREATE UNIQUE INDEX uq_ab ON t(a, b)")
    indexes = inspect(engine).get_indexes("t")
    by_name = {i["name"]: i for i in indexes}
    assert "uq_ab" in by_name
    assert by_name["uq_ab"]["unique"] is True
    assert by_name["uq_ab"]["column_names"] == ["a", "b"]


def test_get_indexes_multi_column_ordering():
    """Composite index column order must match the CREATE INDEX order,
    not alphabetical or hash order."""
    engine = _make_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (x INTEGER, y INTEGER, z INTEGER)")
        conn.exec_driver_sql("CREATE INDEX idx_zyx ON t(z, y, x)")
    indexes = inspect(engine).get_indexes("t")
    by_name = {i["name"]: i for i in indexes}
    assert by_name["idx_zyx"]["column_names"] == ["z", "y", "x"]


def test_get_indexes_empty_when_no_indexes():
    engine = _make_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE flat (a INTEGER, b TEXT)")
    assert inspect(engine).get_indexes("flat") == []


def test_get_indexes_identifier_escaping():
    """Table names with double quotes or SQL keywords must not break
    PRAGMA — they're inlined (PRAGMA can't parameterize), so escaping
    is a security property not just a correctness one."""
    engine = _make_engine()
    # SQLite allows "order" as a delimited identifier — the word is
    # reserved but the quoted form works. If our escaper is buggy the
    # PRAGMA call would error.
    with engine.begin() as conn:
        conn.exec_driver_sql('CREATE TABLE "order" (id INTEGER PRIMARY KEY, x INTEGER)')
        conn.exec_driver_sql('CREATE INDEX idx_order_x ON "order"(x)')
    indexes = inspect(engine).get_indexes("order")
    by_name = {i["name"]: i for i in indexes}
    assert "idx_order_x" in by_name
    assert by_name["idx_order_x"]["column_names"] == ["x"]


def test_sqlite_ident_escaping_unit():
    """``_sqlite_ident`` wraps in double-quotes and doubles internal ones.
    Guards against SQL injection at the PRAGMA layer regardless of
    whether the caller passed a trusted name."""
    from sqlalchemy_adbc.sqlite import _sqlite_ident

    assert _sqlite_ident("foo") == '"foo"'
    assert _sqlite_ident("with spaces") == '"with spaces"'
    assert _sqlite_ident('a"b') == '"a""b"'
    # The classic "); DROP TABLE -- style payload becomes a single
    # (harmless) delimited identifier once escaped.
    assert _sqlite_ident('"; DROP TABLE users; --') == '"""; DROP TABLE users; --"'


# ── get_indexes (PG override — structural test, no live DB) ──────────


def test_pg_get_indexes_query_structure():
    """The PG override query must filter by table name, support a NULL
    schema ("any schema"), exclude the primary-key index, preserve
    column order via LATERAL unnest + WITH ORDINALITY, and group by
    uniqueness. Test the SQL as a string so CI doesn't need a live PG."""
    from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect

    # Capture the SQL + params the dialect would issue.
    captured: dict[str, Any] = {}

    class _FakeCursor:
        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            captured["sql"] = sql
            captured["params"] = params

        def fetchall(self) -> list[Any]:
            return []

        def close(self) -> None:
            pass

    class _FakeConn:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _FakePoolProxy:
        driver_connection = _FakeConn()

    ADBCPostgreSQLDialect().get_indexes(_FakePoolProxy(), "my_table", schema="public")

    sql = captured["sql"].lower()
    assert "pg_index" in sql
    assert "pg_class" in sql
    assert "pg_attribute" in sql
    assert "with ordinality" in sql
    assert "array_agg" in sql
    assert "not ix.indisprimary" in sql
    assert captured["params"] == ("my_table", "public")


def test_pg_get_indexes_null_schema():
    """``schema=None`` must not filter — let the DB's search_path pick."""
    from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect

    captured: dict[str, Any] = {}

    class _FakeCursor:
        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            captured["params"] = params

        def fetchall(self) -> list[Any]:
            return []

        def close(self) -> None:
            pass

    class _FakeConn:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    class _FakePoolProxy:
        driver_connection = _FakeConn()

    ADBCPostgreSQLDialect().get_indexes(_FakePoolProxy(), "t")
    assert captured["params"] == ("t", None)


def test_pg_to_list_normalizes_shapes():
    """``array_agg`` results can come back as list / tuple / None; the
    projection must normalize to plain list in all cases."""
    from sqlalchemy_adbc.postgresql import _to_list

    assert _to_list(None) == []
    assert _to_list([]) == []
    assert _to_list(["a", "b"]) == ["a", "b"]
    assert _to_list(("a", "b")) == ["a", "b"]
    # Any iterable works — simulates a pyarrow-backed scalar.
    assert _to_list(iter(["a", "b"])) == ["a", "b"]


# ── has_table ────────────────────────────────────────────────────────


def test_has_table_present():
    engine = _make_engine()
    _seed(engine)
    assert inspect(engine).has_table("users") is True


def test_has_table_absent():
    engine = _make_engine()
    _seed(engine)
    assert inspect(engine).has_table("nonexistent") is False


# ── MetaData.reflect end-to-end ──────────────────────────────────────


def test_metadata_reflect_full_schema():
    """Top-level integration check: ``MetaData.reflect()`` pulls the
    whole schema through reflection and builds SA Table objects with
    the right columns + FK graph."""
    engine = _make_engine()
    _seed(engine)
    md = sa.MetaData()
    md.reflect(bind=engine)

    assert set(md.tables) >= {"users", "posts"}
    users = md.tables["users"]
    posts = md.tables["posts"]

    # Columns on `users`
    assert {c.name for c in users.columns} == {"id", "email", "name", "created_at"}
    # PK
    assert [c.name for c in users.primary_key.columns] == ["id"]
    # FK posts.user_id → users.id
    fks = list(posts.foreign_keys)
    assert len(fks) == 1
    fk = fks[0]
    assert fk.column.table.name == "users"
    assert fk.column.name == "id"


@pytest.mark.xfail(
    reason="ADBC SQLite driver reports xdbc_is_nullable='YES' for every "
    "column, including NOT NULL ones — upstream limitation in "
    "adbc_driver_sqlite GetObjects. Reflection code is correct; this "
    "test unblocks automatically when the driver is fixed.",
    strict=True,
)
def test_metadata_reflect_column_nullability():
    """NOT NULL constraints round-trip through reflection."""
    engine = _make_engine()
    _seed(engine)
    md = sa.MetaData()
    md.reflect(bind=engine)
    users = md.tables["users"]
    by_name = {c.name: c for c in users.columns}
    assert by_name["email"].nullable is False
    assert by_name["name"].nullable is True


# ── Type mapping unit tests (no DB) ──────────────────────────────────


def test_adbc_type_to_sqla_known():
    from sqlalchemy_adbc.reflection import adbc_type_to_sqla

    assert isinstance(adbc_type_to_sqla("INTEGER"), sa.Integer)
    assert isinstance(adbc_type_to_sqla("TEXT"), sa.Text)
    assert isinstance(adbc_type_to_sqla("VARCHAR"), sa.String)
    assert isinstance(adbc_type_to_sqla("BIGINT"), sa.BigInteger)
    assert isinstance(adbc_type_to_sqla("BOOLEAN"), sa.Boolean)
    assert isinstance(adbc_type_to_sqla("TIMESTAMP"), sa.DateTime)
    assert isinstance(adbc_type_to_sqla("DATE"), sa.Date)
    assert isinstance(adbc_type_to_sqla("JSONB"), sa.JSON)


def test_adbc_type_to_sqla_strips_precision():
    from sqlalchemy_adbc.reflection import adbc_type_to_sqla

    assert isinstance(adbc_type_to_sqla("VARCHAR(255)"), sa.String)
    assert isinstance(adbc_type_to_sqla("NUMERIC(10, 2)"), sa.Numeric)


def test_adbc_type_to_sqla_unknown_returns_nulltype():
    from sqlalchemy_adbc.reflection import adbc_type_to_sqla

    assert isinstance(adbc_type_to_sqla("SOMETHING_WEIRD"), sa.types.NullType)
    assert isinstance(adbc_type_to_sqla(None), sa.types.NullType)
    assert isinstance(adbc_type_to_sqla(""), sa.types.NullType)


# ── Tree walker unit tests (no DB) ───────────────────────────────────


def test_find_table_matches_by_name_and_schema():
    from sqlalchemy_adbc.reflection import find_table

    tree = [
        {
            "catalog_name": "main",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "public",
                    "db_schema_tables": [
                        {"table_name": "users", "table_type": "table"},
                        {"table_name": "users_idx", "table_type": "index"},
                    ],
                },
                {
                    "db_schema_name": "other",
                    "db_schema_tables": [
                        {"table_name": "users", "table_type": "table"},
                    ],
                },
            ],
        }
    ]
    got = find_table(tree, "users", schema="public")
    assert got is not None
    assert got["table_name"] == "users"
    # schema narrows match
    got = find_table(tree, "users", schema="other")
    assert got is not None
    # missing returns None
    assert find_table(tree, "missing") is None


def test_find_table_skips_indexes():
    """An index with the same name as a table should not win."""
    from sqlalchemy_adbc.reflection import find_table

    tree = [
        {
            "catalog_db_schemas": [
                {
                    "db_schema_name": "",
                    "db_schema_tables": [
                        {"table_name": "foo", "table_type": "index"},
                        {"table_name": "foo", "table_type": "table"},
                    ],
                }
            ]
        }
    ]
    got = find_table(tree, "foo")
    assert got is not None
    assert got["table_type"] == "table"
