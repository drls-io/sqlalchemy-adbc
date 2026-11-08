"""Unit tests for the fake Snowflake endpoint in ``tests/fake_snowflake``.

These exercise the fake's own pieces (type model, Arrow encoding, SQL
rewriting, catalog answers, HTTP protocol) without the ADBC driver. The
driver-backed checks live in ``test_snowflake_e2e.py`` and
``test_snowflake_auth_e2e.py``.
"""

from __future__ import annotations

import base64
import datetime as dt
import io
import json
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from tests.fake_snowflake.results import SnowflakeType, encode_rowset, infer_type, parse_type


def decode_rowset(b64: str) -> pa.Table:
    return pa.ipc.open_stream(io.BytesIO(base64.b64decode(b64))).read_all()


# ── results.py: type model ───────────────────────────────────────────


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("NUMBER(10,2)", SnowflakeType("NUMBER", 10, 2)),
        ("INT", SnowflakeType("NUMBER", 38, 0)),
        ("INTEGER", SnowflakeType("NUMBER", 38, 0)),
        ("BIGINT", SnowflakeType("NUMBER", 38, 0)),
        ("DECIMAL(5)", SnowflakeType("NUMBER", 5, 0)),
        ("NUMERIC(10, 2)", SnowflakeType("NUMBER", 10, 2)),
        ("VARCHAR(50)", SnowflakeType("TEXT", length=50)),
        ("VARCHAR", SnowflakeType("TEXT", length=16777216)),
        ("CHAR", SnowflakeType("TEXT", length=1)),
        ("CHAR(3)", SnowflakeType("TEXT", length=3)),
        ("STRING", SnowflakeType("TEXT", length=16777216)),
        ("TEXT", SnowflakeType("TEXT", length=16777216)),
        ("FLOAT", SnowflakeType("FLOAT")),
        ("DOUBLE PRECISION", SnowflakeType("FLOAT")),
        ("REAL", SnowflakeType("FLOAT")),
        ("BOOLEAN", SnowflakeType("BOOLEAN")),
        ("DATE", SnowflakeType("DATE")),
        ("TIME", SnowflakeType("TIME", scale=9)),
        ("TIME(3)", SnowflakeType("TIME", scale=3)),
        ("TIMESTAMP", SnowflakeType("TIMESTAMP_NTZ", scale=9)),
        ("DATETIME", SnowflakeType("TIMESTAMP_NTZ", scale=9)),
        ("TIMESTAMP_NTZ(9)", SnowflakeType("TIMESTAMP_NTZ", scale=9)),
        ("TIMESTAMP_TZ", SnowflakeType("TIMESTAMP_TZ", scale=9)),
        ("TIMESTAMP_LTZ(6)", SnowflakeType("TIMESTAMP_LTZ", scale=6)),
        ("BINARY", SnowflakeType("BINARY", length=8388608)),
        ("VARBINARY(16)", SnowflakeType("BINARY", length=16)),
        ("VARIANT", SnowflakeType("VARIANT")),
        ("OBJECT", SnowflakeType("OBJECT")),
        ("ARRAY", SnowflakeType("ARRAY")),
        ("", SnowflakeType("TEXT", length=16777216)),
        ("number ( 12 , 3 )", SnowflakeType("NUMBER", 12, 3)),
    ],
)
def test_parse_type(declared, expected):
    assert parse_type(declared) == expected


def test_xdbc_matches_live_capture():
    assert parse_type("NUMBER(38,0)").xdbc() == {
        "xdbc_type_name": "NUMBER",
        "xdbc_column_size": 38,
        "xdbc_char_octet_length": None,
        "xdbc_decimal_digits": 0,
        "xdbc_num_prec_radix": 10,
        "xdbc_datetime_sub": None,
    }
    assert parse_type("VARCHAR(50)").xdbc() == {
        "xdbc_type_name": "TEXT",
        "xdbc_column_size": 50,
        "xdbc_char_octet_length": 50,
        "xdbc_decimal_digits": None,
        "xdbc_num_prec_radix": None,
        "xdbc_datetime_sub": None,
    }
    assert parse_type("FLOAT").xdbc()["xdbc_column_size"] is None
    assert parse_type("FLOAT").xdbc()["xdbc_num_prec_radix"] == 10
    assert parse_type("TIMESTAMP_NTZ").xdbc()["xdbc_datetime_sub"] == 9
    assert parse_type("BOOLEAN").xdbc()["xdbc_type_name"] == "BOOLEAN"


def test_declared_round_trips():
    for s in [
        "NUMBER(10,2)",
        "VARCHAR(50)",
        "TIMESTAMP_NTZ(9)",
        "FLOAT",
        "VARIANT",
        "BINARY(16)",
        "TIME(9)",
    ]:
        assert parse_type(s).declared() == s


def test_rowtype_type_names():
    assert parse_type("NUMBER").rowtype_type == "fixed"
    assert parse_type("FLOAT").rowtype_type == "real"
    assert parse_type("VARCHAR").rowtype_type == "text"
    assert parse_type("TIMESTAMP_TZ").rowtype_type == "timestamp_tz"
    assert parse_type("OBJECT").rowtype_type == "object"


def test_infer_type_from_values():
    assert infer_type([1, None]) == SnowflakeType("NUMBER", 38, 0)
    assert infer_type([1.5]) == SnowflakeType("FLOAT")
    assert infer_type(["x"]) == SnowflakeType("TEXT", length=16777216)
    assert infer_type([b"\x01"]) == SnowflakeType("BINARY", length=8388608)
    assert infer_type([None, None]) == SnowflakeType("TEXT", length=16777216)
    assert infer_type([]) == SnowflakeType("TEXT", length=16777216)


# ── results.py: Arrow encoding ───────────────────────────────────────


def test_encode_rowset_types_and_metadata():
    cols = [
        ("N", parse_type("NUMBER(10,2)"), [1, "1.50", Decimal("2.25"), None]),
        ("F", parse_type("FLOAT"), [1.5, None, 2, 3.0]),
        ("S", parse_type("VARCHAR(50)"), ["a", None, "c", "d"]),
        ("B", parse_type("BOOLEAN"), [1, 0, "true", None]),
        ("D", parse_type("DATE"), ["2024-01-02", None, "2024-01-03", "2024-01-04"]),
        ("T", parse_type("TIME"), ["03:04:05", "03:04:05.123456", None, "00:00:00"]),
        (
            "TS",
            parse_type("TIMESTAMP_NTZ"),
            ["2024-01-02 03:04:05", "2024-01-02T03:04:05.5", None, "2024-01-02 00:00:00"],
        ),
        (
            "TZ",
            parse_type("TIMESTAMP_TZ"),
            [
                "2024-01-02 03:04:05+02:00",
                "2024-01-02 03:04:05",
                None,
                "2024-01-02 03:04:05-05:30",
            ],
        ),
        ("BI", parse_type("BINARY"), ["0102", b"\x03", None, ""]),
        ("V", parse_type("VARIANT"), ['{"k": 1}', None, "[1]", "null"]),
    ]
    rowtype, b64 = encode_rowset(cols)
    assert [r["type"] for r in rowtype] == [
        "fixed",
        "real",
        "text",
        "boolean",
        "date",
        "time",
        "timestamp_ntz",
        "timestamp_tz",
        "binary",
        "variant",
    ]
    n = {r["name"]: r for r in rowtype}
    assert (n["N"]["precision"], n["N"]["scale"]) == (10, 2)
    assert n["S"]["length"] == 50 and n["S"]["byteLength"] == 50
    assert n["T"]["scale"] == 9 and n["TS"]["scale"] == 9
    assert all(r["nullable"] is True for r in rowtype)

    tbl = decode_rowset(b64)
    assert tbl.num_rows == 4
    assert tbl.schema.field("N").type == pa.decimal128(10, 2)
    assert tbl.column("N").to_pylist() == [Decimal("1.00"), Decimal("1.50"), Decimal("2.25"), None]
    assert tbl.column("F").to_pylist() == [1.5, None, 2.0, 3.0]
    assert tbl.column("B").to_pylist() == [True, False, True, None]
    assert tbl.column("D").to_pylist()[0] == dt.date(2024, 1, 2)
    assert tbl.schema.field("T").type == pa.int64()
    assert tbl.column("T").to_pylist()[1] == (3 * 3600 + 4 * 60 + 5) * 10**9 + 123456000
    ts = tbl.column("TS").to_pylist()
    epoch = int(dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc).timestamp())
    assert ts[0] == {"epoch": epoch, "fraction": 0}
    assert ts[1]["fraction"] == 500_000_000 and ts[2] is None
    tz = tbl.column("TZ").to_pylist()
    assert tz[0]["timezone"] == 1440 + 120
    assert tz[1]["timezone"] == 1440
    assert tz[3]["timezone"] == 1440 - 330
    assert tz[0]["epoch"] == int(
        dt.datetime(2024, 1, 2, 1, 4, 5, tzinfo=dt.timezone.utc).timestamp()
    )
    assert tbl.column("BI").to_pylist() == [b"\x01\x02", b"\x03", None, b""]
    assert tbl.schema.field("V").type == pa.string()
    md = tbl.schema.field("N").metadata
    assert md[b"logicalType"] == b"FIXED" and md[b"scale"] == b"2" and md[b"precision"] == b"10"
    assert tbl.schema.field("S").metadata[b"charLength"] == b"50"
    assert tbl.schema.field("F").metadata[b"logicalType"] == b"REAL"


def test_encode_rowset_all_null_and_empty():
    rowtype, b64 = encode_rowset([("X", parse_type("VARCHAR"), [None, None])])
    assert rowtype[0]["length"] == 16777216
    assert decode_rowset(b64).column("X").null_count == 2
    rowtype, b64 = encode_rowset([("X", parse_type("NUMBER"), [])])
    assert decode_rowset(b64).num_rows == 0 and rowtype[0]["type"] == "fixed"


def test_encode_rowset_nullable_flags():
    rowtype, _ = encode_rowset(
        [("A", parse_type("INT"), [1]), ("B", parse_type("INT"), [1])], nullable=[False, True]
    )
    assert [r["nullable"] for r in rowtype] == [False, True]


# ── sqlengine.py: tokeniser, rewrite, bindings ───────────────────────


def test_tokenize_preserves_quotes_and_strings():
    from tests.fake_snowflake.sqlengine import tokenize

    sql = """select "A b", 'it''s', x -- c\nfrom t /* z */"""
    toks = tokenize(sql)
    kinds = [t.kind for t in toks if t.kind != "space"]
    assert kinds == [
        "word",
        "quoted",
        "punct",
        "string",
        "punct",
        "word",
        "comment",
        "word",
        "word",
        "comment",
    ]
    assert "".join(t.text for t in toks) == sql


def test_ddl_uppercases_bare_identifiers_only():
    from tests.fake_snowflake.sqlengine import rewrite_statement

    sql, pre = rewrite_statement(
        "create table t (id int not null, name varchar(10) comment 'Hi there', \"Mixed\" date)",
        schema="PUBLIC",
        database="DB",
    )
    assert pre == []
    assert sql == 'CREATE TABLE T (ID INT NOT NULL, NAME VARCHAR(10), "Mixed" DATE)'


def test_create_or_replace_transient_becomes_drop_plus_create():
    from tests.fake_snowflake.sqlengine import rewrite_statement

    sql, pre = rewrite_statement(
        "create or replace transient table s.t (x int) cluster by (x)", schema="S", database="DB"
    )
    assert pre == ["DROP TABLE IF EXISTS T"]
    assert sql == "CREATE TABLE T (X INT)"


def test_schema_and_database_qualifiers_are_stripped_everywhere():
    from tests.fake_snowflake.sqlengine import rewrite_statement

    sql, _ = rewrite_statement(
        'SELECT t.id FROM "DB"."PUBLIC"."T" AS t JOIN public.u ON u.id = t.id',
        schema="PUBLIC",
        database="DB",
    )
    assert sql == 'SELECT t.id FROM "T" AS t JOIN u ON u.id = t.id'
    sql, _ = rewrite_statement("INSERT INTO other.t VALUES (?)", schema="PUBLIC", database="DB")
    assert sql == "INSERT INTO other.t VALUES (?)"


def test_identity_and_autoincrement_dropped():
    from tests.fake_snowflake.sqlengine import rewrite_statement

    sql, _ = rewrite_statement(
        "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, n INT AUTOINCREMENT)",
        schema="PUBLIC",
        database="DB",
    )
    assert sql == "CREATE TABLE T (ID INT PRIMARY KEY, N INT)"


def test_statement_kind():
    from tests.fake_snowflake.sqlengine import statement_kind

    assert statement_kind(" select 1") == "SELECT"
    assert statement_kind("ALTER SESSION SET X=1") == "ALTER_SESSION"
    assert statement_kind("alter table t add x int") == "ALTER"
    assert statement_kind("desc table t") == "DESC"
    assert statement_kind("DESCRIBE TABLE t") == "DESC"
    assert statement_kind("-- hi\nSHOW TERSE OBJECTS") == "SHOW"
    assert statement_kind("WITH x AS (SELECT 1) SELECT * FROM x") == "SELECT"
    assert statement_kind("BEGIN") == "BEGIN" and statement_kind("rollback") == "ROLLBACK"
    assert statement_kind("USE SCHEMA X") == "USE" and statement_kind("") == "OTHER"


def test_coerce_bindings_positional_and_named():
    from tests.fake_snowflake.sqlengine import coerce_bindings

    assert coerce_bindings(None) == []
    assert coerce_bindings(
        {"2": {"type": "TEXT", "value": "b"}, "1": {"type": "FIXED", "value": "7"}}
    ) == [7, "b"]
    assert coerce_bindings(
        {
            "1": {"type": "FIXED", "value": "1.50"},
            "2": {"type": "REAL", "value": "2"},
            "3": {"type": "BOOLEAN", "value": "true"},
            "4": {"type": "TEXT", "value": None},
        }
    ) == ["1.50", 2.0, 1, None]
    assert coerce_bindings({"CATALOG": {"type": "TEXT", "value": "%"}}) == {"CATALOG": "%"}


# ── sqlengine.py: session execution ──────────────────────────────────


@pytest.fixture
def session(tmp_path):
    from tests.fake_snowflake.sqlengine import Database

    db = Database(tmp_path / "fake.db")
    s = db.session()
    yield s
    s.close()
    db.close()


def _status(result) -> list[Any]:
    return decode_rowset(result.rowset_b64).column("status").to_pylist()


def test_ddl_dml_select_round_trip(session):
    from tests.fake_snowflake.sqlengine import STATEMENT_DDL, STATEMENT_INSERT, STATEMENT_SELECT

    r = session.execute(
        'CREATE TABLE "T" ("ID" NUMBER(38,0) NOT NULL, "N" NUMBER(10,2), "NAME" VARCHAR(50), '
        '"D" DATE, "OK" BOOLEAN)'
    )
    assert r.statement_type_id == STATEMENT_DDL
    assert _status(r) == ["Table T successfully created."]
    r = session.execute(
        'INSERT INTO "T" VALUES (?, ?, ?, ?, ?)',
        {
            "1": {"type": "FIXED", "value": "1"},
            "2": {"type": "TEXT", "value": "1.50"},
            "3": {"type": "TEXT", "value": "a"},
            "4": {"type": "TEXT", "value": "2024-01-02"},
            "5": {"type": "FIXED", "value": "1"},
        },
    )
    assert r.statement_type_id == STATEMENT_INSERT
    assert r.rowset_json == [["1"]]
    assert r.rowtype[0]["name"] == "number of rows inserted"
    assert decode_rowset(r.rowset_b64).column("number of rows inserted").to_pylist() == [Decimal(1)]
    r = session.execute('SELECT "ID", "N", "NAME", "D", "OK" FROM "T"')
    assert r.statement_type_id == STATEMENT_SELECT and r.total == 1
    assert [c["type"] for c in r.rowtype] == ["fixed", "fixed", "text", "date", "boolean"]
    tbl = decode_rowset(r.rowset_b64)
    assert tbl.column("N").to_pylist() == [Decimal("1.50")]
    assert tbl.column("OK").to_pylist() == [True]
    assert tbl.column("D").to_pylist() == [dt.date(2024, 1, 2)]


def test_select_expression_columns_are_typed(session):
    session.execute("CREATE TABLE T (ID INT)")
    session.execute("INSERT INTO T VALUES (?)", {"1": {"type": "FIXED", "value": "3"}})
    r = session.execute("SELECT count(*) AS c, 1, NULL AS n, 'x' AS s, 1.5 AS f FROM T")
    assert [c["type"] for c in r.rowtype] == ["fixed", "fixed", "text", "text", "real"]
    assert r.rowtype[2]["length"] == 16777216
    tbl = decode_rowset(r.rowset_b64)
    assert tbl.column("c").to_pylist() == [Decimal("1")]
    assert tbl.column("n").to_pylist() == [None]


def test_result_types_follow_declared_columns_through_alias_and_join(session):
    session.execute("CREATE TABLE A (ID INT, TS TIMESTAMP_NTZ)")
    session.execute("CREATE TABLE B (ID INT, V VARIANT)")
    session.execute("INSERT INTO A VALUES (1, '2024-01-02 03:04:05')")
    session.execute("""INSERT INTO B VALUES (1, '{"k": 1}')""")
    r = session.execute("SELECT a.ts, b.v FROM A AS a JOIN B AS b ON a.id = b.id")
    assert [c["type"] for c in r.rowtype] == ["timestamp_ntz", "variant"]
    r = session.execute("SELECT ts AS when_, v FROM a, b")
    assert [c["type"] for c in r.rowtype] == ["timestamp_ntz", "variant"]


def test_update_delete_counts_and_rollback(session):
    session.execute("CREATE TABLE T (ID INT)")
    session.execute("INSERT INTO T VALUES (1), (2), (3)")
    r = session.execute("UPDATE T SET ID = ID + 10 WHERE ID > 1")
    assert r.rowset_json == [["2"]] and r.rowtype[0]["name"] == "number of rows updated"
    session.execute("BEGIN")
    r = session.execute("DELETE FROM T")
    assert r.rowset_json == [["3"]] and r.rowtype[0]["name"] == "number of rows deleted"
    session.execute("ROLLBACK")
    r = session.execute("SELECT count(*) AS c FROM T")
    assert decode_rowset(r.rowset_b64).column("c").to_pylist() == [Decimal("3")]
    session.execute("COMMIT")  # no active transaction: must not raise
    session.execute("ROLLBACK")


def test_alter_session_and_use_are_noops(session):
    r = session.execute("ALTER SESSION SET AUTOCOMMIT = false")
    assert _status(r) == ["Statement executed successfully."]
    session.execute("USE SCHEMA PUBLIC")


def test_drop_table_status_and_truncate(session):
    session.execute("CREATE TABLE T (ID INT)")
    session.execute("INSERT INTO T VALUES (1)")
    session.execute("TRUNCATE TABLE T")
    r = session.execute("SELECT count(*) AS c FROM T")
    assert decode_rowset(r.rowset_b64).column("c").to_pylist() == [Decimal("0")]
    assert _status(session.execute("DROP TABLE IF EXISTS t")) == ["T successfully dropped."]


def test_snowflake_function_shims(session):
    r = session.execute(
        """SELECT PARSE_JSON('{"k": [1, 2, 3]}') AS v, OBJECT_CONSTRUCT('a', 'b') AS o,
        ARRAY_CONSTRUCT(1, 2) AS a, TO_BINARY('0102', 'HEX') AS b,
        CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sc"""
    )
    tbl = decode_rowset(r.rowset_b64)
    assert tbl.column("v").to_pylist() == ['{"k": [1, 2, 3]}']
    assert tbl.column("o").to_pylist() == ['{"a": "b"}']
    assert tbl.column("a").to_pylist() == ["[1, 2]"]
    assert tbl.column("b").to_pylist() == [b"\x01\x02"]
    assert tbl.column("db").to_pylist() == ["FAKEDB"]
    assert tbl.column("sc").to_pylist() == ["PUBLIC"]
    assert [c["type"] for c in r.rowtype][:4] == ["variant", "object", "array", "binary"]


def test_errors_map_to_snowflake_codes(session):
    from tests.fake_snowflake.sqlengine import SnowflakeError

    with pytest.raises(SnowflakeError) as e:
        session.execute("SELECT * FROM NOPE")
    assert e.value.code == "002003" and e.value.sql_state == "42S02" and "NOPE" in str(e.value)
    with pytest.raises(SnowflakeError) as e:
        session.execute("SELEC 1")
    assert e.value.code == "001003" and e.value.sql_state == "42000"


# ── sqlengine.py: catalog answers ────────────────────────────────────


def test_ilike_escape_and_wildcards():
    from tests.fake_snowflake.sqlengine import ilike

    assert ilike("ABC", "a%") and ilike("A_B", "A!_B") and not ilike("AXB", "A!_B")
    assert ilike("AXB", "A_B") and ilike("anything", "%") and not ilike("", "_")
    assert ilike("50%", "50!%") and not ilike("500", "50!%")
    assert ilike("a.b", "a.b") and not ilike("aXb", "a.b")
    assert ilike("A\\B", "A\\B", escape=None)


def _setup_catalog(session):
    session.execute(
        'CREATE TABLE "P" ("ID" NUMBER(38,0) NOT NULL PRIMARY KEY, "CODE" VARCHAR(10), UNIQUE ("CODE"))'
    )
    session.execute(
        'CREATE TABLE "C" ("ID" INT PRIMARY KEY, "PID" INT REFERENCES "P" ("ID"), "V" VARIANT, '
        '"TS" TIMESTAMP_TZ, "S" VARCHAR(50) NOT NULL)'
    )
    session.execute('CREATE VIEW "VW" AS SELECT "ID", "CODE" FROM "P"')


def test_show_terse_objects_and_schemas(session):
    _setup_catalog(session)
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ OBJECTS IN ACCOUNT")
    tbl = decode_rowset(r.rowset_b64)
    assert [c["name"] for c in r.rowtype] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]
    assert sorted(
        zip(tbl.column("name").to_pylist(), tbl.column("kind").to_pylist(), strict=True)
    ) == [
        ("C", "TABLE"),
        ("P", "TABLE"),
        ("VW", "VIEW"),
    ]
    assert set(tbl.column("schema_name").to_pylist()) == {"PUBLIC"}
    assert set(tbl.column("database_name").to_pylist()) == {"FAKEDB"}
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ TABLES LIKE 'P' IN SCHEMA FAKEDB.PUBLIC")
    assert decode_rowset(r.rowset_b64).column("name").to_pylist() == ["P"]
    r = session.execute('SHOW TERSE /* ADBC:getObjects */ TABLES IN SCHEMA "FAKEDB"."PUBLIC"')
    assert sorted(decode_rowset(r.rowset_b64).column("name").to_pylist()) == ["C", "P"]
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ VIEWS IN DATABASE FAKEDB STARTS WITH 'V'")
    assert decode_rowset(r.rowset_b64).column("name").to_pylist() == ["VW"]
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ OBJECTS IN SCHEMA FAKEDB.OTHER")
    assert r.total == 0
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ OBJECTS LIKE 'c' IN ACCOUNT")
    assert decode_rowset(r.rowset_b64).column("name").to_pylist() == ["C"]
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ SCHEMAS IN ACCOUNT")
    assert sorted(decode_rowset(r.rowset_b64).column("name").to_pylist()) == [
        "INFORMATION_SCHEMA",
        "PUBLIC",
    ]
    assert [c["name"] for c in r.rowtype] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ SCHEMAS LIKE 'PUBLIC' IN ACCOUNT")
    assert decode_rowset(r.rowset_b64).column("name").to_pylist() == ["PUBLIC"]
    r = session.execute("SHOW TERSE /* ADBC:getObjects */ DATABASES IN ACCOUNT")
    assert decode_rowset(r.rowset_b64).column("name").to_pylist() == ["FAKEDB"]


def test_show_keys_and_columns_return_empty_typed_results(session):
    for sql in [
        "SHOW PRIMARY KEYS /* ADBC:getObjectsTables */ IN ACCOUNT",
        "SHOW UNIQUE KEYS /* ADBC:getObjectsTables */ IN SCHEMA FAKEDB.PUBLIC",
        "SHOW IMPORTED KEYS /* ADBC:getObjectsTables */ IN ACCOUNT",
        "SHOW COLUMNS /* ADBC:getObjects */ IN ACCOUNT",
    ]:
        r = session.execute(sql)
        assert r.total == 0 and r.rowtype
    r = session.execute("SHOW IMPORTED KEYS /* ADBC:getObjectsTables */ IN ACCOUNT")
    assert [c["name"] for c in r.rowtype][:4] == [
        "fk_database_name",
        "fk_schema_name",
        "fk_table_name",
        "fk_name",
    ]


GET_OBJECTS_ALL = (
    "-- Licensed to the Apache Software Foundation (ASF) under one\n"
    "WITH columns AS (SELECT table_catalog, ARRAY_AGG({'column_name': column_name}) table_columns, "
    "FROM information_schema.columns WHERE table_catalog ILIKE :CATALOG AND table_schema ILIKE :DB_SCHEMA "
    "AND table_name ILIKE :TABLE AND column_name ILIKE :COLUMN GROUP BY table_catalog) "
    "SELECT { 'catalog_name': database_name, 'catalog_db_schemas': ARRAY_AGG(NULLIF({ "
    "'db_schema_name': schema_name, 'db_schema_tables': db_schema_tables }, {})) } get_objects "
    "FROM information_schema.databases LEFT JOIN db_schemas ON database_name = catalog_name "
    "WHERE database_name ILIKE :CATALOG GROUP BY database_name;"
)
GET_OBJECTS_TABLES = (
    'WITH tables AS (SELECT "database_name" "catalog_name", ARRAY_AGG({ \'table_name\': "name", '
    "'table_type': \"kind\", 'table_constraints': null, 'table_columns': null }) db_schema_tables "
    "FROM TABLE(RESULT_SCAN(:SHOW_TABLE_QUERY_ID)) WHERE \"database_name\" ILIKE :CATALOG ESCAPE '!' "
    "GROUP BY \"database_name\") SELECT { 'catalog_name': \"name\", 'catalog_db_schemas': ARRAY_AGG(NULLIF({ "
    "'db_schema_name': db_schemas.\"schema_name\", 'db_schema_tables': db_schema_tables }, {})) } get_objects "
    'FROM TABLE(RESULT_SCAN(:SHOW_DB_QUERY_ID)) WHERE "name" ILIKE :CATALOG ESCAPE \'!\' GROUP BY "name";'
)
GET_OBJECTS_SCHEMAS = (
    'WITH db_schemas AS (SELECT "database_name" as "catalog_name", "name" as "schema_name" '
    "FROM table(RESULT_SCAN(:SHOW_SCHEMA_QUERY_ID))) SELECT { 'catalog_name': \"database_name\", "
    "'catalog_db_schemas': ARRAY_AGG(NULLIF({ 'db_schema_name': \"schema_name\", 'db_schema_tables': null }, {})) } "
    'get_objects FROM db_info WHERE "database_name" ILIKE :CATALOG ESCAPE \'!\' GROUP BY "database_name";'
)
GET_OBJECTS_CATALOGS = (
    "SELECT { 'catalog_name': \"name\", 'catalog_db_schemas': null } get_objects "
    "FROM TABLE(RESULT_SCAN(:SHOW_DB_QUERY_ID)) WHERE \"name\" ILIKE :CATALOG ESCAPE '!'"
)


def _get_objects(session, sql, **kw):
    binds = {
        "CATALOG": "%",
        "DB_SCHEMA": "%",
        "TABLE": "%",
        "COLUMN": "%",
        "PK_QUERY_ID": "x",
        "FK_QUERY_ID": "x",
        "UNIQUE_QUERY_ID": "x",
        "SHOW_DB_QUERY_ID": "x",
        "SHOW_SCHEMA_QUERY_ID": "x",
        "SHOW_TABLE_QUERY_ID": "x",
        "SHOW_COLUMNS_QUERY_ID": "x",
    }
    binds.update(kw)
    r = session.execute(sql, {k: {"type": "TEXT", "value": v} for k, v in binds.items()})
    assert r.rowtype[0]["name"] == "get_objects" and r.rowtype[0]["type"] == "object"
    return [json.loads(s) for s in decode_rowset(r.rowset_b64).column("get_objects").to_pylist()]


def test_get_objects_depths(session):
    _setup_catalog(session)
    assert _get_objects(session, GET_OBJECTS_CATALOGS) == [
        {"catalog_name": "FAKEDB", "catalog_db_schemas": None}
    ]
    assert _get_objects(session, GET_OBJECTS_CATALOGS, CATALOG="fake%") == [
        {"catalog_name": "FAKEDB", "catalog_db_schemas": None}
    ]
    [cat] = _get_objects(session, GET_OBJECTS_SCHEMAS)
    assert sorted(s["db_schema_name"] for s in cat["catalog_db_schemas"]) == [
        "INFORMATION_SCHEMA",
        "PUBLIC",
    ]
    assert all(s["db_schema_tables"] is None for s in cat["catalog_db_schemas"])
    [cat] = _get_objects(session, GET_OBJECTS_TABLES, DB_SCHEMA="PUBLIC")
    [sch] = cat["catalog_db_schemas"]
    assert sorted((t["table_name"], t["table_type"]) for t in sch["db_schema_tables"]) == [
        ("C", "BASE TABLE"),
        ("P", "BASE TABLE"),
        ("VW", "VIEW"),
    ]
    assert sch["db_schema_tables"][0]["table_columns"] is None
    assert sch["db_schema_tables"][0]["table_constraints"] is None


def test_get_objects_all_columns_and_constraints(session):
    _setup_catalog(session)
    [cat] = _get_objects(session, GET_OBJECTS_ALL, DB_SCHEMA="PUBLIC", TABLE="C")
    [sch] = cat["catalog_db_schemas"]
    [tab] = sch["db_schema_tables"]
    cols = {c["column_name"]: c for c in tab["table_columns"]}
    assert [c["ordinal_position"] for c in tab["table_columns"]] == [1, 2, 3, 4, 5]
    assert cols["ID"]["xdbc_type_name"] == "NUMBER" and cols["ID"]["xdbc_column_size"] == 38
    assert cols["ID"]["xdbc_decimal_digits"] == 0 and cols["ID"]["xdbc_is_nullable"] == "NO"
    assert cols["ID"]["xdbc_nullable"] == 0 and cols["ID"]["remarks"] is None
    assert cols["S"]["xdbc_type_name"] == "TEXT" and cols["S"]["xdbc_column_size"] == 50
    assert cols["S"]["xdbc_is_nullable"] == "NO"
    assert cols["V"]["xdbc_type_name"] == "VARIANT"
    assert cols["TS"]["xdbc_type_name"] == "TIMESTAMP_TZ" and cols["PID"]["xdbc_nullable"] == 1
    by_type = {c["constraint_type"]: c for c in tab["table_constraints"]}
    assert by_type["PRIMARY KEY"]["constraint_column_names"] == ["ID"]
    assert by_type["PRIMARY KEY"]["constraint_column_usage"] == []
    assert by_type["FOREIGN KEY"]["constraint_column_names"] == ["PID"]
    assert by_type["FOREIGN KEY"]["constraint_column_usage"] == [
        {"fk_catalog": "FAKEDB", "fk_db_schema": "PUBLIC", "fk_table": "P", "fk_column_name": "ID"}
    ]
    assert all(c["constraint_name"] for c in tab["table_constraints"])
    [cat] = _get_objects(session, GET_OBJECTS_ALL, DB_SCHEMA="PUBLIC", TABLE="P")
    [tab] = cat["catalog_db_schemas"][0]["db_schema_tables"]
    assert {
        c["constraint_type"]: c["constraint_column_names"] for c in tab["table_constraints"]
    } == {
        "PRIMARY KEY": ["ID"],
        "UNIQUE": ["CODE"],
    }
    [cat] = _get_objects(session, GET_OBJECTS_ALL, DB_SCHEMA="PUBLIC", TABLE="VW")
    [tab] = cat["catalog_db_schemas"][0]["db_schema_tables"]
    assert tab["table_type"] == "VIEW"
    assert [c["column_name"] for c in tab["table_columns"]] == ["ID", "CODE"]
    assert tab["table_constraints"] == []
    assert _get_objects(session, GET_OBJECTS_ALL, DB_SCHEMA="PUBLIC", TABLE="nope") == [
        {
            "catalog_name": "FAKEDB",
            "catalog_db_schemas": [{"db_schema_name": "PUBLIC", "db_schema_tables": []}],
        }
    ]
    [cat] = _get_objects(session, GET_OBJECTS_ALL, DB_SCHEMA="PUBLIC", TABLE="c")
    assert cat["catalog_db_schemas"][0]["db_schema_tables"][0]["table_name"] == "C"
    assert _get_objects(session, GET_OBJECTS_ALL, CATALOG="OTHER") == []
    [cat] = _get_objects(session, GET_OBJECTS_ALL, DB_SCHEMA="PUBLIC", TABLE="C", COLUMN="S")
    assert [
        c["column_name"]
        for c in cat["catalog_db_schemas"][0]["db_schema_tables"][0]["table_columns"]
    ] == ["S"]


def test_desc_table_layout(session):
    from tests.fake_snowflake.sqlengine import SnowflakeError

    _setup_catalog(session)
    r = session.execute('DESC TABLE "FAKEDB"."PUBLIC"."C"')
    assert [c["name"] for c in r.rowtype] == [
        "name",
        "type",
        "kind",
        "null?",
        "default",
        "primary key",
        "unique key",
        "check",
        "expression",
        "comment",
        "policy name",
        "privacy domain",
    ]
    tbl = decode_rowset(r.rowset_b64)
    rows = list(
        zip(
            tbl.column("name").to_pylist(),
            tbl.column("type").to_pylist(),
            tbl.column("null?").to_pylist(),
            tbl.column("primary key").to_pylist(),
            strict=True,
        )
    )
    assert rows[0] == ("ID", "NUMBER(38,0)", "N", "Y")
    assert rows[4] == ("S", "VARCHAR(50)", "N", "N")
    assert rows[2][1] == "VARIANT" and tbl.column("kind").to_pylist()[0] == "COLUMN"
    with pytest.raises(SnowflakeError) as e:
        session.execute("DESC TABLE NOPE")
    assert e.value.code == "002003"
