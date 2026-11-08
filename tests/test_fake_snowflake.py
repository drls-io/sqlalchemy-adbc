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
from decimal import Decimal

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
