"""SQLite-backed statement execution for the fake Snowflake endpoint.

Snowflake SQL is run by SQLite after a light rewrite: unquoted identifiers
in DDL are uppercased (Snowflake folds them; SQLite keeps what it is given),
Snowflake-only clauses (``OR REPLACE``, ``TRANSIENT``, ``COMMENT``,
``CLUSTER BY``, ``IDENTITY``/``AUTOINCREMENT``) are dropped, and a
``<schema>.`` or ``<database>.<schema>.`` qualifier naming the session's
schema is removed because SQLite has a single namespace. Declared column
types survive untouched: SQLite accepts any type name, and
``PRAGMA table_xinfo`` hands it back verbatim, which is what the catalog
answers are built from.

Known fidelity limits (keep this list and the README paragraph in step; do
not "fix" a test to match the fake where they disagree with Snowflake):

- NUMBER values are stored under SQLite's NUMERIC affinity, i.e. as int64 or
  double. Values beyond 15 significant digits (or outside int64) cannot be
  represented; reading one raises a ``100040`` data conversion error instead
  of returning a mangled Decimal.
- Result column labels are whatever SQLite reports, so textual SQL such as
  ``select id from t`` yields ``id`` here where Snowflake would yield ``ID``.
  Compiled SQLAlchemy statements match columns positionally and are
  unaffected.
- Snowflake-only SQL rules are not reproduced: ``PARSE_JSON(?)`` inside
  ``VALUES`` is accepted here and rejected by Snowflake, ``DEFAULT
  CURRENT_TIMESTAMP()`` with parentheses is rejected here and accepted there.
"""

from __future__ import annotations

import base64
import json
import os
import re
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, Literal

from tests.fake_snowflake.results import (
    MAX_BINARY,
    MAX_TEXT,
    SnowflakeType,
    _to_bool,
    encode_rowset,
    infer_type,
    parse_type,
)

TokenKind = Literal["word", "quoted", "string", "punct", "space", "comment"]


@dataclass(frozen=True)
class Token:
    kind: TokenKind
    text: str

    @property
    def upper(self) -> str:
        return self.text.upper()

    def ident(self) -> str | None:
        """Identifier text: bare words uppercased, quoted names unquoted."""
        if self.kind == "word":
            return self.text.upper()
        if self.kind == "quoted":
            return self.text[1:-1].replace('""', '"')
        return None


_TOKEN_RE = re.compile(
    r"(?P<comment>--[^\n]*|/\*.*?\*/)"
    r"|(?P<string>'(?:[^']|'')*')"
    r"|(?P<quoted>\"(?:[^\"]|\"\")*\")"
    r"|(?P<word>[A-Za-z_][A-Za-z0-9_$]*|\d+(?:\.\d+)?)"
    r"|(?P<space>\s+)"
    r"|(?P<punct>.)",
    re.DOTALL,
)


def tokenize(sql: str) -> list[Token]:
    tokens: list[Token] = []
    for m in _TOKEN_RE.finditer(sql):
        kind = m.lastgroup
        assert kind is not None
        tokens.append(Token(kind, m.group()))  # type: ignore[arg-type]
    return tokens


def _significant(tokens: list[Token]) -> list[Token]:
    return [t for t in tokens if t.kind not in ("space", "comment")]


_KINDS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "USE",
    "SHOW",
    "TRUNCATE",
    "MERGE",
}


def statement_kind(sql: str) -> str:
    words = [t.upper for t in _significant(tokenize(sql)) if t.kind == "word"]
    if not words:
        return "OTHER"
    first = words[0]
    if first == "ALTER" and len(words) > 1 and words[1] == "SESSION":
        return "ALTER_SESSION"
    if first in ("DESC", "DESCRIBE"):
        return "DESC"
    if first == "WITH":
        return "SELECT"
    if first == "START":  # START TRANSACTION
        return "BEGIN"
    return first if first in _KINDS else "OTHER"


# ── rewriting ────────────────────────────────────────────────────────


def _strip_qualifiers(tokens: list[Token], names: set[str]) -> list[Token]:
    """Remove ``db.schema.`` / ``schema.`` prefixes naming the session scope."""
    out: list[Token] = []
    i = 0
    n = len(tokens)
    while i < n:
        t = tokens[i]
        ident = t.ident()
        if ident is not None and ident.upper() in names and i + 2 < n and tokens[i + 1].text == ".":
            nxt = tokens[i + 2]
            if nxt.kind in ("word", "quoted"):
                # ``db.schema.x`` → drop ``db.`` now; the loop drops ``schema.`` next.
                # ``schema.x`` → drop ``schema.``.
                i += 2
                continue
        out.append(t)
        i += 1
    return out


_DDL_DROP_WORDS = {"TRANSIENT", "TEMPORARY", "TEMP", "VOLATILE"}


def _skip_parens(tokens: list[Token], i: int) -> int:
    """Index just past the ``( ... )`` group starting at a non-space token ``i``."""
    depth = 0
    while i < len(tokens):
        if tokens[i].text == "(":
            depth += 1
        elif tokens[i].text == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return i


def _next_index(tokens: list[Token], i: int) -> int:
    """Index of the next significant token at or after ``i``."""
    while i < len(tokens) and tokens[i].kind in ("space", "comment"):
        i += 1
    return i


def _rewrite_ddl(tokens: list[Token]) -> tuple[list[Token], list[str]]:
    out: list[Token] = []
    pre: list[str] = []
    or_replace = False
    i = 0
    n = len(tokens)
    while i < n:
        t = tokens[i]
        if t.kind != "word":
            out.append(t)
            i += 1
            continue
        word = t.upper
        if word == "OR":
            j = _next_index(tokens, i + 1)
            if j < n and tokens[j].upper == "REPLACE":
                or_replace = True
                i = _next_index(tokens, j + 1)
                continue
        if word in _DDL_DROP_WORDS:
            i = _next_index(tokens, i + 1)
            continue
        if word == "COMMENT":
            j = _next_index(tokens, i + 1)
            if j < n and tokens[j].text == "=":
                j = _next_index(tokens, j + 1)
            if j < n and tokens[j].kind == "string":
                i = _next_index(tokens, j + 1)
                continue
        if word == "CLUSTER":
            j = _next_index(tokens, i + 1)
            if j < n and tokens[j].upper == "BY":
                k = _next_index(tokens, j + 1)
                i = _next_index(tokens, _skip_parens(tokens, k))
                continue
        if word in ("AUTOINCREMENT", "IDENTITY"):
            j = _next_index(tokens, i + 1)
            if j < n and tokens[j].text == "(":
                j = _next_index(tokens, _skip_parens(tokens, j))
            # START n INCREMENT m [ORDER|NOORDER]
            while j < n and tokens[j].upper in ("START", "INCREMENT"):
                j = _next_index(tokens, j + 1)
                if j < n and tokens[j].text in ("=", "WITH"):
                    j = _next_index(tokens, j + 1)
                j = _next_index(tokens, j + 1)
            if j < n and tokens[j].upper in ("ORDER", "NOORDER"):
                j = _next_index(tokens, j + 1)
            i = j
            continue
        out.append(Token("word", word))
        i += 1

    out = _normalise_column_types(out)
    if or_replace:
        words = [t for t in out if t.kind in ("word", "quoted")]
        # CREATE <TABLE|VIEW> [IF NOT EXISTS] <name>
        if len(words) >= 3 and words[0].text == "CREATE":
            obj = words[1].text
            name_idx = 2
            if len(words) > 5 and words[2].text == "IF":
                name_idx = 5
            if name_idx < len(words):
                pre.append(f"DROP {obj} IF EXISTS {words[name_idx].text}")
    return out, pre


_COLUMN_CONSTRAINT_WORDS = {
    "NOT",
    "NULL",
    "DEFAULT",
    "PRIMARY",
    "UNIQUE",
    "REFERENCES",
    "CHECK",
    "COLLATE",
    "CONSTRAINT",
    "GENERATED",
    "AS",
    "WITH",
    "MASKING",
    "TAG",
    "INCREMENT",
}
_TABLE_CONSTRAINT_WORDS = {"CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK"}


def _normalise_column_types(tokens: list[Token]) -> list[Token]:
    """In ``CREATE TABLE``, rewrite each column's declared type to Snowflake's
    canonical spelling (what ``DESC TABLE`` shows) in the form that gives SQLite
    the right storage affinity."""
    sig = [i for i, t in enumerate(tokens) if t.kind not in ("space", "comment")]
    words = [tokens[i].upper for i in sig[:2]]
    if words != ["CREATE", "TABLE"]:
        return tokens
    try:
        open_idx = next(i for i in sig if tokens[i].text == "(")
    except StopIteration:
        return tokens
    close_idx = _skip_parens(tokens, open_idx) - 1
    # Split the column list at top-level commas.
    items: list[list[int]] = [[]]
    depth = 0
    for i in range(open_idx + 1, close_idx):
        t = tokens[i]
        if t.text == "(":
            depth += 1
        elif t.text == ")":
            depth -= 1
        if depth == 0 and t.text == ",":
            items.append([])
            continue
        items[-1].append(i)
    replacements: dict[int, str] = {}
    removed: set[int] = set()
    for item in items:
        sig_item = [i for i in item if tokens[i].kind not in ("space", "comment")]
        if len(sig_item) < 2 or tokens[sig_item[0]].kind not in ("word", "quoted"):
            continue
        if tokens[sig_item[0]].upper in _TABLE_CONSTRAINT_WORDS:
            continue
        type_idx: list[int] = []
        k = 1
        while k < len(sig_item):
            tok = tokens[sig_item[k]]
            if tok.kind == "word" and tok.upper in _COLUMN_CONSTRAINT_WORDS:
                break
            if tok.text == "(":
                end = _skip_parens(tokens, sig_item[k])
                type_idx.extend(i for i in sig_item[k:] if i < end)
                k = next((n for n, i in enumerate(sig_item) if i >= end), len(sig_item))
                continue
            type_idx.append(sig_item[k])
            k += 1
        if not type_idx:
            continue
        declared = "".join(tokens[i].text for i in range(type_idx[0], type_idx[-1] + 1))
        replacements[type_idx[0]] = parse_type(declared).sqlite_declared()
        removed.update(type_idx[1:])
        removed.update(i for i in range(type_idx[0], type_idx[-1] + 1) if tokens[i].kind == "space")
    out: list[Token] = []
    for i, t in enumerate(tokens):
        if i in replacements:
            out.append(Token("word", replacements[i]))
        elif i not in removed:
            out.append(t)
    return out


def _render(tokens: list[Token]) -> str:
    """Join tokens, collapsing runs of whitespace and tidying around punctuation."""
    parts: list[str] = []
    for t in tokens:
        if t.kind == "space":
            if parts and parts[-1] != " " and parts[-1] != "(":
                parts.append(" ")
            continue
        if t.text in (",", ")") and parts and parts[-1] == " ":
            parts.pop()
        parts.append(t.text)
    return "".join(parts).strip()


def rewrite_statement(sql: str, *, schema: str, database: str) -> tuple[str, list[str]]:
    """→ (statement for SQLite, statements to run before it)."""
    tokens = _strip_qualifiers(tokenize(sql), {schema.upper(), database.upper()})
    kind = statement_kind(sql)
    pre: list[str] = []
    if kind in ("CREATE", "DROP", "ALTER", "TRUNCATE"):
        tokens, pre = _rewrite_ddl(tokens)
        if kind == "TRUNCATE":
            words = [t for t in tokens if t.kind in ("word", "quoted")]
            name = words[-1].text if words else ""
            return f"DELETE FROM {name}", pre
        return _render(tokens), pre
    return "".join(t.text for t in tokens), pre


# ── bindings ─────────────────────────────────────────────────────────


def _coerce_value(binding: dict[str, Any]) -> Any:
    value = binding.get("value")
    if value is None:
        return None
    btype = str(binding.get("type", "TEXT")).upper()
    if btype == "FIXED":
        s = str(value)
        if any(ch in s for ch in ".eE"):
            # Decimal text. Under SQLite's NUMERIC affinity the column stores
            # it as a double, so precision is capped at ~15 significant
            # digits; results.py refuses to decode anything beyond that.
            return s
        return int(s)
    if btype == "REAL":
        return float(value)
    if btype == "BOOLEAN":
        return 1 if _to_bool(value) else 0
    return str(value)


def coerce_bindings(bindings: dict[str, dict[str, Any]] | None) -> list[Any] | dict[str, Any]:
    """Driver ``bindings`` → sqlite3 parameters (positional list or named dict)."""
    if not bindings:
        return []
    if all(key.isdigit() for key in bindings):
        return [_coerce_value(bindings[key]) for key in sorted(bindings, key=int)]
    return {key: _coerce_value(b) for key, b in bindings.items()}


# ── execution ────────────────────────────────────────────────────────

STATEMENT_SELECT = 0x1000
STATEMENT_INSERT = 0x3100
STATEMENT_UPDATE = 0x3200
STATEMENT_DELETE = 0x3300
STATEMENT_DDL = 0x6000

_STATUS_TYPE = SnowflakeType("TEXT", length=MAX_TEXT)
_COUNT_TYPE = SnowflakeType("NUMBER", 19, 0)

# Result type of a select-list expression that is a call to one of the shims.
_FUNCTION_TYPES: dict[str, SnowflakeType] = {
    "PARSE_JSON": SnowflakeType("VARIANT"),
    "TO_VARIANT": SnowflakeType("VARIANT"),
    "TO_JSON": SnowflakeType("TEXT", length=MAX_TEXT),
    "OBJECT_CONSTRUCT": SnowflakeType("OBJECT"),
    "ARRAY_CONSTRUCT": SnowflakeType("ARRAY"),
    "TO_BINARY": SnowflakeType("BINARY", length=MAX_BINARY),
    "CURRENT_DATE": SnowflakeType("DATE"),
    "CURRENT_TIMESTAMP": SnowflakeType("TIMESTAMP_LTZ", scale=9),
}

_ALIAS_STOP_WORDS = {
    "ON",
    "WHERE",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "CROSS",
    "FULL",
    "NATURAL",
    "GROUP",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "HAVING",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "USING",
    "SET",
    "VALUES",
    "SELECT",
    "DEFAULT",
    "RETURNING",
    "WINDOW",
    "FETCH",
    "FOR",
}


class SnowflakeError(Exception):
    """An error in Snowflake's JSON shape: ``code`` (6 digits) and SQLSTATE."""

    def __init__(self, message: str, code: str = "001003", sql_state: str = "42000") -> None:
        super().__init__(message)
        self.code = code
        self.sql_state = sql_state


@dataclass
class Result:
    """One query answer.

    ``rowset_json`` set means the answer goes on the wire in Snowflake's JSON
    result format, which is what Snowflake uses for DML and DDL. The driver
    treats a non-empty ``rowset`` as "read JSON, not Arrow", so the server
    never sends both; ``rowset_b64`` is still filled for the fake's own tests.
    """

    rowtype: list[dict[str, Any]]
    rowset_b64: str
    statement_type_id: int
    total: int
    rowset_json: list[list[str | None]] | None = None


def status_result(message: str, statement_type_id: int = STATEMENT_DDL) -> Result:
    rowtype, b64 = encode_rowset([("status", _STATUS_TYPE, [message])], nullable=[False])
    return Result(rowtype, b64, statement_type_id, 1, rowset_json=[[message]])


def dml_result(label: str, count: int, statement_type_id: int) -> Result:
    rowtype, b64 = encode_rowset([(label, _COUNT_TYPE, [count])], nullable=[False])
    return Result(rowtype, b64, statement_type_id, 1, rowset_json=[[str(count)]])


def _json_value(value: Any) -> Any:
    """Shim argument → JSON value (strings holding JSON are parsed)."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _parse_json(text: Any) -> str | None:
    if text is None:
        return None
    try:
        return json.dumps(json.loads(text))
    except ValueError as exc:
        raise SnowflakeError(f"Error parsing JSON: {exc}", "100069", "22P02") from None


def _object_construct(*args: Any) -> str:
    pairs = zip(args[0::2], args[1::2], strict=False)
    return json.dumps({str(k): _json_value(v) for k, v in pairs if v is not None})


def _array_construct(*args: Any) -> str:
    return json.dumps([_json_value(a) for a in args])


def _to_variant(value: Any) -> str | None:
    return None if value is None else json.dumps(_json_value(value))


def _to_binary(value: Any, fmt: str = "HEX") -> str | None:
    if value is None:
        return None
    fmt = fmt.upper()
    if fmt == "HEX":
        return str(value).upper()
    if fmt == "UTF-8" or fmt == "UTF8":
        return str(value).encode().hex().upper()
    if fmt == "BASE64":
        return base64.b64decode(str(value)).hex().upper()
    raise SnowflakeError(f"Unsupported TO_BINARY format {fmt}")


class Database:
    """One SQLite file standing in for a Snowflake database."""

    def __init__(self, path: str | os.PathLike[str], *, name: str = "FAKEDB") -> None:
        self.path = str(path)
        self.name = name.upper()
        self.lock = threading.RLock()
        self._sessions: list[Session] = []

    def session(self, schema: str = "PUBLIC") -> Session:
        s = Session(self, schema)
        self._sessions.append(s)
        return s

    def close(self) -> None:
        for s in list(self._sessions):
            s.close()


_DDL_NAME_RE = re.compile(
    r'^(CREATE|DROP)\s+(TABLE|VIEW)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?("(?:[^"]|"")*"|[^\s(]+)',
    re.IGNORECASE,
)


def _unquote(ident: str) -> str:
    if ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1].replace('""', '"')
    return ident.upper()


class Session:
    """One Snowflake session: its own sqlite3 connection, shared file."""

    def __init__(self, database: Database, schema: str) -> None:
        self.database = database
        self.schema = schema.upper()
        self.conn = sqlite3.connect(
            database.path, isolation_level=None, check_same_thread=False, timeout=5
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._register_functions()
        self.closed = False

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.conn.close()
            if self in self.database._sessions:
                self.database._sessions.remove(self)

    def _register_functions(self) -> None:
        c = self.conn
        c.create_function("PARSE_JSON", 1, _parse_json)
        c.create_function("TRY_PARSE_JSON", 1, _parse_json)
        c.create_function("TO_VARIANT", 1, _to_variant)
        c.create_function("TO_JSON", 1, lambda v: v)
        c.create_function("OBJECT_CONSTRUCT", -1, _object_construct)
        c.create_function("ARRAY_CONSTRUCT", -1, _array_construct)
        c.create_function("TO_BINARY", -1, _to_binary)
        constants = {
            "CURRENT_DATABASE": self.database.name,
            "CURRENT_SCHEMA": self.schema,
            "CURRENT_WAREHOUSE": "WH",
            "CURRENT_ROLE": "PUBLIC",
            "CURRENT_USER": "CI",
            "CURRENT_ACCOUNT": "FAKEACCT",
            "CURRENT_REGION": "FAKE_REGION",
            "CURRENT_VERSION": "0.0.0 (fake)",
        }
        for name, value in constants.items():
            c.create_function(name, 0, lambda v=value: v)

    # ── dispatch ─────────────────────────────────────────────────────

    def execute(self, sql: str, bindings: dict[str, Any] | None = None) -> Result:
        kind = statement_kind(sql)
        with self.database.lock:
            if kind in ("ALTER_SESSION", "USE"):
                return status_result("Statement executed successfully.")
            if kind in ("BEGIN", "COMMIT", "ROLLBACK"):
                return self._transaction(kind)
            if kind == "SHOW":
                return Catalog(self).show(sql)
            if kind == "DESC":
                return Catalog(self).describe(sql)
            params = coerce_bindings(bindings)
            if kind == "SELECT" and is_get_objects_query(sql):
                return Catalog(self).get_objects(sql, params if isinstance(params, dict) else {})
            return self._run_sql(sql, kind, params)

    def _transaction(self, kind: str) -> Result:
        try:
            self.conn.execute(kind)
        except sqlite3.OperationalError as exc:
            if "transaction" not in str(exc):
                raise _translate(exc) from None
        return status_result("Statement executed successfully.")

    def _run_sql(self, sql: str, kind: str, params: list[Any] | dict[str, Any]) -> Result:
        statement, pre = rewrite_statement(sql, schema=self.schema, database=self.database.name)
        try:
            for extra in pre:
                self.conn.execute(extra)
            cursor = self.conn.execute(statement, params)
        except sqlite3.Error as exc:
            raise _translate(exc) from None
        if cursor.description is not None:
            return self._rows_result(statement, cursor)
        if kind == "INSERT":
            return dml_result("number of rows inserted", cursor.rowcount, STATEMENT_INSERT)
        if kind == "UPDATE":
            return dml_result("number of rows updated", cursor.rowcount, STATEMENT_UPDATE)
        if kind in ("DELETE", "TRUNCATE"):
            return dml_result("number of rows deleted", cursor.rowcount, STATEMENT_DELETE)
        return status_result(_ddl_message(statement))

    # ── row results ──────────────────────────────────────────────────

    def table_columns(self, table: str) -> dict[str, tuple[SnowflakeType, bool]]:
        """Declared column types of ``table`` (upper-cased names), ``{}`` if unknown."""
        quoted = '"' + table.replace('"', '""') + '"'
        try:
            rows = self.conn.execute(f"PRAGMA table_xinfo({quoted})").fetchall()
        except sqlite3.Error:
            return {}
        return {
            str(r[1]).upper(): (parse_type(str(r[2] or "")), not r[3]) for r in rows if r[6] != 2
        }

    def _rows_result(self, statement: str, cursor: sqlite3.Cursor) -> Result:
        names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        columns = [[row[i] for row in rows] for i in range(len(names))]
        tables = _referenced_tables(statement)
        declared: dict[str, dict[str, tuple[SnowflakeType, bool]]] = {
            t: self.table_columns(t) for t in set(tables.values())
        }
        by_name: dict[str, tuple[SnowflakeType, bool]] = {}
        for cols in declared.values():
            for cname, info in cols.items():
                by_name.setdefault(cname, info)
        exprs = _select_expressions(statement)
        typed: list[tuple[str, SnowflakeType, list[Any]]] = []
        nullable: list[bool] = []
        for i, name in enumerate(names):
            found: tuple[SnowflakeType, bool] | None = None
            if i < len(exprs):
                found = _expression_type(exprs[i], tables, declared, by_name)
            if found is None:
                found = by_name.get(name.upper())
            info = found if found is not None else (infer_type(columns[i]), True)
            typed.append((name, info[0], columns[i]))
            nullable.append(info[1])
        try:
            rowtype, b64 = encode_rowset(typed, nullable=nullable)
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise SnowflakeError(
                f"Data conversion error (fake Snowflake precision or format limit): {exc}",
                "100040",
                "22007",
            ) from None
        return Result(rowtype, b64, STATEMENT_SELECT, len(rows))


def _translate(exc: sqlite3.Error) -> SnowflakeError:
    msg = str(exc)
    if "no such table" in msg or "no such column" in msg or "no such view" in msg:
        return SnowflakeError(
            f"SQL compilation error:\n{msg}; object does not exist or not authorized.",
            "002003",
            "42S02",
        )
    if isinstance(exc, sqlite3.IntegrityError):
        return SnowflakeError(f"{msg}", "100072", "22000")
    return SnowflakeError(f"SQL compilation error:\n{msg}", "001003", "42000")


def _ddl_message(statement: str) -> str:
    m = _DDL_NAME_RE.match(statement.strip())
    if not m:
        return "Statement executed successfully."
    verb, obj, name = m.group(1).upper(), m.group(2).capitalize(), _unquote(m.group(3))
    if verb == "CREATE":
        return f"{obj} {name} successfully created."
    return f"{name} successfully dropped."


def _referenced_tables(statement: str) -> dict[str, str]:
    """``{alias_or_name: table_name}`` for tables after FROM/JOIN/INTO/UPDATE."""
    toks = _significant(tokenize(statement))
    tables: dict[str, str] = {}
    i = 0
    n = len(toks)
    while i < n:
        if toks[i].upper in ("FROM", "JOIN", "INTO", "UPDATE") and toks[i].kind == "word":
            i += 1
            while i < n:
                if toks[i].text == "(":
                    i = _skip_parens(toks, i)
                    break
                name = toks[i].ident()
                if name is None:
                    break
                i += 1
                alias = name
                if i < n and toks[i].upper == "AS":
                    i += 1
                if (
                    i < n
                    and toks[i].kind in ("word", "quoted")
                    and toks[i].upper not in _ALIAS_STOP_WORDS
                ):
                    alias = toks[i].ident() or alias
                    i += 1
                tables[alias.upper()] = name
                tables.setdefault(name.upper(), name)
                if i < n and toks[i].text == "," and toks[i - 1].upper != "FROM":
                    i += 1
                    continue
                break
        else:
            i += 1
    return tables


def _select_expressions(statement: str) -> list[list[Token]]:
    """Top-level select-list expressions (alias clauses included), or ``[]``."""
    toks = _significant(tokenize(statement))
    depth = 0
    start = None
    for i, t in enumerate(toks):
        if t.text == "(":
            depth += 1
        elif t.text == ")":
            depth -= 1
        elif depth == 0 and t.kind == "word" and t.upper == "SELECT":
            start = i + 1
            break
    if start is None:
        return []
    if start < len(toks) and toks[start].upper in ("DISTINCT", "ALL"):
        start += 1
    exprs: list[list[Token]] = []
    current: list[Token] = []
    depth = 0
    for t in toks[start:]:
        if t.text == "(":
            depth += 1
        elif t.text == ")":
            depth -= 1
        if (
            depth == 0
            and t.kind == "word"
            and t.upper in ("FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "UNION")
        ):
            break
        if depth == 0 and t.text == ",":
            exprs.append(current)
            current = []
            continue
        current.append(t)
    if current:
        exprs.append(current)
    return exprs


def _expression_type(
    expr: list[Token],
    tables: dict[str, str],
    declared: dict[str, dict[str, tuple[SnowflakeType, bool]]],
    by_name: dict[str, tuple[SnowflakeType, bool]],
) -> tuple[SnowflakeType, bool] | None:
    # Drop a trailing ``AS alias``.
    if len(expr) >= 3 and expr[-2].kind == "word" and expr[-2].upper == "AS":
        expr = expr[:-2]
    if not expr:
        return None
    idents = [t.ident() for t in expr]
    if len(expr) == 1 and idents[0] is not None and expr[0].kind in ("word", "quoted"):
        return by_name.get(idents[0].upper())
    if len(expr) == 3 and expr[1].text == "." and idents[0] and idents[2]:
        table = tables.get(idents[0].upper())
        if table is not None:
            return declared.get(table, {}).get(idents[2].upper())
        return by_name.get(idents[2].upper())
    if expr[0].kind == "word" and len(expr) > 1 and expr[1].text == "(":
        t = _FUNCTION_TYPES.get(expr[0].upper)
        if t is not None:
            return (t, True)
    if expr[0].kind == "word" and expr[0].upper in _FUNCTION_TYPES and len(expr) == 1:
        return (_FUNCTION_TYPES[expr[0].upper], True)
    return None


# ── catalog ──────────────────────────────────────────────────────────


def ilike(value: str, pattern: str, escape: str | None = "!") -> bool:
    """Snowflake ``ILIKE`` (``%``, ``_``, optional ``ESCAPE`` character)."""
    regex = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if escape and ch == escape and i + 1 < len(pattern):
            regex.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        if ch == "%":
            regex.append(".*")
        elif ch == "_":
            regex.append(".")
        else:
            regex.append(re.escape(ch))
        i += 1
    return re.fullmatch("".join(regex), value, re.IGNORECASE | re.DOTALL) is not None


@dataclass
class ColumnInfo:
    name: str
    type: SnowflakeType
    nullable: bool
    position: int
    default: str | None = None


@dataclass
class TableInfo:
    name: str
    kind: str  # TABLE | VIEW
    columns: list[ColumnInfo]
    primary_key: list[str]
    uniques: list[list[str]]
    foreign_keys: list[
        tuple[list[str], str, list[str]]
    ]  # (columns, referred table, referred columns)

    @property
    def table_type(self) -> str:
        return "BASE TABLE" if self.kind == "TABLE" else "VIEW"


_SHOW_RE = re.compile(
    r"^SHOW\s+(?:TERSE\s+)?(?P<obj>DATABASES|SCHEMAS|OBJECTS|TABLES|VIEWS|PRIMARY\s+KEYS|UNIQUE\s+KEYS|"
    r"IMPORTED\s+KEYS|EXPORTED\s+KEYS|COLUMNS)"
    r"(?:\s+LIKE\s+'(?P<like>(?:[^']|'')*)')?"
    r"(?:\s+IN\s+(?P<scope>ACCOUNT|DATABASE|SCHEMA|TABLE)(?:\s+(?P<target>[^\s]+))?)?"
    r"(?:\s+STARTS\s+WITH\s+'(?P<starts>(?:[^']|'')*)')?"
    r"(?:\s+LIMIT\s+\d+)?\s*;?$",
    re.IGNORECASE | re.DOTALL,
)

_EMPTY_KEYS_COLUMNS = [
    "created_on",
    "database_name",
    "schema_name",
    "table_name",
    "column_name",
    "key_sequence",
    "constraint_name",
    "rely",
    "comment",
]
_EMPTY_FK_COLUMNS = [
    "fk_database_name",
    "fk_schema_name",
    "fk_table_name",
    "fk_name",
    "fk_column_name",
    "pk_database_name",
    "pk_schema_name",
    "pk_table_name",
    "pk_column_name",
    "key_sequence",
]
_EMPTY_COLUMNS_COLUMNS = ["database_name", "schema_name", "table_name", "column_name", "data_type"]
_SHOW_TERSE_COLUMNS = ["created_on", "name", "kind", "database_name", "schema_name"]
_DESC_COLUMNS = [
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
_CREATED_ON = "2026-01-01 00:00:00.000 -0800"


def _text_table(names: list[str], rows: list[list[Any]]) -> Result:
    columns = [(n, _STATUS_TYPE, [r[i] for r in rows]) for i, n in enumerate(names)]
    rowtype, b64 = encode_rowset(columns)
    return Result(rowtype, b64, STATEMENT_SELECT, len(rows))


def _strip_comments(sql: str) -> str:
    return "".join(t.text for t in tokenize(sql) if t.kind != "comment").strip()


def _split_qualified(target: str) -> list[str]:
    toks = [t for t in tokenize(target) if t.kind in ("word", "quoted")]
    return [t.ident() or "" for t in toks]


def is_get_objects_query(sql: str) -> bool:
    lowered = sql.lower()
    return "get_objects" in lowered and (
        "result_scan" in lowered or "information_schema" in lowered
    )


class Catalog:
    """Metadata answers (SHOW, DESC, GetObjects) computed from SQLite's own schema."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self.database = session.database.name
        self.schema = session.schema

    @property
    def schemas(self) -> list[str]:
        return [self.schema, "INFORMATION_SCHEMA"]

    # ── introspection ────────────────────────────────────────────────

    def tables(self) -> list[TableInfo]:
        conn = self.session.conn
        rows = conn.execute(
            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        return [
            self._table(str(name), "TABLE" if kind == "table" else "VIEW") for name, kind in rows
        ]

    def table(self, name: str) -> TableInfo | None:
        for t in self.tables():
            if t.name.upper() == name.upper():
                return t
        return None

    def _table(self, name: str, kind: str) -> TableInfo:
        conn = self.session.conn
        quoted = '"' + name.replace('"', '""') + '"'
        columns: list[ColumnInfo] = []
        pk: list[tuple[int, str]] = []
        for row in conn.execute(f"PRAGMA table_xinfo({quoted})").fetchall():
            cid, cname, ctype, notnull, dflt, pkpos, hidden = row[:7]
            if hidden == 2:
                continue
            columns.append(
                ColumnInfo(
                    str(cname),
                    parse_type(str(ctype or "")),
                    nullable=not notnull and not pkpos,
                    position=len(columns) + 1,
                    default=None if dflt is None else str(dflt),
                )
            )
            if pkpos:
                pk.append((int(pkpos), str(cname)))
        uniques: list[list[str]] = []
        if kind == "TABLE":
            for idx in conn.execute(f"PRAGMA index_list({quoted})").fetchall():
                if idx[3] == "u":
                    info = conn.execute(f'PRAGMA index_info("{idx[1]}")').fetchall()
                    uniques.append([str(r[2]) for r in sorted(info, key=lambda r: r[0])])
        fks: dict[int, tuple[list[str], str, list[str | None]]] = {}
        if kind == "TABLE":
            for row in conn.execute(f"PRAGMA foreign_key_list({quoted})").fetchall():
                fk_id, _seq, ref_table, col_from, col_to = (
                    row[0],
                    row[1],
                    str(row[2]),
                    str(row[3]),
                    row[4],
                )
                entry = fks.setdefault(int(fk_id), ([], ref_table, []))
                entry[0].append(col_from)
                entry[2].append(None if col_to is None else str(col_to))
        foreign_keys: list[tuple[list[str], str, list[str]]] = []
        for cols, ref_table, ref_cols in fks.values():
            if any(c is None for c in ref_cols):
                ref = self.table(ref_table)
                ref_cols = list(ref.primary_key) if ref else []
            foreign_keys.append((cols, ref_table, [c for c in ref_cols if c is not None]))
        return TableInfo(
            name=name,
            kind=kind,
            columns=columns,
            primary_key=[c for _, c in sorted(pk)],
            uniques=uniques,
            foreign_keys=foreign_keys,
        )

    # ── SHOW ─────────────────────────────────────────────────────────

    def show(self, sql: str) -> Result:
        m = _SHOW_RE.match(_strip_comments(sql))
        if not m:
            raise SnowflakeError(f"SQL compilation error:\nunsupported SHOW statement: {sql!r}")
        obj = " ".join(m.group("obj").upper().split())
        like = (m.group("like") or "").replace("''", "'") or None
        starts = (m.group("starts") or "").replace("''", "'") or None
        scope = (m.group("scope") or "ACCOUNT").upper()
        target = _split_qualified(m.group("target") or "")
        if obj in ("PRIMARY KEYS", "UNIQUE KEYS"):
            return _text_table(_EMPTY_KEYS_COLUMNS, [])
        if obj in ("IMPORTED KEYS", "EXPORTED KEYS"):
            return _text_table(_EMPTY_FK_COLUMNS, [])
        if obj == "COLUMNS":
            return _text_table(_EMPTY_COLUMNS_COLUMNS, [])

        def wanted(name: str) -> bool:
            if like is not None and not ilike(name, like, escape=None):
                return False
            return starts is None or name.startswith(starts)

        in_db = scope == "ACCOUNT" or (len(target) >= 1 and target[0].upper() == self.database)
        rows: list[list[Any]] = []
        if obj == "DATABASES":
            if wanted(self.database):
                rows.append([_CREATED_ON, self.database, "STANDARD", self.database, None])
        elif obj == "SCHEMAS":
            if in_db and scope in ("ACCOUNT", "DATABASE"):
                rows = [[_CREATED_ON, s, None, self.database, s] for s in self.schemas if wanted(s)]
        else:
            schema_ok = True
            if scope == "SCHEMA":
                parts = target
                schema_ok = len(parts) >= 1 and parts[-1].upper() == self.schema
                if len(parts) == 2:
                    schema_ok = schema_ok and parts[0].upper() == self.database
            if in_db and schema_ok:
                kinds = {"OBJECTS": ("TABLE", "VIEW"), "TABLES": ("TABLE",), "VIEWS": ("VIEW",)}[
                    obj
                ]
                rows = [
                    [_CREATED_ON, t.name, t.kind, self.database, self.schema]
                    for t in self.tables()
                    if t.kind in kinds and wanted(t.name)
                ]
        return _text_table(_SHOW_TERSE_COLUMNS, rows)

    # ── DESC ─────────────────────────────────────────────────────────

    def describe(self, sql: str) -> Result:
        words = [t for t in _significant(tokenize(sql)) if t.kind in ("word", "quoted")]
        name = words[-1].ident() if words else None
        table = self.table(name) if name else None
        if table is None:
            raise SnowflakeError(
                f"SQL compilation error:\nTable '{name}' does not exist or not authorized.",
                "002003",
                "42S02",
            )
        unique_cols = {c for u in table.uniques for c in u}
        rows = [
            [
                c.name,
                c.type.declared(),
                "COLUMN",
                "Y" if c.nullable else "N",
                c.default,
                "Y" if c.name in table.primary_key else "N",
                "Y" if c.name in unique_cols else "N",
                None,
                None,
                None,
                None,
                None,
            ]
            for c in table.columns
        ]
        return _text_table(_DESC_COLUMNS, rows)

    # ── GetObjects ───────────────────────────────────────────────────

    def get_objects(self, sql: str, bindings: dict[str, Any]) -> Result:
        squashed = " ".join(sql.split())
        if "'catalog_db_schemas': null" in squashed:
            depth = "catalogs"
        elif "'db_schema_tables': null" in squashed:
            depth = "db_schemas"
        elif "'table_columns': null" in squashed:
            depth = "tables"
        else:
            depth = "all"
        escape = "!" if "ESCAPE '!'" in squashed.upper() else None

        def pattern(key: str) -> str:
            value = bindings.get(key)
            return "%" if value is None else str(value)

        def match(name: str, key: str) -> bool:
            return ilike(name, pattern(key), escape=escape)

        docs: list[str] = []
        if not match(self.database, "CATALOG"):
            return _get_objects_result(docs)
        catalog: dict[str, Any] = {"catalog_name": self.database, "catalog_db_schemas": None}
        if depth != "catalogs":
            schemas = []
            for s in self.schemas:
                if not match(s, "DB_SCHEMA"):
                    continue
                entry: dict[str, Any] = {"db_schema_name": s, "db_schema_tables": None}
                if depth != "db_schemas":
                    entry["db_schema_tables"] = (
                        [
                            self._table_doc(t, depth, match)
                            for t in self.tables()
                            if match(t.name, "TABLE")
                        ]
                        if s == self.schema
                        else []
                    )
                schemas.append(entry)
            catalog["catalog_db_schemas"] = schemas
        docs.append(json.dumps(catalog))
        return _get_objects_result(docs)

    def _table_doc(self, t: TableInfo, depth: str, match: Any) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "table_name": t.name,
            "table_type": t.table_type,
            "table_columns": None,
            "table_constraints": None,
        }
        if depth != "all":
            return doc
        doc["table_columns"] = [
            {
                "column_name": c.name,
                "ordinal_position": c.position,
                "remarks": None,
                **c.type.xdbc(),
                "xdbc_is_nullable": "YES" if c.nullable else "NO",
                "xdbc_nullable": 1 if c.nullable else 0,
            }
            for c in t.columns
            if match(c.name, "COLUMN")
        ]
        constraints: list[dict[str, Any]] = []
        if t.primary_key:
            constraints.append(
                {
                    "constraint_name": f"SYS_CONSTRAINT_{t.name}_PK",
                    "constraint_type": "PRIMARY KEY",
                    "constraint_column_names": list(t.primary_key),
                    "constraint_column_usage": [],
                }
            )
        for i, cols in enumerate(t.uniques, 1):
            constraints.append(
                {
                    "constraint_name": f"SYS_CONSTRAINT_{t.name}_UK{i}",
                    "constraint_type": "UNIQUE",
                    "constraint_column_names": cols,
                    "constraint_column_usage": [],
                }
            )
        for i, (cols, ref_table, ref_cols) in enumerate(t.foreign_keys, 1):
            constraints.append(
                {
                    "constraint_name": f"SYS_CONSTRAINT_{t.name}_FK{i}",
                    "constraint_type": "FOREIGN KEY",
                    "constraint_column_names": cols,
                    "constraint_column_usage": [
                        {
                            "fk_catalog": self.database,
                            "fk_db_schema": self.schema,
                            "fk_table": ref_table,
                            "fk_column_name": rc,
                        }
                        for rc in ref_cols
                    ],
                }
            )
        doc["table_constraints"] = constraints
        return doc


def _get_objects_result(docs: list[str]) -> Result:
    rowtype, b64 = encode_rowset([("get_objects", SnowflakeType("OBJECT"), docs)])
    return Result(rowtype, b64, STATEMENT_SELECT, len(docs))
