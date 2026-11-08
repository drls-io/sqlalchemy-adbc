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

Result column labels are whatever SQLite reports, so textual SQL such as
``select id from t`` yields ``id`` here where Snowflake would yield ``ID``.
Compiled SQLAlchemy statements match columns positionally and are
unaffected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from tests.fake_snowflake.results import _to_bool

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
            return s  # exact decimal text; SQLite keeps it as given
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
