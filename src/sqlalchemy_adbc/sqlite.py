"""ADBC SQLite dialect — mostly useful for tests and local prototyping."""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine.interfaces import ReflectedIndex
from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect


class ADBCSQLiteDialect(ADBCDialect):
    name = "adbc"
    driver = "sqlite"
    driver_module = "adbc_driver_sqlite.dbapi"
    supports_statement_cache = True

    # SQLite's DBAPI is qmark; matches our base default.
    paramstyle = "qmark"

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        # adbc_driver_sqlite.dbapi.connect(uri=":memory:") — takes a filename.
        database = url.database or ":memory:"
        kwargs: dict[str, Any] = {"uri": database}
        if url.query:
            kwargs["db_kwargs"] = dict(url.query)
        return [], kwargs

    # ── Index reflection ─────────────────────────────────────────────
    #
    # ADBC GetObjects lists SQLite indexes as entries with
    # ``table_type='index'`` but doesn't carry their column lists, so
    # we query the native catalog (``PRAGMA index_list`` +
    # ``PRAGMA index_info``) directly. ``sqlite_autoindex_*`` entries
    # are auto-generated for PRIMARY KEY / UNIQUE constraints and are
    # already surfaced via ``get_pk_constraint`` / ``get_unique_constraints``
    # — skip them to match SQLAlchemy's built-in sqlite dialect behavior.

    def get_indexes(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedIndex]:
        dbapi = self._adbc_connection(connection)
        cursor = dbapi.cursor()
        try:
            # PRAGMA statements cannot be parameterized — SQLite ignores
            # placeholders in that context and would execute the literal
            # "?" as part of the name. Escape identifiers manually:
            # double-quote and double any internal double-quotes per the
            # SQL-92 identifier rule.
            tbl = _sqlite_ident(table_name)
            cursor.execute(f"PRAGMA index_list({tbl})")
            # index_list rows: (seq, name, unique, origin, partial)
            listings = cursor.fetchall()
            out: list[ReflectedIndex] = []
            for row in listings:
                name = row[1]
                is_unique = bool(row[2])
                # SQLite auto-creates an index for every PRIMARY KEY and
                # for some UNIQUE constraints; those are named
                # ``sqlite_autoindex_<table>_<N>`` and are better reached
                # via ``get_pk_constraint``/``get_unique_constraints``.
                # User-created unique indexes (CREATE UNIQUE INDEX ...)
                # DO belong here — SQLAlchemy's convention is that unique
                # indexes surface in both get_indexes and
                # get_unique_constraints so callers can distinguish.
                if name.startswith("sqlite_autoindex_"):
                    continue
                idx = _sqlite_ident(name)
                cursor.execute(f"PRAGMA index_info({idx})")
                # index_info rows: (seqno, cid, column_name) — sort by
                # seqno to preserve multi-column order.
                col_rows = cursor.fetchall()
                columns = [r[2] for r in sorted(col_rows, key=lambda r: r[0])]
                out.append(
                    {
                        "name": name,
                        "column_names": columns,
                        "unique": is_unique,
                    }
                )
            return out
        finally:
            cursor.close()


def _sqlite_ident(name: str) -> str:
    """Double-quote a SQLite identifier, escaping embedded double quotes.

    SQLite supports the SQL-92 standard ``"name"`` form for delimited
    identifiers; any ``"`` inside the name is escaped by doubling.
    """
    return '"' + name.replace('"', '""') + '"'
