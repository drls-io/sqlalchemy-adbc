"""ADBC SQLite dialect — mostly useful for tests and local prototyping."""

from __future__ import annotations

from typing import Any

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
