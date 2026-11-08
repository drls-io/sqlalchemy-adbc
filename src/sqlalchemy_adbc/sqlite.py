"""SQLAlchemy's SQLite dialect over an ADBC connection.

``sqlite+adbc://`` keeps SQLAlchemy's own :class:`SQLiteDialect` for SQL
compilation, DDL, types (ISO-text storage of dates and times, as pysqlite)
and PRAGMA-based reflection (schemas via ``ATTACH``, NOT NULL, constraints,
indexes), and only swaps the wire path: connections come from
``adbc_driver_sqlite`` through :class:`~sqlalchemy_adbc.base.ADBCConnectionMixin`.
That is what lets this dialect run SQLAlchemy's dialect compliance suite.

Two things SQLAlchemy expects of a ``sqlite3``-like DBAPI module that the
ADBC module does not provide are supplied by a thin proxy: ``Binary`` (used
by ``LargeBinary`` binds) and ``sqlite_version_info`` (read once from a
throwaway in-memory connection; the dialect tunes RETURNING and other
features on it).
"""

from __future__ import annotations

import importlib
from typing import Any

from sqlalchemy import pool
from sqlalchemy.dialects.sqlite.base import SQLiteDialect, SQLiteExecutionContext
from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCConnectionMixin, ADBCExecutionContext


class _SQLiteDBAPI:
    """``adbc_driver_sqlite.dbapi`` plus ``Binary`` and ``sqlite_version_info``."""

    def __init__(self, module: Any) -> None:
        self._module = module
        self.Binary = bytes
        with module.connect(":memory:") as conn, conn.cursor() as cursor:
            cursor.execute("SELECT sqlite_version()")
            text = cursor.fetchone()[0]
        self.sqlite_version_info: tuple[int, ...] = tuple(int(part) for part in text.split("."))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._module, name)


class _ExecutionContext(ADBCExecutionContext, SQLiteExecutionContext):
    """Column-wise Arrow cursor plus SQLite's dotted-column-name handling."""


class ADBCSQLiteDialect(ADBCConnectionMixin, SQLiteDialect):
    # `name = "sqlite"` (inherited) makes Alembic pick the standard SQLite
    # DDL impl; `driver = "adbc"` is the wire path.
    driver = "adbc"
    driver_module = "adbc_driver_sqlite.dbapi"
    supports_statement_cache = True
    execution_ctx_cls = _ExecutionContext

    @classmethod
    def import_dbapi(cls) -> Any:
        return _SQLiteDBAPI(importlib.import_module(cls.driver_module or ""))

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        # As pysqlite: a file database can be shared by a QueuePool; an
        # in-memory database lives in one connection, so keep a single one.
        if url.database and url.database != ":memory:" and url.query.get("mode") != "memory":
            return pool.QueuePool
        return pool.SingletonThreadPool

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        return tuple(self.dbapi.sqlite_version_info)  # type: ignore[union-attr]

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        # adbc_driver_sqlite.dbapi.connect(uri=":memory:") — takes a filename.
        database = url.database or ":memory:"
        kwargs: dict[str, Any] = {"uri": database}
        if url.query:
            kwargs["db_kwargs"] = dict(url.query)
        return [], kwargs
