"""Base ADBC dialect.

ADBC exposes a DBAPI 2.0 interface via ``adbc_driver_manager.dbapi``.
Each concrete driver (flightsql, postgresql, sqlite, ...) is a thin
subclass that tells the base dialect which driver module to load and
how to translate a SQLAlchemy URL into the ``db_kwargs`` that driver
expects.

Subclasses implement:
- ``driver_module`` — dotted path to the driver's dbapi module
  (e.g., ``"adbc_driver_flightsql.dbapi"``).
- ``build_connect_args(url)`` — returns ``(args, kwargs)`` passed to
  ``driver.connect(*args, **kwargs)``.
"""

from __future__ import annotations

import importlib
from typing import Any

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.interfaces import (
    ReflectedColumn,
    ReflectedForeignKeyConstraint,
    ReflectedIndex,
    ReflectedPrimaryKeyConstraint,
    ReflectedUniqueConstraint,
)
from sqlalchemy.engine.url import URL


class ADBCDialect(DefaultDialect):
    """Base class for SQLAlchemy dialects backed by ADBC drivers.

    This class is registered as the plain ``adbc://`` scheme. In practice
    callers should use a driver-specific variant like ``adbc+flightsql://``;
    the plain scheme requires ``adbc_driver_name`` in the URL query string.
    """

    # SQLAlchemy convention: `name` = backend identity (used by
    # Alembic's DDL impl registry, by reflection defaults, etc.);
    # `driver` = driver identity (the wire path).
    # Base class has no backend — subclasses override `name` to the
    # backend they target ("sqlite", "postgresql", ...).
    name = "adbc"
    driver = "adbc"
    supports_statement_cache = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    # ADBC drivers speak Python DBAPI, so the SQL dialect defaults are
    # close to "generic SQL-92". Subclasses override as needed.
    paramstyle = "qmark"

    driver_module: str | None = None
    """Dotted path to the concrete ADBC driver's ``.dbapi`` module.

    Must be set by subclasses (e.g., ``adbc_driver_flightsql.dbapi``).
    The base class reads ``adbc_driver_name`` from the URL query string
    when this is None — useful for generic ``adbc://`` URLs.
    """

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the DBAPI module for this dialect."""
        module_name = cls.driver_module
        if module_name is None:
            raise RuntimeError(
                "ADBCDialect subclass must set driver_module, "
                "or URL must specify ?adbc_driver_name=... (not yet implemented)"
            )
        return importlib.import_module(module_name)

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Translate a SQLAlchemy URL into ``(args, kwargs)`` for ADBC.

        Subclasses should override ``build_connect_args`` instead of this.
        """
        return self.build_connect_args(url)

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Hook for subclasses to translate a URL to driver connect args.

        Default implementation forwards ``url.query`` as ``db_kwargs`` and
        uses ``url.database`` (plus host/port when present) as the URI —
        the shape ADBC drivers like ``adbc_driver_flightsql`` expect.
        """
        db_kwargs = dict(url.query)
        uri = self._format_uri(url)
        args: list[Any] = [uri] if uri else []
        kwargs: dict[str, Any] = {}
        if db_kwargs:
            kwargs["db_kwargs"] = db_kwargs
        return args, kwargs

    def _format_uri(self, url: URL) -> str | None:
        """Default URI formatter — joins host/port/database.

        Subclasses (FlightSQL, Snowflake) override to inject scheme-specific
        prefixes like ``grpc+tls://``.
        """
        if url.host and url.port:
            base = f"{url.host}:{url.port}"
        elif url.host:
            base = url.host
        elif url.database:
            return url.database
        else:
            return None
        if url.database:
            return f"{base}/{url.database}"
        return base

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Liveness probe — run a trivial statement on the connection."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchall()
        finally:
            cursor.close()
        return True

    # ── Reflection (Issue #1) ────────────────────────────────────────
    #
    # SQLAlchemy's Inspector routes to these methods. Each one receives
    # a SQLAlchemy Connection; we reach the underlying ADBC DBAPI
    # connection via the pool proxy and call `adbc_get_objects`. The
    # returned Arrow tree is projected onto the Inspector contract by
    # helpers in `reflection.py`.
    #
    # Design notes:
    # - We call `adbc_get_objects` per-Inspector-call rather than
    #   caching. The metadata payload is small for the narrow filter
    #   (one table, one schema) and caching across connection lifetimes
    #   is a footgun when the schema evolves under us.
    # - `table_name_filter` and `db_schema_filter` narrow the ADBC
    #   query, so even large catalogs don't materialize every table
    #   just to inspect one.

    def _adbc_connection(self, connection: Any) -> Any:
        """Unwrap SQLAlchemy Connection → raw ADBC DBAPI connection."""
        raw = getattr(connection, "connection", connection)
        # SQLAlchemy's pool proxy exposes the underlying DBAPI as
        # `.driver_connection` on 2.0+; older code used `.connection`.
        # Falling back to `raw` itself handles cases where the caller
        # already passed the DBAPI connection directly.
        return getattr(raw, "driver_connection", None) or getattr(raw, "connection", raw)

    def _tree(
        self,
        connection: Any,
        *,
        schema: str | None = None,
        table_name: str | None = None,
        depth: str = "all",
    ) -> list[dict[str, Any]]:
        from sqlalchemy_adbc import reflection

        dbapi = self._adbc_connection(connection)
        return reflection.get_objects_tree(
            dbapi,
            depth=depth,
            db_schema_filter=schema,
            table_name_filter=table_name,
        )

    def get_schema_names(self, connection: Any, **kw: Any) -> list[str]:
        from sqlalchemy_adbc import reflection

        return reflection.schema_names_from_tree(self._tree(connection, depth="db_schemas"))

    def get_table_names(self, connection: Any, schema: str | None = None, **kw: Any) -> list[str]:
        from sqlalchemy_adbc import reflection

        return reflection.table_names_from_tree(
            self._tree(connection, schema=schema, depth="tables"),
            schema=schema,
        )

    def get_view_names(self, connection: Any, schema: str | None = None, **kw: Any) -> list[str]:
        from sqlalchemy_adbc import reflection

        return reflection.view_names_from_tree(
            self._tree(connection, schema=schema, depth="tables"),
            schema=schema,
        )

    def get_columns(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedColumn]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name)
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.columns_from_table(tbl)  # type: ignore[return-value]

    def get_pk_constraint(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> ReflectedPrimaryKeyConstraint:
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name)
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return {"constrained_columns": [], "name": None}
        return reflection.pk_from_table(tbl)  # type: ignore[return-value]

    def get_foreign_keys(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedForeignKeyConstraint]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name)
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.foreign_keys_from_table(tbl)  # type: ignore[return-value]

    def get_unique_constraints(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedUniqueConstraint]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name)
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.unique_constraints_from_table(tbl)  # type: ignore[return-value]

    def get_indexes(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedIndex]:
        # ADBC's GetObjects doesn't carry per-index column lists, so the
        # generic path returns an empty list. Driver-specific subclasses
        # can override with a native query (sqlite_master, pg_indexes).
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name)
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.indexes_stub(tbl)  # type: ignore[return-value]

    def has_table(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> bool:
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name, depth="tables")
        return reflection.find_table(tree, table_name, schema=schema) is not None
