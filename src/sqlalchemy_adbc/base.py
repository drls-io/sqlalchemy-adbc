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

import collections
import importlib
import warnings
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from sqlalchemy.engine import reflection as sa_reflection
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.interfaces import (
    ExecutionContext,
    ReflectedColumn,
    ReflectedForeignKeyConstraint,
    ReflectedIndex,
    ReflectedPrimaryKeyConstraint,
    ReflectedUniqueConstraint,
)
from sqlalchemy.engine.url import URL


class ColumnwiseCursor:
    """DBAPI cursor proxy that converts Arrow batches column-wise.

    The ADBC DBAPI cursor builds every row cell by cell (``arr[i].as_py()``
    in ``fetchone``; ``fetchmany`` and ``fetchall`` loop over it). pyarrow's
    per-column ``to_pylist`` is an order of magnitude faster and yields the
    same Python values. The proxy serves PEP 249 fetch calls from a buffer
    filled one record batch at a time, so ``fetchmany`` streams and memory
    stays bounded. Everything else is forwarded to the wrapped cursor.
    """

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self._reader: Any = None
        self._rows: collections.deque[tuple[Any, ...]] = collections.deque()
        self._exhausted = False

    def _reset(self) -> None:
        self._reader = None
        self._rows.clear()
        self._exhausted = False

    def execute(self, operation: Any, parameters: Any = None) -> Any:
        self._reset()
        if parameters is None:
            return self._cursor.execute(operation)
        return self._cursor.execute(operation, parameters)

    def executemany(self, operation: Any, seq_of_parameters: Any) -> Any:
        self._reset()
        # Dialects may hand over an Arrow record batch (Snowflake's typed
        # binds); only plain sequences of parameter sets are inspected here.
        if isinstance(seq_of_parameters, (list, tuple)) and seq_of_parameters:
            if all(not p for p in seq_of_parameters):
                # ``INSERT INTO t DEFAULT VALUES`` for N empty parameter sets:
                # the driver binds an empty batch and inserts nothing, so run
                # the statement once per set instead.
                for _ in seq_of_parameters:
                    self._cursor.execute(operation)
                return None
        return self._cursor.executemany(operation, seq_of_parameters)

    def _fill(self, want: int | None) -> None:
        """Convert batches until the buffer holds ``want`` rows, or all of them."""
        if self._exhausted:
            return
        if self._reader is None:
            self._reader = self._cursor.fetch_record_batch()
        while want is None or len(self._rows) < want:
            try:
                batch = self._reader.read_next_batch()
            except StopIteration:
                self._exhausted = True
                return
            if batch.num_rows:
                self._rows.extend(zip(*(col.to_pylist() for col in batch.columns), strict=True))

    def fetchone(self) -> tuple[Any, ...] | None:
        self._fill(1)
        return self._rows.popleft() if self._rows else None

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        if size is None:
            size = self.arraysize
        self._fill(size)
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    def fetchall(self) -> list[tuple[Any, ...]]:
        self._fill(None)
        rows = list(self._rows)
        self._rows.clear()
        return rows

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        while (row := self.fetchone()) is not None:
            yield row

    @property
    def arraysize(self) -> int:
        return int(self._cursor.arraysize)

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def description(self) -> Sequence[Any] | None:
        # The ADBC cursor reports ``[]`` for a statement without result columns
        # (INSERT, DDL); DBAPI says None, and SQLAlchemy's ``returns_rows``
        # keys on that.
        return self._cursor.description or None

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    def close(self) -> None:
        self._reader = None
        self._rows.clear()
        self._cursor.close()

    def __getattr__(self, name: str) -> Any:
        # Arrow fetch methods, adbc_* extensions, setinputsizes, ...
        return getattr(self._cursor, name)


class ADBCExecutionContext(DefaultExecutionContext):
    def create_default_cursor(self) -> Any:
        return ColumnwiseCursor(self._dbapi_connection.cursor())


class ADBCConnectionMixin(DefaultDialect):
    """The ADBC connection layer, independent of any SQL dialect.

    Carries everything needed to open and execute on an ADBC DBAPI
    connection: locating the driver module, translating a SQLAlchemy URL into
    ``connect()`` arguments, minting per-connection credentials, the
    column-wise cursor, pinging. It is mixed into :class:`ADBCDialect` (the
    generic dialect with ``GetObjects`` reflection) and into SQLAlchemy's own
    dialects where one exists for the backend, e.g.
    ``class ADBCSQLiteDialect(ADBCConnectionMixin, SQLiteDialect)``, which
    keeps that dialect's SQL, DDL, types and reflection and only swaps the
    wire path for ADBC.
    """

    driver = "adbc"
    execution_ctx_cls: type[ExecutionContext] = ADBCExecutionContext
    supports_statement_cache = True
    # ADBC cursors report -1 for the row count of a query-path statement
    # and have no ``lastrowid``; SQLAlchemy uses RETURNING where available.
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    postfetch_lastrowid = False

    _connect_hook: Callable[[dict[str, Any]], None] | None = None
    """Set by ``build_connect_args`` when a credential must be minted per
    connection (an OAuth2 access token, a SPNEGO ticket). SQLAlchemy calls
    ``create_connect_args`` once per engine, so anything minted there would
    be reused for the engine's lifetime; :meth:`connect` runs the hook on a
    copy of ``db_kwargs`` for every new DBAPI connection instead.
    """

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

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        if self._connect_hook is not None:
            cparams = dict(cparams)
            db_kwargs = dict(cparams.get("db_kwargs") or {})
            self._connect_hook(db_kwargs)
            cparams["db_kwargs"] = db_kwargs
        return super().connect(*cargs, **cparams)

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Translate a SQLAlchemy URL into ``(args, kwargs)`` for ADBC.

        Subclasses should override ``build_connect_args`` instead of this.
        """
        self._connect_hook = None
        if url.drivername.startswith("adbc+"):
            backend = url.get_driver_name()
            warnings.warn(
                f"URL scheme 'adbc+{backend}://' is deprecated; use "
                f"'{backend}+adbc://' (SQLAlchemy's backend+driver order). "
                "The old spelling will be removed in 1.0.0.",
                DeprecationWarning,
                stacklevel=2,
            )
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

    def _adbc_connection(self, connection: Any) -> Any:
        """Unwrap SQLAlchemy Connection → raw ADBC DBAPI connection."""
        raw = getattr(connection, "connection", connection)
        # SQLAlchemy's pool proxy exposes the underlying DBAPI as
        # `.driver_connection` on 2.0+; older code used `.connection`.
        # Falling back to `raw` itself handles cases where the caller
        # already passed the DBAPI connection directly.
        return getattr(raw, "driver_connection", None) or getattr(raw, "connection", raw)


class ADBCDialect(ADBCConnectionMixin):
    """Generic SQLAlchemy dialect for any ADBC driver.

    This class is registered as the plain ``adbc://`` scheme. In practice
    callers should use a backend-specific variant like ``flightsql+adbc://``;
    the plain scheme requires ``adbc_driver_name`` in the URL query string.
    The pre-0.4 ``adbc+<backend>://`` spellings remain registered as
    deprecated aliases. Reflection runs on ADBC's ``GetObjects`` so it works
    against any driver; backends SQLAlchemy already knows (SQLite) instead
    mix :class:`ADBCConnectionMixin` into SQLAlchemy's dialect.
    """

    # SQLAlchemy convention: `name` = backend identity (used by
    # Alembic's DDL impl registry, by reflection defaults, etc.);
    # `driver` = driver identity (the wire path).
    # Base class has no backend — subclasses override `name` to the
    # backend they target ("postgresql", "snowflake", ...).
    name = "adbc"

    # ADBC drivers speak Python DBAPI, so the SQL dialect defaults are
    # close to "generic SQL-92". Subclasses override as needed.
    paramstyle = "qmark"

    ischema_names: dict[str, Any] | None = None
    """Per-backend type registry merged over the generic ``xdbc_type_name``
    map during column reflection; see :func:`reflection.adbc_type_to_sqla`.
    """

    # ── Reflection (Issue #1) ────────────────────────────────────────
    #
    # SQLAlchemy's Inspector routes to these methods. Each one receives
    # a SQLAlchemy Connection; we reach the underlying ADBC DBAPI
    # connection via the pool proxy and call `adbc_get_objects`. The
    # returned Arrow tree is projected onto the Inspector contract by
    # helpers in `reflection.py`.
    #
    # Design notes:
    # - The catalog tree is cached per Inspector through SQLAlchemy's
    #   `info_cache`, keyed on (schema, table_name, depth). One autoload
    #   touches five or six reflection methods; without the cache each
    #   re-fetched the tree (4-10 s per fetch on Snowflake). The cache
    #   dies with the Inspector, so schema changes are seen by the next
    #   `inspect()`.
    # - `table_name_filter` and `db_schema_filter` narrow the ADBC
    #   query, so even large catalogs don't materialize every table
    #   just to inspect one.

    @sa_reflection.cache
    def _tree(
        self,
        connection: Any,
        *,
        schema: str | None = None,
        table_name: str | None = None,
        depth: str = "all",
        info_cache: dict[Any, Any] | None = None,
    ) -> list[dict[str, Any]]:
        # `info_cache` is consumed by the decorator; it is part of the
        # signature so the Inspector's cache dict reaches it.
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

        return reflection.schema_names_from_tree(
            self._tree(connection, info_cache=kw.get("info_cache"), depth="db_schemas")
        )

    def get_table_names(self, connection: Any, schema: str | None = None, **kw: Any) -> list[str]:
        from sqlalchemy_adbc import reflection

        return reflection.table_names_from_tree(
            self._tree(connection, info_cache=kw.get("info_cache"), schema=schema, depth="tables"),
            schema=schema,
        )

    def get_view_names(self, connection: Any, schema: str | None = None, **kw: Any) -> list[str]:
        from sqlalchemy_adbc import reflection

        return reflection.view_names_from_tree(
            self._tree(connection, info_cache=kw.get("info_cache"), schema=schema, depth="tables"),
            schema=schema,
        )

    def get_columns(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedColumn]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(
            connection, info_cache=kw.get("info_cache"), schema=schema, table_name=table_name
        )
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.columns_from_table(tbl, ischema_names=self.ischema_names)  # type: ignore[return-value]

    def get_pk_constraint(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> ReflectedPrimaryKeyConstraint:
        from sqlalchemy_adbc import reflection

        tree = self._tree(
            connection, info_cache=kw.get("info_cache"), schema=schema, table_name=table_name
        )
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return {"constrained_columns": [], "name": None}
        return reflection.pk_from_table(tbl)  # type: ignore[return-value]

    def get_foreign_keys(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedForeignKeyConstraint]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(
            connection, info_cache=kw.get("info_cache"), schema=schema, table_name=table_name
        )
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.foreign_keys_from_table(tbl)  # type: ignore[return-value]

    def get_unique_constraints(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedUniqueConstraint]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(
            connection, info_cache=kw.get("info_cache"), schema=schema, table_name=table_name
        )
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

        tree = self._tree(
            connection, info_cache=kw.get("info_cache"), schema=schema, table_name=table_name
        )
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.indexes_stub(tbl)  # type: ignore[return-value]

    def has_table(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> bool:
        from sqlalchemy_adbc import reflection

        tree = self._tree(
            connection,
            info_cache=kw.get("info_cache"),
            schema=schema,
            table_name=table_name,
            depth="tables",
        )
        return reflection.find_table(tree, table_name, schema=schema) is not None
