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
from sqlalchemy.engine.url import URL


class ADBCDialect(DefaultDialect):
    """Base class for SQLAlchemy dialects backed by ADBC drivers.

    This class is registered as the plain ``adbc://`` scheme. In practice
    callers should use a driver-specific variant like ``adbc+flightsql://``;
    the plain scheme requires ``adbc_driver_name`` in the URL query string.
    """

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
