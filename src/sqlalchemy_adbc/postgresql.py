"""ADBC PostgreSQL dialect."""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect


class ADBCPostgreSQLDialect(ADBCDialect):
    name = "adbc"
    driver = "postgresql"
    driver_module = "adbc_driver_postgresql.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        # adbc_driver_postgresql expects a standard libpq URI.
        user = url.username or ""
        password = f":{url.password}" if url.password else ""
        auth = f"{user}{password}@" if user else ""
        host = url.host or "localhost"
        port = f":{url.port}" if url.port else ""
        database = f"/{url.database}" if url.database else ""
        uri = f"postgresql://{auth}{host}{port}{database}"

        kwargs: dict[str, Any] = {}
        if url.query:
            kwargs["db_kwargs"] = dict(url.query)
        return [uri], kwargs
