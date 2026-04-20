"""ADBC PostgreSQL dialect.

We build a libpq connection *URI* rather than putting credentials in a
query string, but everything user-supplied is URL-quoted so that a
password containing ``@``, ``:``, ``/``, ``%``, ``?``, or ``#`` does not
corrupt the DSN or change its parse. Without quoting, a password like
``p@ss`` would silently re-route libpq to the wrong host.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect


class ADBCPostgreSQLDialect(ADBCDialect):
    name = "adbc"
    driver = "postgresql"
    driver_module = "adbc_driver_postgresql.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        # SQLAlchemy's URL parser decodes percent-escapes in userinfo
        # (`url.username`, `url.password`) but leaves the path component
        # (`url.database`) untouched. So we re-quote userinfo before
        # rebuilding the libpq URI — without it, a password like ``p@ss``
        # would silently re-route to a different host — but we pass the
        # database through as-is. Re-quoting the database would
        # double-encode already-percent-escaped names (``my%20db`` →
        # ``my%2520db``).
        user = quote(url.username, safe="") if url.username else ""
        password = f":{quote(url.password, safe='')}" if url.password else ""
        auth = f"{user}{password}@" if user or url.password else ""
        host = url.host or "localhost"
        port = f":{url.port}" if url.port else ""
        database = f"/{url.database}" if url.database else ""
        uri = f"postgresql://{auth}{host}{port}{database}"

        kwargs: dict[str, Any] = {}
        if url.query:
            kwargs["db_kwargs"] = dict(url.query)
        return [uri], kwargs
