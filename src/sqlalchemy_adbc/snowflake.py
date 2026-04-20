"""ADBC Snowflake dialect.

URL form::

    adbc+snowflake://user:password@account/database/schema?warehouse=WH&role=R

adbc_driver_snowflake expects a DSN of the form
``user:password@account/database/schema?warehouse=...``.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect


class ADBCSnowflakeDialect(ADBCDialect):
    name = "adbc"
    driver = "snowflake"
    driver_module = "adbc_driver_snowflake.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        user = url.username or ""
        password = f":{url.password}" if url.password else ""
        auth = f"{user}{password}@" if user else ""
        account = url.host or ""
        path = ""
        if url.database:
            path = f"/{url.database}"
        query_str = ""
        if url.query:
            query_str = "?" + "&".join(f"{k}={v}" for k, v in url.query.items())
        dsn = f"{auth}{account}{path}{query_str}"
        return [dsn], {}
