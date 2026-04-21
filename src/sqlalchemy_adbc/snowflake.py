"""ADBC Snowflake dialect.

URL form::

    adbc+snowflake://user:password@account/database/schema?warehouse=WH&role=R

adbc_driver_snowflake expects a DSN of the form
``user:password@account/database/schema?warehouse=...``. We URL-quote
every user-supplied component so passwords with ``@``, ``:``, ``/``,
``%``, ``?``, ``#`` and query values with spaces don't corrupt the DSN.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect


class ADBCSnowflakeDialect(ADBCDialect):
    name = "snowflake"
    driver = "adbc"
    driver_module = "adbc_driver_snowflake.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        # SQLAlchemy decodes userinfo but leaves the database path
        # untouched. Re-quoting userinfo keeps a password like ``p@ss``
        # from being parsed as a second @-separator; the path is
        # already in the encoded form the caller wrote.
        user = quote(url.username, safe="") if url.username else ""
        password = f":{quote(url.password, safe='')}" if url.password else ""
        auth = f"{user}{password}@" if user or url.password else ""
        account = url.host or ""
        path = f"/{url.database}" if url.database else ""

        query_str = ""
        if url.query:
            pairs = [
                f"{quote(str(k), safe='')}={quote(str(v), safe='')}" for k, v in url.query.items()
            ]
            query_str = "?" + "&".join(pairs)

        dsn = f"{auth}{account}{path}{query_str}"
        return [dsn], {}
