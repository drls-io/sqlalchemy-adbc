"""ADBC BigQuery dialect.

URL form::

    adbc+bigquery:///<project>?dataset=<dataset>&credentials_path=/path/to/sa.json

adbc_driver_bigquery reads the GCP project and optional dataset via
db_kwargs; we pass the URL query string through verbatim.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect


class ADBCBigQueryDialect(ADBCDialect):
    name = "bigquery"
    driver = "adbc"
    driver_module = "adbc_driver_bigquery.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        db_kwargs: dict[str, Any] = dict(url.query)
        if url.database:
            db_kwargs.setdefault("adbc.bigquery.sql.project_id", url.database)
        return [], {"db_kwargs": db_kwargs} if db_kwargs else {}
