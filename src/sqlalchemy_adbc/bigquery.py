"""ADBC BigQuery dialect.

URL form::

    bigquery+adbc:///<project>?dataset=<dataset>[&location=US][&credentials_path=/path/to/sa.json]

The URL's database segment is the GCP project; ``dataset`` is the default
dataset for unqualified table names (the driver requires one to run
queries); ``location`` pins the job location; ``credentials_path`` points at
a service-account JSON file (the default is Application Default
Credentials). Any ``adbc.bigquery.sql.*`` key is passed to the driver as is
and wins over the short spellings.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect

_PROJECT = "adbc.bigquery.sql.project_id"
_DATASET = "adbc.bigquery.sql.dataset_id"
_LOCATION = "adbc.bigquery.sql.location"
_AUTH_TYPE = "adbc.bigquery.sql.auth_type"
_AUTH_CREDENTIALS = "adbc.bigquery.sql.auth_credentials"
_AUTH_JSON_FILE = "adbc.bigquery.sql.auth_type.json_credential_file"

# Short URL spelling → driver option. ``credentials_path`` is special-cased.
_SHORT_KEYS = {"dataset": _DATASET, "location": _LOCATION}


class ADBCBigQueryDialect(ADBCDialect):
    name = "bigquery"
    driver = "adbc"
    driver_module = "adbc_driver_bigquery.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        query: dict[str, Any] = dict(url.query)
        db_kwargs: dict[str, Any] = {}
        for short, option in _SHORT_KEYS.items():
            if short in query:
                db_kwargs[option] = query.pop(short)
        if "credentials_path" in query:
            db_kwargs[_AUTH_TYPE] = _AUTH_JSON_FILE
            db_kwargs[_AUTH_CREDENTIALS] = query.pop("credentials_path")
        if url.database:
            db_kwargs.setdefault(_PROJECT, url.database)
        # Explicit driver keys win over the short spellings.
        db_kwargs.update(query)
        return [], {"db_kwargs": db_kwargs} if db_kwargs else {}
