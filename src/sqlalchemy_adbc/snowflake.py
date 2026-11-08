"""ADBC Snowflake dialect.

URL form::

    snowflake+adbc://user:password@account/database/schema?warehouse=WH&role=R

adbc_driver_snowflake expects a DSN of the form
``user:password@account/database/schema?warehouse=...``. We URL-quote
every user-supplied component so passwords with ``@``, ``:``, ``/``,
``%``, ``?``, ``#`` and query values with spaces don't corrupt the DSN.

Parameter binding: the driver (1.11) accepts only int, str, float and
bool Arrow parameters, and rejects a column that is null in every row
(Arrow ``null`` type). Snowflake converts string parameters into DATE,
TIMESTAMP_*, NUMBER and BINARY columns implicitly, so ``colspecs`` render
those Python types as strings, and ``do_execute`` hands the cursor an
explicitly typed record batch in which all-null columns are strings.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import pyarrow as pa
from sqlalchemy import types as sa_types
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ArgumentError

from sqlalchemy_adbc.auth import require, settings_from_query, token_source_for
from sqlalchemy_adbc.base import ADBCDialect
from sqlalchemy_adbc.config import load_config
from sqlalchemy_adbc.snowflake_types import snowflake_ischema_names

_AUTH_TYPE = "adbc.snowflake.sql.auth_type"
_AUTH_TOKEN = "adbc.snowflake.sql.client_option.auth_token"


class _IsoDate(sa_types.Date):
    def bind_processor(self, dialect: Any) -> Any:
        return lambda v: None if v is None else v.isoformat()


class _IsoDateTime(sa_types.DateTime):
    def bind_processor(self, dialect: Any) -> Any:
        return lambda v: None if v is None else v.isoformat(sep=" ")


class _IsoTime(sa_types.Time):
    def bind_processor(self, dialect: Any) -> Any:
        return lambda v: None if v is None else v.isoformat()


class _StringDecimal(sa_types.Numeric):
    """Decimal → str (exact, driver can't bind decimal128); int/float untouched."""

    def bind_processor(self, dialect: Any) -> Any:
        return lambda v: str(v) if isinstance(v, Decimal) else v


class _HexBinary(sa_types.LargeBinary):
    def bind_processor(self, dialect: Any) -> Any:
        return lambda v: None if v is None else bytes(v).hex()


def to_record_batch(seq_of_parameters: Sequence[Sequence[Any]]) -> pa.RecordBatch | None:
    """Positional parameter rows → record batch with explicit column types.

    pyarrow infers each column from its non-null values; a column that is
    None in every row would infer as Arrow ``null``, which the Snowflake
    driver rejects, so it becomes a nullable string instead (Snowflake
    accepts a NULL string for any column type). Returns None when there is
    nothing to bind.
    """
    rows = [tuple(p) for p in seq_of_parameters]
    if not rows or not rows[0]:
        return None
    columns = list(zip(*rows, strict=True))
    arrays = [
        pa.array(col, type=pa.string()) if all(v is None for v in col) else pa.array(col)
        for col in columns
    ]
    return pa.RecordBatch.from_arrays(arrays, names=[str(i) for i in range(len(arrays))])


def _positional(parameters: Any) -> bool:
    return isinstance(parameters, (tuple, list)) and not isinstance(parameters, dict)


class ADBCSnowflakeDialect(ADBCDialect):
    name = "snowflake"
    driver = "adbc"
    driver_module = "adbc_driver_snowflake.dbapi"
    supports_statement_cache = True
    ischema_names = snowflake_ischema_names
    # NUMBER is exact; without this SQLAlchemy's Numeric binds ints and
    # Decimals as float and NUMBER(38,0) values above 2**53 lose digits.
    supports_native_decimal = True
    # Snowflake has BOOLEAN with true/false literals; without this SQLAlchemy
    # renders booleans as 0/1 and ``WHERE "OK" IS 0`` is a syntax error there.
    supports_native_boolean = True
    colspecs = {
        sa_types.Date: _IsoDate,
        sa_types.DateTime: _IsoDateTime,
        sa_types.Time: _IsoTime,
        sa_types.Numeric: _StringDecimal,
        sa_types.Float: sa_types.Float,  # keep Float off the Numeric→string path
        sa_types.LargeBinary: _HexBinary,
    }

    def do_execute(self, cursor: Any, statement: Any, parameters: Any, context: Any = None) -> None:
        if _positional(parameters):
            parameters = to_record_batch([parameters])
        cursor.execute(statement, parameters)

    def do_executemany(
        self, cursor: Any, statement: Any, parameters: Any, context: Any = None
    ) -> None:
        if _positional(parameters) and all(_positional(p) for p in parameters):
            batch = to_record_batch(parameters)
            if batch is None:
                return
            parameters = batch
        cursor.executemany(statement, parameters)

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        query: dict[str, str] = {
            k: (v[-1] if isinstance(v, tuple) else v) for k, v in url.query.items()
        }
        settings = settings_from_query(query, load_config(), password=url.password)
        kwargs: dict[str, Any] = {}
        url_password = url.password

        if settings.type in ("negotiate", "bearer"):
            raise ArgumentError(f"auth={settings.type} is not supported by the Snowflake driver")
        if settings.type == "oauth2":
            # The driver takes a static token; we mint and cache one per
            # connection via client credentials. The URL password was the
            # client secret, so it must not reach the DSN.
            require(settings, "oauth2_token_url", "oauth2_client_id", "oauth2_client_secret")
            kwargs["db_kwargs"] = {
                _AUTH_TYPE: "auth_oauth",
                _AUTH_TOKEN: token_source_for(settings).token(),
            }
            url_password = None

        # SQLAlchemy decodes userinfo but leaves the database path
        # untouched. Re-quoting userinfo keeps a password like ``p@ss``
        # from being parsed as a second @-separator; the path is
        # already in the encoded form the caller wrote.
        user = quote(url.username, safe="") if url.username else ""
        password = f":{quote(url_password, safe='')}" if url_password else ""
        auth = f"{user}{password}@" if user or url_password else ""
        account = url.host or ""
        path = f"/{url.database}" if url.database else ""

        query_str = ""
        if query:
            pairs = [f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in query.items()]
            query_str = "?" + "&".join(pairs)

        dsn = f"{auth}{account}{path}{query_str}"
        return [dsn], kwargs
