"""ADBC BigQuery dialect.

URL form::

    bigquery+adbc:///<project>?dataset=<dataset>[&location=US][&credentials_path=/path/to/sa.json]

The URL's database segment is the GCP project; ``dataset`` is the default
dataset for unqualified table names (the driver requires one to run
queries); ``location`` pins the job location; ``credentials_path`` points at
a service-account JSON file (the default is Application Default
Credentials). Any ``adbc.bigquery.sql.*`` key is passed to the driver as is
and wins over the short spellings.

SQL is GoogleSQL: identifiers quote with backticks (``"..."`` is a string
literal there), DDL uses BigQuery's type names (``STRING``, ``INT64``,
``FLOAT64``, ``BOOL``, ``NUMERIC``, ``BYTES``, ``DATETIME``, ``TIMESTAMP``),
primary and foreign keys are ``NOT ENFORCED`` (the only kind BigQuery has)
and unique constraints are dropped from DDL (BigQuery has none). Reflection
goes through the driver's ``GetObjects``; ``TIMESTAMP`` (a UTC instant)
reflects as ``TIMESTAMP(timezone=True)`` and ``DATETIME`` (civil time) as
``DateTime``. ARRAY, STRUCT and GEOGRAPHY columns reflect as ``NullType``
and their values pass through as the driver returns them.
"""

from __future__ import annotations

import importlib
from typing import Any

from sqlalchemy import types as sa_types
from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import compiler

from sqlalchemy_adbc.base import ADBCDialect

_PROJECT = "adbc.bigquery.sql.project_id"
_DATASET = "adbc.bigquery.sql.dataset_id"
_LOCATION = "adbc.bigquery.sql.location"
_AUTH_TYPE = "adbc.bigquery.sql.auth_type"
_AUTH_CREDENTIALS = "adbc.bigquery.sql.auth_credentials"
_AUTH_JSON_FILE = "adbc.bigquery.sql.auth_type.json_credential_file"

# Short URL spelling → driver option. ``credentials_path`` is special-cased.
_SHORT_KEYS = {"dataset": _DATASET, "location": _LOCATION}

# GoogleSQL reserved keywords (BigQuery lexical structure documentation).
_RESERVED_WORDS = {
    w.lower()
    for w in """
    ALL AND ANY ARRAY AS ASC ASSERT_ROWS_MODIFIED AT BETWEEN BY CASE CAST COLLATE
    CONTAINS CREATE CROSS CUBE CURRENT DEFAULT DEFINE DESC DISTINCT ELSE END ENUM
    ESCAPE EXCEPT EXCLUDE EXISTS EXTRACT FALSE FETCH FOLLOWING FOR FROM FULL GROUP
    GROUPING GROUPS HASH HAVING IF IGNORE IN INNER INTERSECT INTERVAL INTO IS JOIN
    LATERAL LEFT LIKE LIMIT LOOKUP MERGE NATURAL NEW NO NOT NULL NULLS OF ON OR
    ORDER OUTER OVER PARTITION PRECEDING PROTO QUALIFY RANGE RECURSIVE RESPECT
    RIGHT ROLLUP ROWS SELECT SET SOME STRUCT TABLESAMPLE THEN TO TREAT TRUE
    UNBOUNDED UNION UNNEST USING WHEN WHERE WINDOW WITH WITHIN
    """.split()
} | compiler.RESERVED_WORDS


class _BigQueryDBAPI:
    """``adbc_driver_bigquery.dbapi`` plus ``Binary`` (``LargeBinary`` binds need it)."""

    def __init__(self, module: Any) -> None:
        self._module = module
        self.Binary = bytes

    def __getattr__(self, name: str) -> Any:
        return getattr(self._module, name)


class BigQueryIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = _RESERVED_WORDS

    def __init__(self, dialect: Any, **kw: Any) -> None:
        # A backtick inside a quoted identifier is escaped with a backslash.
        kw.setdefault("initial_quote", "`")
        kw.setdefault("escape_quote", "\\`")
        super().__init__(dialect, **kw)


class BigQueryTypeCompiler(compiler.GenericTypeCompiler):
    def _string(self, type_: Any, **kw: Any) -> str:
        length = getattr(type_, "length", None)
        return f"STRING({length})" if length else "STRING"

    visit_string = visit_VARCHAR = visit_CHAR = visit_text = visit_TEXT = _string
    visit_unicode = visit_NVARCHAR = visit_NCHAR = visit_unicode_text = _string

    def _int64(self, type_: Any, **kw: Any) -> str:
        return "INT64"

    visit_integer = visit_INTEGER = visit_BIGINT = visit_SMALLINT = _int64
    visit_big_integer = visit_small_integer = _int64

    def _float64(self, type_: Any, **kw: Any) -> str:
        return "FLOAT64"

    visit_float = visit_FLOAT = visit_REAL = visit_DOUBLE = visit_DOUBLE_PRECISION = _float64
    visit_double = _float64

    def visit_boolean(self, type_: Any, **kw: Any) -> str:
        return "BOOL"

    visit_BOOLEAN = visit_boolean

    def visit_numeric(self, type_: Any, **kw: Any) -> str:
        if type_.precision is None:
            return "NUMERIC"
        scale = 0 if type_.scale is None else type_.scale
        return f"NUMERIC({type_.precision}, {scale})"

    visit_NUMERIC = visit_DECIMAL = visit_numeric

    def _bytes(self, type_: Any, **kw: Any) -> str:
        return "BYTES"

    visit_large_binary = visit_BLOB = visit_BINARY = visit_VARBINARY = _bytes

    def visit_datetime(self, type_: Any, **kw: Any) -> str:
        return "DATETIME"

    visit_DATETIME = visit_datetime

    def visit_TIMESTAMP(self, type_: Any, **kw: Any) -> str:
        return "TIMESTAMP"

    def visit_JSON(self, type_: Any, **kw: Any) -> str:
        return "JSON"

    def visit_uuid(self, type_: Any, **kw: Any) -> str:
        return "STRING"

    visit_UUID = visit_uuid


class BigQueryCompiler(compiler.SQLCompiler):
    def render_bind_cast(self, type_: Any, dbapi_type: Any, sqltext: str) -> str:
        return f"CAST({sqltext} AS {self.dialect.type_compiler_instance.process(dbapi_type)})"


class BigQueryDDLCompiler(compiler.DDLCompiler):
    def visit_primary_key_constraint(self, constraint: Any, **kw: Any) -> str:
        text = super().visit_primary_key_constraint(constraint, **kw)
        return f"{text} NOT ENFORCED" if text else text

    def visit_foreign_key_constraint(self, constraint: Any, **kw: Any) -> str:
        text = super().visit_foreign_key_constraint(constraint, **kw)
        return f"{text} NOT ENFORCED" if text else text

    def visit_unique_constraint(self, constraint: Any, **kw: Any) -> str:
        # BigQuery has no UNIQUE constraints; dropping it keeps CREATE TABLE valid.
        return ""


class _CastDateTime(sa_types.DateTime):
    """Bind as ``CAST(? AS DATETIME)``.

    The driver sends a naive ``datetime`` as an Arrow timestamp, which BigQuery
    types as ``TIMESTAMP`` (an instant) and refuses to store in a ``DATETIME``
    (civil time) column. The cast reads the instant as UTC, which is how the
    driver encoded it, so the wall-clock value round-trips unchanged.
    """

    render_bind_cast = True


class _CastTimestamp(sa_types.TIMESTAMP):
    """``CAST(? AS TIMESTAMP)``: keeps TIMESTAMP columns off the DATETIME cast above."""

    render_bind_cast = True


def _string_of(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    return sa_types.String(size or None)


def _numeric_of(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    if size:
        return sa_types.Numeric(size, digits or 0)
    return sa_types.Numeric()


def _timestamp_of(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    return sa_types.TIMESTAMP(timezone=True)


# ``xdbc_type_name`` as the driver reports it (legacy spellings) → type.
bigquery_ischema_names: dict[str, Any] = {
    "STRING": _string_of,
    "INTEGER": sa_types.BigInteger,
    "INT64": sa_types.BigInteger,
    "FLOAT": sa_types.Float,
    "FLOAT64": sa_types.Float,
    "BOOLEAN": sa_types.Boolean,
    "BOOL": sa_types.Boolean,
    "NUMERIC": _numeric_of,
    "BIGNUMERIC": _numeric_of,
    "DATE": sa_types.Date,
    "TIME": sa_types.Time,
    "DATETIME": sa_types.DateTime,
    "TIMESTAMP": _timestamp_of,
    "BYTES": sa_types.LargeBinary,
    "JSON": sa_types.JSON,
}


try:
    from alembic.ddl.impl import DefaultImpl as _AlembicDefaultImpl
except ImportError:  # pragma: no cover - alembic is optional
    _AlembicDefaultImpl = None  # type: ignore[assignment,misc]

if _AlembicDefaultImpl is not None:

    class BigQueryAlembicImpl(_AlembicDefaultImpl):
        """Alembic looks impls up by dialect name; it ships none for ``bigquery``."""

        __dialect__ = "bigquery"


class ADBCBigQueryDialect(ADBCDialect):
    name = "bigquery"
    driver = "adbc"
    driver_module = "adbc_driver_bigquery.dbapi"
    supports_statement_cache = True

    preparer = BigQueryIdentifierPreparer  # type: ignore[assignment]  # MySQL-style __init__
    statement_compiler = BigQueryCompiler
    type_compiler_cls = BigQueryTypeCompiler
    ddl_compiler = BigQueryDDLCompiler
    ischema_names = bigquery_ischema_names
    colspecs = {sa_types.DateTime: _CastDateTime, sa_types.TIMESTAMP: _CastTimestamp}
    bind_typing = BindTyping.RENDER_CASTS

    # BOOL with true/false literals; without this ``WHERE flag = 1`` fails.
    supports_native_boolean = True
    # NUMERIC is exact; keep Decimals as Decimals.
    supports_native_decimal = True
    # One job per statement: ``VALUES (...), (...)`` beats executemany.
    supports_multivalues_insert = True
    use_insertmanyvalues = True
    supports_sequences = False

    @classmethod
    def import_dbapi(cls) -> Any:
        return _BigQueryDBAPI(importlib.import_module(cls.driver_module or ""))

    @staticmethod
    def _dataset_of(url: URL) -> str | None:
        value = url.query.get("dataset") or url.query.get(_DATASET)
        return str(value) if value else None

    def _get_default_schema_name(self, connection: Any) -> str | None:  # type: ignore[override]
        return self._default_dataset

    def do_executemany(
        self, cursor: Any, statement: Any, parameters: Any, context: Any = None
    ) -> None:
        # The driver's executemany returns before every job has finished, so a
        # read right after it can miss rows. One statement per parameter set
        # waits for each job; inserts rarely get here because insertmanyvalues
        # batches them into one multi-row VALUES statement.
        for params in parameters:
            cursor.execute(statement, params)

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        self._default_dataset = self._dataset_of(url)
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

    _default_dataset: str | None = None
