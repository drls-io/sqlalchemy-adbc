"""Arrow in and out of a SQLAlchemy connection, without the row round trip.

SQLAlchemy's result contract is rows, so ``Connection.execute()`` turns the
driver's Arrow batches into Python tuples. These helpers reach the ADBC
cursor behind the same :class:`sqlalchemy.engine.Connection` instead, so the
caller keeps SQLAlchemy's URL, pool, authentication and transaction and gets
Arrow::

    from sqlalchemy_adbc.arrow import fetch_arrow_table, ingest_arrow

    with engine.begin() as conn:
        table = fetch_arrow_table(conn, select(orders).where(orders.c.day >= bindparam("d")), {"d": day})
        ingest_arrow(conn, "orders_copy", table, mode="create")

Statements may be SQLAlchemy constructs (compiled with the connection's
dialect, bind processors included, parameters as a mapping) or plain SQL
strings (passed to the driver verbatim, parameters as a positional
sequence). The work runs in the connection's transaction: the helpers begin
SQLAlchemy's transaction when none is open, so ``conn.commit()`` /
``conn.rollback()`` and ``with engine.begin()`` govern it. Driver errors
propagate as the ADBC DBAPI exceptions, not wrapped in
:class:`sqlalchemy.exc.DBAPIError`, because SQLAlchemy's execute path is
bypassed.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, Literal

import pyarrow as pa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import ClauseElement

from sqlalchemy_adbc.base import ADBCConnectionMixin

__all__ = ["fetch_arrow_table", "ingest_arrow", "open_arrow_batch_reader"]

IngestMode = Literal["append", "create", "replace", "create_append"]


def fetch_arrow_table(
    connection: Connection,
    statement: str | ClauseElement,
    parameters: Mapping[str, Any] | Sequence[Any] | None = None,
) -> pa.Table:
    """Execute ``statement`` and return the whole result as a :class:`pyarrow.Table`."""
    cursor = _execute(connection, statement, parameters)
    try:
        table: pa.Table = cursor.fetch_arrow_table()
    finally:
        cursor.close()
    return table


@contextmanager
def open_arrow_batch_reader(
    connection: Connection,
    statement: str | ClauseElement,
    parameters: Mapping[str, Any] | Sequence[Any] | None = None,
) -> Iterator[pa.RecordBatchReader]:
    """Execute ``statement`` and yield a streaming :class:`pyarrow.RecordBatchReader`.

    The driver's cursor stays open for the duration of the ``with`` block and
    is closed on exit, also when the block raises; reading from the reader
    after the block is undefined.
    """
    cursor = _execute(connection, statement, parameters)
    try:
        yield cursor.fetch_record_batch()
    finally:
        cursor.close()


def ingest_arrow(
    connection: Connection,
    table_name: str,
    data: Any,
    *,
    mode: IngestMode = "append",
    schema: str | None = None,
    catalog: str | None = None,
    temporary: bool = False,
) -> int:
    """Bulk-load Arrow data into ``table_name`` via the driver's ingest path.

    ``data`` is anything the ADBC cursor's ``adbc_ingest`` accepts: a
    :class:`pyarrow.Table`, :class:`pyarrow.RecordBatch`,
    :class:`pyarrow.RecordBatchReader`, or an object exposing the Arrow
    PyCapsule interface (a Polars ``DataFrame``). ``mode`` is ``append``
    (table must exist), ``create`` (must not), ``replace`` or
    ``create_append``. For ``append`` the Arrow schema must match the table's
    column types; PostgreSQL, for one, rejects int64 into an INTEGER column.
    Returns the driver's row count, ``-1`` when it does not report one.
    """
    cursor = _raw_cursor(connection)
    try:
        count = cursor.adbc_ingest(
            table_name,
            data,
            mode=mode,
            catalog_name=catalog,
            db_schema_name=schema,
            temporary=temporary,
        )
    finally:
        cursor.close()
    return int(count) if count is not None else -1


# ── internals ────────────────────────────────────────────────────────


def _check_connection(connection: Any) -> ADBCConnectionMixin:
    if not isinstance(connection, Connection):
        if isinstance(connection, Engine):
            hint = "pass engine.connect() (or use `with engine.begin() as conn`)"
        elif isinstance(connection, Session):
            hint = "pass session.connection()"
        else:
            hint = "pass a sqlalchemy.engine.Connection"
        raise TypeError(
            f"expected a SQLAlchemy Connection, got {type(connection).__name__}; {hint}"
        )
    dialect = connection.dialect
    if not isinstance(dialect, ADBCConnectionMixin):
        raise TypeError(
            f"connection uses dialect {dialect.name!r} ({type(dialect).__name__}), "
            "not an ADBC dialect; Arrow helpers need an ADBC-backed engine such as "
            "'sqlite+adbc://' or 'postgresql+adbc://'"
        )
    return dialect


def _raw_cursor(connection: Connection) -> Any:
    """A raw ADBC DBAPI cursor on ``connection``, inside its transaction."""
    dialect = _check_connection(connection)
    # SQLAlchemy only autobegins on execute(); do it here so the caller's
    # commit()/rollback() covers work done on the raw cursor.
    if not connection.in_transaction():
        connection.begin()
    return dialect._adbc_connection(connection).cursor()


def _prepare(
    connection: Connection,
    statement: str | ClauseElement,
    parameters: Mapping[str, Any] | Sequence[Any] | None,
) -> tuple[str, list[Any] | None]:
    """→ (SQL text for the driver, positional parameters)."""
    paramstyle = connection.dialect.paramstyle
    if isinstance(statement, str):
        if parameters is None:
            return statement, None
        if isinstance(parameters, Mapping | str | bytes):
            raise TypeError(
                "a plain SQL string takes positional parameters (a list or tuple) for the "
                f"driver's placeholders (paramstyle {paramstyle!r}); use sqlalchemy.text() "
                "for named parameters"
            )
        return statement, list(parameters)
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("a SQLAlchemy statement takes its parameters as a mapping of name to value")
    compiled: Any = statement.compile(dialect=connection.dialect)
    # construct_expanded_state renders post-compile binds (IN lists, expanding
    # and literal_execute parameters) for these values and reports the final
    # positional order; escape_names=False keeps raw bind names, which is what
    # the processors and positiontup are keyed by (as in SQLAlchemy's own
    # execution context).
    state = compiled.construct_expanded_state(dict(parameters or {}), escape_names=False)
    processors: Mapping[str, Any] = {**compiled._bind_processors, **state.processors}
    values: Mapping[str, Any] = state.parameters
    positional = [
        processors[name](values[name])
        if name in processors and values[name] is not None
        else values[name]
        for name in state.positiontup or []
    ]
    return str(state.statement), positional


def _execute(
    connection: Connection,
    statement: str | ClauseElement,
    parameters: Mapping[str, Any] | Sequence[Any] | None,
) -> Any:
    _check_connection(connection)
    sql, positional = _prepare(connection, statement, parameters)
    cursor = _raw_cursor(connection)
    try:
        # The dialect's do_execute carries per-backend binding (Snowflake's
        # typed record batch), so the Arrow path binds exactly like execute().
        connection.dialect.do_execute(cursor, sql, positional, None)
    except BaseException:
        cursor.close()
        raise
    return cursor
