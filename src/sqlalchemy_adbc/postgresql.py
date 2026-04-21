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

from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedIndex
from sqlalchemy.engine.url import URL

from sqlalchemy_adbc.base import ADBCDialect
from sqlalchemy_adbc.postgresql_types import pg_ischema_names


class ADBCPostgreSQLDialect(ADBCDialect):
    # `name = "postgresql"` makes Alembic pick the standard Postgres
    # DDL impl (alembic.ddl.postgresql.PostgresqlImpl). Needed because
    # Alembic looks up DDL dialects by `.name`.
    name = "postgresql"
    driver = "adbc"
    driver_module = "adbc_driver_postgresql.dbapi"
    supports_statement_cache = True

    # adbc_driver_postgresql binds parameters positionally via libpq's
    # native ``$1, $2, ...`` placeholders. SQLAlchemy's DefaultDialect
    # would otherwise copy ``paramstyle`` from ``dbapi.paramstyle``
    # (adbc's module-level default), which triggers named binds via
    # ADBC's ``adbc.statement.bind_by_name`` option — libpq rejects
    # that with NOT_IMPLEMENTED. Class-level ``paramstyle = "numeric"``
    # is NOT enough because ``DefaultDialect.__init__`` clobbers it;
    # we have to force it through the kwarg so SQLAlchemy emits
    # ``$1``-style placeholders and packs parameters positionally.
    paramstyle = "numeric"

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("paramstyle", "numeric")
        super().__init__(**kwargs)

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

    # ── Column reflection (PG-typed) ─────────────────────────────────
    #
    # Override the base ``get_columns`` to pass ``pg_ischema_names``
    # through to the type mapper, so JSONB/UUID/INET/etc. columns
    # surface as their TypeDecorator wrappers (with JSON parsing /
    # UUID instantiation) rather than plain Text.

    def get_columns(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedColumn]:
        from sqlalchemy_adbc import reflection

        tree = self._tree(connection, schema=schema, table_name=table_name)
        tbl = reflection.find_table(tree, table_name, schema=schema)
        if tbl is None:
            return []
        return reflection.columns_from_table(  # type: ignore[return-value]
            tbl, ischema_names=pg_ischema_names
        )

    # ── Index reflection ─────────────────────────────────────────────
    #
    # ADBC GetObjects doesn't carry per-index column lists, so we query
    # ``pg_index`` / ``pg_class`` / ``pg_attribute`` directly. The PK
    # index is excluded here because ``get_pk_constraint`` already
    # surfaces it via the constraint path; including it would double-
    # count. Unique-constraint indexes stay — SQLAlchemy's convention
    # is that unique *indexes* show up in ``get_indexes`` and unique
    # *constraints* in ``get_unique_constraints`` (a DB may have one,
    # the other, or both for the same column set).

    def get_indexes(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[ReflectedIndex]:
        dbapi = self._adbc_connection(connection)
        cursor = dbapi.cursor()
        try:
            # Use LATERAL unnest to expand indkey into one row per column
            # while preserving column order (WITH ORDINALITY). The COALESCE
            # on the schema filter lets the caller pass None for "any
            # schema in the search_path" — matches SQLAlchemy's convention.
            sql = """
            SELECT
                i.relname AS index_name,
                array_agg(a.attname ORDER BY x.n) AS column_names,
                ix.indisunique AS is_unique
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n)
            LEFT JOIN pg_catalog.pg_attribute a
                ON a.attrelid = t.oid AND a.attnum = x.attnum
            WHERE t.relname = $1
              AND ($2::text IS NULL OR n.nspname = $2)
              AND NOT ix.indisprimary
            GROUP BY i.relname, ix.indisunique
            ORDER BY i.relname
            """
            cursor.execute(sql, (table_name, schema))
            rows = cursor.fetchall()
            return [
                {
                    "name": row[0],
                    "column_names": _to_list(row[1]),
                    "unique": bool(row[2]),
                }
                for row in rows
            ]
        finally:
            cursor.close()


def _to_list(value: Any) -> list[Any]:
    """Normalize a column-names aggregate to a plain Python list.

    ADBC Postgres returns ``array_agg(...)`` results through pyarrow;
    depending on driver version the Python-level value can be a
    ``list``, a ``tuple``, or a ``pyarrow.Array``. Iteration works on
    all three, and handles the ``None`` (empty aggregate) case.
    """
    if value is None:
        return []
    return list(value)
