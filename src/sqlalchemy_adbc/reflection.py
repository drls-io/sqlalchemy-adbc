"""Reflection helpers — ADBC ``get_objects`` → SQLAlchemy Inspector data.

ADBC's ``adbc_get_objects`` returns a ``RecordBatchReader`` whose schema is
the canonical ``GetObjects`` tree: catalog → db_schemas → db_schema_tables →
(table_columns, table_constraints). We collapse that to plain-Python dicts
and then project it onto SQLAlchemy's Inspector contract (``get_columns``,
``get_pk_constraint``, ``get_foreign_keys``, ``get_indexes``,
``get_table_names``, ``get_schema_names``, ``has_table``).

ADBC spec reference:
https://arrow.apache.org/adbc/current/format/database_metadata.html
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import types as sa_types

# ── Type mapping ─────────────────────────────────────────────────────

# xdbc_type_name (the SQL type string the backend reports) → SQLAlchemy
# type class. Keys are upper-cased and whitespace-stripped. Unknown names
# fall through to NullType — the caller still gets the column with the
# raw string type via the `type` key, but SQLAlchemy can't bind values
# through it, which is usually the right signal to fix the mapping.
_TYPE_MAP: dict[str, type[sa_types.TypeEngine[Any]]] = {
    "INTEGER": sa_types.Integer,
    "INT": sa_types.Integer,
    "INT2": sa_types.SmallInteger,
    "SMALLINT": sa_types.SmallInteger,
    "INT4": sa_types.Integer,
    "INT8": sa_types.BigInteger,
    "BIGINT": sa_types.BigInteger,
    "TINYINT": sa_types.SmallInteger,
    "REAL": sa_types.Float,
    "FLOAT": sa_types.Float,
    "FLOAT4": sa_types.Float,
    "FLOAT8": sa_types.Float,
    "DOUBLE": sa_types.Float,
    "DOUBLE PRECISION": sa_types.Float,
    "NUMERIC": sa_types.Numeric,
    "DECIMAL": sa_types.Numeric,
    "TEXT": sa_types.Text,
    "VARCHAR": sa_types.String,
    "CHARACTER VARYING": sa_types.String,
    "CHAR": sa_types.CHAR,
    "CHARACTER": sa_types.CHAR,
    "BLOB": sa_types.LargeBinary,
    "BYTEA": sa_types.LargeBinary,
    "BINARY": sa_types.LargeBinary,
    "VARBINARY": sa_types.LargeBinary,
    "BOOLEAN": sa_types.Boolean,
    "BOOL": sa_types.Boolean,
    "DATE": sa_types.Date,
    "TIME": sa_types.Time,
    "TIMESTAMP": sa_types.DateTime,
    "TIMESTAMPTZ": sa_types.DateTime,
    "TIMESTAMP WITH TIME ZONE": sa_types.DateTime,
    "TIMESTAMP WITHOUT TIME ZONE": sa_types.DateTime,
    "DATETIME": sa_types.DateTime,
    "JSON": sa_types.JSON,
    "JSONB": sa_types.JSON,
    "UUID": sa_types.Uuid,
}


def adbc_type_to_sqla(type_name: str | None) -> sa_types.TypeEngine[Any]:
    """Map an ADBC ``xdbc_type_name`` to a SQLAlchemy type instance."""
    if not type_name:
        return sa_types.NullType()
    normalized = type_name.strip().upper()
    # Strip trailing length/precision (e.g. ``VARCHAR(32)`` → ``VARCHAR``).
    base = normalized.split("(", 1)[0].strip()
    cls = _TYPE_MAP.get(base) or _TYPE_MAP.get(normalized)
    if cls is None:
        return sa_types.NullType()
    return cls()


# ── get_objects → Python dict tree ───────────────────────────────────


def get_objects_tree(
    connection: Any,
    depth: str = "all",
    catalog_filter: str | None = None,
    db_schema_filter: str | None = None,
    table_name_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Call ``adbc_get_objects`` and return the catalog-level list.

    The raw ADBC return is a RecordBatchReader. We materialize it to an
    Arrow table, convert to Python (``to_pylist``), and hand back the
    top-level list of catalog dicts. For small metadata payloads (the
    only use case — this isn't in a query hot path) this trades no
    meaningful memory for much simpler downstream code.
    """
    reader = connection.adbc_get_objects(
        depth=depth,
        catalog_filter=catalog_filter,
        db_schema_filter=db_schema_filter,
        table_name_filter=table_name_filter,
    )
    table = reader.read_all()
    return table.to_pylist()


def find_table(
    tree: list[dict[str, Any]],
    table_name: str,
    schema: str | None = None,
) -> dict[str, Any] | None:
    """Walk the tree and return the first matching table dict, or None.

    ADBC lumps tables and indexes together under ``db_schema_tables``
    with a ``table_type`` discriminator. We only match ``table``/``view``
    entries — never indexes, which SQLAlchemy reaches via ``get_indexes``.
    """
    for catalog in tree:
        for sch in catalog.get("catalog_db_schemas") or []:
            if schema is not None and sch.get("db_schema_name") != schema:
                continue
            for tbl in sch.get("db_schema_tables") or []:
                if tbl.get("table_name") != table_name:
                    continue
                if tbl.get("table_type") in {"index", "sqlite_autoindex"}:
                    continue
                return tbl
    return None


# ── Projections for the Inspector contract ───────────────────────────


def columns_from_table(tbl: dict[str, Any]) -> list[dict[str, Any]]:
    """Project ADBC table_columns → SQLAlchemy ``get_columns`` shape.

    SQLAlchemy's contract: each dict has ``name``, ``type``, ``nullable``,
    ``default``, ``autoincrement``, and (optionally) ``comment``.
    """
    out: list[dict[str, Any]] = []
    for col in tbl.get("table_columns") or []:
        name = col.get("column_name")
        if not name:
            continue
        # xdbc_is_nullable is a string ('YES'/'NO'); xdbc_nullable is an
        # int enum. Prefer the string because it's the most consistently
        # populated across drivers; fall back to the enum.
        is_nullable_str = col.get("xdbc_is_nullable")
        if is_nullable_str is not None:
            nullable = is_nullable_str.upper() != "NO"
        else:
            # ADBC enum: 0 = no nulls, 1 = nullable, 2 = unknown. Treat
            # unknown as nullable (SQL default).
            nullable_enum = col.get("xdbc_nullable")
            nullable = nullable_enum != 0
        out.append(
            {
                "name": name,
                "type": adbc_type_to_sqla(col.get("xdbc_type_name")),
                "nullable": nullable,
                "default": col.get("xdbc_column_def"),
                "autoincrement": bool(col.get("xdbc_is_autoincrement")) or "auto",
                "comment": col.get("remarks"),
            }
        )
    return out


def pk_from_table(tbl: dict[str, Any]) -> dict[str, Any]:
    """Project table_constraints → ``get_pk_constraint`` shape.

    Contract: ``{'constrained_columns': [...], 'name': Optional[str]}``.
    If no PK constraint is present, return the empty sentinel SQLAlchemy
    expects (empty list, no name).
    """
    for c in tbl.get("table_constraints") or []:
        if c.get("constraint_type") == "PRIMARY KEY":
            return {
                "constrained_columns": list(c.get("constraint_column_names") or []),
                "name": c.get("constraint_name"),
            }
    return {"constrained_columns": [], "name": None}


def foreign_keys_from_table(tbl: dict[str, Any]) -> list[dict[str, Any]]:
    """Project FOREIGN KEY constraints → ``get_foreign_keys`` shape.

    Each FK becomes one dict with ``constrained_columns`` (local cols,
    in order), ``referred_table``, ``referred_schema``, and
    ``referred_columns``. ADBC lists target columns one-per-element in
    ``constraint_column_usage``, so we zip the lists position-wise.
    """
    out: list[dict[str, Any]] = []
    for c in tbl.get("table_constraints") or []:
        if c.get("constraint_type") != "FOREIGN KEY":
            continue
        local = list(c.get("constraint_column_names") or [])
        usage = c.get("constraint_column_usage") or []
        if not usage:
            continue
        referred_table = usage[0].get("fk_table")
        referred_schema = usage[0].get("fk_db_schema") or None
        referred_columns = [u.get("fk_column_name") for u in usage]
        out.append(
            {
                "name": c.get("constraint_name"),
                "constrained_columns": local,
                "referred_table": referred_table,
                "referred_schema": referred_schema,
                "referred_columns": referred_columns,
            }
        )
    return out


def unique_constraints_from_table(tbl: dict[str, Any]) -> list[dict[str, Any]]:
    """Project UNIQUE constraints → ``get_unique_constraints`` shape."""
    out: list[dict[str, Any]] = []
    for c in tbl.get("table_constraints") or []:
        if c.get("constraint_type") == "UNIQUE":
            out.append(
                {
                    "name": c.get("constraint_name"),
                    "column_names": list(c.get("constraint_column_names") or []),
                }
            )
    return out


def table_names_from_tree(tree: list[dict[str, Any]], schema: str | None = None) -> list[str]:
    """All table names (not views, not indexes) for one schema."""
    names: list[str] = []
    for catalog in tree:
        for sch in catalog.get("catalog_db_schemas") or []:
            if schema is not None and sch.get("db_schema_name") != schema:
                continue
            for tbl in sch.get("db_schema_tables") or []:
                if tbl.get("table_type") == "table":
                    nm = tbl.get("table_name")
                    if nm:
                        names.append(nm)
    return names


def view_names_from_tree(tree: list[dict[str, Any]], schema: str | None = None) -> list[str]:
    """All view names for one schema."""
    names: list[str] = []
    for catalog in tree:
        for sch in catalog.get("catalog_db_schemas") or []:
            if schema is not None and sch.get("db_schema_name") != schema:
                continue
            for tbl in sch.get("db_schema_tables") or []:
                if tbl.get("table_type") == "view":
                    nm = tbl.get("table_name")
                    if nm:
                        names.append(nm)
    return names


def schema_names_from_tree(tree: list[dict[str, Any]]) -> list[str]:
    """Unique non-empty schema names across all catalogs."""
    out: list[str] = []
    seen: set[str] = set()
    for catalog in tree:
        for sch in catalog.get("catalog_db_schemas") or []:
            name = sch.get("db_schema_name")
            if name and name not in seen:
                seen.add(name)
                out.append(name)
    return out


# ── Indexes (via the same tree, different extraction) ────────────────


# ADBC's `get_objects` doesn't directly expose user-created indexes in
# a uniform way — some drivers (sqlite) leak them as table_type="index"
# entries, others (postgres) hide them. SQLAlchemy's get_indexes contract
# needs a per-index column list, which ADBC does not provide at all. So
# we return an empty list here; driver-specific subclasses can override
# by issuing a native SELECT against pg_indexes / sqlite_master.
def indexes_stub(tbl: dict[str, Any]) -> list[dict[str, Any]]:
    """Empty list — ADBC GetObjects doesn't carry index column metadata.

    Driver-specific subclasses override this by querying the backend's
    native catalog views (``sqlite_master`` for SQLite, ``pg_indexes``
    for Postgres, etc.). Keeping the no-op here rather than raising
    means ``MetaData.reflect()`` doesn't fail on the common case of
    a table without SQLAlchemy caring about its indexes.
    """
    return []
