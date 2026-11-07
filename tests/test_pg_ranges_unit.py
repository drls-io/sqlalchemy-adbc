"""No-database tests for PostgreSQL range/multirange/hstore support (issue #11).

Server strings below were captured from PostgreSQL 16 through the ADBC
driver on 2026-11-07; see the design note in .claude/superpowers/specs.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect


def test_pg_type_compiler_renders_pg_type_names():
    d = ADBCPostgreSQLDialect()
    t = sa.Table(
        "t",
        sa.MetaData(),
        sa.Column("r", pg.INT4RANGE),
        sa.Column("h", pg.HSTORE),
        sa.Column("j", pg.JSONB),
        sa.Column("ts", sa.DateTime()),
    )
    ddl = str(sa.schema.CreateTable(t).compile(dialect=d))
    assert "r INT4RANGE" in ddl and "h HSTORE" in ddl and "j JSONB" in ddl
    assert "ts TIMESTAMP WITHOUT TIME ZONE" in ddl  # generic compiler said DATETIME
