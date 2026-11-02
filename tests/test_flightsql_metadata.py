"""Flight SQL metadata variants and optional live read/write coverage."""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
import sqlalchemy as sa

from sqlalchemy_adbc import reflection


def _tree(table_type: str) -> list[dict[str, Any]]:
    return [
        {
            "catalog_name": "redis",
            "catalog_db_schemas": [
                {
                    "db_schema_name": "public",
                    "db_schema_tables": [{"table_name": "orders", "table_type": table_type}],
                },
                {
                    "db_schema_name": "other",
                    "db_schema_tables": [{"table_name": "hidden", "table_type": table_type}],
                },
            ],
        }
    ]


@pytest.mark.parametrize("table_type", ["table", "TABLE", "Table"])
def test_table_type_case(table_type: str) -> None:
    assert reflection.table_names_from_tree(_tree(table_type), schema="public") == ["orders"]
    assert reflection.view_names_from_tree(_tree(table_type), schema="public") == []


@pytest.mark.parametrize("table_type", ["view", "VIEW", "View"])
def test_view_type_case(table_type: str) -> None:
    assert reflection.view_names_from_tree(_tree(table_type), schema="public") == ["orders"]
    assert reflection.table_names_from_tree(_tree(table_type), schema="public") == []


@pytest.mark.parametrize("table_type", ["index", "INDEX", "sqlite_autoindex", "SQLITE_AUTOINDEX"])
def test_index_type_case(table_type: str) -> None:
    tree = _tree(table_type)
    tree[0]["catalog_db_schemas"][0]["db_schema_tables"].append(
        {"table_name": "orders", "table_type": "TABLE"}
    )
    found = reflection.find_table(tree, "orders", schema="public")
    assert found is not None
    assert found["table_type"] == "TABLE"
    assert reflection.table_names_from_tree(_tree(table_type)) == []
    assert reflection.view_names_from_tree(_tree(table_type)) == []


@pytest.mark.parametrize(
    "code,expected",
    [
        (-9, sa.String),
        (-8, sa.CHAR),
        (-7, sa.Boolean),
        (-6, sa.SmallInteger),
        (-5, sa.BigInteger),
        (-4, sa.LargeBinary),
        (-3, sa.LargeBinary),
        (-2, sa.LargeBinary),
        (-1, sa.Text),
        (1, sa.CHAR),
        (2, sa.Numeric),
        (3, sa.Numeric),
        (4, sa.Integer),
        (5, sa.SmallInteger),
        (6, sa.Float),
        (7, sa.Float),
        (8, sa.Float),
        (12, sa.String),
        (91, sa.Date),
        (92, sa.Time),
        (93, sa.DateTime),
    ],
)
def test_columns_without_type_name(code: int, expected: type[sa.types.TypeEngine[Any]]) -> None:
    columns = reflection.columns_from_table(
        {
            "table_columns": [
                {"column_name": "value", "xdbc_type_name": None, "xdbc_data_type": code}
            ]
        }
    )
    assert type(columns[0]["type"]) is expected


@pytest.mark.parametrize("code", [None, 0, 9, 10, 12345])
def test_unknown_numeric_type_stays_null(code: int | None) -> None:
    columns = reflection.columns_from_table(
        {"table_columns": [{"column_name": "value", "xdbc_data_type": code}]}
    )
    assert isinstance(columns[0]["type"], sa.types.NullType)


def test_type_name_and_dialect_override_take_precedence() -> None:
    columns = reflection.columns_from_table(
        {
            "table_columns": [
                {"column_name": "value", "xdbc_type_name": "JSONB", "xdbc_data_type": 12}
            ]
        },
        ischema_names={"JSONB": sa.JSON},
    )
    assert type(columns[0]["type"]) is sa.JSON
    columns = reflection.columns_from_table(
        {
            "table_columns": [
                {"column_name": "value", "xdbc_type_name": "BIGINT", "xdbc_data_type": 4}
            ]
        }
    )
    assert type(columns[0]["type"]) is sa.BigInteger


def test_live_flightsql_reflected_table_reads_and_writes() -> None:
    """Verify discovery, typed reflection and CRUD against a disposable endpoint."""
    url = os.environ.get("DRLS_FLIGHTSQL_URL")
    if not url:
        pytest.skip("Set DRLS_FLIGHTSQL_URL to a disposable Flight SQL endpoint")
    schema = os.environ.get("DRLS_FLIGHTSQL_SCHEMA", "public")
    name = f"adbc_reflection_{uuid.uuid4().hex[:12]}"
    engine = sa.create_engine(url, connect_args={"autocommit": True})
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(f'CREATE TABLE "{name}" (id BIGINT, label TEXT, amount DOUBLE)')
        inspector = sa.inspect(engine)
        assert name in inspector.get_table_names(schema=schema)
        table = sa.Table(name, sa.MetaData(), schema=schema, autoload_with=engine)
        assert isinstance(table.c.id.type, sa.BigInteger)
        assert isinstance(table.c.label.type, sa.String)
        assert isinstance(table.c.amount.type, sa.Float)
        with engine.connect() as conn:
            conn.execute(table.insert().values(id=1, label="west", amount=10.0))
            conn.execute(table.insert().values(id=2, label="east", amount=None))
            assert conn.execute(sa.select(table).order_by(table.c.id)).all() == [
                (1, "west", 10.0),
                (2, "east", None),
            ]
            conn.execute(table.update().where(table.c.id == 1).values(amount=25.0))
            assert conn.execute(sa.select(table.c.amount).where(table.c.id == 1)).scalar() == 25.0
            conn.execute(table.delete().where(table.c.id == 2))
            assert conn.execute(sa.select(table)).all() == [(1, "west", 25.0)]
    finally:
        with engine.connect() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"')
        engine.dispose()
