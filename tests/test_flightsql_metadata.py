"""Flight SQL metadata variants: upper-case table types and columns that carry
only a numeric ``xdbc_data_type``.

Contributed by Albert T. Wong (#17), seen with Redis's Arrow Flight SQL server:
queries worked but the Inspector returned no tables and Superset showed every
column as UNKNOWN.
"""

from __future__ import annotations

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


@pytest.mark.parametrize("table_type", ["table", "TABLE", "Table", "BASE TABLE"])
def test_table_type_case(table_type: str) -> None:
    assert reflection.table_names_from_tree(_tree(table_type), schema="public") == ["orders"]
    assert reflection.view_names_from_tree(_tree(table_type), schema="public") == []


@pytest.mark.parametrize("table_type", ["view", "VIEW", "View"])
def test_view_type_case(table_type: str) -> None:
    assert reflection.view_names_from_tree(_tree(table_type), schema="public") == ["orders"]
    assert reflection.table_names_from_tree(_tree(table_type), schema="public") == []


@pytest.mark.parametrize("table_type", ["index", "INDEX", "sqlite_autoindex", "SQLITE_AUTOINDEX"])
def test_index_entries_are_skipped_regardless_of_case(table_type: str) -> None:
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
    ("code", "expected"),
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
def test_columns_without_type_name_fall_back_to_xdbc_code(
    code: int, expected: type[sa.types.TypeEngine[Any]]
) -> None:
    columns = reflection.columns_from_table(
        {
            "table_columns": [
                {"column_name": "value", "xdbc_type_name": None, "xdbc_data_type": code}
            ]
        }
    )
    assert type(columns[0]["type"]) is expected


@pytest.mark.parametrize("code", [None, 0, 9, 10, 12345])
def test_unknown_or_ambiguous_codes_stay_null(code: int | None) -> None:
    # 9 (DATETIME) and 10 (INTERVAL) need subtype metadata; guessing is worse
    # than NullType.
    columns = reflection.columns_from_table(
        {"table_columns": [{"column_name": "value", "xdbc_data_type": code}]}
    )
    assert isinstance(columns[0]["type"], sa.types.NullType)


def test_type_name_and_dialect_override_take_precedence_over_code() -> None:
    columns = reflection.columns_from_table(
        {"table_columns": [{"column_name": "v", "xdbc_type_name": "JSONB", "xdbc_data_type": 12}]},
        ischema_names={"JSONB": sa.JSON},
    )
    assert type(columns[0]["type"]) is sa.JSON
    columns = reflection.columns_from_table(
        {"table_columns": [{"column_name": "v", "xdbc_type_name": "BIGINT", "xdbc_data_type": 4}]}
    )
    assert type(columns[0]["type"]) is sa.BigInteger


def test_unmapped_type_name_falls_back_to_code() -> None:
    columns = reflection.columns_from_table(
        {"table_columns": [{"column_name": "v", "xdbc_type_name": "WEIRD", "xdbc_data_type": -5}]}
    )
    assert type(columns[0]["type"]) is sa.BigInteger
