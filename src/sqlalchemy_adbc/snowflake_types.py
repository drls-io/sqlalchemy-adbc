"""Snowflake type registry — reflection of Snowflake-specific type names.

The ADBC Snowflake driver reports clean ``xdbc_type_name`` values
(``NUMBER``, ``TIMESTAMP_TZ``, ``VARIANT``, ...) plus ``xdbc_column_size``
and ``xdbc_decimal_digits``. The generic map in :mod:`reflection` knows
none of these, so without this registry they reflected as ``NullType``.

Values already arrive through Arrow as the right Python objects
(``Decimal`` for NUMBER, aware ``datetime`` for TIMESTAMP_TZ, JSON text
for VARIANT/OBJECT/ARRAY); the decorators here only adjust what needs it:

- NUMBER(p, 0)        → ``int`` (Snowflake's INTEGER is NUMBER(38,0))
- NUMBER(p, s>0)      → ``Decimal`` (unchanged)
- VARIANT/OBJECT/ARRAY → ``json.loads`` on read, ``PARSE_JSON(json)`` on bind
- TEXT(n)             → ``String(n)``
- TIMESTAMP_TZ / _LTZ → ``DateTime(timezone=True)``; TIMESTAMP_NTZ → ``DateTime()``

Registry entries are either a type class (instantiated bare, as in
``pg_ischema_names``) or a factory ``f(size, decimal_digits)``.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy import types as sa_types


class NUMBER(sa_types.TypeDecorator[Any]):
    """Snowflake ``NUMBER(p, s)``: ``int`` when ``s == 0``, else ``Decimal``."""

    impl = sa_types.Numeric
    cache_ok = True

    def __init__(self, precision: int | None = None, scale: int | None = None) -> None:
        super().__init__(precision=precision, scale=scale)
        self.precision = precision
        self.scale = scale

    @property
    def python_type(self) -> type:
        return int if self.scale == 0 else Decimal

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None or self.scale != 0:
            return value
        return int(value)


class VARIANT(sa_types.TypeDecorator[Any]):
    """Snowflake semi-structured value: JSON text on the wire, Python on read.

    Binds render as ``PARSE_JSON(?)``. Snowflake rejects functions over
    parameters inside a ``VALUES`` clause, so ``insert().values(v=...)``
    fails with "Invalid expression [PARSE_JSON(?)] in VALUES clause"; use
    ``insert().from_select([...], select(...))`` or an UPDATE, where the
    expression is allowed.
    """

    impl = sa_types.Text
    cache_ok = True

    def bind_expression(self, bindvalue: Any) -> Any:
        return sa.func.PARSE_JSON(bindvalue)

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        return None if value is None else json.dumps(value)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is None or not isinstance(value, str):
            return value
        return json.loads(value)


class OBJECT(VARIANT):
    cache_ok = True


class ARRAY(VARIANT):
    cache_ok = True


def _number(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    return NUMBER(precision=size, scale=digits)


def _text(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    return sa_types.String(size) if size else sa_types.Text()


def _timestamp_tz(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    return sa_types.DateTime(timezone=True)


def _timestamp_ntz(size: int | None, digits: int | None) -> sa_types.TypeEngine[Any]:
    return sa_types.DateTime(timezone=False)


snowflake_ischema_names: dict[str, Any] = {
    "NUMBER": _number,
    "TEXT": _text,
    "TIMESTAMP_NTZ": _timestamp_ntz,
    "TIMESTAMP_TZ": _timestamp_tz,
    "TIMESTAMP_LTZ": _timestamp_tz,
    "VARIANT": VARIANT,
    "OBJECT": OBJECT,
    "ARRAY": ARRAY,
}
