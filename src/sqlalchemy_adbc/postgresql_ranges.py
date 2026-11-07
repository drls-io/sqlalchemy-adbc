"""PostgreSQL range, multirange and hstore support over ADBC.

The ADBC PostgreSQL driver returns these columns in PG's binary send
format and rejects text parameters for them. Every decorator here
therefore casts on both sides of the wire: ``CAST(:p AS <pgtype>)`` on
bind and ``CAST(col AS TEXT)`` on read, so Python only ever sees PG's
text representation, parsed by the small codecs below. The Python objects
are SQLAlchemy's own ``Range`` / ``MultiRange`` so code written for the
psycopg dialect works unchanged.

Text grammar handled (PG 16 output): ``empty``; ``<lb><lower>,<upper><ub>``
with ``lb`` in ``[(`` and ``ub`` in ``])``; an element is bare or
double-quoted (``""`` → ``"``, ``\\x`` → ``x``); a missing element is an
unbounded side. ``infinity`` / ``-infinity`` read as unbounded too, since
SQLAlchemy's ``Range`` has no infinity representation and PG treats both
alike for containment.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy import types as sa_types
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.dialects.postgresql import MultiRange, Range

_VALID_BOUNDS = {"[)", "[]", "(]", "()"}
_INFINITE = {"infinity", "-infinity"}
_NEEDS_QUOTES = re.compile(r'[\s,"\\()\[\]]')
_BARE_OFFSET = re.compile(r"([+-]\d{2})$")


@dataclass(frozen=True)
class ElementCodec:
    """How one range family's bound values travel as text."""

    parse: Callable[[str], Any]
    format: Callable[[Any], str]
    sql_type: sa_types.TypeEngine[Any]


def _parse_timestamp(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text)


def _parse_timestamptz(text: str) -> dt.datetime:
    # PG renders a whole-hour offset as "+00"; Python 3.10 needs "+00:00".
    return dt.datetime.fromisoformat(_BARE_OFFSET.sub(r"\1:00", text))


def _format_timestamptz(value: dt.datetime) -> str:
    if value.tzinfo is None:
        raise TypeError("tstzrange bounds must be timezone-aware datetimes")
    return value.isoformat(sep=" ")


INT_CODEC = ElementCodec(int, str, sa_types.Integer())
BIGINT_CODEC = ElementCodec(int, str, sa_types.BigInteger())
NUMERIC_CODEC = ElementCodec(Decimal, str, sa_types.Numeric())
DATE_CODEC = ElementCodec(dt.date.fromisoformat, dt.date.isoformat, sa_types.Date())
TIMESTAMP_CODEC = ElementCodec(
    _parse_timestamp, lambda v: v.isoformat(sep=" "), sa_types.DateTime()
)
TIMESTAMPTZ_CODEC = ElementCodec(
    _parse_timestamptz, _format_timestamptz, sa_types.DateTime(timezone=True)
)
TEXT_CODEC = ElementCodec(str, str, sa_types.Text())


def _split_bounds(body: str, text: str) -> tuple[str, str]:
    """Split the inside of a range at its top-level comma, honouring quotes."""
    in_quotes = False
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and in_quotes:
            i += 2
            continue
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == "," and not in_quotes:
            return body[:i], body[i + 1 :]
        i += 1
    raise ValueError(f"invalid range text: {text!r}")


def _unquote(element: str, text: str) -> str:
    if not element.startswith('"'):
        return element
    if len(element) < 2 or not element.endswith('"'):
        raise ValueError(f"invalid range text: {text!r}")
    out: list[str] = []
    inner = element[1:-1]
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == "\\" and i + 1 < len(inner):
            out.append(inner[i + 1])
            i += 2
        elif ch == '"':
            if inner[i : i + 2] != '""':
                raise ValueError(f"invalid range text: {text!r}")
            out.append('"')
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _parse_element(raw: str, codec: ElementCodec, text: str) -> Any:
    if raw == "":
        return None
    value = _unquote(raw, text)
    if value in _INFINITE:
        return None
    try:
        return codec.parse(value)
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(f"invalid range text: {text!r}") from exc


def parse_range(text: str, codec: ElementCodec) -> Range[Any]:
    """PG range text → ``Range``; raises ``ValueError`` naming bad input."""
    if text == "empty":
        return Range(empty=True)
    if len(text) < 3 or text[0] not in "[(" or text[-1] not in "])":
        raise ValueError(f"invalid range text: {text!r}")
    lower_raw, upper_raw = _split_bounds(text[1:-1], text)
    return Range(
        _parse_element(lower_raw, codec, text),
        _parse_element(upper_raw, codec, text),
        bounds=text[0] + text[-1],  # type: ignore[arg-type]
    )


def _quote(element: str) -> str:
    if element == "" or _NEEDS_QUOTES.search(element):
        return '"' + element.replace("\\", "\\\\").replace('"', '""') + '"'
    return element


def _format_element(value: Any, codec: ElementCodec) -> str:
    return "" if value is None else _quote(codec.format(value))


def format_range(value: Range[Any], codec: ElementCodec) -> str:
    """``Range`` → PG range text, validating bounds before anything leaves."""
    if value.empty:
        return "empty"
    bounds = value.bounds
    if bounds not in _VALID_BOUNDS:
        raise ValueError(
            f"invalid range bounds {bounds!r}; expected one of {sorted(_VALID_BOUNDS)}"
        )
    lower = _format_element(value.lower, codec)
    upper = _format_element(value.upper, codec)
    return f"{bounds[0]}{lower},{upper}{bounds[1]}"


def _split_top_level(body: str, text: str) -> list[str]:
    """Split ``{...}`` contents on commas outside brackets and quotes."""
    parts: list[str] = []
    depth = 0
    in_quotes = False
    start = 0
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and in_quotes:
            i += 2
            continue
        if ch == '"':
            in_quotes = not in_quotes
        elif not in_quotes:
            if ch in "[(":
                depth += 1
            elif ch in "])":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append(body[start:i])
                start = i + 1
        i += 1
    parts.append(body[start:])
    if in_quotes or depth != 0 or any(p == "" for p in parts):
        raise ValueError(f"invalid multirange text: {text!r}")
    return parts


def parse_multirange(text: str, codec: ElementCodec) -> MultiRange[Any]:
    """PG multirange text (``{r1,r2}``) → ``MultiRange``."""
    if len(text) < 2 or text[0] != "{" or text[-1] != "}":
        raise ValueError(f"invalid multirange text: {text!r}")
    body = text[1:-1]
    if body == "":
        return MultiRange([])
    try:
        return MultiRange([parse_range(part, codec) for part in _split_top_level(body, text)])
    except ValueError as exc:
        raise ValueError(f"invalid multirange text: {text!r}") from exc


def format_multirange(values: Sequence[Range[Any]], codec: ElementCodec) -> str:
    """``MultiRange`` or plain ``list[Range]`` → PG multirange text."""
    return "{" + ",".join(format_range(r, codec) for r in values) + "}"


# ── Decorators ───────────────────────────────────────────────────────


class _ElementCast(sa_types.TypeDecorator[Any]):
    """Scalar operand of a range operator, cast to the element SQL type.

    The driver binds a Python ``int`` as ``int8``; without the cast PG
    reports ``operator does not exist: int4range @> bigint``.
    """

    impl: Any = sa_types.TypeEngine  # replaced per instance; SQLAlchemy accepts an instance
    cache_ok = True

    def __init__(self, element_type: sa_types.TypeEngine[Any]) -> None:
        super().__init__()
        self.impl = element_type

    def bind_expression(self, bindvalue: Any) -> Any:
        return sa.cast(bindvalue, self.impl)


class _RangeType(sa_types.TypeDecorator[Any]):
    """Shared machinery: cast on bind, cast-to-text on read, text codec."""

    impl = sa_types.TypeEngine
    cache_ok = True
    codec: ElementCodec

    def bind_expression(self, bindvalue: Any) -> Any:
        return sa.cast(bindvalue, self.impl)

    def column_expression(self, colexpr: Any) -> Any:
        return sa.type_coerce(sa.cast(colexpr, sa_types.Text), self)

    def coerce_compared_value(self, op: Any, value: Any) -> sa_types.TypeEngine[Any]:
        if isinstance(value, Range):
            return self
        return _ElementCast(self.codec.sql_type)

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        return None if value is None else format_range(value, self.codec)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        return None if value is None else parse_range(value, self.codec)


class INT4RANGE(_RangeType):
    impl = pg.INT4RANGE
    cache_ok = True
    codec = INT_CODEC


class INT8RANGE(_RangeType):
    impl = pg.INT8RANGE
    cache_ok = True
    codec = BIGINT_CODEC


class NUMRANGE(_RangeType):
    impl = pg.NUMRANGE
    cache_ok = True
    codec = NUMERIC_CODEC


class TSRANGE(_RangeType):
    impl = pg.TSRANGE
    cache_ok = True
    codec = TIMESTAMP_CODEC


class TSTZRANGE(_RangeType):
    impl = pg.TSTZRANGE
    cache_ok = True
    codec = TIMESTAMPTZ_CODEC


class DATERANGE(_RangeType):
    impl = pg.DATERANGE
    cache_ok = True
    codec = DATE_CODEC
