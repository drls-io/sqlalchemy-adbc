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
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import types as sa_types
from sqlalchemy.dialects.postgresql import Range

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
