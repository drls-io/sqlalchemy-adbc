"""Unit tests for the Snowflake URL → ADBC DSN translation."""

from __future__ import annotations

from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.snowflake import ADBCSnowflakeDialect


def _build(url_str: str):
    return ADBCSnowflakeDialect().build_connect_args(make_url(url_str))


def test_basic_dsn():
    args, kwargs = _build("adbc+snowflake://user:pw@myacct/db/schema?warehouse=WH")
    assert args == ["user:pw@myacct/db/schema?warehouse=WH"]
    assert kwargs == {}


def test_multiple_query_params_preserved():
    args, _ = _build("adbc+snowflake://u:p@acct/db/s?warehouse=WH&role=R")
    # Order of dict.items() in Python 3.7+ is insertion order; SQLAlchemy
    # preserves URL order, so this is deterministic.
    assert args[0].endswith("?warehouse=WH&role=R")


def test_password_at_sign_escaped():
    args, _ = _build("adbc+snowflake://u:p%40w@acct/db")
    assert args == ["u:p%40w@acct/db"]


def test_query_value_with_space_escaped():
    """warehouse names can legally contain spaces when quoted in SQL."""
    args, _ = _build("adbc+snowflake://u:p@acct/db?warehouse=MY%20WH")
    assert args[0].endswith("?warehouse=MY%20WH")


def test_query_ampersand_in_value_escaped():
    args, _ = _build("adbc+snowflake://u:p@acct/db?role=A%26B")
    # & inside a value must be %26 or the parser will treat it as a separator
    assert args[0].endswith("?role=A%26B")


def test_no_database():
    args, _ = _build("adbc+snowflake://u:p@acct")
    assert args == ["u:p@acct"]


def test_no_query():
    args, _ = _build("adbc+snowflake://u:p@acct/db")
    assert args == ["u:p@acct/db"]


def test_username_only():
    args, _ = _build("adbc+snowflake://u@acct/db")
    assert args == ["u@acct/db"]


def test_database_path_with_schema():
    # SQLAlchemy treats everything after the host as `database`; we must
    # NOT over-escape ``/`` between db and schema.
    args, _ = _build("adbc+snowflake://u:p@acct/my_db/public")
    assert args == ["u:p@acct/my_db/public"]
