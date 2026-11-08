"""Unit tests for the base dialect."""

from __future__ import annotations

import pytest
from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.base import ADBCDialect


def test_import_dbapi_without_driver_module_raises():
    """Plain ADBCDialect has no driver_module — must error clearly."""

    class NoDriver(ADBCDialect):
        name = "adbc"
        driver = "none"

    with pytest.raises(RuntimeError, match="driver_module"):
        NoDriver.import_dbapi()


def test_import_dbapi_subclass_imports_module():
    """A subclass pointing at a real module successfully imports it."""

    class Fake(ADBCDialect):
        name = "adbc"
        driver = "fake"
        driver_module = "importlib"  # any importable module works for the smoke

    assert Fake.import_dbapi().__name__ == "importlib"


def test_format_uri_host_port_database():
    url = make_url("adbc://host:9999/mydb")
    assert ADBCDialect()._format_uri(url) == "host:9999/mydb"


def test_format_uri_host_only():
    url = make_url("adbc://host")
    assert ADBCDialect()._format_uri(url) == "host"


def test_format_uri_database_only():
    url = make_url("adbc:///mydb")
    assert ADBCDialect()._format_uri(url) == "mydb"


def test_format_uri_empty_returns_none():
    url = make_url("adbc://")
    assert ADBCDialect()._format_uri(url) is None


def test_build_connect_args_forwards_query_as_db_kwargs():
    class WithDriver(ADBCDialect):
        driver_module = "adbc_driver_sqlite.dbapi"

    url = make_url("adbc://host:1234/db?key=value")
    args, kwargs = WithDriver().build_connect_args(url)
    assert args == ["host:1234/db"]
    assert kwargs == {"db_kwargs": {"key": "value"}}


def test_build_connect_args_empty_query_omits_db_kwargs():
    class WithDriver(ADBCDialect):
        driver_module = "adbc_driver_sqlite.dbapi"

    url = make_url("adbc://host:1234/db")
    args, kwargs = WithDriver().build_connect_args(url)
    assert args == ["host:1234/db"]
    assert kwargs == {}


def test_dialect_name_and_driver_attrs():
    """SQLAlchemy uses these for URL scheme resolution."""
    assert ADBCDialect.name == "adbc"
    assert ADBCDialect.driver == "adbc"


def test_supports_statement_cache_true():
    """Required to silence SQLAlchemy's caching-disabled warning."""
    assert ADBCDialect.supports_statement_cache is True


def test_connection_mixin_is_reusable_with_another_base():
    """The ADBC connection plumbing must be mixable into one of SQLAlchemy's own
    dialects (the SQLite dialect uses it that way), independent of the generic
    GetObjects reflection that ADBCDialect adds on top."""
    from sqlalchemy.engine.default import DefaultDialect

    from sqlalchemy_adbc.base import ADBCConnectionMixin

    class X(ADBCConnectionMixin, DefaultDialect):
        name = "x"
        driver = "adbc"
        driver_module = "adbc_driver_sqlite.dbapi"

    assert hasattr(X.import_dbapi(), "connect")
    x = X()
    assert x.postfetch_lastrowid is False
    assert x.supports_sane_rowcount is False
    assert x._connect_hook is None
    assert ADBCConnectionMixin in ADBCDialect.__mro__
    # Reflection belongs to ADBCDialect, not the mixin.
    assert "get_columns" not in ADBCConnectionMixin.__dict__
    assert "get_columns" in ADBCDialect.__dict__
    args, kw = x.create_connect_args(make_url("sqlite+adbc:///:memory:?a=1"))
    assert args == [":memory:"] and kw == {"db_kwargs": {"a": "1"}}
