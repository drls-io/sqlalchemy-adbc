"""Unit tests for the Flight SQL URL → ADBC kwargs translation.

These tests do not open a connection, so they don't require an
``adbc_driver_flightsql`` install — they just verify we translate
SQLAlchemy URLs to the expected driver args.
"""

from __future__ import annotations

from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.flightsql import ADBCFlightSQLDialect


def _build(url_str: str):
    return ADBCFlightSQLDialect().build_connect_args(make_url(url_str))


def test_tls_is_default():
    """No ``tls`` query param → scheme is grpc+tls (secure by default)."""
    args, kwargs = _build("adbc+flightsql://flight-sql.example.com:443")
    assert args == ["grpc+tls://flight-sql.example.com:443"]
    assert "db_kwargs" not in kwargs


def test_tls_explicit_opt_out():
    """``?tls=false`` drops back to plaintext gRPC — needed for local dev."""
    args, kwargs = _build("adbc+flightsql://localhost:50051?tls=false")
    assert args == ["grpc://localhost:50051"]
    assert "db_kwargs" not in kwargs


def test_default_port_tls():
    args, _ = _build("adbc+flightsql://flight-sql.example.com")
    assert args == ["grpc+tls://flight-sql.example.com:443"]


def test_default_port_plaintext():
    args, _ = _build("adbc+flightsql://localhost?tls=false")
    assert args == ["grpc://localhost:50051"]


def test_tls_scheme_explicit():
    args, _ = _build("adbc+flightsql://flight-sql.example.com:443?tls=true")
    assert args == ["grpc+tls://flight-sql.example.com:443"]


def test_bearer_from_password():
    args, kwargs = _build("adbc+flightsql://user:TOKEN@host:443?tls=true")
    assert args == ["grpc+tls://host:443"]
    # We can't assert on the exact key name without the driver installed —
    # the flightsql dialect has a fallback constant for this case.
    db = kwargs["db_kwargs"]
    assert any(v == "Bearer TOKEN" for v in db.values())


def test_authorization_passthrough():
    args, kwargs = _build("adbc+flightsql://host:443?tls=true&authorization=Bearer%20abc")
    db = kwargs["db_kwargs"]
    assert any(v == "Bearer abc" for v in db.values())


def test_custom_headers():
    _, kwargs = _build("adbc+flightsql://host:443?tls=true&header.x-workspace-id=ws-1")
    db = kwargs["db_kwargs"]
    # Header keys use the RPC_CALL_HEADER_PREFIX; just check the value made
    # it through.
    assert any(v == "ws-1" for v in db.values())


def test_unknown_query_passthrough():
    _, kwargs = _build("adbc+flightsql://host:443?tls=true&some_opt=value")
    assert kwargs["db_kwargs"]["some_opt"] == "value"
