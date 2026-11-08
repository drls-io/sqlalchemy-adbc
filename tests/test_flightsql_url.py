"""Unit tests for the Flight SQL URL → ADBC kwargs translation.

These tests do not open a connection, so they don't require an
``adbc_driver_flightsql`` install — they just verify we translate
SQLAlchemy URLs to the expected driver args.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.flightsql import ADBCFlightSQLDialect


def _build(url_str: str):
    return ADBCFlightSQLDialect().build_connect_args(make_url(url_str))


def test_tls_is_default():
    """No ``tls`` query param → scheme is grpc+tls (secure by default)."""
    args, kwargs = _build("flightsql+adbc://flight-sql.example.com:443")
    assert args == ["grpc+tls://flight-sql.example.com:443"]
    assert "db_kwargs" not in kwargs


def test_tls_explicit_opt_out():
    """``?tls=false`` drops back to plaintext gRPC — needed for local dev."""
    args, kwargs = _build("flightsql+adbc://localhost:50051?tls=false")
    assert args == ["grpc://localhost:50051"]
    assert "db_kwargs" not in kwargs


def test_default_port_tls():
    args, _ = _build("flightsql+adbc://flight-sql.example.com")
    assert args == ["grpc+tls://flight-sql.example.com:443"]


def test_default_port_plaintext():
    args, _ = _build("flightsql+adbc://localhost?tls=false")
    assert args == ["grpc://localhost:50051"]


def test_tls_scheme_explicit():
    args, _ = _build("flightsql+adbc://flight-sql.example.com:443?tls=true")
    assert args == ["grpc+tls://flight-sql.example.com:443"]


def test_bearer_from_password():
    args, kwargs = _build("flightsql+adbc://user:TOKEN@host:443?tls=true")
    assert args == ["grpc+tls://host:443"]
    # We can't assert on the exact key name without the driver installed —
    # the flightsql dialect has a fallback constant for this case.
    db = kwargs["db_kwargs"]
    assert any(v == "Bearer TOKEN" for v in db.values())


def test_authorization_passthrough():
    args, kwargs = _build("flightsql+adbc://host:443?tls=true&authorization=Bearer%20abc")
    db = kwargs["db_kwargs"]
    assert any(v == "Bearer abc" for v in db.values())


def test_custom_headers():
    _, kwargs = _build("flightsql+adbc://host:443?tls=true&header.x-workspace-id=ws-1")
    db = kwargs["db_kwargs"]
    # Header keys use the RPC_CALL_HEADER_PREFIX; just check the value made
    # it through.
    assert any(v == "ws-1" for v in db.values())


def test_unknown_query_passthrough():
    _, kwargs = _build("flightsql+adbc://host:443?tls=true&some_opt=value")
    assert kwargs["db_kwargs"]["some_opt"] == "value"


# ── Authentication modes (auth=…, auth_profile=…) ────────────────────

import pytest  # noqa: E402
from sqlalchemy.exc import ArgumentError  # noqa: E402

AUTH_HDR = "adbc.flight.sql.authorization_header"


def test_auth_basic_uses_handshake_credentials():
    args, kw = _build("flightsql+adbc://alice:pw@h:443?auth=basic")
    dbk = kw["db_kwargs"]
    assert dbk["username"] == "alice" and dbk["password"] == "pw"
    assert AUTH_HDR not in dbk


def test_auth_basic_without_password_names_key():  # Review Focus 5
    with pytest.raises(ArgumentError, match="auth=basic requires password"):
        _build("flightsql+adbc://alice@h:443?auth=basic")


def test_auth_bearer_is_legacy_behaviour():
    legacy = _build("flightsql+adbc://u:tok@h:443")[1]["db_kwargs"]
    explicit = _build("flightsql+adbc://u:tok@h:443?auth=bearer")[1]["db_kwargs"]
    assert legacy == explicit == {AUTH_HDR: "Bearer tok"}


def _connect_db_kwargs(dialect, url_str, monkeypatch):
    """Run ``create_connect_args`` + ``connect`` with a stub DBAPI; return db_kwargs."""
    seen = []

    class FakeDBAPI:
        @staticmethod
        def connect(*cargs, **cparams):
            seen.append(cparams)
            return object()

    monkeypatch.setattr(type(dialect), "loaded_dbapi", FakeDBAPI, raising=False)
    args, kw = dialect.create_connect_args(make_url(url_str))
    dialect.connect(*args, **kw)
    return seen[0]["db_kwargs"]


def test_auth_negotiate_spn_is_the_connection_host(monkeypatch):
    import sqlalchemy_adbc.flightsql as fs

    monkeypatch.setattr(fs, "negotiate_header", lambda spn: f"Negotiate <{spn}>")
    dbk = _connect_db_kwargs(
        ADBCFlightSQLDialect(), "flightsql+adbc://flight.example:443?auth=negotiate", monkeypatch
    )
    assert dbk[AUTH_HDR] == "Negotiate <HTTP@flight.example>"
    # A URL may not point the ticket at another service (relay risk).
    with pytest.raises(ArgumentError, match="spn may only be set in a config profile"):
        _build("flightsql+adbc://flight.example:443?auth=negotiate&spn=HTTP%40other")


def test_auth_negotiate_spn_from_profile(monkeypatch, tmp_path):
    import sqlalchemy_adbc.flightsql as fs

    monkeypatch.setattr(fs, "negotiate_header", lambda spn: f"Negotiate <{spn}>")
    p = tmp_path / "c.toml"
    p.write_text('[auth.krb]\ntype = "negotiate"\nspn = "HTTP@flight.corp.example"\n')
    p.chmod(0o600)
    monkeypatch.setenv("SQLALCHEMY_ADBC_CONFIG", str(p))
    dbk = _connect_db_kwargs(
        ADBCFlightSQLDialect(), "flightsql+adbc://10.0.0.5:443?auth_profile=krb", monkeypatch
    )
    assert dbk[AUTH_HDR] == "Negotiate <HTTP@flight.corp.example>"


def test_auth_oauth2_maps_to_driver_options():
    # client id and secret travel as URL user:password (masked by SQLAlchemy)
    args, kw = _build(
        "flightsql+adbc://cid:sec@h:443?auth=oauth2&oauth2_token_url=https://idp/t&oauth2_scope=openid"
    )
    dbk = kw["db_kwargs"]
    assert dbk["adbc.flight.sql.oauth.flow"] == "client_credentials"
    assert dbk["adbc.flight.sql.oauth.token_uri"] == "https://idp/t"
    assert dbk["adbc.flight.sql.oauth.client_id"] == "cid"
    assert dbk["adbc.flight.sql.oauth.client_secret"] == "sec"
    assert dbk["adbc.flight.sql.oauth.scope"] == "openid"
    assert AUTH_HDR not in dbk  # the password is a client secret here, not a bearer
    assert not any("oauth2" in k for k in dbk)  # auth keys never leak as driver options


def test_auth_oauth2_missing_token_url_names_key():
    with pytest.raises(ArgumentError, match="auth=oauth2 requires oauth2_token_url"):
        _build("flightsql+adbc://cid:sec@h:443?auth=oauth2")


def test_auth_oauth2_secret_in_query_rejected():
    with pytest.raises(
        ArgumentError, match="oauth2_client_secret is not accepted in the URL query"
    ):
        _build(
            "flightsql+adbc://h:443?auth=oauth2&oauth2_token_url=https://idp/t&oauth2_client_secret=s"
        )


def test_auth_profile_from_config(tmp_path, monkeypatch):
    p = tmp_path / "c.toml"
    p.write_text(
        '[auth.fl]\ntype = "oauth2"\ntoken_url = "https://idp/t"\nclient_id = "cid"\n'
        'client_secret = "env:S"\n'
    )
    p.chmod(0o600)
    monkeypatch.setenv("SQLALCHEMY_ADBC_CONFIG", str(p))
    monkeypatch.setenv("S", "shh")
    dbk = _build("flightsql+adbc://h:443?auth_profile=fl")[1]["db_kwargs"]
    assert dbk["adbc.flight.sql.oauth.client_secret"] == "shh"
    assert dbk["adbc.flight.sql.oauth.client_id"] == "cid"


def test_unknown_auth_value():
    with pytest.raises(ArgumentError, match="auth must be one of"):
        _build("flightsql+adbc://h:443?auth=magic")


def test_auth_negotiate_header_is_minted_per_connection(monkeypatch):
    """A SPNEGO token is single use; ``create_connect_args`` runs once per
    engine, so minting there would replay one token for every connection."""
    import sqlalchemy_adbc.flightsql as fs

    calls = []

    def mint(spn):
        calls.append(spn)
        return f"Negotiate <{spn}#{len(calls)}>"

    monkeypatch.setattr(fs, "negotiate_header", mint)
    dialect = fs.ADBCFlightSQLDialect()
    args, kw = dialect.create_connect_args(
        sa.make_url("flightsql+adbc://flight.example:443?auth=negotiate")
    )
    assert calls == [] and AUTH_HDR not in kw.get("db_kwargs", {})

    seen = []

    class FakeDBAPI:
        @staticmethod
        def connect(*cargs, **cparams):
            seen.append((cargs, cparams))
            return object()

    monkeypatch.setattr(fs.ADBCFlightSQLDialect, "loaded_dbapi", FakeDBAPI, raising=False)
    dialect.connect(*args, **kw)
    dialect.connect(*args, **kw)
    assert [c[1]["db_kwargs"][AUTH_HDR] for c in seen] == [
        "Negotiate <HTTP@flight.example#1>",
        "Negotiate <HTTP@flight.example#2>",
    ]
    assert AUTH_HDR not in kw.get("db_kwargs", {})
