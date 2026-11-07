"""Authentication settings, OAuth2 token source, Negotiate header."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemy_adbc.auth import AuthSettings, require, settings_from_query
from sqlalchemy_adbc.config import Config

CFG = Config(
    {},
    {
        "flight": {
            "type": "oauth2",
            "token_url": "https://idp/token",
            "client_id": "cid",
            "client_secret": "env:SEC",
            "scope": "openid",
        },
        "krb": {"type": "negotiate", "spn": "HTTP@fs"},
    },
    None,
)


def test_url_only():
    q = {"auth": "basic", "tls": "true"}
    s = settings_from_query(q, CFG)
    assert s.type == "basic" and q == {"tls": "true"}  # auth keys popped, others kept


def test_profile_fills_and_url_wins():
    q = {"auth_profile": "flight", "oauth2_scope": "custom"}
    s = settings_from_query(q, CFG, env={"SEC": "s3cret"})
    assert (
        s.type,
        s.oauth2_token_url,
        s.oauth2_client_id,
        s.oauth2_client_secret,
        s.oauth2_scope,
        s.oauth2_flow,
    ) == ("oauth2", "https://idp/token", "cid", "s3cret", "custom", "client_credentials")
    assert "s3cret" not in repr(s) and "***" in repr(s)


def test_negotiate_profile():
    s = settings_from_query({"auth_profile": "krb"}, CFG)
    assert (s.type, s.spn) == ("negotiate", "HTTP@fs")


def test_unknown_profile_names_profile_not_values():
    with pytest.raises(ArgumentError, match="auth_profile 'nope'"):
        settings_from_query({"auth_profile": "nope"}, CFG)


def test_env_ref_missing_names_key():
    with pytest.raises(ArgumentError, match="client_secret") as ei:
        settings_from_query({"auth_profile": "flight"}, CFG, env={})
    assert "s3cret" not in str(ei.value)


def test_no_auth_keys_gives_type_none():
    assert settings_from_query({"tls": "false"}, Config({}, {}, None)).type is None


def test_require_names_missing_key():
    s = AuthSettings("oauth2", None, "client_credentials", None, "cid", "x", None, None)
    with pytest.raises(ArgumentError, match="auth=oauth2 requires oauth2_token_url"):
        require(s, "oauth2_token_url", "oauth2_client_id")
