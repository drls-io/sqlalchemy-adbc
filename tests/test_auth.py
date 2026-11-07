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


# ── OAuth2 client credentials ────────────────────────────────────────

import base64  # noqa: E402
import json  # noqa: E402
import threading  # noqa: E402
from http.server import BaseHTTPRequestHandler, HTTPServer  # noqa: E402
from urllib.parse import parse_qs  # noqa: E402

from sqlalchemy_adbc.auth import OAuth2ClientCredentials  # noqa: E402


@pytest.fixture
def token_server():
    state: dict = {
        "requests": [],
        "status": 200,
        "body": {"access_token": "tok-1", "expires_in": 3600, "token_type": "Bearer"},
    }

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers["Content-Length"])
            form = parse_qs(self.rfile.read(n).decode())
            state["requests"].append(
                {
                    "path": self.path,
                    "auth": self.headers.get("Authorization"),
                    "ctype": self.headers.get("Content-Type"),
                    "form": form,
                }
            )
            self.send_response(state["status"])
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(state["body"]).encode())

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    state["url"] = f"http://127.0.0.1:{srv.server_port}/token"
    yield state
    srv.shutdown()


def test_client_credentials_request_shape_and_caching(token_server):
    clock = [1000.0]
    src = OAuth2ClientCredentials(
        token_server["url"], "cid", "s3cret", scope="openid", now=lambda: clock[0]
    )
    assert src.token() == "tok-1"
    req = token_server["requests"][0]
    assert req["form"] == {"grant_type": ["client_credentials"], "scope": ["openid"]}
    assert req["auth"] == "Basic " + base64.b64encode(b"cid:s3cret").decode()
    assert req["ctype"].startswith("application/x-www-form-urlencoded")
    assert src.token() == "tok-1" and len(token_server["requests"]) == 1  # cached
    clock[0] += 3600 - 30  # inside the 60 s safety margin → refresh
    token_server["body"]["access_token"] = "tok-2"
    assert src.token() == "tok-2" and len(token_server["requests"]) == 2


def test_non_2xx_does_not_leak_secret(token_server):
    token_server["status"] = 401
    with pytest.raises(ArgumentError, match="HTTP 401") as ei:
        OAuth2ClientCredentials(token_server["url"], "cid", "s3cret").token()
    assert "s3cret" not in str(ei.value)


def test_body_without_access_token_and_string_expires_in(token_server):  # Review Focus 4
    token_server["body"] = {"token_type": "Bearer"}
    with pytest.raises(ArgumentError, match="access_token"):
        OAuth2ClientCredentials(token_server["url"], "cid", "s").token()
    token_server["body"] = {"access_token": "t", "expires_in": "120"}
    assert OAuth2ClientCredentials(token_server["url"], "cid", "s").token() == "t"


def test_token_source_for_is_shared_per_settings():
    from sqlalchemy_adbc.auth import token_source_for

    a = AuthSettings("oauth2", None, "client_credentials", "https://idp/t", "cid", "s", None, None)
    b = AuthSettings("oauth2", None, "client_credentials", "https://idp/t", "cid", "s", None, None)
    c = AuthSettings(
        "oauth2", None, "client_credentials", "https://idp/t", "other", "s", None, None
    )
    assert token_source_for(a) is token_source_for(b)
    assert token_source_for(a) is not token_source_for(c)
