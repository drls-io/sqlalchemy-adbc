"""ADBC Flight SQL dialect.

URL form::

    flightsql+adbc://host:port?tls=true&authorization=Bearer%20...

All query-string keys are forwarded to the driver as ``db_kwargs``. The
dialect also understands a few shorthands:

- ``tls=true`` → use ``grpc+tls://`` scheme (default is ``grpc://``)
- ``authorization=...`` → canonicalised to the ADBC ``AUTHORIZATION_HEADER``
  db_kwarg (matches ``adbc_driver_flightsql.DatabaseOptions``).
- ``header.<name>=<value>`` → forwarded via the RPC header prefix.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from sqlalchemy.engine.url import URL
from sqlalchemy.exc import ArgumentError

from sqlalchemy_adbc.auth import negotiate_header, require, settings_from_query
from sqlalchemy_adbc.base import ADBCDialect
from sqlalchemy_adbc.config import load_config

# Driver option names (adbc-driver-flightsql >= 1.13); spelled out so the
# package stays importable without the driver extra.
_OAUTH_FLOW = "adbc.flight.sql.oauth.flow"
_OAUTH_TOKEN_URI = "adbc.flight.sql.oauth.token_uri"
_OAUTH_CLIENT_ID = "adbc.flight.sql.oauth.client_id"
_OAUTH_CLIENT_SECRET = "adbc.flight.sql.oauth.client_secret"
_OAUTH_SCOPE = "adbc.flight.sql.oauth.scope"


class ADBCFlightSQLDialect(ADBCDialect):
    # Flight SQL has no standard Alembic impl (it's a protocol, not a
    # backend); Alembic will fall back to DefaultImpl, which emits
    # ANSI SQL DDL. Good enough for most Flight SQL servers.
    name = "flightsql"
    driver = "adbc"
    driver_module = "adbc_driver_flightsql.dbapi"
    supports_statement_cache = True

    def build_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        # SQLAlchemy's URL.query is `immutabledict[str, str | tuple[str, ...]]`
        # — repeated keys (``?h=a&h=b``) collapse to a tuple. Flatten to
        # `dict[str, str]` so the ADBC driver always sees scalar values;
        # for repeated keys we keep the last (typical shell + HTTP layer
        # behavior) rather than silently dropping them.
        query: dict[str, str] = {k: _scalar(v) for k, v in url.query.items()}

        # auth / auth_profile / oauth2_* / spn leave the query here, before
        # the remainder is passed through to the driver verbatim.
        settings = settings_from_query(query, load_config(), password=url.password)
        if settings.type == "oauth2" and not settings.oauth2_client_id and url.username:
            settings = replace(settings, oauth2_client_id=url.username)

        # TLS defaults to ON — Flight SQL is typically exposed as a
        # public service (e.g., flight-sql.drls.io), and a bearer token
        # sent over plaintext gRPC is a credential leak. Local/dev
        # setups on a plaintext listener can opt out with ``?tls=false``.
        tls = _pop_bool(query, "tls", default=True)
        scheme = "grpc+tls" if tls else "grpc"
        host = url.host or "localhost"
        # Flight SQL convention: 443 for TLS (matches HTTPS), 50051 for
        # plaintext gRPC (the Arrow Flight default).
        port = url.port or (443 if tls else 50051)
        uri = f"{scheme}://{host}:{port}"

        db_kwargs: dict[str, str] = {}

        # Lazy import keeps the package installable without the driver
        # extra — tests and IDE tooling shouldn't have to pull the
        # flightsql wheel.
        try:
            from adbc_driver_flightsql import DatabaseOptions

            auth_key: str = DatabaseOptions.AUTHORIZATION_HEADER.value
            header_prefix: str = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
        except ImportError:  # pragma: no cover - hit only without extra
            auth_key = "adbc.flight.sql.authorization_header"
            header_prefix = "adbc.flight.sql.rpc.call_header."

        if settings.type in (None, "bearer"):
            if "authorization" in query:
                db_kwargs[auth_key] = query.pop("authorization")
            # Default mode: the URL password is a bearer token.
            #     flightsql+adbc://user:<token>@host:443?tls=true
            # The "user" part is ignored by Flight SQL.
            if url.password:
                db_kwargs[auth_key] = f"Bearer {url.password}"
        elif settings.type == "basic":
            # Handshake basic auth, as the Flight SQL JDBC driver does it.
            if not url.username:
                raise ArgumentError("auth=basic requires username")
            if not url.password:
                raise ArgumentError("auth=basic requires password")
            db_kwargs["username"] = url.username
            db_kwargs["password"] = url.password
        elif settings.type == "negotiate":
            db_kwargs[auth_key] = negotiate_header(settings.spn or f"HTTP@{host}")
        elif settings.type == "oauth2":
            # The driver runs the OAuth2 flow itself and refreshes tokens.
            require(settings, "oauth2_token_url", "oauth2_client_id", "oauth2_client_secret")
            db_kwargs[_OAUTH_FLOW] = settings.oauth2_flow
            db_kwargs[_OAUTH_TOKEN_URI] = str(settings.oauth2_token_url)
            db_kwargs[_OAUTH_CLIENT_ID] = str(settings.oauth2_client_id)
            db_kwargs[_OAUTH_CLIENT_SECRET] = str(settings.oauth2_client_secret)
            if settings.oauth2_scope:
                db_kwargs[_OAUTH_SCOPE] = settings.oauth2_scope
            query.pop("authorization", None)

        # header.x-workspace-id=abc → <prefix>x-workspace-id: abc
        for key in list(query.keys()):
            if key.startswith("header."):
                db_kwargs[f"{header_prefix}{key[len('header.') :]}"] = query.pop(key)

        # Anything left in the query string is passed through verbatim —
        # useful for driver-specific options we don't know about.
        db_kwargs.update(query)

        return [uri], {"db_kwargs": db_kwargs} if db_kwargs else {}


def _scalar(v: str | tuple[str, ...]) -> str:
    """Collapse a possibly-repeated URL query value to a single string.

    SQLAlchemy gives us a tuple for ``?k=a&k=b`` (both values); we keep
    the last, matching shell/HTTP conventions where the later value wins.
    """
    return v[-1] if isinstance(v, tuple) else v


def _pop_bool(d: dict[str, str], key: str, *, default: bool) -> bool:
    if key not in d:
        return default
    return d.pop(key).lower() in {"1", "true", "yes", "on"}
