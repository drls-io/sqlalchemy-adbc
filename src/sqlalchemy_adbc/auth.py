"""Authentication settings and credential material for the dialects.

Settings come from URL query keys (``auth=``, ``auth_profile=``,
``oauth2_*``, ``spn``) merged over a named ``[auth.<profile>]`` table in
the local config; the URL wins on conflict and ``env:NAME`` references are
resolved last. Dialects turn an :class:`AuthSettings` into driver options
inside ``build_connect_args``, which runs on every new DBAPI connection, so
anything minted here (a Negotiate token, an OAuth2 access token) refreshes
per connection without a ``creator`` hack.

Secrets never appear in exception messages or ``repr``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.exc import ArgumentError

from sqlalchemy_adbc.config import Config, resolve_env_ref

AUTH_TYPES = ("bearer", "basic", "negotiate", "oauth2")

AUTH_URL_KEYS = (
    "auth",
    "auth_profile",
    "oauth2_flow",
    "oauth2_token_url",
    "oauth2_client_id",
    "oauth2_scope",
    "oauth2_audience",
)
# With auth_profile, the URL may only tweak these; everything that decides
# where the profile's secret is sent stays under the config owner's control.
_PROFILE_OVERRIDABLE = {"oauth2_scope", "oauth2_audience"}

# config profile key → AuthSettings field
_PROFILE_FIELDS = {
    "type": "type",
    "spn": "spn",
    "flow": "oauth2_flow",
    "token_url": "oauth2_token_url",
    "client_id": "oauth2_client_id",
    "client_secret": "oauth2_client_secret",
    "scope": "oauth2_scope",
    "audience": "oauth2_audience",
}
# URL key → AuthSettings field
_URL_FIELDS = {
    "auth": "type",
    "oauth2_flow": "oauth2_flow",
    "oauth2_token_url": "oauth2_token_url",
    "oauth2_client_id": "oauth2_client_id",
    "oauth2_client_secret": "oauth2_client_secret",
    "oauth2_scope": "oauth2_scope",
    "oauth2_audience": "oauth2_audience",
}
_SECRET_FIELDS = {"oauth2_client_secret"}


@dataclass(frozen=True, repr=False)
class AuthSettings:
    type: str | None = None
    spn: str | None = None
    oauth2_flow: str = "client_credentials"
    oauth2_token_url: str | None = None
    oauth2_client_id: str | None = None
    oauth2_client_secret: str | None = field(default=None)
    oauth2_scope: str | None = None
    oauth2_audience: str | None = None

    def __repr__(self) -> str:
        parts = []
        for name in self.__dataclass_fields__:
            value: Any = getattr(self, name)
            if name in _SECRET_FIELDS and value is not None:
                value = "***"
            parts.append(f"{name}={value!r}")
        return f"AuthSettings({', '.join(parts)})"


def settings_from_query(
    query: dict[str, str],
    config: Config,
    *,
    env: Mapping[str, str] = os.environ,
    password: str | None = None,
) -> AuthSettings:
    """Pop the auth keys out of ``query`` and merge them over a config profile.

    Trust model: the URL author (e.g. a Superset database admin) may be less
    trusted than the owner of the config file and process environment. So
    ``env:`` references resolve only from the config, a URL that names a
    profile cannot redirect that profile's secret (only scope/audience may be
    overridden), and a client secret is never read from the query string: it
    comes from the profile or from the URL password, which SQLAlchemy masks.
    """
    if "oauth2_client_secret" in query:
        raise ArgumentError(
            "oauth2_client_secret is not accepted in the URL query; put it in the URL "
            "password (user = client_id) or in a config profile"
        )
    if "spn" in query:
        # A ticket for a service other than the connection host could be
        # relayed by that host; only the config owner may pin an SPN.
        raise ArgumentError("spn may only be set in a config profile, not in the URL")
    url_values = {k: query.pop(k) for k in AUTH_URL_KEYS if k in query}
    for key, raw in url_values.items():
        if raw.startswith("env:"):
            raise ArgumentError(f"{key}: env: references are only allowed in the config file")

    merged: dict[str, str] = {}
    profile_name = url_values.pop("auth_profile", None)
    if profile_name is not None:
        profile = config.auth_profiles.get(profile_name)
        if profile is None:
            where = str(config.path) if config.path else "no config file"
            raise ArgumentError(f"auth_profile {profile_name!r} not found in {where}")
        for key, raw in profile.items():
            if key in _PROFILE_FIELDS:
                fname = _PROFILE_FIELDS[key]
                merged[fname] = resolve_env_ref(raw, key=fname, env=env)
        for key in url_values:
            if _URL_FIELDS[key] not in _PROFILE_OVERRIDABLE:
                raise ArgumentError(
                    f"cannot combine auth_profile with {key}; set it in the profile"
                )

    for key, raw in url_values.items():
        merged[_URL_FIELDS[key]] = raw

    if merged.get("type") == "oauth2" and not merged.get("oauth2_client_secret") and password:
        merged["oauth2_client_secret"] = password

    settings = AuthSettings(**merged)
    if settings.type is not None and settings.type not in AUTH_TYPES:
        raise ArgumentError(f"auth must be one of {', '.join(AUTH_TYPES)}")
    return settings


def require(settings: AuthSettings, *names: str) -> None:
    """Raise naming the first missing setting; never echoes values."""
    for name in names:
        if not getattr(settings, name):
            raise ArgumentError(f"auth={settings.type} requires {name}")


# ── OAuth2 client credentials ────────────────────────────────────────

_TOKEN_SAFETY_MARGIN_S = 60.0
_LOOPBACK = {"localhost", "127.0.0.1", "::1"}


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Credentials must never follow a 30x to another host."""

    def redirect_request(
        self, req: Any, fp: Any, code: Any, msg: Any, headers: Any, newurl: Any
    ) -> Any:
        return None


_default_opener = urllib.request.build_opener(_NoRedirects)


def _check_token_url(token_url: str) -> None:
    parsed = urllib.parse.urlsplit(token_url)
    if parsed.scheme == "https":
        return
    if parsed.scheme == "http" and parsed.hostname in _LOOPBACK:
        return
    raise ArgumentError(f"OAuth2 token_url must use https (got {parsed.scheme or 'no'} scheme)")


class OAuth2ClientCredentials:
    """Mint and cache an access token with the client-credentials grant.

    Stdlib only. The token is cached until ``expires_in`` minus a 60 s
    margin; one lock serialises refreshes. Errors name the endpoint and the
    HTTP status, never the client secret.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        audience: str | None = None,
        *,
        opener: Callable[..., Any] = _default_opener.open,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        _check_token_url(token_url)
        self.token_url = token_url
        self.client_id = client_id
        self._client_secret = client_secret
        self.scope = scope
        self.audience = audience
        self._opener = opener
        self._now = now
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at = 0.0

    def __repr__(self) -> str:
        return (
            f"OAuth2ClientCredentials(token_url={self.token_url!r}, client_id={self.client_id!r})"
        )

    def invalidate(self) -> None:
        with self._lock:
            self._token = None
            self._expires_at = 0.0

    def token(self) -> str:
        with self._lock:
            if self._token is not None and self._now() < self._expires_at:
                return self._token
            token, expires_in = self._fetch()
            self._token = token
            self._expires_at = self._now() + expires_in - _TOKEN_SAFETY_MARGIN_S
            return token

    def _fetch(self) -> tuple[str, float]:
        form: dict[str, str] = {"grant_type": "client_credentials"}
        if self.scope:
            form["scope"] = self.scope
        if self.audience:
            form["audience"] = self.audience
        basic = base64.b64encode(f"{self.client_id}:{self._client_secret}".encode()).decode()
        request = urllib.request.Request(
            self.token_url,
            data=urllib.parse.urlencode(form).encode(),
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with self._opener(request, timeout=30) as response:
                status = getattr(response, "status", 200)
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise ArgumentError(
                f"OAuth2 token request to {self.token_url} failed with HTTP {exc.code}"
            ) from None
        except urllib.error.URLError as exc:
            raise ArgumentError(
                f"OAuth2 token request to {self.token_url} failed: {exc.reason}"
            ) from None
        if not 200 <= status < 300:
            raise ArgumentError(
                f"OAuth2 token request to {self.token_url} failed with HTTP {status}"
            )
        try:
            payload = json.loads(body)
        except ValueError:
            raise ArgumentError("OAuth2 token response is not JSON") from None
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not token:
            raise ArgumentError("OAuth2 token response lacks access_token")
        try:
            expires_in = float(payload.get("expires_in", 3600))
        except (TypeError, ValueError):
            expires_in = 3600.0
        return str(token), expires_in


_sources: dict[tuple[str, str, str | None, str | None, str], OAuth2ClientCredentials] = {}
_sources_lock = threading.Lock()


def token_source_for(settings: AuthSettings) -> OAuth2ClientCredentials:
    """One shared token source per (token_url, client_id, scope, audience)."""
    require(settings, "oauth2_token_url", "oauth2_client_id", "oauth2_client_secret")
    assert settings.oauth2_token_url and settings.oauth2_client_id and settings.oauth2_client_secret
    key = (
        settings.oauth2_token_url,
        settings.oauth2_client_id,
        settings.oauth2_scope,
        settings.oauth2_audience,
        # Digest, not plaintext: a different secret must never reuse a token.
        hashlib.sha256(settings.oauth2_client_secret.encode()).hexdigest(),
    )
    with _sources_lock:
        source = _sources.get(key)
        if source is None:
            source = OAuth2ClientCredentials(
                settings.oauth2_token_url,
                settings.oauth2_client_id,
                settings.oauth2_client_secret,
                scope=settings.oauth2_scope,
                audience=settings.oauth2_audience,
            )
            _sources[key] = source
        return source


# ── Negotiate (Kerberos / SPNEGO) ────────────────────────────────────


def negotiate_header(spn: str, *, client_factory: Callable[..., Any] | None = None) -> str:
    """``Authorization`` value for Kerberos/SPNEGO: ``Negotiate <base64 token>``.

    ``spn`` is ``SERVICE@hostname`` (``HTTP@flight.example``). Single-leg:
    the client's initial token is enough for Kerberos. Uses the system
    GSSAPI/SSPI credential cache through ``pyspnego`` (extra ``kerberos``);
    a fresh context per connection, which is cheap.
    """
    service, sep, hostname = spn.partition("@")
    if not sep or not service or not hostname:
        raise ArgumentError(f"spn must look like SERVICE@hostname, got {spn!r}")
    if client_factory is None:
        try:
            import spnego
        except ImportError:
            raise ArgumentError(
                "auth=negotiate requires the 'kerberos' extra: "
                "pip install 'sqlalchemy-adbc[kerberos]'"
            ) from None
        client_factory = spnego.client
    context = client_factory(hostname=hostname, service=service, protocol="negotiate")
    token = context.step(None)
    if not token:
        raise ArgumentError("SPNEGO produced no initial token; is there a Kerberos ticket?")
    return "Negotiate " + base64.b64encode(token).decode()
