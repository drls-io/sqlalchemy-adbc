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

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from sqlalchemy.exc import ArgumentError

from sqlalchemy_adbc.config import Config, resolve_env_ref

AUTH_TYPES = ("bearer", "basic", "negotiate", "oauth2")

AUTH_URL_KEYS = (
    "auth",
    "auth_profile",
    "spn",
    "oauth2_flow",
    "oauth2_token_url",
    "oauth2_client_id",
    "oauth2_client_secret",
    "oauth2_scope",
    "oauth2_audience",
)

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
    "spn": "spn",
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
) -> AuthSettings:
    """Pop the auth keys out of ``query`` and merge them over a config profile."""
    url_values = {k: query.pop(k) for k in AUTH_URL_KEYS if k in query}
    merged: dict[str, str] = {}

    profile_name = url_values.pop("auth_profile", None)
    if profile_name is not None:
        profile = config.auth_profiles.get(profile_name)
        if profile is None:
            where = str(config.path) if config.path else "no config file"
            raise ArgumentError(f"auth_profile {profile_name!r} not found in {where}")
        for key, raw in profile.items():
            if key in _PROFILE_FIELDS:
                merged[_PROFILE_FIELDS[key]] = raw

    for key, raw in url_values.items():
        merged[_URL_FIELDS[key]] = raw

    settings = AuthSettings()
    for fname, raw in merged.items():
        settings = replace(settings, **{fname: resolve_env_ref(raw, key=fname, env=env)})

    if settings.type is not None and settings.type not in AUTH_TYPES:
        raise ArgumentError(f"auth must be one of {', '.join(AUTH_TYPES)}")
    return settings


def require(settings: AuthSettings, *names: str) -> None:
    """Raise naming the first missing setting; never echoes values."""
    for name in names:
        if not getattr(settings, name):
            raise ArgumentError(f"auth={settings.type} requires {name}")
