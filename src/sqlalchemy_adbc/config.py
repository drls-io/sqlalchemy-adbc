"""Local configuration file for authentication profiles.

Discovery order (first hit wins; absence is normal and silent):

1. the path in ``$SQLALCHEMY_ADBC_CONFIG``
2. ``~/.config/sqlalchemy-adbc/config.toml``

The working directory is deliberately not searched: a file dropped into a
cloned repository could otherwise point OAuth2 at a token endpoint that
harvests ``env:`` secrets. For the same reason the file must be owned by the
current user and not writable by group or others.

Shape::

    [auth.<profile>]
    type = "oauth2"          # bearer | basic | negotiate | oauth2
    token_url = "..."
    client_id = "..."
    client_secret = "env:MY_SECRET"   # resolved from the environment at connect time

Any string value may be ``env:NAME`` to defer to an environment variable,
so secrets stay out of files and URLs.

A ``[proxy]`` section (0.5.0 to 0.8.0) is ignored with a warning: the
Go-based drivers read ``HTTP_PROXY`` / ``HTTPS_PROXY`` / ``NO_PROXY`` from
the environment the process was started with, so set them in the shell or
launcher.
"""

from __future__ import annotations

import os
import tomllib
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.exc import ArgumentError

CONFIG_ENV = "SQLALCHEMY_ADBC_CONFIG"
USER_CONFIG = Path("~/.config/sqlalchemy-adbc/config.toml")


@dataclass(frozen=True)
class Config:
    auth_profiles: dict[str, dict[str, str]]
    path: Path | None


EMPTY_CONFIG = Config({}, None)


def find_config_path(
    env: Mapping[str, str] = os.environ,
    home: Path | None = None,
) -> Path | None:
    """Return the explicit path, else the user config if it exists, else None."""
    explicit = env.get(CONFIG_ENV)
    if explicit:
        return Path(explicit)
    user_config = (
        (home / USER_CONFIG.relative_to("~")) if home is not None else USER_CONFIG.expanduser()
    )
    return user_config if user_config.is_file() else None


def _check_trusted(path: Path) -> None:
    """Refuse a config another user could have written (POSIX only)."""
    if os.name != "posix":
        return
    st = path.stat()
    if st.st_mode & 0o022:
        raise ArgumentError(
            f"cannot read config {path}: file is group/world-writable; use chmod 600"
        )
    if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
        raise ArgumentError(f"cannot read config {path}: file is not owned by the current user")


def _str_table(raw: Any, section: str, path: Path) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise ArgumentError(f"cannot read config {path}: [{section}] must be a table")
    return {str(k): str(v) for k, v in raw.items()}


def load_config(path: Path | None = None, *, env: Mapping[str, str] = os.environ) -> Config:
    """Load the config at ``path`` (or the discovered one). Missing file → empty."""
    if path is None:
        path = find_config_path(env)
        if path is None:
            return EMPTY_CONFIG
    if not path.exists():
        return EMPTY_CONFIG
    _check_trusted(path)
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ArgumentError(f"cannot read config {path}: {exc}") from exc
    if data.get("proxy"):
        warnings.warn(
            f"{path}: the [proxy] section is ignored since 0.9.0. The Go-based ADBC "
            "drivers only read HTTP_PROXY, HTTPS_PROXY and NO_PROXY from the environment "
            "the process was started with; set them in the shell or launcher.",
            DeprecationWarning,
            stacklevel=2,
        )
    auth_raw = data.get("auth", {})
    if not isinstance(auth_raw, dict):
        raise ArgumentError(f"cannot read config {path}: [auth] must hold profile tables")
    profiles = {str(name): _str_table(tbl, f"auth.{name}", path) for name, tbl in auth_raw.items()}
    return Config(profiles, path)


def resolve_env_ref(value: str, *, key: str, env: Mapping[str, str] = os.environ) -> str:
    """``env:NAME`` → the variable's value; anything else passes through."""
    if not value.startswith("env:"):
        return value
    name = value[len("env:") :]
    if name not in env:
        raise ArgumentError(f"{key}: environment variable {name} is not set")
    return env[name]
