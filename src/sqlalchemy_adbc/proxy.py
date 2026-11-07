"""Export ``[proxy]`` config into the process environment.

The Flight SQL, Snowflake and BigQuery drivers are Go libraries loaded
in-process; Go's HTTP transport and grpc-go read ``HTTP_PROXY``,
``HTTPS_PROXY`` and ``NO_PROXY`` (either case) from the environment at dial
time and expose no per-connection proxy option. So the only lever is the
environment, and the rule is: a variable the user already set always wins;
the config file only fills gaps. Both spellings are written so Go and any
Python HTTP client in the same process agree.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import MutableMapping

from sqlalchemy_adbc.config import Config, load_config

PROXY_KEYS = ("http_proxy", "https_proxy", "no_proxy")

log = logging.getLogger("sqlalchemy_adbc.proxy")

_applied = False
_lock = threading.Lock()


def apply_proxy_env(
    config: Config, environ: MutableMapping[str, str] = os.environ
) -> dict[str, str]:
    """Fill unset proxy variables from ``config.proxy``; return what was written."""
    written: dict[str, str] = {}
    for key in PROXY_KEYS:
        upper = key.upper()
        value = environ.get(upper) or environ.get(key) or config.proxy.get(key)
        if not value:
            continue
        for name in (upper, key):
            if environ.get(name) != value:
                environ[name] = value
                written[name] = value
    if written:
        # Values are proxy URLs, not secrets; the path makes a surprising
        # proxy traceable to the file that set it.
        log.info(
            "exported proxy variables %s from %s",
            sorted(written),
            config.path or "<no config file>",
        )
    return written


def ensure_proxy_env_once() -> None:
    """Apply the discovered config's proxy section once per process."""
    global _applied
    if _applied:
        return
    with _lock:
        if _applied:
            return
        apply_proxy_env(load_config())
        _applied = True


def _reset_for_tests() -> None:
    global _applied
    _applied = False
