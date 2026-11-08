"""The ``[proxy]`` config section is retired.

The Go-based drivers (Flight SQL, Snowflake, BigQuery) read ``HTTP_PROXY`` /
``HTTPS_PROXY`` / ``NO_PROXY`` from the environment the process was started
with; nothing written from Python reaches them. So the export this package
did in 0.5.0 to 0.8.0 could never route the drivers, and only ever served
its own OAuth2 token request. It is gone: a ``[proxy]`` section is ignored
with a warning, and the environment is left alone.
"""

from __future__ import annotations

import importlib.util
import os

import pytest
import sqlalchemy as sa

from sqlalchemy_adbc.config import load_config


def test_proxy_module_is_gone():
    assert importlib.util.find_spec("sqlalchemy_adbc.proxy") is None


def test_proxy_section_is_ignored_with_a_warning(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text('[proxy]\nhttps_proxy = "http://cfg:1"\n\n[auth.x]\ntype = "basic"\n')
    p.chmod(0o600)
    with pytest.warns(DeprecationWarning, match="HTTPS_PROXY"):
        cfg = load_config(p)
    assert cfg.auth_profiles == {"x": {"type": "basic"}}
    assert not hasattr(cfg, "proxy") or cfg.proxy == {}


def test_connecting_never_writes_proxy_variables(monkeypatch, tmp_path):
    p = tmp_path / "config.toml"
    p.write_text('[proxy]\nhttps_proxy = "http://cfg:1"\n')
    p.chmod(0o600)
    monkeypatch.setenv("SQLALCHEMY_ADBC_CONFIG", str(p))
    for k in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(k, raising=False)
    e = sa.create_engine("sqlite+adbc:///:memory:")
    # Connecting no longer reads the config file at all (only auth profiles
    # do, on the dialects that use them), so nothing is exported.
    with e.connect() as c:
        assert c.exec_driver_sql("SELECT 1").scalar() == 1
    assert "HTTPS_PROXY" not in os.environ and "https_proxy" not in os.environ
    e.dispose()
