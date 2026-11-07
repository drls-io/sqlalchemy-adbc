"""Proxy settings: exported from config into the environment the drivers read."""

from __future__ import annotations

import os

import sqlalchemy as sa

from sqlalchemy_adbc.config import Config
from sqlalchemy_adbc.proxy import apply_proxy_env

CFG = Config(
    {"https_proxy": "http://p:3128", "http_proxy": "http://p:3128", "no_proxy": "localhost"},
    {},
    None,
)


def test_exports_both_spellings_when_unset():
    env: dict[str, str] = {}
    written = apply_proxy_env(CFG, env)
    assert env == {
        "HTTPS_PROXY": "http://p:3128",
        "https_proxy": "http://p:3128",
        "HTTP_PROXY": "http://p:3128",
        "http_proxy": "http://p:3128",
        "NO_PROXY": "localhost",
        "no_proxy": "localhost",
    }
    assert set(written) == set(env)


def test_never_overwrites_existing_variables():  # Review Focus 3
    env = {"NO_PROXY": "mine"}
    apply_proxy_env(CFG, env)
    assert env["NO_PROXY"] == "mine" and env["no_proxy"] == "mine"
    assert env["HTTPS_PROXY"] == "http://p:3128"


def test_lowercase_existing_is_honoured_too():
    env = {"https_proxy": "http://user:7"}
    apply_proxy_env(CFG, env)
    assert env["https_proxy"] == "http://user:7" and env["HTTPS_PROXY"] == "http://user:7"


def test_noop_without_proxy_section():
    env: dict[str, str] = {}
    assert apply_proxy_env(Config({}, {}, None), env) == {} and env == {}


def test_dialect_applies_once(monkeypatch, tmp_path):
    from sqlalchemy_adbc import proxy

    p = tmp_path / "sqlalchemy-adbc.toml"
    p.write_text('[proxy]\nhttps_proxy = "http://cfg:1"\n')
    monkeypatch.setenv("SQLALCHEMY_ADBC_CONFIG", str(p))
    for k in ("HTTPS_PROXY", "https_proxy"):
        monkeypatch.delenv(k, raising=False)
    proxy._reset_for_tests()
    e = sa.create_engine("sqlite+adbc:///:memory:")
    with e.connect() as c:
        c.exec_driver_sql("SELECT 1")
    assert os.environ["HTTPS_PROXY"] == "http://cfg:1"
    # Second engine: config no longer consulted (flag set), env already present.
    p.write_text('[proxy]\nhttps_proxy = "http://changed:2"\n')
    sa.create_engine("sqlite+adbc:///:memory:").connect().close()
    assert os.environ["HTTPS_PROXY"] == "http://cfg:1"


def test_logs_which_file_set_the_proxy(caplog):
    import logging
    from pathlib import Path

    cfg = Config(
        {"https_proxy": "http://p:1"}, {}, Path("/home/me/.config/sqlalchemy-adbc/config.toml")
    )
    with caplog.at_level(logging.INFO, logger="sqlalchemy_adbc.proxy"):
        apply_proxy_env(cfg, {})
    assert "config.toml" in caplog.text and "HTTPS_PROXY" in caplog.text
