"""Local config file: discovery, loading, env: references."""

from __future__ import annotations

import os

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemy_adbc.config import Config, find_config_path, load_config, resolve_env_ref

TOML = """
[proxy]
https_proxy = "http://proxy:3128"
no_proxy = "localhost"

[auth.flight]
type = "oauth2"
client_secret = "env:SECRET"
"""


def test_discovery_order(tmp_path):
    explicit = tmp_path / "explicit.toml"
    explicit.write_text(TOML)
    (tmp_path / "cwd").mkdir()
    cwd_file = tmp_path / "cwd" / "sqlalchemy-adbc.toml"
    cwd_file.write_text(TOML)
    home = tmp_path / "home"
    (home / ".config" / "sqlalchemy-adbc").mkdir(parents=True)
    home_file = home / ".config" / "sqlalchemy-adbc" / "config.toml"
    home_file.write_text(TOML)
    env = {"SQLALCHEMY_ADBC_CONFIG": str(explicit)}
    assert find_config_path(env, cwd=tmp_path / "cwd", home=home) == explicit
    assert find_config_path({}, cwd=tmp_path / "cwd", home=home) == cwd_file
    assert find_config_path({}, cwd=tmp_path, home=home) == home_file
    assert find_config_path({}, cwd=tmp_path, home=tmp_path / "nohome") is None


def test_load_parses_sections_and_keeps_env_refs_unresolved(tmp_path):
    p = tmp_path / "c.toml"
    p.write_text(TOML)
    cfg = load_config(p)
    assert cfg.path == p
    assert cfg.proxy == {"https_proxy": "http://proxy:3128", "no_proxy": "localhost"}
    assert cfg.auth_profiles == {"flight": {"type": "oauth2", "client_secret": "env:SECRET"}}


def test_missing_file_is_empty_config(tmp_path):
    assert load_config(tmp_path / "nope.toml") == Config({}, {}, None)
    env = {"SQLALCHEMY_ADBC_CONFIG": str(tmp_path / "nope.toml")}
    assert load_config(None, env=env).proxy == {}


def test_malformed_toml_names_path(tmp_path):  # Review Focus 1
    p = tmp_path / "bad.toml"
    p.write_text("[proxy\nx = ")
    with pytest.raises(ArgumentError, match=str(p)):
        load_config(p)


def test_unreadable_file_names_path(tmp_path):  # Review Focus 1
    p = tmp_path / "locked.toml"
    p.write_text(TOML)
    p.chmod(0)
    try:
        if os.access(p, os.R_OK):
            pytest.skip("running as root")
        with pytest.raises(ArgumentError, match=str(p)):
            load_config(p)
    finally:
        p.chmod(0o600)


def test_resolve_env_ref():  # Review Focus 2
    assert resolve_env_ref("plain", key="k") == "plain"
    assert resolve_env_ref("env:TOK", key="k", env={"TOK": "abc"}) == "abc"
    with pytest.raises(ArgumentError, match=r"client_secret.*MISSING"):
        resolve_env_ref("env:MISSING", key="client_secret", env={})
