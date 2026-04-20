"""Unit tests for the PostgreSQL URL → ADBC kwargs translation."""

from __future__ import annotations

from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect


def _build(url_str: str):
    return ADBCPostgreSQLDialect().build_connect_args(make_url(url_str))


def test_full_url():
    args, kwargs = _build("adbc+postgresql://user:pass@host:5432/mydb")
    assert args == ["postgresql://user:pass@host:5432/mydb"]
    assert kwargs == {}


def test_no_credentials():
    args, _ = _build("adbc+postgresql://host:5432/mydb")
    assert args == ["postgresql://host:5432/mydb"]


def test_no_port():
    args, _ = _build("adbc+postgresql://user:pass@host/mydb")
    assert args == ["postgresql://user:pass@host/mydb"]


def test_query_options_in_db_kwargs():
    _, kwargs = _build("adbc+postgresql://user:pass@host:5432/mydb?sslmode=require&app=x")
    assert kwargs["db_kwargs"] == {"sslmode": "require", "app": "x"}


def test_password_with_at_sign_quoted():
    """A password containing @ must not be interpreted as host delimiter."""
    args, _ = _build("adbc+postgresql://user:p%40ss@host:5432/db")
    # SQLAlchemy decodes %40 → @; we must re-encode for the DSN.
    assert args == ["postgresql://user:p%40ss@host:5432/db"]


def test_password_with_colon_quoted():
    args, _ = _build("adbc+postgresql://user:a%3Ab@host/db")
    assert args == ["postgresql://user:a%3Ab@host/db"]


def test_password_with_slash_quoted():
    args, _ = _build("adbc+postgresql://user:a%2Fb@host/db")
    assert args == ["postgresql://user:a%2Fb@host/db"]


def test_password_with_hash_quoted():
    """``#`` in a DSN starts a fragment; must be quoted."""
    args, _ = _build("adbc+postgresql://user:a%23b@host/db")
    assert args == ["postgresql://user:a%23b@host/db"]


def test_password_with_percent_quoted():
    args, _ = _build("adbc+postgresql://user:a%25b@host/db")
    # SQLAlchemy decodes %25 → %; we must re-encode.
    assert args == ["postgresql://user:a%25b@host/db"]


def test_username_special_chars_quoted():
    args, _ = _build("adbc+postgresql://u%40me:pw@host/db")
    assert args == ["postgresql://u%40me:pw@host/db"]


def test_database_with_spaces_quoted():
    args, _ = _build("adbc+postgresql://host/my%20db")
    assert args == ["postgresql://host/my%20db"]


def test_defaults_localhost():
    args, _ = _build("adbc+postgresql:///mydb")
    assert args == ["postgresql://localhost/mydb"]


def test_password_no_username_still_builds():
    """SQLAlchemy URL can have a password-only userinfo (edge case)."""
    args, _ = _build("adbc+postgresql://:pw@host/db")
    # We emit auth=":pw@" — libpq accepts this as password-only.
    assert args[0].startswith("postgresql://:pw@host")
