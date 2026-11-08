"""Auth, proxy and binding checks against the fake Snowflake endpoint.

These cannot run against a real account: they assert on what the Go driver
puts on the wire (the login request, the bindings) and route the driver
through the fake acting as an HTTP proxy.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

import pytest
import sqlalchemy as sa

from sqlalchemy_adbc import auth
from sqlalchemy_adbc.snowflake_types import VARIANT
from tests.conftest import FAKE_ENGINE_URL, attach_fake
from tests.fake_snowflake import FakeSnowflake

pytest.importorskip("adbc_driver_snowflake")

REPO_ROOT = Path(__file__).resolve().parent.parent


def _engine(url: str, fake: FakeSnowflake, host: str | None = None) -> sa.Engine:
    return attach_fake(sa.create_engine(url, poolclass=sa.pool.NullPool), fake, host=host)


@pytest.fixture
def fresh_token_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auth, "_sources", {})


def _write_config(path: Path, text: str) -> Path:
    path.write_text(text)
    path.chmod(0o600)
    return path


# ── OAuth2 ───────────────────────────────────────────────────────────


def test_oauth2_profile_mints_token_that_reaches_login(
    fake_snowflake: FakeSnowflake,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_token_cache: None,
) -> None:
    cfg = _write_config(
        tmp_path / "config.toml",
        "[auth.sf]\n"
        'type = "oauth2"\n'
        f'token_url = "{fake_snowflake.base_url}/oauth/token"\n'
        'client_id = "ci-client"\n'
        'client_secret = "env:SF_SECRET"\n'
        'scope = "session:role:ANALYST"\n',
    )
    monkeypatch.setenv("SQLALCHEMY_ADBC_CONFIG", str(cfg))
    monkeypatch.setenv("SF_SECRET", "ci-secret")
    fake_snowflake.reset_log()
    e = _engine(
        "snowflake+adbc://svc@FAKEACCT/FAKEDB/PUBLIC?warehouse=WH&auth_profile=sf", fake_snowflake
    )
    for _ in range(2):
        with e.connect() as c:
            assert c.execute(sa.text("SELECT 1")).scalar() == 1
    logins = [login for login in fake_snowflake.logins if login.get("AUTHENTICATOR") == "OAUTH"]
    assert len(logins) == 2
    assert all(login["TOKEN"] in fake_snowflake.issued_tokens for login in logins)
    assert all(login.get("PASSWORD") in (None, "") for login in logins)
    # The token is cached across connections: one client-credentials request.
    assert len(fake_snowflake.token_requests) == 1
    assert fake_snowflake.token_requests[0]["scope"] == "session:role:ANALYST"
    assert fake_snowflake.token_requests[0]["grant_type"] == "client_credentials"


def test_oauth2_wrong_secret_fails_before_login(
    fake_snowflake: FakeSnowflake,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_token_cache: None,
) -> None:
    monkeypatch.setenv("SQLALCHEMY_ADBC_CONFIG", str(tmp_path / "none.toml"))
    fake_snowflake.reset_log()
    token_url = quote(fake_snowflake.base_url + "/oauth/token", safe="")
    e = _engine(
        "snowflake+adbc://ci-client:wrong@FAKEACCT/FAKEDB/PUBLIC?warehouse=WH"
        f"&auth=oauth2&oauth2_token_url={token_url}&oauth2_client_id=ci-client",
        fake_snowflake,
    )
    with pytest.raises(sa.exc.ArgumentError, match="HTTP 401"):
        e.connect()
    assert fake_snowflake.logins == []


def test_oauth_login_with_foreign_token_is_rejected(
    fake_snowflake: FakeSnowflake, monkeypatch: pytest.MonkeyPatch, fresh_token_cache: None
) -> None:
    import sqlalchemy_adbc.snowflake as sf

    class Forged:
        def token(self) -> str:
            return "forged"

    monkeypatch.setattr(sf, "token_source_for", lambda settings: Forged())
    e = _engine(
        "snowflake+adbc://svc:sec@FAKEACCT/FAKEDB/PUBLIC?warehouse=WH&auth=oauth2"
        "&oauth2_token_url=https://idp.invalid/token&oauth2_client_id=svc",
        fake_snowflake,
    )
    with pytest.raises(sa.exc.DBAPIError, match="390303|OAuth"):
        e.connect()


# ── Proxy ────────────────────────────────────────────────────────────

# The Go runtime inside the driver reads the environment it was started
# with: HTTP_PROXY set later by Python (including by apply_proxy_env) is not
# seen, on macOS or Linux. So the check runs in a child process whose
# environment carries the proxy from the start, targets an unresolvable host,
# and can only succeed if the driver went through the fake acting as proxy.
PROXY_CHILD = r"""
import json, sys
import sqlalchemy as sa
from tests.fake_snowflake import endpoint_db_kwargs

e = sa.create_engine(
    sys.argv[1],
    connect_args={"db_kwargs": endpoint_db_kwargs("snowflake.invalid", 80)},
)
with e.connect() as c:
    value = c.execute(sa.text("SELECT 1")).scalar()
e.dispose()
print(json.dumps({"value": int(value)}))
"""


def test_http_proxy_from_the_environment_routes_the_go_driver(
    fake_snowflake: FakeSnowflake,
) -> None:
    env = {
        k: v
        for k, v in os.environ.items()
        if k.lower() not in ("http_proxy", "https_proxy", "no_proxy", "all_proxy")
    }
    env["HTTP_PROXY"] = fake_snowflake.base_url
    env["NO_PROXY"] = "localhost"
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(REPO_ROOT), env.get("PYTHONPATH", "")) if p)
    fake_snowflake.reset_log()
    out = subprocess.run(
        [sys.executable, "-c", PROXY_CHILD, FAKE_ENGINE_URL],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(REPO_ROOT),
    )
    assert out.returncode == 0, out.stderr[-3000:]
    assert json.loads(out.stdout.strip().splitlines()[-1]) == {"value": 1}
    assert "snowflake.invalid" in fake_snowflake.proxied_hosts
    assert any(login.get("LOGIN_NAME") == "ci" for login in fake_snowflake.logins)


# ── Bindings on the wire ─────────────────────────────────────────────


def test_bind_types_on_the_wire(fake_snowflake: FakeSnowflake) -> None:
    e = _engine(FAKE_ENGINE_URL, fake_snowflake)
    t = sa.Table(
        "ADBC_BINDS",
        sa.MetaData(),
        sa.Column("I", sa.Integer),
        sa.Column("S", sa.String(10)),
        sa.Column("N", sa.Numeric(10, 2)),
        sa.Column("D", sa.Date),
        sa.Column("TS", sa.DateTime),
        sa.Column("B", sa.Boolean),
        sa.Column("F", sa.Float),
        sa.Column("BY", sa.LargeBinary),
        sa.Column("J", VARIANT()),
    )
    t.metadata.create_all(e)
    try:
        fake_snowflake.reset_log()
        with e.begin() as c:
            c.execute(
                t.insert().values(
                    I=1,
                    S="a",
                    N=Decimal("1.50"),
                    D=dt.date(2024, 1, 2),
                    TS=dt.datetime(2024, 1, 2, 3, 4, 5),
                    B=True,
                    F=1.5,
                    BY=b"\x01\xff",
                    J={"k": [1]},
                )
            )
    finally:
        t.metadata.drop_all(e)
    inserts = [
        body
        for _, body in fake_snowflake.requests
        if str(body.get("sqlText", "")).startswith("INSERT")
    ]
    assert len(inserts) == 1
    binds = [inserts[0]["bindings"][str(i)] for i in range(1, 10)]
    assert [b["type"] for b in binds] == [
        "FIXED",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "BOOLEAN",
        "REAL",
        "TEXT",
        "TEXT",
    ]
    assert [b["value"] for b in binds] == [
        "1",
        "a",
        "1.50",
        "2024-01-02",
        "2024-01-02 03:04:05",
        "true",
        "1.5",
        "01ff",
        '{"k": [1]}',
    ]
