"""Shared fixtures.

``fake_snowflake`` starts one in-process fake Snowflake endpoint per test
session (see ``tests/fake_snowflake``). ``snowflake_engine`` runs a test
twice: against the fake, and against a real account when
``DRLS_SNOWFLAKE_URL`` is set (otherwise that cell skips), so the fake and
Snowflake can be checked against each other.

``flightsql_engine`` connects to the Flight SQL server named by
``DRLS_FLIGHTSQL_URL`` (CI: a GizmoSQL service container; see
``tests/test_flightsql_e2e.py``), retrying the first query while the server
is still starting. ``flightsql_bearer_url`` performs the Flight handshake
with the URL's credentials and returns the bearer-token form of the URL.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from typing import Any

import pytest
import sqlalchemy as sa

from tests.fake_snowflake import FakeSnowflake

FAKE_ENGINE_URL = "snowflake+adbc://ci:ci@FAKEACCT/FAKEDB/PUBLIC?warehouse=WH"
OAUTH_CLIENT = ("ci-client", "ci-secret")


@pytest.fixture(scope="session")
def fake_snowflake(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FakeSnowflake]:
    pytest.importorskip("adbc_driver_snowflake")
    path = tmp_path_factory.mktemp("fake-snowflake") / "fake.db"
    with FakeSnowflake(path, oauth_client=OAUTH_CLIENT) as server:
        yield server


def attach_fake(engine: sa.Engine, server: FakeSnowflake, host: str | None = None) -> sa.Engine:
    """Point ``engine`` at ``server`` without clobbering the dialect's own db_kwargs."""

    @sa.event.listens_for(engine, "do_connect")
    def _merge(dialect: Any, conn_rec: Any, cargs: Any, cparams: dict[str, Any]) -> None:
        cparams.setdefault("db_kwargs", {}).update(server.db_kwargs(host=host))

    return engine


@pytest.fixture(params=["fake", "live"])
def snowflake_engine(request: pytest.FixtureRequest) -> Iterator[sa.Engine]:
    pytest.importorskip("adbc_driver_snowflake")
    if request.param == "live":
        url = os.environ.get("DRLS_SNOWFLAKE_URL")
        if not url:
            pytest.skip("Requires DRLS_SNOWFLAKE_URL")
        engine = sa.create_engine(url)
    else:
        server: FakeSnowflake = request.getfixturevalue("fake_snowflake")
        engine = attach_fake(sa.create_engine(FAKE_ENGINE_URL), server)
    yield engine
    engine.dispose()


# ── Flight SQL (live server, gated) ──────────────────────────────────


def wait_for_flightsql(engine: sa.Engine, timeout: float = 60.0) -> None:
    """Run ``SELECT 1`` until it succeeds or ``timeout`` seconds pass.

    A GitHub Actions service container has no readiness gate the job can
    wait on (the GizmoSQL image guarantees no curl or wget for a health
    command), so the first connection absorbs the server's start-up. The
    last error is re-raised once the deadline passes.
    """
    deadline = time.monotonic() + timeout
    delay = 0.5
    while True:
        try:
            with engine.connect() as c:
                c.execute(sa.text("SELECT 1")).scalar()
            return
        except sa.exc.DBAPIError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 3.0)


@pytest.fixture(scope="session")
def flightsql_url() -> str:
    """The live server's URL, returned only once the server answers.

    The readiness wait lives here so every fixture built on the URL (the
    engine, the bearer handshake, ad-hoc engines with a wrong password) is
    gated, not just the first one that happens to run.
    """
    url = os.environ.get("DRLS_FLIGHTSQL_URL")
    if not url:
        pytest.skip("Requires DRLS_FLIGHTSQL_URL")
    pytest.importorskip("adbc_driver_flightsql")
    probe = sa.create_engine(url)
    try:
        wait_for_flightsql(probe)
    finally:
        probe.dispose()
    return url


@pytest.fixture(scope="module")
def flightsql_engine(flightsql_url: str) -> Iterator[sa.Engine]:
    engine = sa.create_engine(flightsql_url)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def flightsql_bearer_url(flightsql_url: str) -> str:
    """The Flight handshake's bearer token, in our default ``bearer`` URL form."""
    flight = pytest.importorskip("pyarrow.flight")
    url = sa.make_url(flightsql_url)
    tls = str(url.query.get("tls", "true")).lower() in ("1", "true", "yes", "on")
    scheme = "grpc+tls" if tls else "grpc"
    host, port = url.host or "localhost", url.port or (443 if tls else 50051)
    client = flight.FlightClient(f"{scheme}://{host}:{port}")
    try:
        _, header_value = client.authenticate_basic_token(url.username or "", url.password or "")
    finally:
        client.close()
    token = header_value.decode().split(" ", 1)[-1]
    bearer = sa.URL.create(
        "flightsql+adbc",
        username="x",
        password=token,
        host=host,
        port=port,
        query={"tls": "true" if tls else "false"},
    )
    return bearer.render_as_string(hide_password=False)
