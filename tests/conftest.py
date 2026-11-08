"""Shared fixtures.

``fake_snowflake`` starts one in-process fake Snowflake endpoint per test
session (see ``tests/fake_snowflake``). ``snowflake_engine`` runs a test
twice: against the fake, and against a real account when
``DRLS_SNOWFLAKE_URL`` is set (otherwise that cell skips), so the fake and
Snowflake can be checked against each other.
"""

from __future__ import annotations

import os
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
