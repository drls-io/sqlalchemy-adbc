"""Flight SQL dialect against a real Flight SQL server.

Skips without ``DRLS_FLIGHTSQL_URL``. CI points it at a GizmoSQL service
container (DuckDB backend); locally::

    docker run -d --name gizmosql -p 31337:31337 -e GIZMOSQL_USERNAME=ci \\
        -e GIZMOSQL_PASSWORD=ci-password -e TLS_ENABLED=0 -e PRINT_QUERIES=0 \\
        gizmodata/gizmosql:v1.42.2
    export DRLS_FLIGHTSQL_URL="flightsql+adbc://ci:ci-password@127.0.0.1:31337?auth=basic&tls=false"

Server-dependent behaviour (GizmoSQL reports no constraints or nullability
through GetObjects) is deliberately not asserted.
"""

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.skipif(
    not os.environ.get("DRLS_FLIGHTSQL_URL"), reason="Requires DRLS_FLIGHTSQL_URL"
)


def test_select_one(flightsql_engine: sa.Engine) -> None:
    with flightsql_engine.connect() as c:
        assert c.execute(sa.text("SELECT 1")).scalar() == 1
