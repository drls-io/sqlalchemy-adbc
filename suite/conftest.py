"""SQLAlchemy dialect compliance suite for sqlalchemy-adbc.

Run from the repository root (never collected by plain ``pytest``, whose
``testpaths`` is ``tests``)::

    pytest suite/ --dburi "sqlite+adbc:///suite_sqlite_adbc.db" -p no:cacheprovider

``requirements.py`` declares what the dialect or driver does not support;
``docs/compliance.md`` lists each exclusion with its reason.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import event
from sqlalchemy.dialects import registry

registry.register("sqlite.adbc", "sqlalchemy_adbc.sqlite", "ADBCSQLiteDialect")
registry.register("postgresql.adbc", "sqlalchemy_adbc.postgresql", "ADBCPostgreSQLDialect")
registry.register("flightsql.adbc", "sqlalchemy_adbc.flightsql", "ADBCFlightSQLDialect")
registry.register("snowflake.adbc", "sqlalchemy_adbc.snowflake", "ADBCSnowflakeDialect")
registry.register("bigquery.adbc", "sqlalchemy_adbc.bigquery", "ADBCBigQueryDialect")

# Provisioning hooks (test_schema, temp tables, ...) are keyed by backend name
# and registered only when the backend's provision module is imported.
import sqlalchemy.dialects.postgresql.provision  # noqa: E402,F401
import sqlalchemy.dialects.sqlite.provision  # noqa: E402,F401
from sqlalchemy.testing import provision  # noqa: E402


def _attach_test_schema(engine, attach_path):
    """Attach ``test_schema`` on every new connection of ``engine``.

    SQLAlchemy's SQLite provisioning does this with ``dbapi_connection.execute``,
    which the ADBC connection lacks, and only for the main engine; the suite's
    fixtures also build their own engines (``testing_engine``), whose
    connections need the attachment too.
    """

    # The main engine passes through both post_configure hooks; one listener
    # per engine, or every connection would ATTACH twice and fail.
    if getattr(engine, "_adbc_test_schema_attached", False):
        return
    engine._adbc_test_schema_attached = True

    @event.listens_for(engine, "connect")
    def attach(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f'ATTACH DATABASE "{attach_path}" AS test_schema')
        finally:
            cursor.close()


def _attach_path(engine, follower_ident):
    return (
        f"{follower_ident}_{engine.driver}_test_schema.db"
        if follower_ident
        else f"{engine.driver}_test_schema.db"
    )


_sqlalchemy_post_configure_testing_engine = provision.post_configure_testing_engine.fns["sqlite"]


@provision.post_configure_engine.for_db("sqlite")
def _sqlite_adbc_post_configure_engine(url, engine, follower_ident):
    attach_path = _attach_path(engine, follower_ident)
    _attach_test_schema(engine, attach_path)

    @event.listens_for(engine, "engine_disposed")
    def dispose(engine):
        for path in (attach_path, engine.url.database):
            if path and path != ":memory:" and os.path.exists(path):
                os.remove(path)


@provision.post_configure_testing_engine.for_db("sqlite")
def _sqlite_adbc_post_configure_testing_engine(url, engine, options, scope):
    _sqlalchemy_post_configure_testing_engine(url, engine, options, scope)
    if engine.dialect.driver == "adbc" and not options.get("sqlite_share_pool"):
        _attach_test_schema(engine, _attach_path(engine, None))


pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402,F401,F403
