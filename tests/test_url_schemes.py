"""URL scheme registration.

SQLAlchemy URLs are ``<backend>+<driver>://``. ADBC is the driver, so the
documented form is ``postgresql+adbc://``. The original ``adbc+postgresql://``
spelling stays registered as a deprecated alias (#15).
"""

from __future__ import annotations

import warnings

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.bigquery import ADBCBigQueryDialect
from sqlalchemy_adbc.flightsql import ADBCFlightSQLDialect
from sqlalchemy_adbc.postgresql import ADBCPostgreSQLDialect
from sqlalchemy_adbc.snowflake import ADBCSnowflakeDialect
from sqlalchemy_adbc.sqlite import ADBCSQLiteDialect

BACKENDS = [
    ("flightsql", ADBCFlightSQLDialect),
    ("sqlite", ADBCSQLiteDialect),
    ("postgresql", ADBCPostgreSQLDialect),
    ("snowflake", ADBCSnowflakeDialect),
    ("bigquery", ADBCBigQueryDialect),
]


@pytest.mark.parametrize(("backend", "dialect_cls"), BACKENDS)
def test_backend_plus_adbc_resolves(backend, dialect_cls):
    url = make_url(f"{backend}+adbc://host/db")
    assert url.get_dialect() is dialect_cls
    # URL and dialect agree on which side is the backend and which the driver.
    assert url.get_backend_name() == dialect_cls.name
    assert url.get_driver_name() == dialect_cls.driver == "adbc"


@pytest.mark.parametrize(("backend", "dialect_cls"), BACKENDS)
def test_legacy_adbc_plus_backend_still_resolves(backend, dialect_cls):
    assert make_url(f"adbc+{backend}://host/db").get_dialect() is dialect_cls


def test_legacy_scheme_warns_on_engine_creation():
    with pytest.warns(DeprecationWarning, match=r"sqlite\+adbc://"):
        sa.create_engine("adbc+sqlite:///:memory:")


def test_documented_scheme_does_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        engine = sa.create_engine("sqlite+adbc:///:memory:")
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT 1").scalar() == 1
