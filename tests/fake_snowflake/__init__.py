"""In-process fake Snowflake endpoint for the ADBC Snowflake driver.

``FakeSnowflake`` speaks enough of the gosnowflake HTTP protocol for the
real ``adbc-driver-snowflake`` to log in, run SQL (executed by SQLite) and
reflect metadata, so the dialect is exercised end to end in CI without an
account. See the module docstrings in ``server.py``, ``sqlengine.py`` and
``results.py`` for what is and is not emulated.
"""

from tests.fake_snowflake.server import FakeSnowflake, endpoint_db_kwargs

__all__ = ["FakeSnowflake", "endpoint_db_kwargs"]
