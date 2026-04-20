"""sqlalchemy-adbc — Generic SQLAlchemy dialect for any ADBC driver.

Register dialects via the ``sqlalchemy.dialects`` entry-point group (see
pyproject.toml). URL forms::

    adbc+flightsql://host:port/...?tls=true
    adbc+sqlite:///path/to/db.sqlite
    adbc+postgresql://user:pass@host:port/db
    adbc+snowflake://user:pass@account/db/schema?warehouse=WH
"""

from sqlalchemy_adbc.base import ADBCDialect

__all__ = ["ADBCDialect", "__version__"]
__version__ = "0.1.0"
