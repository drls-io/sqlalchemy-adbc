"""sqlalchemy-adbc — Generic SQLAlchemy dialect for any ADBC driver.

Register dialects via the ``sqlalchemy.dialects`` entry-point group (see
pyproject.toml). URL forms::

    flightsql+adbc://host:port/...?tls=true
    sqlite+adbc:///path/to/db.sqlite
    postgresql+adbc://user:pass@host:port/db
    snowflake+adbc://user:pass@account/db/schema?warehouse=WH
"""

from sqlalchemy_adbc.base import ADBCDialect

__all__ = ["ADBCDialect", "__version__"]
__version__ = "0.1.0"
