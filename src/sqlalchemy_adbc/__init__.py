"""sqlalchemy-adbc — Generic SQLAlchemy dialect for any ADBC driver.

Register dialects via the ``sqlalchemy.dialects`` entry-point group (see
pyproject.toml). URL forms::

    flightsql+adbc://host:port/...?tls=true
    sqlite+adbc:///path/to/db.sqlite
    postgresql+adbc://user:pass@host:port/db
    snowflake+adbc://user:pass@account/db/schema?warehouse=WH
"""

from importlib.metadata import PackageNotFoundError, version

from sqlalchemy_adbc.arrow import fetch_arrow_table, ingest_arrow, open_arrow_batch_reader
from sqlalchemy_adbc.base import ADBCDialect

__all__ = [
    "ADBCDialect",
    "__version__",
    "fetch_arrow_table",
    "ingest_arrow",
    "open_arrow_batch_reader",
]

try:
    __version__ = version("sqlalchemy-adbc")
except PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0"
