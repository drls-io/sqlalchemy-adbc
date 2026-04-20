# sqlalchemy-adbc

[![PyPI](https://img.shields.io/pypi/v/sqlalchemy-adbc.svg)](https://pypi.org/project/sqlalchemy-adbc/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Generic [SQLAlchemy](https://www.sqlalchemy.org/) dialect for any
[Apache Arrow ADBC](https://arrow.apache.org/adbc/) driver — so any tool that
speaks SQLAlchemy (pandas, marimo, Superset, dbt, Polars) can read and write
against Flight SQL, PostgreSQL, SQLite, Snowflake, and BigQuery through the
Arrow-native wire path.

## Why this exists

ADBC drivers speak Python DBAPI and return results as Arrow record batches —
zero per-row materialization, no driver-specific JDBC/ODBC install. But most
of the Python ecosystem wires into databases through SQLAlchemy, and ADBC
doesn't ship a dialect. This package is a thin translation layer: SQLAlchemy
URL → ADBC `db_kwargs` → DBAPI connection.

## Install

```bash
pip install sqlalchemy-adbc

# With a driver extra:
pip install "sqlalchemy-adbc[flightsql]"
pip install "sqlalchemy-adbc[postgresql]"
pip install "sqlalchemy-adbc[sqlite]"
pip install "sqlalchemy-adbc[snowflake]"
pip install "sqlalchemy-adbc[bigquery]"
```

## Quickstart

```python
import sqlalchemy

# Flight SQL
engine = sqlalchemy.create_engine(
    "adbc+flightsql://flight-sql.example.com:443?tls=true",
    connect_args={"db_kwargs": {
        "adbc.flight.sql.authorization_header": f"Bearer {token}",
    }},
)

# SQLite
engine = sqlalchemy.create_engine("adbc+sqlite:///path/to/db.sqlite")

# PostgreSQL
engine = sqlalchemy.create_engine(
    "adbc+postgresql://user:pass@host:5432/mydb"
)

# Works with pandas, Polars, marimo SQL cells, etc.
import pandas as pd
df = pd.read_sql("SELECT 1 AS n", engine)
```

## URL forms

| Driver       | URL                                                        |
|--------------|------------------------------------------------------------|
| Flight SQL   | `adbc+flightsql://host:port?tls=true&authorization=...`    |
| Flight SQL   | `adbc+flightsql://user:TOKEN@host:port?tls=true` (bearer)  |
| PostgreSQL   | `adbc+postgresql://user:pass@host:port/database`           |
| SQLite       | `adbc+sqlite:///path/to/db` or `adbc+sqlite:///:memory:`   |
| Snowflake    | `adbc+snowflake://user:pass@account/db/schema?warehouse=W` |
| BigQuery     | `adbc+bigquery:///my-project?dataset=my_dataset`           |

### Flight SQL query-string options

| Key                 | Meaning                                            |
|---------------------|----------------------------------------------------|
| `tls=true`          | Use `grpc+tls://` instead of `grpc://`             |
| `authorization=...` | Sets the ADBC `AUTHORIZATION_HEADER` db_kwarg      |
| `header.<name>=<v>` | Forwarded via ADBC's `RPC_CALL_HEADER_PREFIX`      |
| (any other key)     | Passed through as a db_kwarg verbatim              |

## Status

Alpha — the core dialect machinery works for simple `SELECT` / `INSERT` /
`CREATE TABLE` use. More advanced SQLAlchemy features are still to come;
the known gaps are called out below so you don't waste time debugging a
missing feature:

- **Table reflection** (`MetaData.reflect()`): not implemented. The ADBC
  driver exposes table/column metadata via the Arrow-based `GetObjects`
  call, but this library doesn't yet wire that into SQLAlchemy's
  `Inspector` contract. PRs welcome.
- **Alembic autogenerate**: follows from reflection — won't work until
  reflection lands. `alembic upgrade` against already-written migrations
  is fine (it's pure SQL).
- **PostgreSQL-specific types** (JSONB, ARRAY, UUID, TSTZMULTIRANGE,
  …): ADBC's Arrow-over-the-wire codec returns these as strings / lists
  of primitives, not as SQLAlchemy's typed objects. If you need ORM-level
  round-tripping of PG-specific types, use `psycopg2` until we add a
  type compiler.
- **Query parameterization**: uses ADBC's native `$1`-style placeholders
  for PostgreSQL and `?` for SQLite. Flight SQL and Snowflake delegate
  to the driver's default.

PRs welcome.

Tests: 52+ passing against ADBC SQLite + URL/DSN translators on Python 3.9–3.13.

## Development

```bash
git clone https://github.com/drls-io/sqlalchemy-adbc
cd sqlalchemy-adbc
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check src/ tests/
```

## Releasing

Publishing is automated via [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC, no API tokens). One-time setup on pypi.org: add a *pending publisher*
for project `sqlalchemy-adbc`, owner `drls-io`, repository `sqlalchemy-adbc`,
workflow `publish.yml`, environment `pypi`. Same for TestPyPI if you want
dry-runs.

Cutting a release:

```bash
# 1. Bump version in pyproject.toml
# 2. Commit, tag, push
git commit -am "chore: release v0.1.1"
git tag v0.1.1
git push && git push --tags
```

The `Publish to PyPI` workflow fires on the tag, builds sdist+wheel, and
uploads to pypi.org. For TestPyPI dry-runs use `workflow_dispatch` with
target `testpypi`.

## License

Apache-2.0. See [LICENSE](LICENSE).
