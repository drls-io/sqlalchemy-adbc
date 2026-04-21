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

**Beta.** Core dialect + reflection + Alembic autogenerate work on
the ADBC drivers we exercise in CI (SQLite end-to-end, PostgreSQL
via service container). Not 1.0 yet — we want a few months of
real-world use and at least one release without breaking changes
before committing to a stable API.

Full history in [CHANGELOG.md](CHANGELOG.md). Download tags on
[GitHub Releases](https://github.com/drls-io/sqlalchemy-adbc/releases);
published wheels on [PyPI](https://pypi.org/project/sqlalchemy-adbc/).

### What works

- `SELECT` / `INSERT` / `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE`
  for every advertised driver
- `MetaData.reflect(bind=engine)` — full Inspector contract backed
  by ADBC `GetObjects`, including columns, PK, FK, unique
  constraints, indexes (native PRAGMA / `pg_catalog` queries on
  SQLite and PG)
- Alembic `autogenerate` — end-to-end, reusing SQLAlchemy's native
  backend DDL impls via standard dialect-name lookup
- URL forms for Flight SQL, PostgreSQL, SQLite, Snowflake, BigQuery
  with correct special-character escaping in passwords

### Known limitations

- **PostgreSQL range types** (tstzrange, tstzmultirange, int4range,
  …) and **hstore** — not yet mapped to typed decorators; returned
  as raw strings. JSONB / JSON / UUID / INET / CIDR / MACADDR /
  MACADDR8 all round-trip through typed decorators as of 0.3.0
  ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))
- **Drivers exercised in CI** — SQLite fully, PostgreSQL
  integration-tested via service container. Snowflake / BigQuery /
  Flight SQL inherit the shared reflection path but have no
  per-driver CI
  ([#12](https://github.com/drls-io/sqlalchemy-adbc/issues/12))
- **ADBC SQLite NOT NULL reporting** — upstream driver always
  reports nullable=True; reflection faithfully reflects that
  (tracked, auto-reactivating xfail)
  ([#13](https://github.com/drls-io/sqlalchemy-adbc/issues/13))

Tests: 106 passing on Python 3.10–3.13, including 17 PostgreSQL
integration tests against a service-container Postgres.

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
