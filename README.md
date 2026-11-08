# sqlalchemy-adbc

[![PyPI](https://img.shields.io/pypi/v/sqlalchemy-adbc.svg)](https://pypi.org/project/sqlalchemy-adbc/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/drls-io/sqlalchemy-adbc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/drls-io/sqlalchemy-adbc/actions/workflows/ci.yml)

Generic [SQLAlchemy](https://www.sqlalchemy.org/) dialect for any
[Apache Arrow ADBC](https://arrow.apache.org/adbc/) driver — so any tool that
speaks SQLAlchemy (pandas, marimo, Superset, dbt, Polars) can read and write
against Flight SQL, PostgreSQL, SQLite, Snowflake, and BigQuery through the
Arrow-native wire path.

## Why this exists

ADBC drivers speak Python DBAPI and return results as Arrow record batches,
with no driver-specific JDBC/ODBC install. But most of the Python ecosystem
wires into databases through SQLAlchemy, and ADBC doesn't ship a dialect.
This package is a thin translation layer: SQLAlchemy URL → ADBC `db_kwargs`
→ DBAPI connection. SQLAlchemy's own results are rows; the "Arrow-native
path" section below skips that step when you want Arrow, Polars or pandas.

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
    "flightsql+adbc://flight-sql.example.com:443?tls=true",
    connect_args={"db_kwargs": {
        "adbc.flight.sql.authorization_header": f"Bearer {token}",
    }},
)

# SQLite
engine = sqlalchemy.create_engine("sqlite+adbc:///path/to/db.sqlite")

# PostgreSQL
engine = sqlalchemy.create_engine(
    "postgresql+adbc://user:pass@host:5432/mydb"
)

# Works with pandas, Polars, marimo SQL cells, etc.
import pandas as pd
df = pd.read_sql("SELECT 1 AS n", engine)
```

## Arrow-native path

`Connection.execute()` returns rows, so the driver's Arrow batches are
converted to Python tuples. `sqlalchemy_adbc.arrow` reaches the ADBC cursor
behind the same SQLAlchemy connection instead, so you keep the URL, pool,
authentication and transaction and get Arrow in and out:

```python
import polars as pl
from sqlalchemy import bindparam, select, text
from sqlalchemy_adbc import fetch_arrow_table, ingest_arrow, open_arrow_batch_reader

with engine.begin() as conn:
    table = fetch_arrow_table(conn, select(orders).where(orders.c.day >= bindparam("d")), {"d": day})
    frame = pl.from_arrow(table)                 # or table.to_pandas()

    with open_arrow_batch_reader(conn, text("SELECT * FROM events WHERE kind = :k"), {"k": "click"}) as reader:
        for batch in reader:                     # streams, one Arrow batch at a time
            ...

    ingest_arrow(conn, "orders_copy", table, mode="create")   # append | create | replace | create_append
```

Statements are SQLAlchemy constructs (compiled with the dialect, bind
processors and IN-list expansion included, parameters as a mapping) or
plain SQL strings passed to the driver verbatim (parameters as a positional
sequence, placeholders in the driver's own style: `?` for SQLite, Flight SQL
and Snowflake, `$1` for PostgreSQL; `text()` gives you `:name` everywhere).
The helpers begin the SQLAlchemy transaction when none is open, so
`conn.commit()`, `conn.rollback()` and `with engine.begin()` govern Arrow
work as well; `Session.connection()` is accepted. Driver errors propagate as
the ADBC exceptions. For `append` the Arrow schema must match the table
(PostgreSQL rejects int64 into an `INTEGER` column). Snowflake ingest goes
through a stage upload the fake endpoint cannot serve, so it is not covered
by the test suite.

| Path | Time (500,000 rows × 5 cols, median of 3) | vs rows |
|---|---:|---:|
| SQLAlchemy rows: `conn.execute(select).all()` | 1.20 s | 1.0× |
| Arrow: `fetch_arrow_table(conn, select)` | 0.16 s | 7.5× |
| Polars: `pl.from_arrow(fetch_arrow_table(...))` | 0.17 s | 7.1× |
| pandas via Arrow: `fetch_arrow_table(...).to_pandas()` | 0.16 s | 7.4× |
| pandas via rows: `pd.read_sql(select, conn)` | 1.33 s | 0.9× |

Measured with `benchmarks/arrow_vs_rows.py` on an arm64 Mac (Darwin 27),
Python 3.12.12, SQLAlchemy 2.0.49, pyarrow 23.0.1, adbc-driver-sqlite 1.11.0,
polars 2.1.0, pandas 3.1.1, SQLite in memory. Absolute numbers are machine
specific; the ratio is the point.

## URL forms

URLs follow SQLAlchemy's `<backend>+<driver>://` convention, with `adbc`
as the driver. The pre-0.4 `adbc+<backend>://` spelling still works but
emits a `DeprecationWarning` and will be removed in 1.0.

| Backend      | URL                                                        |
|--------------|------------------------------------------------------------|
| Flight SQL   | `flightsql+adbc://host:port?tls=true&authorization=...`    |
| Flight SQL   | `flightsql+adbc://user:TOKEN@host:port?tls=true` (bearer)  |
| PostgreSQL   | `postgresql+adbc://user:pass@host:port/database`           |
| SQLite       | `sqlite+adbc:///path/to/db` or `sqlite+adbc:///:memory:`   |
| Snowflake    | `snowflake+adbc://user:pass@account/db/schema?warehouse=W` |
| BigQuery     | `bigquery+adbc:///my-project?dataset=my_dataset`           |

### BigQuery query-string options

The database segment is the GCP project. Credentials default to Application
Default Credentials (`gcloud auth application-default login` or a service
account on the VM).

| Key                   | Meaning                                                               |
|-----------------------|-----------------------------------------------------------------------|
| `dataset`             | Default dataset for unqualified table names (required to run queries) |
| `location`            | Job location, e.g. `US` or `europe-west2`                             |
| `credentials_path`    | Service-account JSON file; switches auth to that file                 |
| `adbc.bigquery.sql.*` | Any driver option, passed through as is; wins over the short keys     |

### Flight SQL query-string options

| Key                 | Meaning                                            |
|---------------------|----------------------------------------------------|
| `tls=true`          | Use `grpc+tls://` instead of `grpc://`             |
| `authorization=...` | Sets the ADBC `AUTHORIZATION_HEADER` db_kwarg      |
| `header.<name>=<v>` | Forwarded via ADBC's `RPC_CALL_HEADER_PREFIX`      |
| (any other key)     | Passed through as a db_kwarg verbatim              |

**Tested servers.** CI runs the Flight SQL suite against
[GizmoSQL](https://github.com/gizmodata/gizmosql) v1.42 (DuckDB backend):
basic and bearer auth, type round trips, parameters, transactions,
reflection and `pandas.read_sql`. Server-dependent: GizmoSQL's `GetObjects`
reports no constraints or nullability, so reflected primary keys are empty
and every column reflects `nullable=True`; its SQLite backend is not usable
through the Go driver.

## Proxies

The Flight SQL, Snowflake and BigQuery drivers are Go libraries that read
`HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` (either case) from the process
environment. **Set them in the shell or launcher that starts Python.** The
Go runtime snapshots the environment at process start, so a variable written
later from Python, by this package or anything else, is not seen by the
drivers. A `[proxy]` section in the local config file (below) is still
exported into the environment on the first connect: it serves this package's
own HTTP calls (the OAuth2 token request) and anything in the process that
reads the environment at request time, and a WARNING is logged so the gap
is visible. A variable you already set always wins. PostgreSQL (libpq) and
SQLite have no HTTP proxy concept.

## Authentication

Pick a mode with `auth=` in the URL, or name a profile from the config file
with `auth_profile=`:

| Mode        | Flight SQL | Snowflake | Credentials |
|-------------|-----------:|----------:|-------------|
| `bearer`    | default    | –         | URL password is the token (`flightsql+adbc://u:TOKEN@host`) |
| `basic`     | yes        | default   | URL `user:password@`; Flight SQL uses the Handshake |
| `negotiate` | yes        | –         | Kerberos/SPNEGO via `pip install "sqlalchemy-adbc[kerberos]"`; SPN is `HTTP@<host>` unless a profile pins `spn` |
| `oauth2`    | yes        | yes       | client credentials: `oauth2_token_url=`, URL `client_id:client_secret@` (or a profile); optional `oauth2_scope=`, `oauth2_audience=` |

Flight SQL runs the OAuth2 flow inside the driver and refreshes tokens
itself. Snowflake receives a token minted by this package when each DBAPI
connection opens (cached until it expires, so a pool does not re-request it);
the Negotiate ticket on Flight SQL is likewise minted per connection.
Snowflake's own `authenticator=` DSN options (key pair, Okta, external
browser) keep working unchanged through the query string.

### Config file

Secrets stay out of URLs by living in a profile. The file is read from
`$SQLALCHEMY_ADBC_CONFIG`, else `~/.config/sqlalchemy-adbc/config.toml`;
it must be owned by you and not group- or world-writable, and the working
directory is deliberately never searched. Any value may be `env:NAME`.

```toml
[proxy]
https_proxy = "http://proxy.corp.example:3128"
no_proxy    = "localhost,127.0.0.1,.corp.example"

[auth.drls-flight]
type = "oauth2"
token_url = "https://keycloak.example/realms/drls/protocol/openid-connect/token"
client_id = "marimo"
client_secret = "env:DRLS_FLIGHT_CLIENT_SECRET"
scope = "openid"

[auth.corp-kerberos]
type = "negotiate"
spn = "HTTP@flight-sql.corp.example"
```

```python
engine = sqlalchemy.create_engine(
    "flightsql+adbc://flight-sql.corp.example:443?auth_profile=drls-flight"
)
```

A URL that names a profile may only adjust `oauth2_scope` and
`oauth2_audience`; the token endpoint, SPN and client identity stay under
the config owner's control, `env:` references resolve only from the file,
and `oauth2_client_secret` is never accepted in a query string.

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
- BigQuery as GoogleSQL: backtick quoting, `STRING` / `INT64` / `FLOAT64` /
  `BOOL` / `NUMERIC` / `BYTES` / `DATETIME` / `TIMESTAMP` DDL, `NOT ENFORCED`
  primary and foreign keys, native booleans, multi-row inserts, and typed
  reflection of every scalar type (`TIMESTAMP` as `TIMESTAMP(timezone=True)`)
- PostgreSQL typed decorators: JSONB / JSON / UUID / INET / CIDR /
  MACADDR, and all range and multirange types (`int4range` …
  `tstzmultirange`) plus `hstore`, returning SQLAlchemy's own
  `Range` / `MultiRange` / `dict` with working `@>`, `&&`, `?`
  operators and reflection

### Known limitations

- **Raw SQL readers see PostgreSQL range / hstore values as bytes** —
  `pd.read_sql("SELECT period FROM t", engine)` and SQL Lab style
  queries carry no column type information, so the ADBC binary form
  reaches Python untouched. Typed columns via Core, ORM or reflection
  are decoded
  ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))
- **Drivers exercised in CI** — SQLite fully, PostgreSQL and Flight SQL
  (GizmoSQL) via service containers, Snowflake through the real driver
  against the fake endpoint under `tests/fake_snowflake/`. BigQuery has
  compile-only tests in CI; the live tests in `tests/test_bigquery_e2e.py`
  run when `DRLS_BIGQUERY_URL` is set and need Google Cloud credentials
  ([#12](https://github.com/drls-io/sqlalchemy-adbc/issues/12))
- **ADBC SQLite NOT NULL reporting** — fixed upstream in
  `adbc_driver_sqlite` 1.13 (Python ≥ 3.11). On older drivers every
  column reflects as nullable=True
  ([#13](https://github.com/drls-io/sqlalchemy-adbc/issues/13))

Tests run on Python 3.12–3.14 in CI, including PostgreSQL and Flight SQL
integration tests against service containers; see the badge above for the
current state.

## Development

```bash
git clone https://github.com/drls-io/sqlalchemy-adbc
cd sqlalchemy-adbc
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                 # unit tests + the fake Snowflake endpoint; live backends via DRLS_PG_URL / DRLS_FLIGHTSQL_URL / DRLS_SNOWFLAKE_URL / DRLS_BIGQUERY_URL
ruff check src/ tests/ && ruff format --check src/ tests/
mypy src tests
tox                    # same gates across Python 3.12–3.14 × SQLAlchemy 2.0/2.1 (tox.ini)
```

### Flight SQL server

The Flight SQL suite (`tests/test_flightsql_e2e.py`) needs a server. CI uses
GizmoSQL as a service container; locally:

```bash
docker run -d --name gizmosql -p 31337:31337 -e GIZMOSQL_USERNAME=ci \
  -e GIZMOSQL_PASSWORD=ci-password -e TLS_ENABLED=0 -e PRINT_QUERIES=0 \
  gizmodata/gizmosql:v1.42.2
export DRLS_FLIGHTSQL_URL="flightsql+adbc://ci:ci-password@127.0.0.1:31337?auth=basic&tls=false"
pytest tests/test_flightsql_e2e.py
```

Any Flight SQL server works through the same variable; tests that depend
on GizmoSQL specifics are documented in the module.

### Fake Snowflake endpoint

`tests/fake_snowflake/` is an in-process HTTP server that speaks enough of
Snowflake's wire protocol for the real `adbc-driver-snowflake` to log in,
run SQL and reflect metadata, with SQLite executing the statements. Every
`pytest` run drives the Snowflake dialect through the real driver against
it: login (password and OAuth2 with a token the fake issued), CRUD and type
round trips, `executemany`, reflection of tables, views and constraints,
error propagation, proxy routing and the exact bindings put on the wire.
The same behaviour suite (`tests/test_snowflake_e2e.py`) runs against a real
account when `DRLS_SNOWFLAKE_URL` is set, so the fake and Snowflake are
checked against each other by anyone with an account.

The fake is a model of Snowflake, not Snowflake. It does not reproduce
Snowflake-only SQL rules (`PARSE_JSON(?)` inside `VALUES` is accepted here
and rejected there), result chunking, async queries, stages or bulk ingest;
result column labels of textual SQL follow SQLite (`select id` yields `id`
where Snowflake yields `ID`); and NUMBER values are limited to what SQLite
stores exactly (int64, or 15 significant digits), beyond which the fake
raises a data conversion error rather than return a wrong value. Treat a
green fake run as "the dialect and driver agree", and the live run as the
truth.

## Releasing

Publishing is automated via [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC, no API tokens). One-time setup on pypi.org: add a *pending publisher*
for project `sqlalchemy-adbc`, owner `drls-io`, repository `sqlalchemy-adbc`,
workflow `publish.yml`, environment `pypi`. Same for TestPyPI if you want
dry-runs.

Cutting a release (`X.Y.Z`):

1. On a branch, bump `version` in `pyproject.toml` and move the
   `[Unreleased]` entries in `CHANGELOG.md` under a new `## [X.Y.Z] — YYYY-MM-DD`
   heading with a one-paragraph summary. Open a PR, let CI pass, merge.
2. Tag the merge commit on `main` and push the tag:

   ```bash
   git checkout main && git pull
   git tag vX.Y.Z && git push origin vX.Y.Z
   ```

The `Publish to PyPI` workflow fires on the tag, builds sdist and wheel,
uploads to pypi.org via trusted publishing and then creates the GitHub
Release from that version's changelog section. For TestPyPI dry-runs use
`workflow_dispatch` with target `testpypi`.

## License

Apache-2.0. See [LICENSE](LICENSE).
