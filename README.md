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

### Flight SQL query-string options

| Key                 | Meaning                                            |
|---------------------|----------------------------------------------------|
| `tls=true`          | Use `grpc+tls://` instead of `grpc://`             |
| `authorization=...` | Sets the ADBC `AUTHORIZATION_HEADER` db_kwarg      |
| `header.<name>=<v>` | Forwarded via ADBC's `RPC_CALL_HEADER_PREFIX`      |
| (any other key)     | Passed through as a db_kwarg verbatim              |

## Proxies

The Flight SQL, Snowflake and BigQuery drivers are Go libraries that read
`HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` (either case) from the process
environment, so if those are set nothing more is needed. When they are not,
a `[proxy]` section in the local config file (below) is exported into the
environment on the first connect. A variable you already set always wins;
the export is process-wide because that is the only lever the drivers have.
PostgreSQL (libpq) and SQLite have no HTTP proxy concept.

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
itself. Snowflake receives a token minted by this package per connection.
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
- **Drivers exercised in CI** — SQLite fully, PostgreSQL
  integration-tested via service container. Snowflake / BigQuery /
  Flight SQL inherit the shared reflection path but have no
  per-driver CI
  ([#12](https://github.com/drls-io/sqlalchemy-adbc/issues/12))
- **ADBC SQLite NOT NULL reporting** — fixed upstream in
  `adbc_driver_sqlite` 1.13 (Python ≥ 3.11). On older drivers every
  column reflects as nullable=True
  ([#13](https://github.com/drls-io/sqlalchemy-adbc/issues/13))

Tests run on Python 3.10–3.13 in CI, including PostgreSQL integration
tests against a service-container Postgres; see the badge above for the
current state.

## Development

```bash
git clone https://github.com/drls-io/sqlalchemy-adbc
cd sqlalchemy-adbc
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                 # unit tests; live backends via DRLS_PG_URL / DRLS_SNOWFLAKE_URL
ruff check src/ tests/ && ruff format --check src/ tests/
mypy src tests
tox                    # same gates across Python 3.10–3.13 × SQLAlchemy 2.0/2.1 (tox.ini)
```

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
