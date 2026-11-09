# sqlalchemy-adbc

[![PyPI](https://img.shields.io/pypi/v/sqlalchemy-adbc.svg)](https://pypi.org/project/sqlalchemy-adbc/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/drls-io/sqlalchemy-adbc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/drls-io/sqlalchemy-adbc/actions/workflows/ci.yml)

[SQLAlchemy](https://www.sqlalchemy.org/) access to
[Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
servers, with the authentication an enterprise deployment needs: OAuth2
client credentials, Kerberos/Negotiate, bearer tokens, and config profiles
that keep secrets out of URLs. Any tool that speaks SQLAlchemy (pandas,
Polars, marimo, Superset) gets a Flight SQL connection from one URL,
and an Arrow-native path when it wants Arrow rather than rows.

The same package also drives PostgreSQL, SQLite, Snowflake and BigQuery
through their [ADBC](https://arrow.apache.org/adbc/) drivers; see
[Other backends](#other-backends).

## Why this exists

Flight SQL servers (and the other ADBC drivers) speak Python DBAPI and return
Arrow record batches, with no JDBC/ODBC install. But the Python ecosystem
wires into databases through SQLAlchemy, and ADBC ships no dialect. This
package is that dialect: SQLAlchemy URL → ADBC `db_kwargs` → DBAPI
connection, with the authentication flows (token endpoints, SPNEGO tickets,
per-connection credentials) handled here so a BI tool or notebook only has to
hold a URL.

How this compares with the other Flight SQL and ADBC options, including where
they are the better choice, is in [docs/alternatives.md](docs/alternatives.md).

## Install

```bash
pip install "sqlalchemy-adbc[flightsql]"            # Flight SQL
pip install "sqlalchemy-adbc[flightsql,kerberos]"   # + Kerberos/Negotiate

# Other backends
pip install "sqlalchemy-adbc[postgresql]"
pip install "sqlalchemy-adbc[sqlite]"
pip install "sqlalchemy-adbc[snowflake]"
pip install "sqlalchemy-adbc[bigquery]"
```

Requires Python 3.12 or newer. On an older interpreter `pip` does not fail: it
installs the newest release that allows that interpreter (0.6.x on Python 3.11,
0.5.x on 3.10), which lacks every later fix. Check with
`pip show sqlalchemy-adbc` if you are unsure which version you got.

## Quickstart

```python
import sqlalchemy

# Flight SQL with a bearer token (the URL password is the token)
engine = sqlalchemy.create_engine(
    "flightsql+adbc://user:TOKEN@flight-sql.example.com:443?tls=true"
)

# Flight SQL with OAuth2 or Kerberos from a config profile (no secret in the URL)
engine = sqlalchemy.create_engine(
    "flightsql+adbc://flight-sql.corp.example:443?auth_profile=analytics"
)

# Works with pandas, Polars, marimo SQL cells, Superset, ...
import pandas as pd
df = pd.read_sql("SELECT 1 AS n", engine)
```

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
[auth.analytics]
type = "oauth2"
token_url = "https://keycloak.example/realms/analytics/protocol/openid-connect/token"
client_id = "marimo"
client_secret = "env:FLIGHT_CLIENT_SECRET"
scope = "openid"

[auth.corp-kerberos]
type = "negotiate"
spn = "HTTP@flight-sql.corp.example"
```

```python
engine = sqlalchemy.create_engine(
    "flightsql+adbc://flight-sql.corp.example:443?auth_profile=analytics"
)
```

A URL that names a profile may only adjust `oauth2_scope` and
`oauth2_audience`; the token endpoint, SPN and client identity stay under
the config owner's control, `env:` references resolve only from the file,
and `oauth2_client_secret` is never accepted in a query string.

### Proxies

The Flight SQL, Snowflake and BigQuery drivers are Go libraries that read
`HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` (either case) from the process
environment. Set them in the shell or launcher that starts Python: the Go
runtime snapshots the environment at process start, so a variable written
later from Python, by this package or anything else, is not seen by the
drivers. Versions before 0.9.0 exported a `[proxy]` section of the config
file into the environment; it could never reach the drivers and is ignored
with a warning. PostgreSQL (libpq) and SQLite have no HTTP proxy concept.

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
sequence, placeholders in the driver's own style: `?` for SQLite, Flight SQL,
Snowflake and BigQuery, `$1` for PostgreSQL; `text()` gives you `:name`
everywhere). The helpers begin the SQLAlchemy transaction when none is open,
so `conn.commit()`, `conn.rollback()` and `with engine.begin()` govern Arrow
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
specific; the ratio is the point. The "no per-row materialisation" benefit
exists only on this path; plain `Connection.execute()` still builds rows.

## URL forms

URLs follow SQLAlchemy's `<backend>+<driver>://` convention, with `adbc`
as the driver. The pre-0.4 `adbc+<backend>://` spelling still works but
emits a `DeprecationWarning` and is removed in 1.0.0 (see
[Stability](#stability)).

| Backend      | URL                                                        |
|--------------|------------------------------------------------------------|
| Flight SQL   | `flightsql+adbc://user:TOKEN@host:port?tls=true` (bearer)  |
| Flight SQL   | `flightsql+adbc://host:port?tls=true&auth_profile=<name>`  |
| PostgreSQL   | `postgresql+adbc://user:pass@host:port/database`           |
| SQLite       | `sqlite+adbc:///path/to/db` or `sqlite+adbc:///:memory:`   |
| Snowflake    | `snowflake+adbc://user:pass@account/db/schema?warehouse=W` |
| BigQuery     | `bigquery+adbc:///my-project?dataset=my_dataset`           |

### Flight SQL query-string options

| Key                     | Meaning                                                  |
|-------------------------|----------------------------------------------------------|
| `tls=true`              | Use `grpc+tls://` instead of `grpc://`                   |
| `auth=<mode>`           | `bearer` (default), `basic`, `negotiate`, `oauth2`       |
| `auth_profile=<name>`   | Take the auth settings from the config file              |
| `oauth2_token_url=` …   | OAuth2 client-credentials settings (see Authentication)  |
| `authorization=...`     | Sets the ADBC `AUTHORIZATION_HEADER` db_kwarg directly   |
| `header.<name>=<v>`     | Forwarded via ADBC's `RPC_CALL_HEADER_PREFIX`            |
| (any other key)         | Passed through as a db_kwarg verbatim                    |

**Tested servers.** CI runs the Flight SQL suite against
[GizmoSQL](https://github.com/gizmodata/gizmosql) v1.42 (DuckDB backend):
basic and bearer auth, type round trips, parameters, transactions,
reflection and `pandas.read_sql`. Server-dependent: GizmoSQL's `GetObjects`
reports no constraints or nullability, so reflected primary keys are empty
and every column reflects `nullable=True`; its SQLite backend is not usable
through the Go driver. Servers that report `TABLE`/`VIEW` in upper case or
only numeric XDBC column type codes (Redis's Arrow Flight SQL server) reflect
correctly since 0.9.1.

## Other backends

The connection layer, Arrow helpers, config profiles and proxy notes above
apply to every backend. What differs is SQL compilation and reflection.

**PostgreSQL** (`postgresql+adbc://`, libpq driver, `adbc-driver-postgresql>=1.13.0`).
Typed decorators for JSONB / JSON / UUID / INET / CIDR / MACADDR, every range
and multirange type (`int4range` … `tstzmultirange`) and `hstore`, returning
SQLAlchemy's own `Range` / `MultiRange` / `dict` with working `@>`, `&&`, `?`
operators and reflection. Reflection uses `pg_catalog`. Limitation: raw SQL
readers (`pd.read_sql("SELECT period FROM t", engine)`, SQL Lab) see range and
hstore values as bytes because plain SQL carries no column type; typed
columns via Core, ORM or reflection are decoded
([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11)); raw-SQL
decoding is tracked in
[#59](https://github.com/drls-io/sqlalchemy-adbc/issues/59).

**SQLite** (`sqlite+adbc://`). SQLAlchemy's own SQLite dialect over the ADBC
connection: its compiler, DDL, types, PRAGMA reflection and `ATTACH` schemas,
passing SQLAlchemy's dialect compliance suite with documented exclusions
([docs/compliance.md](docs/compliance.md)). `:memory:` URLs share one
connection. Limitation: `UPDATE`/`DELETE` rowcount is not reported by the
driver.

**Snowflake** (`snowflake+adbc://`). Basic, OAuth2 and Snowflake's own
`authenticator=` modes; `BOOLEAN`, `NUMBER(p, s)`, `VARIANT` / `OBJECT` /
`ARRAY`, binary and temporal round trips; record-batch binds; reflection of
tables, views and constraints. Limitation: `ingest_arrow` needs a stage
upload and is untested; the fake endpoint used in CI is a model of Snowflake
(see [Fake Snowflake endpoint](#fake-snowflake-endpoint)).

**BigQuery** (`bigquery+adbc:///<project>?dataset=<dataset>`). GoogleSQL
compilation: backtick quoting, `STRING` / `INT64` / `FLOAT64` / `BOOL` /
`NUMERIC` / `BYTES` / `DATETIME` / `TIMESTAMP` DDL, `NOT ENFORCED` primary and
foreign keys, native booleans, multi-row inserts, typed reflection of every
scalar type, an Alembic impl. Credentials default to Application Default
Credentials. Limitations: ARRAY / STRUCT / GEOGRAPHY reflect as `NullType`;
no partitioning or clustering DDL; `UPDATE`/`DELETE` rowcount is not
reported.

| Key                   | Meaning                                                               |
|-----------------------|-----------------------------------------------------------------------|
| `dataset`             | Default dataset for unqualified table names (required to run queries) |
| `location`            | Job location, e.g. `US` or `europe-west2`                             |
| `credentials_path`    | Service-account JSON file; switches auth to that file                 |
| `adbc.bigquery.sql.*` | Any driver option, passed through as is; wins over the short keys     |

## Status

**Beta, approaching 1.0.** Every claim in this README is backed by a CI job
or listed as a limitation. Full history in [CHANGELOG.md](CHANGELOG.md);
tags on [GitHub Releases](https://github.com/drls-io/sqlalchemy-adbc/releases),
wheels on [PyPI](https://pypi.org/project/sqlalchemy-adbc/).

### What CI runs

Python 3.12, 3.13 and 3.14; SQLAlchemy 2.0 and 2.1 through `tox`.

| Backend    | In CI on every change | Live, when the variable is set |
|------------|-----------------------|--------------------------------|
| Flight SQL | GizmoSQL service container: auth modes, types, parameters, transactions, reflection, Arrow helpers | any server via `DRLS_FLIGHTSQL_URL` |
| PostgreSQL | service container: typed round trips, ranges/hstore, reflection, Arrow helpers | `DRLS_PG_URL` |
| SQLite     | full unit suite, Alembic autogenerate, SQLAlchemy's dialect compliance suite | – |
| Snowflake  | real driver against the in-process fake endpoint: auth, CRUD, types, reflection, bindings | `DRLS_SNOWFLAKE_URL` |
| BigQuery   | compile-only tests (DDL, quoting, reflection map) | `DRLS_BIGQUERY_URL` ([#12](https://github.com/drls-io/sqlalchemy-adbc/issues/12)) |

Reflection (`MetaData.reflect`, the Inspector) runs on ADBC `GetObjects`
for Flight SQL, Snowflake and BigQuery, and on the native catalog for SQLite
and PostgreSQL. Alembic autogenerate works where an Alembic impl exists:
SQLite and PostgreSQL (Alembic's own) and BigQuery (shipped here); Flight SQL
and Snowflake have none.

### Stability

The surface below has been held stable since 0.9.0 (2026-11-08) and changes
only with a major version. It is provisional until the 1.0 criteria below are
met, because it has had little time in other people's hands:

- the `<backend>+adbc://` URL schemes and the query keys documented above;
- the `auth=` modes and `auth_profile=`;
- the config file location, permission rules and `[auth.<name>]` keys;
- the `sqlalchemy_adbc.arrow` helper signatures.

May still change before 1.0: reflection details on the generic `GetObjects`
path (Flight SQL, Snowflake), type mappings for BigQuery ARRAY / STRUCT /
GEOGRAPHY and Snowflake semi-structured values, `ischema_names` contents, and
anything marked server-dependent above.

Deprecated: the pre-0.4 `adbc+<backend>://` URL spellings warn today and are
removed in **1.0.0**.

1.0 ships when all of these hold:

- Done: Flight SQL, PostgreSQL and SQLite are integration-tested in CI, the
  compliance suite is green on SQLite, and Python 3.12–3.14 × SQLAlchemy
  2.0/2.1 are tested.
- Flight SQL is tested against at least two different servers (today: GizmoSQL
  only), and a compliance or behaviour profile for Flight SQL is documented
  (PostgreSQL's is either run or explicitly ruled out in
  [docs/compliance.md](docs/compliance.md)).
- At least 60 days have passed since 0.9.0 (released 2026-11-08, so not
  before 2027-01-07) and at least two further releases have shipped with no
  breaking change to the stable surface (done: 0.9.1 through 0.9.4).
- No open bug is filed against the stable surface, and at least three people
  outside the maintainers have reported running it against their own
  servers (an issue using the server-report template counts).
- The deprecated spellings are removed in the 1.0.0 release itself.

## Development

[CONTRIBUTING.md](CONTRIBUTING.md) covers the test layers, the live-backend
variables, what a change needs before it merges, and how to add a server or
backend. The short version:

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
