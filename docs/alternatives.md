# Alternatives

How `sqlalchemy-adbc` compares with the other ways to reach a Flight SQL
server or an ADBC driver from Python, and when another choice is the better
one.

Facts about other projects come from their own PyPI or GitHub pages as read
on 2026-11-09. "Not documented" means the page does not say, not that the
feature is missing; check the project before relying on a cell. If something
here is out of date or wrong, open an issue and it will be corrected.

## Flight SQL

| | sqlalchemy-adbc | sqlalchemy-gizmosql-adbc-dialect | flightsql-dbapi | ADBC Flight SQL driver, no SQLAlchemy |
|---|---|---|---|---|
| Status | Beta (current version on the PyPI badge in the README) | 0.0.27, released 2026-05-10 | repository archived 2026-03-31 | maintained by the Apache Arrow ADBC project |
| Servers it targets | any Flight SQL server over ADBC; tested against GizmoSQL (DuckDB backend) in CI, plus one outside report against Redis's Flight SQL server | GizmoSQL with the DuckDB backend (its page says the SQLite backend does not work) | primary dialect targets DataFusion; a base class for others | any |
| Transport | ADBC | ADBC | its own Flight client; ADBC not mentioned | ADBC |
| SQL generation | generic; no server-specific compiler | DuckDB dialect | DataFusion dialect | none; you write the SQL |
| Authentication documented | bearer, basic (Handshake), Kerberos/Negotiate, OAuth2 client credentials, config-file profiles | user and password in the URL | basic (Handshake to bearer), bearer token | whatever driver options you set |
| SQLAlchemy versions | 2.0 and 2.1 (matrix run through `tox`) | not documented | not stated; examples use 1.x-era APIs | not applicable |
| Python | 3.12 or newer | 3.10 or newer | not stated | depends on the driver |
| Reflection | through ADBC `GetObjects` | `MetaData.reflect` shown in its example | table, column and schema names through Flight SQL calls | `adbc_get_objects` |
| Arrow results | `fetch_arrow_table`, a streaming reader and `ingest_arrow`, inside the SQLAlchemy transaction | not documented | not documented | native |
| Tests described | Flight SQL suite against GizmoSQL in CI | not described on its PyPI page | not described on its page | not applicable |
| Tools exercised in CI | pandas; see note | not documented; a separate Superset package exists | states an aim of easing Superset use | not applicable |

Note on tools: CI runs the pandas `read_sql` smoke test. The Polars test
skips unless Polars is installed, and CI does not install it. marimo and
Superset are not tested in CI; one outside user ran 0.9.1 in Superset against
Redis's Flight SQL server (see the 0.9.1 notes in the changelog).

### When another choice fits better

- **Your server is GizmoSQL on DuckDB** and you want DuckDB-specific SQL
  generation, or a package built for Superset on that server: use
  `sqlalchemy-gizmosql-adbc-dialect`. `sqlalchemy-adbc` connects to the same
  server through `flightsql+adbc://` but generates generic SQL.
- **You are on Python 3.10 or 3.11**: this package needs 3.12. Releases up to
  0.6.x supported 3.11, and 0.5.x supported 3.10, without later fixes.
- **You do not need SQLAlchemy**: call the ADBC driver directly, or hand its
  connection to a tool that accepts one, such as Polars' `read_database`.
  A dialect only adds value where SQLAlchemy is required: Alembic, the ORM,
  Superset, pandas' `read_sql` on a non-SQLite database, marimo SQL cells.
- **Your server vendor ships its own dialect**: Denodo and IOMETE publish
  SQLAlchemy dialects built on the ADBC Flight SQL driver, and KurrentDB
  publishes a SQLAlchemy repository. Prefer the vendor's dialect if the vendor
  supports your server version. Those dialects work around that vendor's
  quirks; this package has no per-server code, though it tolerates the
  metadata differences it has met so far (see the 0.9.1 notes in the
  changelog).
- **You need `flightsql-dbapi`'s DataFusion dialect**: it is archived and its
  own README warns that functionality may not be maintained.

### When this package fits

- You need OAuth2, Kerberos or bearer authentication with secrets kept in a
  config profile and out of URLs.
- You want Arrow results and bulk ingest inside the same pooled SQLAlchemy
  connection and transaction.
- Your server is not GizmoSQL, or you run more than one Flight SQL server and
  want one dialect for all of them. Only GizmoSQL is tested in CI so far; a
  server report helps (see the issue templates).

## PostgreSQL

| | sqlalchemy-adbc | pgarrow | psycopg (native) |
|---|---|---|---|
| Dialect name | `postgresql+adbc` | `postgresql+pgarrow` | `postgresql+psycopg` |
| Status | Beta | 0.0.9, released 2025-12-07 | mature |
| Python | 3.12 or newer | 3.10 or newer | broad |
| ADBC driver | `adbc-driver-postgresql` 1.13.0 or newer | 1.9.0 or newer | not used |
| Types | JSONB, JSON, UUID, INET, CIDR, MACADDR, all range and multirange types, hstore | no type table documented | the driver's own |
| Reflection | `pg_catalog` | not documented | native |
| Arrow results | helpers inside the SQLAlchemy transaction | cursor from `driver_connection` | not applicable |

Range, multirange and hstore values decode to SQLAlchemy's `Range` /
`MultiRange` / `dict` on both the typed path and plain SQL
(`pd.read_sql("SELECT period FROM t")`). The Arrow helpers return them as the
driver's opaque binary, since Arrow has no range type. If you do not need
Arrow-native transfer, psycopg is the safer default.

## Other backends

- **SQLite**: SQLAlchemy's built-in driver is the default. This package passes
  SQLAlchemy's dialect compliance suite on SQLite with documented exclusions
  ([compliance.md](compliance.md)); there is little reason to switch unless
  you want the ADBC path.
- **Snowflake and BigQuery**: native dialects exist (`snowflake-sqlalchemy`,
  `sqlalchemy-bigquery`) and are the safer choice for ordinary use. CI covers
  Snowflake against a fake endpoint and BigQuery with compile-only tests, so
  choose this package there only when you specifically want ADBC and Arrow.
