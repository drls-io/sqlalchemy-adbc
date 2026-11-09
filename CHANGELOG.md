# Changelog

All notable changes to `sqlalchemy-adbc` are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Comparison with alternatives.** `docs/alternatives.md` sets this package
  beside the other Flight SQL and ADBC options, states what each project
  documents, and says where another choice fits better.

### Changed

- **1.0 criteria record progress.** The release-count criterion is met
  (0.9.1 through 0.9.4 shipped with no breaking change); the README now says
  so and states the date the 60-day clock runs out (2027-01-07).

### Fixed

- **PostgreSQL raw SQL decodes range, multirange and hstore.**
  `pd.read_sql("SELECT period FROM t", engine)`, Superset's SQL Lab and other
  plain-SQL readers now get `Range` / `MultiRange` / `dict` instead of bytes,
  the same objects the typed path returns. The cursor decodes PostgreSQL's
  binary send format using the driver's per-column type metadata; hstore's
  OID is looked up once per connection. The Arrow helpers still return these
  columns as the driver's opaque binary. (#59)

## [0.9.4] — 2026-11-08

Documentation release: a security policy, the Python floor stated next to
the install commands, and a 1.0 criterion that names the server-report
issue template.

### Added

- **Security policy.** `SECURITY.md` explains how to report a vulnerability
  privately (GitHub private vulnerability reporting), which versions get
  fixes, and what counts as in scope for a package that handles OAuth2
  secrets, Kerberos tickets and a trusted config file.

### Changed

- **README states the Python floor next to the install commands**, including
  that `pip` on an older interpreter silently installs an old release.
- **1.0 criteria no longer mention Discussions**, which are disabled; the
  server-report issue template is what counts as an outside report.

## [0.9.3] — 2026-11-08

Documentation release: a contributor guide with issue templates, package
metadata that is easier to find on PyPI, and 1.0 criteria that require a
soak period and reports from outside users.

### Added

- **Contributor guide.** `CONTRIBUTING.md` (setup, gates, test layers and the
  live-backend variables, change checklist, adding a server or backend,
  releases) and issue templates for bug reports and server reports.

### Changed

- **Package metadata for discoverability.** The summary names Flight SQL and
  ADBC together, the keywords cover the common spellings (`flight-sql`,
  `flightsql`, `arrow-flight-sql`, `sqlalchemy-dialect`, `dbapi`, ...), and
  the project gains classifiers plus Documentation and Changelog URLs.
- **1.0 criteria now include a soak period and outside usage.** The README
  gates 1.0 on 60 days since 0.9.0, two further non-breaking releases, no open
  bug on the stable surface, three outside reports, and a second Flight SQL
  server in CI. The stable surface is marked provisional until then.

## [0.9.2] — 2026-11-08

Documentation and CI release: the README now leads with Flight SQL and
enterprise authentication and carries a stability policy; CI runs on
Ubuntu 26.04 with the Node 24 action majors.

### Changed

- **README leads with Flight SQL and authentication.** The package is
  positioned as SQLAlchemy access to Flight SQL servers with OAuth2,
  Kerberos/Negotiate, bearer and config profiles; PostgreSQL, SQLite,
  Snowflake and BigQuery move to an "Other backends" section with their
  limitations. A "Status" section lists what CI runs per backend, and a new
  "Stability" section states the stable surface (URL schemes and query keys,
  `auth=` modes, config file format, Arrow helper signatures), what may still
  change before 1.0, and the 1.0 criteria. `pyproject.toml` description and
  keywords follow.

### Deprecated

- **`adbc+<backend>://` URL spellings are removed in 1.0.0.** The warning now
  names the version. Use `<backend>+adbc://`.

### Internal

- CI and publish workflows run on `ubuntu-26.04` and `actions/checkout@v7`,
  `setup-python@v7`, `upload-artifact@v7`, `download-artifact@v8` (Node 24).

## [0.9.1] — 2026-11-08

Thanks to Albert T. Wong ([@alberttwong](https://github.com/alberttwong)) for
finding and fixing the three items below while putting the dialect in front of
Superset with Redis's Arrow Flight SQL server (#17, #20).

### Fixed

- **Flight SQL reflection with upper-case table types and name-less column
  types.** Some servers report `TABLE` / `VIEW` and fill in only the numeric
  `xdbc_data_type`; the Inspector returned no tables and every column was
  `NullType`. Table-type matching is case-insensitive everywhere (including
  `find_table`'s index skip), and columns without a recognised
  `xdbc_type_name` fall back to the XDBC/ODBC type code (DATETIME and INTERVAL
  codes stay `NullType` because they need subtype metadata).
- **PostgreSQL IPv6 hosts.** SQLAlchemy strips the brackets from `[::1]`, so
  the libpq URI came out as `postgresql://user@::1:5432/db`. Brackets are
  restored. Database names with literal percent sequences (`my%2520db`,
  `my%2Fdb`) are pinned by tests on both SQLAlchemy lines (#19).

### Changed

- **PostgreSQL extra requires `adbc-driver-postgresql>=1.13.0`.** 1.0.0 fails
  the typed read/write tests with "Prepared statements returning result sets
  are not implemented"; 1.13.0 is the version those tests were run on (#18).

## [0.9.0] — 2026-11-08

`sqlite+adbc` is now SQLAlchemy's own SQLite dialect over an ADBC connection
and passes SQLAlchemy's dialect compliance suite, which runs in CI from here
on. Dates, times, binary and decimal values round-trip as Python values,
RETURNING and multi-row inserts work, attached schemas reflect, and NOT NULL
reflects on every driver version.

### Added

- **BigQuery dialect speaks GoogleSQL.** Identifiers quote with backticks
  (`"..."` is a string literal in BigQuery, so any reserved-word or mixed-case
  name used to be a syntax error), with backslash escaping of backticks inside
  names and of quotes inside rendered string literals; DDL emits `STRING(n)`, `INT64`, `FLOAT64`,
  `BOOL`, `NUMERIC(p, s)`, `BYTES`, `DATETIME`, `TIMESTAMP`, `JSON` instead of
  `VARCHAR` / `BLOB` / `FLOAT`, which BigQuery rejects; primary and foreign keys
  render `NOT ENFORCED` (the only kind BigQuery has) and unique constraints are
  omitted. Native booleans (`WHERE flag`, not `flag = 1`), exact `NUMERIC`
  decimals, multi-row `VALUES` inserts via insertmanyvalues, and `LargeBinary`
  binds (the driver module lacks `Binary`). `DateTime` binds render
  `CAST(? AS DATETIME)`: the driver sends a naive datetime as a `TIMESTAMP`
  parameter, which BigQuery refuses to store in a `DATETIME` column.
- **BigQuery reflection types.** `STRING(n)`, `BYTES`, `BIGNUMERIC`,
  `NUMERIC(p, s)` with precision and scale, `INTEGER` → `BigInteger`, and
  `TIMESTAMP` → `TIMESTAMP(timezone=True)` (a UTC instant; `DATETIME` stays the
  naive `DateTime`). Previously `STRING` and `BYTES` reflected as `NullType`.
  ARRAY, STRUCT and GEOGRAPHY still reflect as `NullType`.
- **Alembic on BigQuery.** Alembic ships no impl for the `bigquery` dialect
  name, so `MigrationContext.configure` raised `KeyError`; importing
  `sqlalchemy_adbc.bigquery` now registers one (verified: autogenerate against
  a live dataset with `compare_type=True` reports only the real diff).
- **Live BigQuery tests** (`tests/test_bigquery_e2e.py`), run when
  `DRLS_BIGQUERY_URL` is set: `create_all`, Core insert/select/update/delete
  round trips for every scalar type, boolean filters, reserved-word columns,
  `IN` lists, LIMIT/OFFSET and reflection. `executemany` runs one statement per
  parameter set because the driver's own `executemany` returns before every
  job has finished.

### Fixed

- **BigQuery URL options.** `?dataset=`, `?location=` and `?credentials_path=`
  were passed to the driver verbatim, which rejected them ("Unknown database
  option 'dataset'"), so the documented URL never connected. They now map to
  `adbc.bigquery.sql.dataset_id`, `adbc.bigquery.sql.location` and the
  JSON-credential-file auth type; any `adbc.bigquery.sql.*` key still passes
  through and wins. Verified against a live project.
### Removed

- **The `[proxy]` config section.** It exported `HTTP_PROXY` / `HTTPS_PROXY` /
  `NO_PROXY` into the environment on the first connect, but the Go-based
  drivers (Flight SQL, Snowflake, BigQuery) only read the environment the
  process was started with, so it never routed them (verified on macOS and
  Linux, see 0.6.0). The section is now ignored with a `DeprecationWarning`;
  set the variables in the shell or launcher. `sqlalchemy_adbc.proxy` is gone
  and `Config` has no `proxy` field.
### Added

- **SQLAlchemy dialect compliance suite in CI** — `suite/` runs SQLAlchemy's
  own dialect test suite against `sqlite+adbc` on every change (SQLAlchemy
  2.0: 903 passed, 446 skipped; 2.1: 1022 passed, 400 skipped; 0 failed on
  both); `docs/compliance.md` lists every exclusion with its reason.

### Changed

- **`sqlite+adbc` is now SQLAlchemy's SQLite dialect over an ADBC
  connection.** SQL compilation, DDL, types and reflection come from
  SQLAlchemy's `SQLiteDialect`; only the wire path is ADBC. Visible effects:
  `DateTime`, `Date`, `Time`, `LargeBinary` and `Numeric` round-trip as Python
  values (dates and times were ISO strings, `Time` binds failed), `RETURNING`
  and multi-row `INSERT ... VALUES` work, `inserted_primary_key` works
  without `lastrowid`, attached schemas are reflected, an unknown table
  raises `NoSuchTableError`, and `:memory:` URLs share one connection
  (`SingletonThreadPool`) as with pysqlite.
- `ADBCDialect` is split into `ADBCConnectionMixin` (connection layer) and
  the generic `GetObjects` reflection on top; other dialects are unchanged.

### Fixed

- **SQLite NOT NULL reflected as nullable on drivers before 1.13** (#13):
  reflection now reads SQLite's own pragmas, so every driver version reports
  it.
- An INSERT or DDL statement no longer reports `returns_rows`; the ADBC
  cursor's empty description is normalised to `None`.
- `executemany` with all-empty parameter sets (`INSERT ... DEFAULT VALUES`)
  inserts one row per set; the driver had bound an empty batch and inserted
  none.

## [0.8.0] — 2026-11-08

The Arrow-native path: `fetch_arrow_table`, `open_arrow_batch_reader` and
`ingest_arrow` run on the ADBC cursor behind a SQLAlchemy connection, inside
its transaction, so Arrow, Polars and pandas users skip the row round trip
(about 7× faster than the row path on the bundled benchmark).

### Added

- **Arrow-native helpers** — `sqlalchemy_adbc.arrow` (also exported from the
  package): `fetch_arrow_table(conn, statement, parameters)`,
  `open_arrow_batch_reader(...)` (streaming context manager) and
  `ingest_arrow(conn, table, data, mode=...)` run on the ADBC cursor behind a
  SQLAlchemy `Connection`, inside its transaction, with SQLAlchemy statements
  compiled by the dialect (bind processors included) or plain SQL strings.
  Tested on SQLite, PostgreSQL, Flight SQL and the fake Snowflake endpoint;
  `benchmarks/arrow_vs_rows.py` measures ~7× over the row path on SQLite.
- `sqlalchemy_adbc.__version__` now reports the installed package version
  (it was a stale constant).

## [0.7.0] — 2026-11-08

Flight SQL, the backend this package is about, is now integration-tested in
CI against a real server (GizmoSQL), so three of the five drivers run end to
end on every change. Python 3.11 support is dropped: the floor is 3.12.

### Removed

- **Python 3.11 support.** `requires-python` is now `>=3.12` and CI, tox,
  ruff and mypy all target 3.12–3.14. 3.11 is supported upstream until
  October 2027; dropping it early keeps a single toolchain target (mypy
  already needed 3.12 to parse numpy's stubs). Install 0.6.x on 3.11.

### Added

- **Flight SQL integration tests in CI** — a GizmoSQL (DuckDB backend) service
  container runs `tests/test_flightsql_e2e.py` in every cell: basic and bearer
  auth, failure modes, type round trips, parameters, transactions, reflection
  and `pandas.read_sql`. The suite runs against any Flight SQL server via
  `DRLS_FLIGHTSQL_URL`. With Snowflake covered by the fake endpoint, only
  BigQuery remains URL-tests-only (#12).

### Changed

- An OAuth2 token-endpoint failure now surfaces when a connection is opened
  (`engine.connect()`, as `sqlalchemy.exc.ArgumentError`) rather than at
  `create_engine()`, a consequence of 0.6.0's per-connection minting.
- Dev extra: `adbc-driver-flightsql` and `pandas` join it; the
  `adbc-driver-snowflake` floor is 1.11, the wire details the fake endpoint
  encodes.
- The fake Snowflake endpoint answers an unexpected error during login with a
  Snowflake error instead of dropping the connection, types `SELECT *` over a
  join positionally in `FROM` order, and reflects foreign keys declared
  without a column list (self-referential, mutual, or to another table).

## [0.6.0] — 2026-11-08

Snowflake is now tested end to end in CI through a fake endpoint that drives the
real driver, which surfaced and fixed five dialect bugs, including OAuth2
tokens that were never refreshed and a Snowflake OAuth2 path that could not
connect at all. Python 3.10 support is dropped and 3.14 joins the matrix.

### Removed

- **Python 3.10 support.** 3.10 reached end of life in October 2026, and
  SQLAlchemy 2.1 and ADBC 1.13 already require 3.11. `requires-python` is now
  `>=3.11`, the `tomli` dependency is gone (`tomllib` is in the standard
  library), and CI and tox run 3.11–3.14. Python 3.14 is new in the matrix.

### Added

- **Fake Snowflake endpoint in CI** — `tests/fake_snowflake/` speaks
  Snowflake's wire protocol to the real `adbc-driver-snowflake`, backed by
  SQLite, so every test run covers the Snowflake dialect end to end without
  credentials: login and OAuth2 token minting, CRUD and type round trips,
  `executemany`, reflection of tables, views and constraints, error
  propagation, proxy routing and the bindings on the wire. The same behaviour
  suite runs against a real account with `DRLS_SNOWFLAKE_URL`.

### Fixed

- **OAuth2 and Negotiate credentials are now minted per connection.**
  SQLAlchemy evaluates connect arguments once per engine, so the token minted
  there in 0.5.0 was reused for the engine's lifetime and never refreshed.
- **Snowflake `auth=oauth2` connected nowhere**: the driver validates the DSN
  before its auth options apply and demanded a password. The DSN now declares
  `authenticator=oauth`.
- **Snowflake `Inspector.get_table_names()` / `get_view_names()` returned
  nothing** — reflection only recognised SQLite's `table`/`view` spellings,
  not Snowflake's and BigQuery's `BASE TABLE`/`VIEW`.
- **Snowflake booleans rendered as `0`/`1`**, which Snowflake rejects in
  `WHERE x IS 0`; the dialect now declares native booleans.
- **Reflected Snowflake columns came back in arbitrary order**; they are now
  sorted by ordinal position.

### Changed

- **`[proxy]` config export does not reach the Go drivers.** The Go runtime
  snapshots the environment at process start (verified on macOS and Linux),
  so Flight SQL, Snowflake and BigQuery only honour `HTTP_PROXY` /
  `HTTPS_PROXY` / `NO_PROXY` set by the shell or launcher. The export stays
  for the package's own HTTP calls (OAuth2 token requests) and now logs a
  WARNING naming the file. README updated accordingly.
- `tests/test_snowflake_live.py` is now `tests/test_snowflake_e2e.py`,
  parametrised over the fake endpoint and a live account.
- `adbc-driver-snowflake` joins the `dev` extra.
- CI's PostgreSQL integration tests now use the documented
  `postgresql+adbc://` URL spelling; the deprecated `adbc+postgresql://` form
  stays covered by `tests/test_url_schemes.py`.
- README: stale hard-coded test count replaced by the CI badge; the release
  procedure now matches `publish.yml` (changelog section, tag on `main`,
  GitHub Release created by the workflow). The 0.5.0 changelog entry named a
  config file `sqlalchemy-adbc.toml` that never existed; corrected.

## [0.5.0] — 2026-11-07

Proxy and authentication support (basic, Kerberos/Negotiate, OAuth2), PostgreSQL
range, multirange and hstore types, a Snowflake type registry and binding fixes,
and large fetch and reflection speedups.

### Added

- **Proxy settings from a local config file** — `HTTP_PROXY` /
  `HTTPS_PROXY` / `NO_PROXY` are honoured as before; when unset, a
  `[proxy]` section in the config file (`$SQLALCHEMY_ADBC_CONFIG` or
  `~/.config/sqlalchemy-adbc/config.toml`) is exported into the process
  environment the Go-based drivers read, never overriding what you set.
- **Authentication modes** — `auth=bearer|basic|negotiate|oauth2` and
  `auth_profile=<name>` on Flight SQL (basic Handshake, Kerberos/SPNEGO via
  the new `kerberos` extra, native driver OAuth2 client credentials) and
  `auth=oauth2` on Snowflake (token minted per connection). Profiles live
  in the config file (`$SQLALCHEMY_ADBC_CONFIG` or
  `~/.config/sqlalchemy-adbc/config.toml`, owner-only), with `env:NAME`
  references for secrets. URL authors cannot redirect a profile's secret,
  use `env:` references, or pass a client secret in the query string.
  `tomli` is a dependency on Python 3.10.
- **PostgreSQL range, multirange and hstore types** — `INT4RANGE`,
  `INT8RANGE`, `NUMRANGE`, `TSRANGE`, `TSTZRANGE`, `DATERANGE`, the six
  `*MULTIRANGE` types and `HSTORE` in `sqlalchemy_adbc.postgresql_types`.
  Values are SQLAlchemy's own `Range` / `MultiRange` / `dict`, so code
  written for psycopg works unchanged. ADBC returns these columns as PG
  binary and rejects text parameters, so the decorators cast to the PG
  type on bind and to text on read. Range operators work with range and
  scalar operands (`period.contains(now)`); reflection returns the
  decorators; `create_all` emits the real type names. Unbounded and
  `±infinity` bounds read as `None`.
- **Snowflake parameter binding for every Python type** — the ADBC
  Snowflake driver binds only `int`/`str`/`float`/`bool` and rejects a
  parameter that is `None` in every row. The dialect now renders `Decimal`,
  `date`, `datetime`, `time` and `bytes` as the strings Snowflake converts
  implicitly, and hands the cursor an explicitly typed Arrow batch so
  all-null columns bind as NULL. ORM inserts with unset attributes and
  `insert().values(d=date(...))` now work on Snowflake.
  ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))
- **Snowflake type registry** — `NUMBER`, `TIMESTAMP_NTZ`, `TIMESTAMP_TZ`,
  `TIMESTAMP_LTZ`, `VARIANT`, `OBJECT` and `ARRAY` columns reflected as
  `NullType`; they now reflect to typed SQLAlchemy types. `NUMBER(p,0)`
  reads as `int`, `NUMBER(p,s)` as `Decimal`, `TEXT(n)` as `String(n)`,
  `TIMESTAMP_TZ`/`_LTZ` as `DateTime(timezone=True)`. `VARIANT`/`OBJECT`/
  `ARRAY` decode JSON on read and bind through `PARSE_JSON`. Registries can
  now carry the driver's column size and decimal digits into the type.
  Opt-in live tests gated on `DRLS_SNOWFLAKE_URL`.
  ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))
### Changed

- **Reflection fetches the catalog once per Inspector** — every reflection
  method re-fetched `adbc_get_objects`; one table autoload made five or six
  round-trips (40 s on Snowflake). The tree is now cached through
  SQLAlchemy's `info_cache`, so an autoload costs one fetch per table
  (10 s on Snowflake, the rest being the driver's single round-trip).
  ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))
- **Column-wise result conversion** — the dialect now wraps the ADBC DBAPI
  cursor and converts each Arrow record batch column-wise instead of cell
  by cell. `fetchall()` on 1M rows × 4 columns drops from 4.6 s to 0.5 s,
  `pandas.read_sql` from 4.9 s to 0.9 s, and
  `polars.read_database(connection=engine)` from 4.8 s to 0.79 s. Values
  are identical. `fetchmany`
  still streams one batch at a time. Every SQLAlchemy consumer (Superset,
  pandas, marimo, Polars via the engine) benefits without changes.
  `cursor.fetch_arrow_table()` and the other Arrow extensions still pass
  through. ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))

### Fixed

- **PostgreSQL DDL rendering** — the dialect now uses SQLAlchemy's PG
  type compiler. `create_all` and `CAST` targets render `JSONB`,
  `INT4RANGE`, `HSTORE`, and `DateTime` renders `TIMESTAMP WITHOUT TIME
  ZONE` instead of `DATETIME`, which PG rejected.
  ([#11](https://github.com/drls-io/sqlalchemy-adbc/issues/11))

## [0.4.0] — 2026-11-07

URL schemes now follow SQLAlchemy's `<backend>+adbc://` convention,
plus compatibility fixes for SQLAlchemy 2.1 and ADBC 1.13. Existing
`adbc+<backend>://` URLs keep working and emit a `DeprecationWarning`.

### Changed

- **URL schemes are now `<backend>+adbc://`** — `postgresql+adbc://`,
  `sqlite+adbc://`, `flightsql+adbc://`, `snowflake+adbc://`,
  `bigquery+adbc://`. This follows SQLAlchemy's `dialect+driver`
  convention: ADBC is the driver. `URL.get_backend_name()` now agrees
  with `dialect.name`, so tools that pick behavior from the URL (for
  example Superset's engine specs) see the real backend.
  ([#15](https://github.com/drls-io/sqlalchemy-adbc/issues/15))

### Deprecated

- **`adbc+<backend>://` URL spellings** — still registered and
  functional, but `create_engine` emits a `DeprecationWarning`. They
  will be removed in 1.0.

### Fixed

- **PostgreSQL database names with SQLAlchemy 2.1** — SQLAlchemy 2.1
  percent-decodes `URL.database`, so the libpq URI lost its escapes
  (`my%20db` became `my db`). The dialect now detects the parser's
  behavior and quotes the database path exactly once on both 2.0 and
  2.1. ([#19](https://github.com/drls-io/sqlalchemy-adbc/issues/19))
- **CI on ADBC 1.13** — `adbc_driver_sqlite` 1.13.0 reports NOT NULL
  correctly, which turned the strict xfail for column nullability into
  a hard failure. The xfail now applies only to drivers older than
  1.13. ([#13](https://github.com/drls-io/sqlalchemy-adbc/issues/13))

## [0.3.0] — 2026-04-21

Largest release so far: table reflection, Alembic autogenerate,
PostgreSQL type decorators, and breaking fixes to dialect naming
and Python version support. Not backwards-compatible for callers
that read `engine.dialect.name` or run on Python 3.9.

### Added

- **Table reflection via `adbc_get_objects`** — the full SQLAlchemy
  Inspector contract now works for any ADBC driver that implements
  the GetObjects metadata call. Covers `get_columns`,
  `get_pk_constraint`, `get_foreign_keys`, `get_unique_constraints`,
  `get_table_names`, `get_view_names`, `get_schema_names`, and
  `has_table`. `MetaData.reflect(bind=engine)` works end-to-end.
  ([#5](https://github.com/drls-io/sqlalchemy-adbc/pull/5),
  closes [#1](https://github.com/drls-io/sqlalchemy-adbc/issues/1))
- **Native `get_indexes` for SQLite and PostgreSQL** — ADBC's
  GetObjects spec doesn't carry per-index column lists, so we
  query the backend's native catalog (PRAGMA `index_list` /
  `index_info` for SQLite; `pg_index` / `pg_class` / `pg_attribute`
  with `LATERAL unnest WITH ORDINALITY` for PG). Identifiers are
  properly escaped — PRAGMA can't be parameterized.
  ([#6](https://github.com/drls-io/sqlalchemy-adbc/pull/6))
- **Alembic autogenerate support** — fallout from the dialect-name
  fix below. Alembic's DDL impl registry now finds the correct
  per-backend impl (`SQLiteImpl`, `PostgresqlImpl`) for free.
  Covers create/drop table, add/drop column, and roundtrip
  idempotency (apply autogenerate output → re-run → no-op).
  ([#7](https://github.com/drls-io/sqlalchemy-adbc/pull/7),
  closes [#2](https://github.com/drls-io/sqlalchemy-adbc/issues/2))
- **PostgreSQL type decorators** — `JSONB`, `JSON`, `UUID`, `INET`,
  `CIDR`, `MACADDR`, `MACADDR8`. Round-trip PG-specific types
  through ADBC as the Python objects SQLAlchemy users expect:
  JSONB → dict/list, UUID → `uuid.UUID` (or `str` with
  `as_uuid=False`), INET/CIDR/MAC as strings. PG column reflection
  now queries `information_schema.columns.udt_name` directly
  (ADBC's `xdbc_type_name` is unreliable for PG-extension types),
  so `MetaData.reflect()` surfaces these as TypeDecorators, not
  `NullType`. ([#8](https://github.com/drls-io/sqlalchemy-adbc/pull/8),
  closes [#3](https://github.com/drls-io/sqlalchemy-adbc/issues/3))
- **CI Postgres service** — integration tests (`test_postgresql_types.py`)
  now run against a real `postgres:16` container, gated by
  `DRLS_PG_URL`. Local `pytest` runs without Postgres continue to
  skip these cleanly.

### Changed

- **BREAKING — dialect `name` and `driver` attributes flipped** to
  match SQLAlchemy's convention: `name` is the backend identity
  (`sqlite`, `postgresql`, `flightsql`, `snowflake`, `bigquery`),
  `driver` is the wire path (`adbc`). Previously every dialect
  reported `name="adbc"`, which caused Alembic's DDL-impl lookup
  to `KeyError`. URL forms (`adbc+sqlite://`, etc.) are unchanged.
  Callers inspecting `engine.dialect.name` must update expectations.
- **BREAKING — dropped Python 3.9.** Python 3.9 reached end-of-life
  in October 2025. `requires-python` is now `>=3.10`; the CI matrix
  covers 3.10–3.13.
- **Development Status classifier: Alpha → Beta.** Matches the
  README's scope statement — core functionality is stable enough
  for real use, but a few months of external feedback still needed
  before 1.0.

### Fixed

- **PG `paramstyle` — `numeric_dollar`.** Forces SQLAlchemy to emit
  `$1, $2, ...` (libpq's native wire form) instead of `:name` binds
  via ADBC's `adbc.statement.bind_by_name` (which libpq rejects
  with `NOT_IMPLEMENTED`) or `:1, :2, ...` (which PG's parser
  rejects with `syntax error at or near ":"`). Set via
  `__init__(kwargs.setdefault(...))` because `DefaultDialect.__init__`
  clobbers the class-level attribute from `dbapi.paramstyle`.
- `get_columns` now honors SQLAlchemy's `ReflectedColumn` shape
  ([#5](https://github.com/drls-io/sqlalchemy-adbc/pull/5))
- Mypy is gated in CI with proper `sqlalchemy.engine.interfaces`
  return types
- `cache_ok = True` declared explicitly on `JSON` and `CIDR`
  subclasses — the attribute does not inherit across `TypeDecorator`
  subclasses, so the absence silenced statement caching and
  emitted SAWarning on every engine.

## [0.2.0] — 2026-04-19

Security + correctness release. Not backwards-compatible for
Flight SQL users relying on the plaintext-by-default behavior.

### Changed

- **BREAKING — Flight SQL TLS is now default-on** (`tls=true`).
  Send a bearer token over plaintext gRPC was an unmissable
  credential-leak shape. Local/dev users on plaintext listeners
  must now opt out with `?tls=false`. Default port auto-picks
  443 for TLS, 50051 for plaintext (Arrow Flight convention).

### Fixed

- **URL → DSN special-character escaping** for PostgreSQL and
  Snowflake. A password containing `@`, `:`, `/`, `#`, `%` was
  previously concatenated into the DSN unquoted and would silently
  re-route libpq to the wrong host. Username/password now routed
  through `urllib.parse.quote`; database path stays as-is since
  SQLAlchemy's URL parser leaves it percent-encoded.
- mypy errors in the Flight SQL dialect around `URL.query`
  tuple vs str handling. A `_scalar()` helper normalizes repeated
  keys to their last value (HTTP/shell convention).
- Formatting drift on three files.

### Added

- CI gates `mypy src tests` alongside the existing ruff check.
- Extensive URL-parse test coverage: 13 PG, 9 Snowflake, 5 BigQuery
  tests covering special-char escaping, no-credentials paths, and
  edge cases. Total suite: 51 (up from 16).
- Documentation of known gaps in the README (table reflection,
  Alembic autogenerate, PG-specific types) with links to tracking
  issues.

## [0.1.0] — 2026-04-18

Initial public release.

### Added

- Generic `ADBCDialect` base class with per-driver subclasses for
  Flight SQL, SQLite, PostgreSQL, Snowflake, and BigQuery
- URL → `db_kwargs` translation per driver
- Entry-point registration under `sqlalchemy.dialects` so any
  `adbc+<driver>://` URL resolves automatically
- PyPI trusted publishing via OIDC

[Unreleased]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.9.4...HEAD
[0.9.4]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/drls-io/sqlalchemy-adbc/releases/tag/v0.1.0
