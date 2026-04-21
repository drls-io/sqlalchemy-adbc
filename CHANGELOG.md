# Changelog

All notable changes to `sqlalchemy-adbc` are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/drls-io/sqlalchemy-adbc/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/drls-io/sqlalchemy-adbc/releases/tag/v0.1.0
