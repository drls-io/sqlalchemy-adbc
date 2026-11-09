# Contributing to sqlalchemy-adbc

Thanks for helping. This guide covers setting up, running the test layers,
what a change needs before it merges, and how releases happen. For what the
package does and which backends it supports, start with the
[README](README.md).

## Set up

```bash
git clone https://github.com/drls-io/sqlalchemy-adbc
cd sqlalchemy-adbc
python3 -m venv .venv && source .venv/bin/activate   # Python 3.12 or newer
pip install -e ".[dev,postgresql,flightsql]"
```

The `dev` extra brings pytest, ruff, mypy, Alembic, the SQLite ADBC driver
and the dependencies of the fake Snowflake endpoint. Add `snowflake`,
`bigquery` or `kerberos` when you work on those paths.

## The gates

Every pull request must pass these, and CI runs them on Python 3.12, 3.13
and 3.14:

```bash
ruff check src/ tests/ && ruff format --check src/ tests/
mypy src tests
pytest tests/ -q
```

`tox` runs the same gates across the interpreters you have installed and
both SQLAlchemy series (`tox -e py312-sa21` for one cell; see `tox.ini`).

## Test layers

**Unit and fake-backend tests** (`pytest tests/`) need nothing but the
install. They cover URL handling, compilation, reflection on SQLite, the
Arrow helpers, and the Snowflake dialect driven through the real
`adbc-driver-snowflake` against the in-process fake endpoint in
`tests/fake_snowflake/`.

**Live backends** are opt-in through environment variables. A test that
needs one skips when its variable is unset, so a plain `pytest` run always
passes without servers.

| Variable             | Backend    | How CI provides it                                                                          |
|----------------------|------------|---------------------------------------------------------------------------------------------|
| `DRLS_PG_URL`        | PostgreSQL | `postgres:16` service container, `postgresql+adbc://test:test@localhost:5432/test`          |
| `DRLS_FLIGHTSQL_URL` | Flight SQL | GizmoSQL service container, `flightsql+adbc://ci:ci-password@localhost:31337?auth=basic&tls=false` |
| `DRLS_SNOWFLAKE_URL` | Snowflake  | not in CI; a real account, e.g. `snowflake+adbc://user@ORG-ACCT/DB/SCHEMA?warehouse=WH&authenticator=snowflake_jwt&privateKey=...` |
| `DRLS_BIGQUERY_URL`  | BigQuery   | not in CI; `bigquery+adbc:///project?dataset=ds&location=US` with Application Default Credentials |

Locally:

```bash
# PostgreSQL
docker run -d --name pg -p 5432:5432 -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16
export DRLS_PG_URL="postgresql+adbc://test:test@localhost:5432/test"

# Flight SQL (GizmoSQL, DuckDB backend)
docker run -d --name gizmosql -p 31337:31337 -e GIZMOSQL_USERNAME=ci \
  -e GIZMOSQL_PASSWORD=ci-password -e TLS_ENABLED=0 -e PRINT_QUERIES=0 \
  gizmodata/gizmosql:v1.42.2
export DRLS_FLIGHTSQL_URL="flightsql+adbc://ci:ci-password@127.0.0.1:31337?auth=basic&tls=false"

pytest tests/ -rs      # -rs lists what still skipped and why
```

Live tests create uniquely named tables and drop them afterwards. Point
them only at disposable databases.

**SQLAlchemy's dialect compliance suite** runs on `sqlite+adbc` in the
`compliance` CI job. It lives in `suite/` and is never collected by plain
`pytest`:

```bash
pytest suite/ --dburi "sqlite+adbc:///suite_sqlite_adbc.db" -p no:cacheprovider
```

Every requirement turned off in `suite/requirements.py` has a one-line
reason there and a row in [docs/compliance.md](docs/compliance.md). Keep
both in step.

## Making a change

- **Test first.** A bug fix comes with a test that failed before the fix;
  a feature comes with tests for its behaviour. Prefer the layer closest
  to the code: a compile-only test for SQL generation, a fake-backend test
  for wire behaviour, a live test only for what the fake cannot model.
- **Keep other backends unchanged.** The dialects share
  `ADBCConnectionMixin` and the generic reflection in `reflection.py`. A
  change there must keep `tests/test_flightsql_*`, `test_postgresql_*`,
  `test_snowflake_*`, `test_bigquery*` and `test_sqlite*` green.
- **Document what users will notice.** Add an entry under `## [Unreleased]`
  in `CHANGELOG.md` (`Added`, `Changed`, `Fixed`, `Removed`, `Deprecated`),
  written for someone upgrading. Update the README when a URL key, auth
  mode, limitation or supported server changes.
- **Stable surface.** URL schemes and documented query keys, `auth=` modes
  and `auth_profile=`, the config file format and the Arrow helper
  signatures are stable (see the README's Stability section). Changing them
  needs a deprecation period and a major version; open an issue first.
- **Commits and PRs.** Conventional-commit style subjects
  (`fix(postgresql): ...`, `feat(bigquery): ...`, `docs: ...`). The PR
  description says what changed, why, and how it was verified, including
  which live backends were exercised. Your authorship stays on your
  commits; if a maintainer adapts your branch, the adapted commits keep you
  as author and the CHANGELOG credits you.

## Adding support for a server or backend

For a new Flight SQL server, run the Flight SQL suite against it with
`DRLS_FLIGHTSQL_URL` and report what differs: the suite documents
GizmoSQL-specific expectations in `tests/test_flightsql_e2e.py`. Metadata
differences (table type spellings, missing `xdbc_type_name`) belong in
`reflection.py` with a unit test built from the server's `GetObjects`
output; see `tests/test_flightsql_metadata.py` for the shape.

A new backend is a module under `src/sqlalchemy_adbc/` subclassing
`ADBCDialect` (generic reflection) or, when SQLAlchemy ships a dialect for
that database, `ADBCConnectionMixin` over that dialect as `sqlite.py` does.
Register it under `[project.entry-points."sqlalchemy.dialects"]` as
`<backend>.adbc`, add compile-only tests and a live test module gated on a
`DRLS_<BACKEND>_URL` variable, and describe its URL form and limitations in
the README's "Other backends" section. Open an issue before starting so the
scope is agreed.

## Reporting a problem

Use the issue templates. For a bug, include the URL form with secrets
removed, the server or driver and its version, the SQLAlchemy version, and
the smallest statement or reflection call that misbehaves. Output of
`pip list | grep -i -E "sqlalchemy|adbc|arrow"` saves a round trip.

## Releases

Maintainers cut releases from `main`; the steps are in the README's
"Releasing" section. Publishing is automated through PyPI trusted
publishing on a `vX.Y.Z` tag, and the GitHub Release is generated from that
version's CHANGELOG section, so a complete `[Unreleased]` section is what
makes a release note good.

## Code of conduct

Be kind and specific. Review the code, not the person; assume the author
had a reason and ask what it was.
