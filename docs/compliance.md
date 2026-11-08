# SQLAlchemy dialect compliance

`sqlite+adbc` runs [SQLAlchemy's dialect compliance suite](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst)
on every CI run (`compliance` job). The suite lives under `suite/` and is
never collected by plain `pytest`; run it with:

```bash
pytest suite/ --dburi "sqlite+adbc:///suite_sqlite_adbc.db" -p no:cacheprovider
```

## Result

| Date | Dialect | Driver | SQLAlchemy | Passed | Skipped | Failed |
|---|---|---|---|---:|---:|---:|
| 2026-11-08 | `sqlite+adbc` | adbc-driver-sqlite 1.11 | 2.0.49 | 903 | 446 | 0 |
| 2026-11-08 | `sqlite+adbc` | adbc-driver-sqlite 1.13 | 2.1.8 (CI) | 1022 | 400 | 0 |

Skips are requirement-driven: SQLAlchemy skips every test whose
requirement the dialect declares closed, plus features SQLite itself lacks
(sequences, schemas-as-databases, comments, ...). `suite/requirements.py`
declares the ones specific to this dialect; each is listed below.

## Exclusions

| Requirement | Why | Scope |
|---|---|---|
| `sane_rowcount`, `sane_multi_rowcount` | ADBC cursors report `-1` for the row count of UPDATE/DELETE on the query path; the dialect sets `supports_sane_rowcount = False`. | ADBC connection layer, all backends |
| `implements_get_lastrowid` | ADBC cursors have no `lastrowid`; `inserted_primary_key` comes from implicit RETURNING, which the suite's sibling test covers. | ADBC connection layer |
| `parens_in_union_contained_select_w_limit_offset`, `..._wo_limit_offset` | SQLite rejects parenthesised SELECTs inside UNION; SQLAlchemy marks these as failing on SQLite too. | SQLite |
| `independent_connections` | SQLAlchemy never runs the independent-connection tests on a SQLite driver (file locking); `independent_readonly_connections` stays open. | SQLite |
| `temporary_tables`, `temp_table_reflection` | A temporary table lives in the connection that created it, and the suite reflects from other pooled connections of a file database. SQLAlchemy skips these for file-backed SQLite as well. | SQLite, file database |
| `implicitly_named_constraints` | SQLite does not assign names to unnamed constraints (as in SQLAlchemy's own requirements). | SQLite |
| `duplicate_key_raises_integrity_error` | `adbc-driver-sqlite` before 1.13 surfaces a UNIQUE violation as `InternalError "(unknown error)"`; 1.13 raises `IntegrityError` and the test runs. | driver < 1.13 |

Opened against SuiteRequirements' defaults because SQLite supports them:
`views`, `reflects_pk_names`, and `reflect_table_options` when the installed
SQLAlchemy's `SQLiteDialect` implements `get_table_options` (2.1 and later;
the suite expects `NotImplementedError` otherwise).

## Other backends

PostgreSQL and Flight SQL do not run the suite yet. PostgreSQL would
benefit from the same arrangement as SQLite (SQLAlchemy's `PGDialect` over
the ADBC connection layer), which is a separate decision because the
current dialect carries its own range/hstore and reflection work. Flight
SQL has no SQLAlchemy base dialect and reflects through ADBC `GetObjects`,
so it needs its own requirements profile.
