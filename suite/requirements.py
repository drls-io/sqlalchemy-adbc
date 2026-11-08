"""What sqlalchemy-adbc dialects do not support, as SQLAlchemy's suite asks.

Every property closed or skipped here has a row in docs/compliance.md with the
reason. Properties not listed take SuiteRequirements' defaults.
"""

from importlib.metadata import version

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


def _sqlite_driver_version() -> tuple[int, ...]:
    return tuple(int(part) for part in version("adbc-driver-sqlite").split(".")[:2])


class Requirements(SuiteRequirements):
    # ── ADBC connection layer ────────────────────────────────────────

    @property
    def sane_rowcount(self):
        # ADBC cursors report -1 for UPDATE/DELETE run through the query path.
        return exclusions.closed()

    @property
    def sane_multi_rowcount(self):
        return exclusions.closed()

    @property
    def implements_get_lastrowid(self):
        # No lastrowid on ADBC cursors; inserted_primary_key comes from
        # implicit RETURNING, which the sibling test covers.
        return exclusions.closed()

    # ── SQLite (same as SQLAlchemy's own requirements for sqlite) ────

    @property
    def views(self):
        # SuiteRequirements defaults to closed; SQLite has views.
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        # SQLite reflects the names of named PRIMARY KEY constraints.
        return exclusions.open()

    @property
    def temporary_tables(self):
        # As SQLAlchemy's own suite for a file-backed SQLite database: a
        # temporary table lives in the connection that created it, and the
        # suite reflects from other pooled connections.
        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        # SuiteRequirements keeps this open independently of temporary_tables;
        # the suite's temp table is created under this one.
        return exclusions.closed()

    @property
    def implicitly_named_constraints(self):
        # SQLite does not assign names to unnamed constraints (same as
        # SQLAlchemy's own requirements for sqlite).
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        return exclusions.closed()

    @property
    def independent_connections(self):
        # SQLAlchemy never lets a SQLite driver run the independent-connection
        # tests (file locking); independent_readonly_connections stays open.
        return exclusions.closed()

    # ── driver versions ──────────────────────────────────────────────

    @property
    def duplicate_key_raises_integrity_error(self):
        # adbc-driver-sqlite < 1.13 surfaces a UNIQUE violation as
        # InternalError "(unknown error)"; 1.13 raises IntegrityError.
        return exclusions.skip_if(
            lambda config: _sqlite_driver_version() < (1, 13),
            "adbc-driver-sqlite < 1.13 raises InternalError for UNIQUE violations",
        )
