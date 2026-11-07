"""Unit tests for the BigQuery URL → ADBC kwargs translation."""

from __future__ import annotations

from sqlalchemy.engine.url import make_url

from sqlalchemy_adbc.bigquery import ADBCBigQueryDialect


def _build(url_str: str):
    return ADBCBigQueryDialect().build_connect_args(make_url(url_str))


def test_project_from_database_segment():
    """``bigquery+adbc:///my-project`` → project_id set."""
    args, kwargs = _build("bigquery+adbc:///my-project")
    assert args == []
    assert kwargs["db_kwargs"]["adbc.bigquery.sql.project_id"] == "my-project"


def test_project_plus_dataset():
    _, kwargs = _build("bigquery+adbc:///my-project?dataset=events")
    db = kwargs["db_kwargs"]
    assert db["adbc.bigquery.sql.project_id"] == "my-project"
    assert db["dataset"] == "events"


def test_only_query_params():
    _, kwargs = _build("bigquery+adbc://?adbc.bigquery.sql.project_id=p&x=y")
    db = kwargs["db_kwargs"]
    assert db["adbc.bigquery.sql.project_id"] == "p"
    assert db["x"] == "y"


def test_explicit_project_kwarg_wins_over_url():
    """If both URL-database and ``?project_id=...`` are present, the
    explicit query string must not be overwritten by the implicit default."""
    _, kwargs = _build("bigquery+adbc:///implicit?adbc.bigquery.sql.project_id=explicit")
    assert kwargs["db_kwargs"]["adbc.bigquery.sql.project_id"] == "explicit"


def test_no_kwargs_at_all():
    args, kwargs = _build("bigquery+adbc://")
    assert args == []
    assert kwargs == {}
