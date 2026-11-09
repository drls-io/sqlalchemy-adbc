---
name: Server or backend report
about: Report running the dialect against a Flight SQL server or backend not covered by CI
labels: server-report
---

## Server or backend

<!-- Name and version, and how it is deployed (Docker image, managed service, ...). -->

## URL form and auth mode

```text
flightsql+adbc://host:port?tls=true&auth=...   # secrets removed
```

## What worked

<!-- e.g. SELECT, parameters, reflection (table names, columns, types), inserts, Arrow helpers, pandas/Superset/marimo. -->

## What did not

<!-- Errors or wrong results, with the statement. If reflection is wrong, the output of
     connection.connection.dbapi_connection.adbc_get_objects(depth="all").read_all().to_pylist()
     for one table helps. -->

## Versions

- sqlalchemy-adbc, SQLAlchemy, ADBC driver (`pip list | grep -i -E "sqlalchemy|adbc|arrow"`):
