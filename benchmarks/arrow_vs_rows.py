"""Rows versus Arrow through the same SQLAlchemy engine.

Times fetching one table from an in-memory SQLite database over ADBC:

- ``conn.execute(select(t)).all()`` — SQLAlchemy's row path (the
  ``ColumnwiseCursor`` already converts Arrow batches column-wise);
- ``fetch_arrow_table(conn, select(t))`` — the Arrow path;
- ``polars.from_arrow(fetch_arrow_table(...))`` when Polars is installed;
- ``pandas.read_sql(select(t), conn)`` when pandas is installed.

Run::

    python benchmarks/arrow_vs_rows.py --rows 500000

and paste the Markdown table it prints into the README together with the
machine and versions it reports. Numbers depend on the machine; the ratio
is the point.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import platform
import statistics
import sys
import time
from collections.abc import Callable
from importlib.metadata import version

import sqlalchemy as sa
from sqlalchemy.pool import StaticPool

from sqlalchemy_adbc.arrow import fetch_arrow_table

TABLE = sa.Table(
    "bench",
    sa.MetaData(),
    sa.Column("id", sa.Integer),
    sa.Column("value", sa.Float),
    sa.Column("label", sa.String(16)),
    sa.Column("day", sa.Date),
    sa.Column("flag", sa.Boolean),
)


def build(rows: int) -> sa.Engine:
    engine = sa.create_engine("sqlite+adbc:///:memory:", poolclass=StaticPool)
    TABLE.metadata.create_all(engine)
    chunk = 50_000
    with engine.begin() as conn:
        for start in range(0, rows, chunk):
            conn.execute(
                TABLE.insert(),
                [
                    {
                        "id": i,
                        "value": i / 7,
                        "label": f"label-{i % 1000}",
                        "day": dt.date(2024, 1, 1) + dt.timedelta(days=i % 365),
                        "flag": i % 3 == 0,
                    }
                    for i in range(start, min(start + chunk, rows))
                ],
            )
    return engine


def timeit(fn: Callable[[], object], repeats: int) -> float:
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rows", type=int, default=500_000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    engine = build(args.rows)
    stmt = sa.select(TABLE)
    results: list[tuple[str, float]] = []
    with engine.connect() as conn:
        results.append(
            (
                "SQLAlchemy rows: `conn.execute(select).all()`",
                timeit(lambda: conn.execute(stmt).all(), args.repeats),
            )
        )
        results.append(
            (
                "Arrow: `fetch_arrow_table(conn, select)`",
                timeit(lambda: fetch_arrow_table(conn, stmt), args.repeats),
            )
        )
        for module, label, fn in (
            (
                "polars",
                "Polars: `pl.from_arrow(fetch_arrow_table(...))`",
                lambda m: m.from_arrow(fetch_arrow_table(conn, stmt)),
            ),
            (
                "pandas",
                "pandas via Arrow: `fetch_arrow_table(...).to_pandas()`",
                lambda m: fetch_arrow_table(conn, stmt).to_pandas(),
            ),
            (
                "pandas",
                "pandas via rows: `pd.read_sql(select, conn)`",
                lambda m: m.read_sql(stmt, conn),
            ),
        ):
            try:
                mod = importlib.import_module(module)
            except ImportError:
                continue
            results.append((label, timeit(lambda fn=fn, mod=mod: fn(mod), args.repeats)))
    engine.dispose()

    baseline = results[0][1]
    print(
        f"| Path | Time ({args.rows:,} rows × {len(TABLE.columns)} cols, median of {args.repeats}) | vs rows |"
    )
    print("|---|---:|---:|")
    for label, seconds in results:
        print(f"| {label} | {seconds:.2f} s | {baseline / seconds:.1f}× |")
    print()
    pieces = [
        f"{platform.machine()} {platform.system()} {platform.release()}",
        f"Python {sys.version.split()[0]}",
        f"SQLAlchemy {version('sqlalchemy')}",
        f"pyarrow {version('pyarrow')}",
        f"adbc-driver-sqlite {version('adbc-driver-sqlite')}",
        f"sqlalchemy-adbc {version('sqlalchemy-adbc')}",
    ]
    for module in ("polars", "pandas"):
        try:
            pieces.append(f"{module} {version(module)}")
        except Exception:  # noqa: BLE001 - optional
            pass
    print("Measured on: " + ", ".join(pieces) + ".")


if __name__ == "__main__":
    main()
