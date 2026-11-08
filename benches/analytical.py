"""Compare identical SQL workloads on native SQLite, DuckDB Parquet and DuckDB VPF.

Requires duckdb matching the built VPF extension. See BENCHMARKS.md for reproduction.
"""

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import random
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_NOW)
import duckdb  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = "id, region, product_id, day, quantity, revenue_cents, discount_cents"
BATCH_ROWS = 8192
ROW_GROUP_ROWS = 65536
ENGINES = ("sqlite", "parquet", "vpf")


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect(extension=None):
    con = duckdb.connect(config={
        "allow_unsigned_extensions": "true",
        "threads": "1",
        "memory_limit": "512MB",
    })
    if extension is not None:
        con.execute(f"LOAD {literal(extension)}")
    return con


def queries(rows):
    middle = rows // 2
    return {
        "count": "SELECT COUNT(*) FROM orders",
        "aggregate": (
            "SELECT SUM(quantity), SUM(revenue_cents), SUM(discount_cents), "
            "COUNT(discount_cents), MIN(revenue_cents), MAX(revenue_cents) FROM orders"
        ),
        "range": (
            "SELECT COUNT(*), SUM(revenue_cents), SUM(quantity) FROM orders "
            f"WHERE id >= {middle} AND id < {middle + 100}"
        ),
        "filtered_aggregate": (
            "SELECT COUNT(*), SUM(revenue_cents), SUM(quantity) FROM orders "
            "WHERE region = 'r7' AND quantity >= 5"
        ),
        "group_by": (
            "SELECT region, COUNT(*), SUM(quantity), SUM(revenue_cents) "
            "FROM orders GROUP BY region ORDER BY region"
        ),
        "filtered_group_by": (
            "SELECT day, COUNT(*), SUM(revenue_cents), SUM(discount_cents) "
            "FROM orders WHERE day >= 90 AND day < 180 "
            "GROUP BY day ORDER BY day"
        ),
        "top_n": (
            "SELECT product_id, SUM(revenue_cents) AS revenue FROM orders "
            "GROUP BY product_id ORDER BY revenue DESC, product_id LIMIT 20"
        ),
        "distinct": "SELECT COUNT(DISTINCT product_id) FROM orders",
    }


def file_bytes(path):
    return sum(
        candidate.stat().st_size
        for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm"))
        if candidate.exists()
    )


def prepare(directory, rows, extension, vpf_bin, stack):
    parquet = directory / "orders.parquet"
    sqlite = directory / "orders.sqlite"
    vpf = directory / "orders.vpf"
    parquet_con = stack.enter_context(connect())
    parquet_con.execute(f"""
        COPY (
            SELECT i AS id, 'r' || (i % 32) AS region,
                   (i * 17) % 4096 AS product_id, (i * 13) % 365 AS day,
                   1 + i % 10 AS quantity, (i * 7919) % 100000 AS revenue_cents,
                   CASE WHEN i % 7 = 0 THEN NULL ELSE i % 2000 END AS discount_cents
            FROM range({rows}) r(i) ORDER BY i
        ) TO {literal(parquet)}
        (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE {ROW_GROUP_ROWS})
    """)
    parquet_con.execute(
        f"CREATE VIEW orders AS SELECT * FROM read_parquet({literal(parquet)})"
    )
    sqlite_con = sqlite3.connect(sqlite)
    stack.callback(sqlite_con.close)
    sqlite_con.execute("""
        CREATE TABLE orders (
            id BIGINT NOT NULL, region TEXT NOT NULL, product_id BIGINT NOT NULL,
            day BIGINT NOT NULL, quantity BIGINT NOT NULL, revenue_cents BIGINT NOT NULL,
            discount_cents BIGINT
        )
    """)
    source = parquet_con.execute(f"SELECT {COLUMNS} FROM orders")
    with sqlite_con:
        while batch := source.fetchmany(BATCH_ROWS):
            sqlite_con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
    sqlite_con.close()

    subprocess.run(
        [str(vpf_bin), str(vpf), "import", str(parquet), "--name", "orders"],
        check=True, stdout=subprocess.DEVNULL,
    )
    sqlite_con = sqlite3.connect(sqlite.as_uri() + "?mode=ro", uri=True)
    stack.callback(sqlite_con.close)
    sqlite_con.execute("PRAGMA cache_size = -65536")
    sqlite_con.execute("PRAGMA mmap_size = 0")
    vpf_con = stack.enter_context(connect(extension))
    vpf_con.execute(f"ATTACH {literal(vpf)} AS data (TYPE vpf, READ_ONLY)")
    vpf_con.execute("CREATE VIEW orders AS SELECT * FROM data.orders")

    # Single-threaded scans retain imported row order; SQLite's rowid is insertion order.
    cursors = [
        parquet_con.execute(f"SELECT {COLUMNS} FROM orders"),
        sqlite_con.execute(f"SELECT {COLUMNS} FROM orders ORDER BY rowid"),
        vpf_con.execute(f"SELECT {COLUMNS} FROM orders"),
    ]
    verified = 0
    while True:
        batches = [cursor.fetchmany(BATCH_ROWS) for cursor in cursors]
        if batches[0] != batches[1] or batches[0] != batches[2]:
            raise AssertionError(f"full-table mismatch near row {verified}")
        verified += len(batches[0])
        if not batches[0]:
            break
    if verified != rows:
        raise AssertionError(f"expected {rows} rows, verified {verified}")
    return (
        dict(zip(ENGINES, (sqlite_con, parquet_con, vpf_con))),
        {name: file_bytes(path) for name, path in zip(ENGINES, (sqlite, parquet, vpf))},
        verified,
    )


def benchmark_size(directory, rows, args):
    print(f"Preparing and verifying {rows:,} rows...", flush=True)
    with ExitStack() as stack:
        connections, sizes, verified = prepare(
            directory, rows, args.extension, args.vpf_bin, stack,
        )
        workload = queries(rows)
        expected = {}
        plans = {}
        samples = {query: {engine: [] for engine in ENGINES} for query in workload}
        for name, sql in workload.items():
            expected[name] = connections["sqlite"].execute(sql).fetchall()
            for engine in ENGINES:
                for _ in range(args.warmups):
                    result = connections[engine].execute(sql).fetchall()
                    if result != expected[name]:
                        raise AssertionError(f"{rows}/{name}/{engine}: result mismatch")
            plans[name] = {
                "sqlite": connections["sqlite"].execute("EXPLAIN QUERY PLAN " + sql).fetchall(),
                "parquet": connections["parquet"].execute("EXPLAIN " + sql).fetchall(),
                "vpf": connections["vpf"].execute("EXPLAIN " + sql).fetchall(),
            }

        rng = random.Random(20261108 + rows)
        for _ in range(args.repeats):
            jobs = [(name, engine) for name in workload for engine in ENGINES]
            rng.shuffle(jobs)
            for name, engine in jobs:
                start = time.perf_counter_ns()
                result = connections[engine].execute(workload[name]).fetchall()
                elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
                if result != expected[name]:
                    raise AssertionError(f"{rows}/{name}/{engine}: timed result mismatch")
                samples[name][engine].append(elapsed_ms)

        medians = {
            name: {engine: statistics.median(values) for engine, values in timings.items()}
            for name, timings in samples.items()
        }
        for name, timings in medians.items():
            print(
                f"  {name:20} SQLite {timings['sqlite']:9.3f} ms  "
                f"Parquet {timings['parquet']:9.3f} ms  VPF {timings['vpf']:9.3f} ms",
                flush=True,
            )
        return {
            "rows": rows,
            "verified_rows": verified,
            "file_bytes": sizes,
            "sql": workload,
            "results": expected,
            "plans": plans,
            "samples_ms": samples,
            "median_ms": medians,
        }


def markdown(report):
    lines = [
        "# Analytical SQL benchmark",
        "",
        f"Recorded {report['environment']['recorded_at']} on "
        f"`{report['environment']['commit']}`.",
        "",
        "Median warm-cache query latency in milliseconds (lower is better).",
        "",
        "| Rows | Query | SQLite | Parquet / DuckDB | VPF / DuckDB |",
        "|---:|---|---:|---:|---:|",
    ]
    for size in report["sizes"]:
        for name, timings in size["median_ms"].items():
            lines.append(
                f"| {size['rows']:,} | {name} | {timings['sqlite']:.3f} | "
                f"{timings['parquet']:.3f} | {timings['vpf']:.3f} |"
            )
    lines += [
        "",
        "Total of per-query medians, excluding metadata-friendly `COUNT(*)`. "
        "This is an equally weighted workload score, not one timed transaction.",
        "",
        "| Rows | SQLite (ms) | Parquet (ms) | VPF (ms) | SQLite / VPF | VPF / Parquet |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for size in report["sizes"]:
        totals = {
            engine: sum(times[engine] for name, times in size["median_ms"].items() if name != "count")
            for engine in ENGINES
        }
        lines.append(
            f"| {size['rows']:,} | {totals['sqlite']:.3f} | {totals['parquet']:.3f} | "
            f"{totals['vpf']:.3f} | {totals['sqlite'] / totals['vpf']:.2f}x | "
            f"{totals['vpf'] / totals['parquet']:.2f}x |"
        )
    lines += [
        "",
        "| Rows | SQLite (MiB) | Parquet (MiB) | VPF (MiB) |",
        "|---:|---:|---:|---:|",
    ]
    for size in report["sizes"]:
        sizes = size["file_bytes"]
        lines.append(
            f"| {size['rows']:,} | {sizes['sqlite'] / 2**20:.2f} | "
            f"{sizes['parquet'] / 2**20:.2f} | {sizes['vpf'] / 2**20:.2f} |"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=positive_int, default=[10000, 100000, 1000000, 10000000])
    parser.add_argument("--repeats", type=positive_int, default=7)
    parser.add_argument("--warmups", type=positive_int, default=2)
    parser.add_argument("--extension", type=Path, default=ROOT / "crates/vpf-duckdb/build/vpf.duckdb_extension")
    parser.add_argument("--vpf-bin", type=Path, default=ROOT / "target/release/vpf")
    parser.add_argument("--output", type=Path, required=True, help="JSON samples and adjacent Markdown tables")
    args = parser.parse_args()
    args.extension = args.extension.resolve(strict=True)
    args.vpf_bin = args.vpf_bin.resolve(strict=True)
    report = {
        "environment": {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "platform": platform.platform(),
            "cpu": next(
                (line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                 if line.startswith("model name")),
                platform.processor(),
            ),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "sqlite": sqlite3.sqlite_version,
            "duckdb_threads": 1,
            "duckdb_memory_limit": "512MB",
            "sqlite_cache_kib": 65536,
            "sqlite_mmap_bytes": 0,
            "sqlite_indexes": [],
            "parquet_codec": "SNAPPY",
            "parquet_row_group_rows": ROW_GROUP_ROWS,
            "extension": str(args.extension),
            "vpf_bin": str(args.vpf_bin),
        },
        "repeats": args.repeats,
        "warmups": args.warmups,
        "sizes": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for rows in args.sizes:
        with tempfile.TemporaryDirectory(prefix="vpf-analytics-") as directory:
            report["sizes"].append(benchmark_size(Path(directory), rows, args))
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        args.output.with_suffix(".md").write_text(markdown(report))


if __name__ == "__main__":
    main()
