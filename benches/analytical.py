"""Compare SQL reads and optional durable writes on SQLite, Parquet and VPF.

Requires duckdb matching the built VPF extension. See BENCHMARKS.md for reproduction.
"""

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import shutil
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
WRITE_WORKLOADS = ("fresh", "append", "update", "delete")
SQLITE_CREATE_TABLE = """
    CREATE TABLE orders (
        id BIGINT NOT NULL, region TEXT NOT NULL, product_id BIGINT NOT NULL,
        day BIGINT NOT NULL, quantity BIGINT NOT NULL, revenue_cents BIGINT NOT NULL,
        discount_cents BIGINT
    )
"""


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect(extension=None, temp_directory=None):
    config = {
        "allow_unsigned_extensions": "true",
        "threads": "1",
        "memory_limit": "512MB",
    }
    if temp_directory is not None:
        config["temp_directory"] = str(temp_directory)
    con = duckdb.connect(config=config)
    if extension is not None:
        con.execute(f"LOAD {literal(extension)}")
    return con


def queries(rows):
    middle = rows // 2
    return {
        "count": "SELECT COUNT(*) FROM orders",
        "point_lookup": f"SELECT {COLUMNS} FROM orders WHERE id = {middle}",
        "range_lookup": (
            f"SELECT {COLUMNS} FROM orders WHERE id >= {middle} "
            f"AND id < {middle + 100} ORDER BY id"
        ),
        "equality_filter": (
            "SELECT COUNT(*), SUM(revenue_cents) FROM orders WHERE product_id = 17"
        ),
        "null_filter": (
            "SELECT COUNT(*), SUM(revenue_cents) FROM orders WHERE discount_cents IS NULL"
        ),
        "like_filter": (
            "SELECT COUNT(*), SUM(revenue_cents) FROM orders WHERE region LIKE 'r1%'"
        ),
        "or_filter": (
            "SELECT COUNT(*), SUM(revenue_cents) FROM orders "
            f"WHERE id = {middle} OR id = {middle + 2}"
        ),
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


def parquet_fixture(con, path, start, rows):
    con.execute(f"""
        COPY (
            SELECT i AS id, 'r' || (i % 32) AS region,
                   (i * 17) % 4096 AS product_id, (i * 13) % 365 AS day,
                   1 + i % 10 AS quantity, (i * 7919) % 100000 AS revenue_cents,
                   CASE WHEN i % 7 = 0 THEN NULL ELSE i % 2000 END AS discount_cents
            FROM range({start}, {start + rows}) r(i) ORDER BY i
        ) TO {literal(path)}
        (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE {ROW_GROUP_ROWS})
    """)


def sqlite_fixture(con, path, parquet):
    sqlite_con = sqlite3.connect(path)
    try:
        sqlite_con.execute(SQLITE_CREATE_TABLE)
        source = con.execute(f"SELECT {COLUMNS} FROM read_parquet({literal(parquet)})")
        with sqlite_con:
            while batch := source.fetchmany(BATCH_ROWS):
                sqlite_con.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", batch)
    finally:
        sqlite_con.close()


def prepare(directory, rows, extension, vpf_bin, metadata_mode, stack):
    parquet = directory / "orders.parquet"
    sqlite = directory / "orders.sqlite"
    vpf = directory / "orders.vpf"
    parquet_con = stack.enter_context(connect(temp_directory=directory / "read-parquet-spill"))
    parquet_fixture(parquet_con, parquet, 0, rows)
    parquet_con.execute(
        f"CREATE VIEW orders AS SELECT * FROM read_parquet({literal(parquet)})"
    )
    sqlite_fixture(parquet_con, sqlite, parquet)

    import_start = time.perf_counter_ns()
    subprocess.run(
        [str(vpf_bin), str(vpf), "import", str(parquet), "--name", "orders",
         "--metadata-mode", metadata_mode],
        check=True, stdout=subprocess.DEVNULL,
    )
    import_ms = (time.perf_counter_ns() - import_start) / 1_000_000
    sqlite_con = sqlite3.connect(sqlite.as_uri() + "?mode=ro", uri=True)
    stack.callback(sqlite_con.close)
    sqlite_con.execute("PRAGMA cache_size = -65536")
    sqlite_con.execute("PRAGMA mmap_size = 0")
    vpf_con = stack.enter_context(connect(extension, directory / "read-vpf-spill"))
    vpf_con.execute(
        f"ATTACH {literal(vpf)} AS data "
        f"(TYPE vpf, READ_ONLY, METADATA_MODE {literal(metadata_mode)})"
    )
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
        import_ms,
    )


def sync_file(path):
    with path.open("rb") as file:
        os.fsync(file.fileno())


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def checkpoint(path):
    con = sqlite3.connect(path)
    try:
        busy, _, _ = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if busy:
            raise RuntimeError(f"WAL checkpoint was blocked for {path}")
    finally:
        con.close()


def write_spec(rows, incoming):
    appended = max(1, rows // 100)
    changed = min(100, rows - rows // 2)
    predicate = f"id >= {rows // 2} AND id < {rows // 2 + changed}"
    original = f"SELECT {COLUMNS} FROM orders"
    return {
        "fresh": {
            "logical_rows": rows, "output_rows": rows, "expected_sql": original,
        },
        "append": {
            "logical_rows": appended, "output_rows": rows + appended,
            "expected_sql": (
                f"{original} UNION ALL SELECT {COLUMNS} FROM read_parquet({literal(incoming)})"
            ),
        },
        "update": {
            "logical_rows": changed, "output_rows": rows, "predicate": predicate,
            "expected_sql": (
                "SELECT id, region, product_id, day, quantity, "
                f"CASE WHEN {predicate} THEN 12345 ELSE revenue_cents END AS revenue_cents, "
                "discount_cents FROM orders"
            ),
        },
        "delete": {
            "logical_rows": changed, "output_rows": rows - changed, "predicate": predicate,
            "expected_sql": f"{original} WHERE NOT ({predicate})",
        },
    }


def verify_written_rows(source, actual, sql, actual_sql, expected_rows, context):
    bounds_sql = "SELECT MIN(id), MAX(id), COUNT(*) FROM ({})"
    bounds = source.execute(bounds_sql.format(sql)).fetchone()
    if actual.execute(bounds_sql.format(actual_sql)).fetchone() != bounds:
        raise AssertionError(f"{context}: output mismatch in ID bounds")
    checked = 0
    # Repeated ID-window queries would rescan the entire unindexed SQLite table.
    sqlite_rows = (
        actual.execute(actual_sql + " ORDER BY id")
        if isinstance(actual, sqlite3.Connection) else None
    )
    if bounds[0] is not None:
        for start in range(bounds[0], bounds[1] + 1, BATCH_ROWS):
            predicate = f"id >= {start} AND id < {start + BATCH_ROWS}"
            select = "SELECT * FROM ({}) WHERE " + predicate + " ORDER BY id"
            left = source.execute(select.format(sql)).fetchmany(BATCH_ROWS + 1)
            if sqlite_rows is None:
                right = actual.execute(select.format(actual_sql)).fetchmany(BATCH_ROWS + 1)
            else:
                right = sqlite_rows.fetchmany(len(left)) if left else []
            if left != right or len(left) > BATCH_ROWS:
                raise AssertionError(f"{context}: output mismatch near row {checked}")
            checked += len(left)
    if sqlite_rows is not None and sqlite_rows.fetchone() is not None:
        raise AssertionError(f"{context}: output mismatch after row {checked}")
    if checked != expected_rows:
        raise AssertionError(f"{context}: expected {expected_rows} rows, verified {checked}")
    return checked


def write_iteration(directory, rows, args, source, workload, engine, spec, full_verify):
    context = f"{rows}/{workload}/{engine}"
    with tempfile.TemporaryDirectory(prefix="write-", dir=directory) as temporary:
        iteration = Path(temporary)
        path = iteration / f"orders.{engine}"
        if workload != "fresh":
            shutil.copyfile(directory / f"orders.{engine}", path)
        with ExitStack() as stack:
            if engine == "sqlite":
                con = sqlite3.connect(path, uri=True)
                stack.callback(con.close)
                con.execute("PRAGMA journal_mode = WAL")
                con.execute("PRAGMA synchronous = NORMAL")
                con.execute("PRAGMA cache_size = -65536")
                con.execute("PRAGMA mmap_size = 0")
                input_path = directory / ("incoming.sqlite" if workload == "append" else "orders.sqlite")
                con.execute("ATTACH DATABASE ? AS input", [input_path.as_uri() + "?mode=ro"])
                table = "orders"
            else:
                con = stack.enter_context(connect(
                    args.extension if engine == "vpf" else None, iteration / "spill",
                ))
                if engine == "vpf":
                    con.execute(
                        f"ATTACH {literal(path)} AS data "
                        f"(TYPE vpf, CODEC 'snappy', METADATA_MODE {literal(args.metadata_mode)})"
                    )
                    table = "data.orders"
                else:
                    table = f"read_parquet({literal(path)})"
            if path.exists():
                sync_file(path)
            sync_directory(iteration)

            statements = []
            start = time.perf_counter_ns()
            if engine == "sqlite":
                with con:
                    con.execute("BEGIN IMMEDIATE")
                    if workload == "fresh":
                        con.execute(SQLITE_CREATE_TABLE)
                        statements.append(SQLITE_CREATE_TABLE)
                    if workload in ("fresh", "append"):
                        sql = "INSERT INTO orders SELECT * FROM input.orders"
                    elif workload == "update":
                        sql = f"UPDATE orders SET revenue_cents = 12345 WHERE {spec['predicate']}"
                    else:
                        sql = f"DELETE FROM orders WHERE {spec['predicate']}"
                    statements.append(sql)
                    cursor = con.execute(sql)
                    changed = cursor.rowcount
                output = path
            elif engine == "vpf":
                if workload == "fresh":
                    sql = (
                        "CREATE TABLE data.orders AS SELECT * FROM "
                        f"read_parquet({literal(directory / 'orders.parquet')})"
                    )
                elif workload == "append":
                    sql = (
                        "INSERT INTO data.orders SELECT * FROM "
                        f"read_parquet({literal(directory / 'incoming.parquet')})"
                    )
                elif workload == "update":
                    sql = f"UPDATE data.orders SET revenue_cents = 12345 WHERE {spec['predicate']}"
                else:
                    sql = f"DELETE FROM data.orders WHERE {spec['predicate']}"
                statements.append(sql)
                result = con.execute(sql).fetchall()
                if len(result) != 1 or len(result[0]) != 1:
                    raise AssertionError(f"{context}: unexpected write result {result}")
                changed = result[0][0]
                output = path
            else:
                base = (
                    f"read_parquet({literal(directory / 'orders.parquet')})"
                    if workload == "fresh" else table
                )
                selection = f"SELECT {COLUMNS} FROM {base}"
                if workload == "append":
                    selection += (
                        f" UNION ALL SELECT {COLUMNS} FROM "
                        f"read_parquet({literal(directory / 'incoming.parquet')})"
                    )
                elif workload == "update":
                    selection = (
                        "SELECT id, region, product_id, day, quantity, "
                        f"CASE WHEN {spec['predicate']} THEN 12345 ELSE revenue_cents END AS revenue_cents, "
                        f"discount_cents FROM {base}"
                    )
                elif workload == "delete":
                    selection += f" WHERE NOT ({spec['predicate']})"
                output = path if workload == "fresh" else iteration / "rewrite.parquet"
                sql = (
                    f"COPY ({selection}) TO {literal(output)} "
                    f"(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE {ROW_GROUP_ROWS})"
                )
                statements.append(sql)
                result = con.execute(sql).fetchall()
                if result != [(spec["output_rows"],)]:
                    raise AssertionError(f"{context}: unexpected COPY result {result}")
                changed = spec["logical_rows"]
            sql_end = time.perf_counter_ns()
            if engine in ("sqlite", "vpf"):
                checkpoint(path)
            sync_file(output)
            if output != path:
                os.replace(output, path)
            sync_directory(iteration)
            end = time.perf_counter_ns()
            if changed != spec["logical_rows"]:
                raise AssertionError(f"{context}: expected {spec['logical_rows']} changed rows, got {changed}")
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchall()
            if count != [(spec["output_rows"],)]:
                raise AssertionError(f"{context}: unexpected output count {count}")
            verified = None
            if full_verify:
                verified = verify_written_rows(
                    source, con, spec["expected_sql"], f"SELECT {COLUMNS} FROM {table}",
                    spec["output_rows"], context,
                )
            return {
                "elapsed_ms": (end - start) / 1_000_000,
                "sql_ms": (sql_end - start) / 1_000_000,
                "durability_ms": (end - sql_end) / 1_000_000,
                "file_bytes": file_bytes(path),
                "verified_rows": verified,
                "sql": statements,
            }


def benchmark_writes(directory, rows, args, source):
    print(f"Measuring durable writes for {rows:,} rows...", flush=True)
    incoming = directory / "incoming.parquet"
    parquet_fixture(source, incoming, rows, max(1, rows // 100))
    sqlite_fixture(source, directory / "incoming.sqlite", incoming)
    specs = write_spec(rows, incoming)
    for engine in ("sqlite", "vpf"):
        checkpoint(directory / f"orders.{engine}")
    for name in ("orders.sqlite", "orders.parquet", "orders.vpf", "incoming.sqlite", "incoming.parquet"):
        sync_file(directory / name)
    sync_directory(directory)
    samples = {workload: {engine: [] for engine in ENGINES} for workload in WRITE_WORKLOADS}
    sizes = {workload: {engine: [] for engine in ENGINES} for workload in WRITE_WORKLOADS}
    verified = {workload: {} for workload in WRITE_WORKLOADS}
    statements = {workload: {} for workload in WRITE_WORKLOADS}
    stages = {
        stage: {workload: {engine: [] for engine in ENGINES} for workload in WRITE_WORKLOADS}
        for stage in ("sql_ms", "durability_ms")
    }
    rng = random.Random(20261109 + rows)
    for repetition in range(args.write_warmups + args.write_repeats):
        jobs = [(workload, engine) for workload in WRITE_WORKLOADS for engine in ENGINES]
        rng.shuffle(jobs)
        for workload, engine in jobs:
            if repetition == 0:
                print(f"  verifying write/{workload}/{engine}...", flush=True)
            result = write_iteration(
                directory, rows, args, source, workload, engine, specs[workload],
                full_verify=repetition == 0,
            )
            if result["verified_rows"] is not None:
                verified[workload][engine] = result["verified_rows"]
                statements[workload][engine] = result["sql"]
            if repetition >= args.write_warmups:
                samples[workload][engine].append(result["elapsed_ms"])
                sizes[workload][engine].append(result["file_bytes"])
                for stage, timings in stages.items():
                    timings[workload][engine].append(result[stage])
    medians = {
        workload: {engine: statistics.median(values) for engine, values in timings.items()}
        for workload, timings in samples.items()
    }
    for workload, timings in medians.items():
        print(
            f"  write/{workload:14} SQLite {timings['sqlite']:9.3f} ms  "
            f"Parquet {timings['parquet']:9.3f} ms  VPF {timings['vpf']:9.3f} ms",
            flush=True,
        )
    return {
        "workloads": specs,
        "verified_rows": verified,
        "sql": statements,
        "samples_ms": samples,
        "median_ms": medians,
        "stage_samples_ms": stages,
        "output_file_bytes": sizes,
    }


def benchmark_size(directory, rows, args):
    print(f"Preparing and verifying {rows:,} rows...", flush=True)
    with ExitStack() as stack:
        connections, sizes, verified, import_ms = prepare(
            directory, rows, args.extension, args.vpf_bin, args.metadata_mode, stack,
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
        result = {
            "rows": rows,
            "verified_rows": verified,
            "vpf_import_ms": import_ms,
            "file_bytes": sizes,
            "sql": workload,
            "results": expected,
            "plans": plans,
            "samples_ms": samples,
            "median_ms": medians,
        }
        if args.writes:
            result["writes"] = benchmark_writes(directory, rows, args, connections["parquet"])
        return result


def markdown(report):
    lines = [
        "# SQL read/write benchmark" if report.get("write_repeats") else "# Analytical SQL benchmark",
        "",
        f"Recorded {report['environment']['recorded_at']} on "
        f"`{report['environment']['commit']}`.",
        "",
        "Median warm-cache query latency in milliseconds (lower is better).",
        f"VPF metadata mode: `{report['environment']['metadata_mode']}`.",
        f"Read workload version: {report['read_workload_version']} "
        f"({len(report['sizes'][0]['sql'])} queries).",
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
    lines += [
        "",
        "VPF fixture import elapsed time includes CLI process startup; one setup "
        "observation per size, not repeated write measurements.",
        "",
        "| Rows | Import (ms) |",
        "|---:|---:|",
    ]
    for size in report["sizes"]:
        lines.append(f"| {size['rows']:,} | {size['vpf_import_ms']:.3f} |")
    if report.get("write_repeats"):
        lines += [
            "",
            "## Durable writes",
            "",
            "Median milliseconds including commit, WAL checkpoint where applicable, "
            "file fsync and directory fsync. Fixture copying/syncing and verification "
            "are excluded. SQLite uses native INSERT SELECT, not Python row ingestion; "
            "Parquet appends/updates/deletes rewrite the complete file.",
            "",
            "| Rows | Write | Logical rows changed | SQLite | Parquet / DuckDB | VPF / DuckDB |",
            "|---:|---|---:|---:|---:|---:|",
        ]
        for size in report["sizes"]:
            for workload, timings in size["writes"]["median_ms"].items():
                changed = size["writes"]["workloads"][workload]["logical_rows"]
                lines.append(
                    f"| {size['rows']:,} | {workload} | {changed:,} | {timings['sqlite']:.3f} | "
                    f"{timings['parquet']:.3f} | {timings['vpf']:.3f} |"
                )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=positive_int, default=[10000, 100000, 1000000, 10000000])
    parser.add_argument("--repeats", type=positive_int, default=7)
    parser.add_argument("--warmups", type=positive_int, default=2)
    parser.add_argument("--metadata-mode", choices=("lazy", "eager"), default="lazy")
    parser.add_argument("--writes", action="store_true", help="also measure fresh writes, appends, updates and deletes")
    parser.add_argument("--write-repeats", type=positive_int, default=5)
    parser.add_argument("--write-warmups", type=positive_int, default=1)
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
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
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
            "metadata_mode": args.metadata_mode,
            "extension": str(args.extension),
            "vpf_bin": str(args.vpf_bin),
        },
        "repeats": args.repeats,
        "warmups": args.warmups,
        "read_workload_version": 2,
        "sizes": [],
    }
    if args.writes:
        report.update({
            "write_repeats": args.write_repeats,
            "write_warmups": args.write_warmups,
            "write_durability": "WAL checkpoint/TRUNCATE, file fsync and directory fsync",
            "write_inputs": "native SQLite table for SQLite; canonical Parquet for DuckDB Parquet/VPF",
            "write_codec": "SNAPPY",
            "write_layout": "Parquet COPY: 65536-row groups; VPF CTAS: production append layout",
            "vpf_write_policy": "auto append: buffers fewer than 65536 rows; otherwise direct",
        })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for rows in args.sizes:
        with tempfile.TemporaryDirectory(prefix="vpf-analytics-") as directory:
            report["sizes"].append(benchmark_size(Path(directory), rows, args))
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        args.output.with_suffix(".md").write_text(markdown(report))


if __name__ == "__main__":
    main()
