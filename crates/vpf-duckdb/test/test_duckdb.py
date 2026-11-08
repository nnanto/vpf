"""End-to-end tests for the VPF DuckDB extension.

Run: python crates/vpf-duckdb/test/test_duckdb.py [path/to/vpf.duckdb_extension]
Needs duckdb matching the extension's DuckDB version (see build.sh).
"""

import json
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import traceback

# Python's duckdb module is loaded RTLD_LOCAL; C++ extensions need its symbols global.
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_NOW)
import duckdb  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EXTENSION = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "build", "vpf.duckdb_extension")
VPF_BIN = os.environ.get("VPF_BIN")


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    return con


def rows(con, sql):
    return con.execute(sql).fetchall()


def expect_error(fn, needle):
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        assert needle.lower() in str(e).lower(), f"expected {needle!r} in {e}"
        return
    raise AssertionError(f"expected error containing {needle!r}")


def test_crud(tmp):
    con = connect()
    con.execute(f"ATTACH '{tmp}/t.vpf' AS db (TYPE vpf)")
    con.execute(
        "CREATE TABLE db.t (id BIGINT NOT NULL, name VARCHAR, tags VARCHAR[], s STRUCT(a INT, b VARCHAR))"
    )
    assert rows(con, "SELECT count(*) FROM db.t") == [(0,)]
    assert rows(
        con, "INSERT INTO db.t SELECT i, 'n' || i, ['x', 'y'], {'a': i::INT, 'b': 'b'} FROM range(5) r(i)"
    ) == [(5,)]
    assert rows(con, "SELECT id, name, tags, s FROM db.t WHERE id = 1") == [
        (1, "n1", ["x", "y"], {"a": 1, "b": "b"})
    ]
    assert rows(con, "UPDATE db.t SET name = 'upd', s = {'a': 99, 'b': 'z'} WHERE id = 2") == [(1,)]
    assert rows(con, "UPDATE db.t SET tags = ['q'] WHERE id >= 3") == [(2,)]
    assert rows(con, "DELETE FROM db.t WHERE id = 4") == [(1,)]
    assert rows(con, "SELECT id, name, tags, s FROM db.t ORDER BY id") == [
        (0, "n0", ["x", "y"], {"a": 0, "b": "b"}),
        (1, "n1", ["x", "y"], {"a": 1, "b": "b"}),
        (2, "upd", ["x", "y"], {"a": 99, "b": "z"}),
        (3, "n3", ["q"], {"a": 3, "b": "b"}),
    ]
    assert rows(con, "SELECT sum(id), max(name) FROM db.t") == [(6, "upd")]
    expect_error(lambda: con.execute("INSERT INTO db.t (name) VALUES ('x')"), "NULL")
    expect_error(lambda: con.execute("CREATE TABLE db.other (x INT)"), "already")
    con.execute("CREATE TABLE IF NOT EXISTS db.t (id BIGINT)")
    con.close()

    # Data survives reopening.
    con = connect()
    con.execute(f"ATTACH '{tmp}/t.vpf' AS db (TYPE vpf, READ_ONLY)")
    assert rows(con, "SELECT count(*) FROM db.t") == [(4,)]
    expect_error(lambda: con.execute("DELETE FROM db.t"), "read-only")


def test_transactions(tmp):
    con = connect()
    con.execute(f"ATTACH '{tmp}/x.vpf' AS db (TYPE vpf)")
    con.execute("CREATE TABLE db.t AS SELECT range AS id, range * 2 AS v FROM range(100)")
    assert rows(con, "SELECT count(*), sum(v) FROM db.t") == [(100, 9900)]

    con.execute("BEGIN")
    con.execute("DELETE FROM db.t WHERE id < 50")
    con.execute("INSERT INTO db.t VALUES (1000, 1)")
    assert rows(con, "SELECT count(*) FROM db.t") == [(51,)]
    con.execute("ROLLBACK")
    assert rows(con, "SELECT count(*) FROM db.t") == [(100,)]

    con.execute("BEGIN")
    con.execute("UPDATE db.t SET v = 0 WHERE id < 10")
    con.execute("DELETE FROM db.t WHERE id >= 90")
    con.execute("COMMIT")
    assert rows(con, "SELECT count(*), sum(v) FROM db.t") == [(90, 9900 - 90 - sum(range(180, 200, 2)))]

    # A second connection sees committed data only.
    other = connect()
    other.execute(f"ATTACH '{tmp}/x.vpf' AS db (TYPE vpf)")
    con.execute("BEGIN")
    con.execute("DELETE FROM db.t")
    assert rows(other, "SELECT count(*) FROM db.t") == [(90,)]
    con.execute("COMMIT")
    assert rows(other, "SELECT count(*) FROM db.t") == [(0,)]


def test_update_from_and_copy(tmp):
    con = connect()
    con.execute(f"ATTACH '{tmp}/u.vpf' AS db (TYPE vpf)")
    con.execute("CREATE TABLE db.t (id INT, v VARCHAR)")
    con.execute("INSERT INTO db.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    con.execute("CREATE TABLE src AS SELECT * FROM (VALUES (1, 'x'), (1, 'y'), (3, 'z')) s(id, v)")
    con.execute("UPDATE db.t SET v = src.v FROM src WHERE db.t.id = src.id")
    got = dict(rows(con, "SELECT id, v FROM db.t"))
    assert got[2] == "b" and got[1] in ("x", "y") and got[3] == "z", got
    con.execute(f"COPY db.t TO '{tmp}/out.parquet'")
    assert rows(con, f"SELECT count(*) FROM '{tmp}/out.parquet'") == [(3,)]
    con.execute(f"INSERT INTO db.t SELECT * FROM '{tmp}/out.parquet'")
    assert rows(con, "SELECT count(*) FROM db.t") == [(6,)]


def test_types(tmp):
    con = connect()
    con.execute(f"ATTACH '{tmp}/ty.vpf' AS db (TYPE vpf)")
    select = """SELECT
        true AS b, 1::TINYINT AS i8, 2::SMALLINT AS i16, 3::INT AS i32, 4::BIGINT AS i64,
        5::UTINYINT AS u8, 6::UINTEGER AS u32, 1.5::FLOAT AS f32, 2.5::DOUBLE AS f64,
        12.34::DECIMAL(10, 2) AS dec, 'hello' AS s, '\\xAA'::BLOB AS bin,
        DATE '2024-01-02' AS d, TIMESTAMP '2024-01-02 03:04:05' AS ts,
        TIMESTAMPTZ '2024-01-02 03:04:05+00' AS tstz, TIME '01:02:03' AS t,
        [1, 2] AS l, {'k': 'v'} AS st, MAP {'a': 1} AS m
        UNION ALL SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL"""
    con.execute(f"CREATE TABLE db.t AS {select}")
    con.execute(f"CREATE TABLE mem AS {select}")
    # pytz is needed to fetch TIMESTAMPTZ into Python, so compare as text.
    q = "SELECT * REPLACE (tstz::VARCHAR AS tstz) FROM {} ORDER BY b NULLS LAST"
    assert rows(con, q.format("db.t")) == rows(con, q.format("mem"))
    cols = "SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = '{}' ORDER BY ordinal_position"
    assert rows(con, cols.format("db")) == rows(con, cols.format("memory"))


def test_concurrent_writers(tmp):
    con = connect()
    con.execute(f"ATTACH '{tmp}/c.vpf' AS db (TYPE vpf)")
    con.execute("CREATE TABLE db.t (id INT)")
    errors = []

    def writer(n):
        try:
            c = connect()
            c.execute(f"ATTACH '{tmp}/c.vpf' AS db (TYPE vpf)")
            for i in range(20):
                c.execute(f"INSERT INTO db.t VALUES ({n * 100 + i})")
        except Exception:  # noqa: BLE001
            errors.append(traceback.format_exc())

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors[0]
    assert rows(con, "SELECT count(*), count(DISTINCT id) FROM db.t") == [(80, 80)]


def scanned_rows(con, tmp, sql, params=None):
    """Rows the VPF scan produced for `sql` (before DuckDB's filter)."""
    path = os.path.join(tmp, "profile.json")
    con.execute(f"SET enable_profiling='json'; SET profiling_output='{path}'")
    con.execute(sql, params or []).fetchall()
    con.execute("RESET enable_profiling")

    def find(node):
        if "Table" in node.get("extra_info", {}):
            return node["intermediate_rows"]
        for child in node.get("children", []) + node.get("operator", []):
            n = find(child)
            if n is not None:
                return n
        return None

    with open(path) as f:
        return find(json.load(f))


def same(a, b):
    def key(row):
        return tuple("nan" if isinstance(v, float) and math.isnan(v) else v for v in row)

    return sorted(map(key, a), key=repr) == sorted(map(key, b), key=repr)


def test_filter_pushdown(tmp):
    con = connect()
    con.execute(f"ATTACH '{tmp}/f.vpf' AS db (TYPE vpf)")
    con.execute("CREATE SCHEMA mem")
    data = """SELECT range AS id,
        CASE WHEN range % 1000 = 7 THEN NULL WHEN range % 1000 = 8 THEN 'nan'::DOUBLE
             WHEN range % 1000 = 9 THEN -0.0 ELSE (range / 10)::DOUBLE END AS x,
        CASE WHEN range % 3 = 0 THEN NULL ELSE 'name' || lpad(range::VARCHAR, 7, '0') END AS s,
        DATE '2020-01-01' + (range // 1000)::INT AS d,
        (range / 100)::DECIMAL(12, 2) AS m,
        range % 2 = 0 AS b
        FROM range(300000)"""
    con.execute(f"CREATE TABLE db.t AS {data}")
    con.execute(f"CREATE TABLE mem.t AS {data}")
    con.execute("INSERT INTO db.t VALUES (-1, 'nan', NULL, NULL, NULL, NULL), (1000000, -0.0, 'zz', DATE '1999-01-01', 0, true)")
    con.execute("INSERT INTO mem.t VALUES (-1, 'nan', NULL, NULL, NULL, NULL), (1000000, -0.0, 'zz', DATE '1999-01-01', 0, true)")

    def check(where, params=None):
        sql = f"SELECT * FROM {{}}.t WHERE {where}"
        got = rows(con, sql.format("db")) if params is None else con.execute(sql.format("db"), params).fetchall()
        want = rows(con, sql.format("mem")) if params is None else con.execute(sql.format("mem"), params).fetchall()
        assert same(got, want), f"{where}: {len(got)} rows vs {len(want)}"

    predicates = [
        "id = 123456", "id <> 5", "id < 10", "id <= 0", "id > 299990", "id >= 1000000", "id = -1",
        "id BETWEEN 1000 AND 1010", "id IN (5, 250000)", "id = 5 OR id = 250000", "id + 1 = 7",
        "id % 7 = 3", "id = 400000", "id IS NULL", "id IS NOT NULL",
        "x = 0", "x = -0.0", "x > 29999", "x >= 'nan'::DOUBLE", "x = 'nan'::DOUBLE", "x < 0", "x <> 0",
        "x <= -0.0", "x IS NULL", "x IS NOT NULL", "isnan(x)",
        "s = 'name0123457'", "s > 'name0299990'", "s < 'name0000003'", "s >= 'zz'", "s IS NULL", "s LIKE 'name00001%'",
        "d = DATE '2020-03-01'", "d < DATE '2000-01-01'", "d > DATE '2020-10-25'", "d IS NULL",
        "m = 1234.56", "m > 2999.98", "m BETWEEN 1.00 AND 1.02",
        "b = true AND id < 4", "NOT b AND id > 299996",
        "id > 100 AND id < 105 AND s IS NOT NULL",
        "(id = 5 OR id = 250000) AND s IS NOT NULL",
        "id = 5 OR id = 123456 OR id = 250000",
        "(id BETWEEN 1000 AND 1010) OR (id > 299990 AND b = true)",
        "(id = 5 AND s IS NOT NULL) OR s = 'zz'",
        "id = 123456 OR s IS NULL",
        "x = 'nan'::DOUBLE OR id = 5", "x IS NULL OR id = 250000",
        "id = 5 OR id % 7 = 3",
        "id = 5 OR s LIKE 'name029999%'",
        "(id = 5 OR id % 7 = 3) AND id < 10",
        "(id = 5 AND id % 7 = 3) OR id = 250000",
        "NOT (id = 5 OR id = 250000)",
        "id = NULL OR id = 250000",
    ]
    for p in predicates:
        check(p)
    check("id = ?", [77777])
    check("s = ? AND id > ?", ["name0000005", 1])
    check("id = ? OR id = ?", [5, 250000])

    # Point and range lookups read about a page, not the table.
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 123456") <= 25000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id BETWEEN 1000 AND 1010") <= 25000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE s = 'name0123457'") <= 25000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = ?", [42]) <= 25000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 400000") <= 2
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id % 7 = 3") == 300002
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 123456 OR id = 123458") <= 25000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 5 OR id = 250000") <= 50000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 5 OR id = 123456 OR id = 250000") <= 75000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = ? OR id = ?", [5, 250000]) <= 50000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 400000 OR id = 500000") <= 2
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 5 OR id % 7 = 3") == 300002
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE id = 5 OR s LIKE 'name029999%'") == 300002
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE (id = 5 OR id % 7 = 3) AND id < 10") <= 25000
    assert scanned_rows(con, tmp, "SELECT * FROM db.t WHERE (id = 5 AND id % 7 = 3) OR id = 250000") <= 50000
    plan = rows(con, "EXPLAIN SELECT * FROM db.t WHERE id = 5")[0][1]
    assert "VPF Pruning" in plan and "id = 5" in plan, plan
    plan = rows(con, "EXPLAIN SELECT * FROM db.t WHERE id = 5 OR id = 250000")[0][1]
    assert "VPF Pruning" in plan and " OR " in plan, plan

    # Writes through pruned scans touch exactly the matching rows.
    for sql in [
        "UPDATE {}.t SET s = 'upd' WHERE id BETWEEN 200000 AND 200004",
        "DELETE FROM {}.t WHERE id = 150000 OR id = 299999",
        "DELETE FROM {}.t WHERE x = 'nan'::DOUBLE AND id > 250000",
        "UPDATE {}.t SET x = 1e9 WHERE s = 'name0000010'",
        "UPDATE {}.t SET s = 'upd' WHERE id = 5 OR id = 250000",
    ]:
        assert rows(con, sql.format("db")) == rows(con, sql.format("mem")), sql
    for p in predicates + ["s = 'upd'", "x = 1e9"]:
        check(p)
    assert same(rows(con, "SELECT * FROM db.t"), rows(con, "SELECT * FROM mem.t"))


def test_cli_interop(tmp):
    if not VPF_BIN:
        print("  skipped (set VPF_BIN)")
        return
    con = connect()
    con.execute(f"COPY (SELECT range AS id, 'r' || range AS v FROM range(1000)) TO '{tmp}/in.parquet'")
    subprocess.run([VPF_BIN, f"{tmp}/cli.vpf", "import", f"{tmp}/in.parquet", "--name", "events"], check=True)
    con.execute(f"ATTACH '{tmp}/cli.vpf' AS db (TYPE vpf)")
    assert rows(con, "SELECT count(*) FROM db.events") == [(1000,)]
    con.execute("DELETE FROM db.events WHERE id % 2 = 0")
    con.close()
    subprocess.run([VPF_BIN, f"{tmp}/cli.vpf", "export", f"{tmp}/out.parquet"], check=True)
    assert rows(connect(), f"SELECT count(*), min(id) FROM '{tmp}/out.parquet'") == [(500, 1)]

def test_metadata_modes(tmp):
    for mode in ("lazy", "eager"):
        path = f"{tmp}/{mode}.vpf"
        con = connect()
        con.execute(f"ATTACH '{path}' AS db (TYPE vpf, METADATA_MODE '{mode}')")
        con.execute("""
            CREATE TABLE db.t AS SELECT i AS id, 'n' || i AS name,
            [i, NULL] AS items, {'x': i, 's': 'v'} AS details
            FROM range(140000) r(i)
        """)
        assert rows(con, "SELECT COUNT(*), SUM(id) FROM db.t WHERE id BETWEEN 70000 AND 70099") == [(100, 7004950)]
        assert "VPF Pruning" in str(rows(con, "EXPLAIN SELECT * FROM db.t WHERE id = 70001"))
        before = rows(con, "SELECT id, name, items, details FROM db.t WHERE id = 70001")
        con.execute("BEGIN")
        con.execute("UPDATE db.t SET name = 'changed' WHERE id = 70001")
        assert rows(con, "SELECT name FROM db.t WHERE id = 70001") == [("changed",)]
        con.execute("ROLLBACK")
        assert rows(con, "SELECT id, name, items, details FROM db.t WHERE id = 70001") == before
        con.execute("DELETE FROM db.t WHERE id >= 139990")
        assert rows(con, "SELECT COUNT(*) FROM db.t") == [(139990,)]
        con.close()
        with sqlite3.connect(path) as metadata:
            count = metadata.execute("SELECT COUNT(*) FROM snapshot_layouts").fetchone()[0]
            assert (count > 0) == (mode == "eager")
        con = connect()
        con.execute(f"ATTACH '{path}' AS db (TYPE vpf, READ_ONLY, METADATA_MODE 'lazy')")
        assert rows(con, "SELECT COUNT(*) FROM db.t") == [(139990,)]
        con.close()
    expect_error(
        lambda: connect().execute(f"ATTACH '{tmp}/bad.vpf' (TYPE vpf, METADATA_MODE 'bad')"),
        "must be",
    )


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for test in tests:
        tmp = tempfile.mkdtemp()
        try:
            print(test.__name__)
            test(tmp)
        except Exception:  # noqa: BLE001
            failed += 1
            traceback.print_exc()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
