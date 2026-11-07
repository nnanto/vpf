"""End-to-end tests for the VPF DuckDB extension.

Run: python crates/vpf-duckdb/test/test_duckdb.py [path/to/vpf.duckdb_extension]
Needs duckdb matching the extension's DuckDB version (see build.sh).
"""

import os
import shutil
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
