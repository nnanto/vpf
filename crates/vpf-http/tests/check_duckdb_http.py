"""Exercise DuckDB's native HTTP Parquet reader, never a Python filesystem adapter."""

import json
import os
import sys
import urllib.request

import duckdb


base, old_id = sys.argv[1], int(sys.argv[2])
con = duckdb.connect()
extension = os.environ.get("VPF_DUCKDB_HTTPFS")
if extension:
    # DuckDB verifies extension signatures; never enable allow_unsigned_extensions.
    con.execute("LOAD '" + extension.replace("'", "''") + "'")
else:
    con.execute("LOAD httpfs")
con.execute("SET threads = 4")


def check(snapshot_id, current):
    url = f"{base}/files/f/snapshots/{snapshot_id}.parquet"
    expected = []
    for i in range(30 if current else 0, 1200 if current else 1000):
        name = None if i % 5 == 0 else f"name-{i % 97}"
        if current and 100 <= i < 150:
            name = "changed"
        expected.append((i, None if i % 7 == 0 else i * 3, name,
                         None if i % 11 == 0 else i * 1.5,
                         None if i % 3 == 0 else i % 2 == 0))
    got = con.execute(
        "SELECT id, i32, name, score, flag FROM read_parquet(?) ORDER BY id", [url]
    ).fetchall()
    assert got == expected, f"snapshot {snapshot_id}: data mismatch"
    got = con.execute(
        "SELECT id, name FROM read_parquet(?) WHERE id >= 95 AND id < 155 ORDER BY id",
        [url],
    ).fetchall()
    assert got == [(row[0], row[2]) for row in expected if 95 <= row[0] < 155]


check(old_id, False)
print("mutate", flush=True)
new_id = int(sys.stdin.readline())
with urllib.request.urlopen(f"{base}/files/f/latest") as response:
    assert json.load(response)["snapshot_id"] == new_id
check(old_id, False)
check(new_id, True)
con.close()
