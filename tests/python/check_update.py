"""Checks a file equals `src` after applying statements with DuckDB.

Usage: check_update.py actual.parquet src.parquet stmt [stmt ...]
Each stmt is either "DELETE WHERE <pred>" or "UPDATE SET <assignments> WHERE <pred>".
"""
import sys

import duckdb
import pyarrow.parquet as pq

act, src, stmts = sys.argv[1], sys.argv[2], sys.argv[3:]
con = duckdb.connect()
con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{src}')")
for s in stmts:
    if s.startswith("DELETE "):
        con.execute(f"DELETE FROM t {s[len('DELETE '):]}")
    elif s.startswith("UPDATE "):
        con.execute(f"UPDATE t {s[len('UPDATE '):]}")
    else:
        raise SystemExit(f"bad stmt {s}")
got = f"read_parquet('{act}')"
q = "SELECT * FROM {} EXCEPT ALL SELECT * FROM {}"
for x, y in (("t", got), (got, "t")):
    sub = q.format(x, y)
    n = con.execute(f"SELECT count(*) FROM ({sub})").fetchone()[0]
    diff = con.execute(f"SELECT CAST(id AS VARCHAR) FROM ({sub}) LIMIT 5").fetchall() if n else []
    assert not diff, f"duckdb diff ({x} minus {y}): {diff[:3]}"
assert con.execute(f"SELECT count(*) FROM {got}").fetchone()[0] == con.execute("SELECT count(*) FROM t").fetchone()[0]
ids = [r[0] for r in con.execute("SELECT id FROM t ORDER BY id").fetchall()]
assert pq.read_table(act).column("id").to_pylist() == ids, "row order differs"
pf = pq.ParquetFile(act)
for i in range(pf.num_row_groups):
    pf.read_row_group(i)
