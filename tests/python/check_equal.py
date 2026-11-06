"""Asserts two Parquet files hold identical data according to pyarrow and duckdb.

Usage: check_equal.py expected.parquet actual.parquet [expected_rows]
"""
import sys

import duckdb
import pyarrow.parquet as pq

exp, act = sys.argv[1], sys.argv[2]
pf = pq.ParquetFile(act)
_ = pf.metadata, pf.schema_arrow
a = pq.read_table(exp)
b = pq.read_table(act)
assert a.schema.equals(b.schema), f"schema mismatch:\n{a.schema}\n---\n{b.schema}"
assert a.equals(b), "pyarrow table mismatch"
# Row-group-at-a-time and column-projected reads exercise random access.
for i in range(pf.num_row_groups):
    pf.read_row_group(i)
if b.num_columns:
    pq.read_table(act, columns=[b.column_names[-1]])
con = duckdb.connect()
q = "SELECT * FROM read_parquet('{}') EXCEPT ALL SELECT * FROM read_parquet('{}')"
for x, y in ((exp, act), (act, exp)):
    sub = q.format(x, y)
    n = con.execute(f"SELECT count(*) FROM ({sub})").fetchone()[0]
    diff = con.execute(f"SELECT CAST(id AS VARCHAR) FROM ({sub}) LIMIT 5").fetchall() if n else []
    assert not diff, f"duckdb diff {x} vs {y}: {diff[:5]}"
cnt = con.execute(f"SELECT count(*) FROM read_parquet('{act}')").fetchone()[0]
assert cnt == a.num_rows, (cnt, a.num_rows)
if len(sys.argv) > 3:
    assert cnt == int(sys.argv[3]), (cnt, sys.argv[3])
print("ok", cnt)
