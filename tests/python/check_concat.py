"""Asserts a Parquet file equals a concatenation of slices of other Parquet files.

Usage: check_concat.py actual.parquet src.parquet:offset:len [src.parquet:offset:len ...]
pyarrow compares tables (after unifying dictionary columns) and duckdb compares row multisets.
"""
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

act = sys.argv[1]
parts = []
for spec in sys.argv[2:]:
    path, off, ln = spec.rsplit(":", 2)
    parts.append(pq.read_table(path).slice(int(off), int(ln)))
exp = pa.concat_tables(parts).combine_chunks()
got = pq.read_table(act).combine_chunks()
assert exp.schema.equals(got.schema), f"schema mismatch:\n{exp.schema}\n---\n{got.schema}"
for name in exp.column_names:
    a, b = exp.column(name), got.column(name)
    if pa.types.is_dictionary(a.type):
        a, b = a.cast(a.type.value_type), b.cast(b.type.value_type)
    assert a.equals(b), f"column {name} differs"
pf = pq.ParquetFile(act)
for i in range(pf.num_row_groups):
    pf.read_row_group(i)
con = duckdb.connect()
con.register("exp", exp)
q = "SELECT * FROM {} EXCEPT ALL SELECT * FROM {}"
src = f"read_parquet('{act}')"
for x, y in ((src, "exp"), ("exp", src)):
    diff = con.execute(q.format(x, y)).fetchall()
    assert not diff, f"duckdb diff: {diff[:5]}"
assert con.execute(f"SELECT count(*) FROM {src}").fetchone()[0] == exp.num_rows
