"""Checks the result of `DELETE WHERE id >= 100 AND id < 160` then `SET <col> = NULL WHERE id < 50`.

Usage: check_nested_mutation.py source.parquet actual.parquet [col]
"""
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

src, act = sys.argv[1], sys.argv[2]
col = sys.argv[3] if len(sys.argv) > 3 else ""
t = pq.read_table(src)
ids = t["id"]
t = t.filter(pc.invert(pc.and_(pc.greater_equal(ids, 100), pc.less(ids, 160))))
if col:
    i = t.schema.get_field_index(col)
    c = t[col].combine_chunks()
    mask = pc.less(t["id"], 50).combine_chunks()
    nulled = pc.if_else(mask, pa.nulls(len(c), c.type), c)
    t = t.set_column(i, t.schema.field(i), nulled)
b = pq.read_table(act)
assert t.schema.equals(b.schema), f"{t.schema}\n---\n{b.schema}"
assert t.equals(b), "mismatch"
import duckdb
n = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{act}')").fetchone()[0]
assert n == t.num_rows, (n, t.num_rows)
print("ok", n)
