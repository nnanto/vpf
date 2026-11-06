"""Asserts a vpf export carries page indexes and that statistics-based pruning is sound.

Usage: check_page_index.py file.parquet expect_column_index(0|1)

pyarrow must see an offset index on every column chunk (and a column index if expected);
filtered DuckDB queries, which prune with statistics, must agree with pyarrow filtering in
memory.
"""
import sys

import duckdb
import pyarrow.compute as pc
import pyarrow.parquet as pq

path, expect_ci = sys.argv[1], sys.argv[2] == "1"
md = pq.ParquetFile(path).metadata
for rg in range(md.num_row_groups):
    for c in range(md.num_columns):
        col = md.row_group(rg).column(c)
        assert col.has_offset_index, (rg, c, "offset index")
        if expect_ci and col.physical_type != "INT96":
            assert col.has_column_index, (rg, c, col.path_in_schema, "column index")
t = pq.read_table(path)
con = duckdb.connect()
names = set(t.column_names)
preds = []
if "id" in names:
    n = t.num_rows
    for lo, hi in [(0, 10), (n // 3, n // 3 + 50), (n - 5, n + 5), (-10, -1), (10**9, 10**9 + 1)]:
        preds.append((f"id BETWEEN {lo} AND {hi}", (pc.field("id") >= lo) & (pc.field("id") <= hi)))
if "name" in names:
    for v in ["name-1", "name-50", "changed", "zzz"]:
        preds.append((f"name = '{v}'", pc.field("name") == v))
        preds.append((f"name > '{v}'", pc.field("name") > v))
if "score" in names:
    preds.append(("score < 30", pc.field("score") < 30))
for sql, expr in preds:
    want = t.filter(expr).num_rows
    got = con.execute(f"SELECT count(*) FROM read_parquet('{path}') WHERE {sql}").fetchone()[0]
    assert got == want, (sql, got, want)
