"""Checks a file equals `src` minus rows matching any of the given SQL predicates.

Usage: check_delete.py actual.parquet src.parquet pred [pred ...]
The predicates are evaluated by DuckDB (NULL = no match), cross-validating vpf's evaluator.
Also checks that filtered DuckDB queries (which use row group statistics) agree.
"""
import sys

import duckdb
import pyarrow.parquet as pq

act, src, preds = sys.argv[1], sys.argv[2], sys.argv[3:]
con = duckdb.connect()
cond = " AND ".join(f"NOT COALESCE(({p}), false)" for p in preds) or "true"
exp_rel = f"(SELECT * FROM read_parquet('{src}') WHERE {cond})"
got_rel = f"read_parquet('{act}')"
q = "SELECT * FROM {} EXCEPT ALL SELECT * FROM {}"
for x, y in ((exp_rel, got_rel), (got_rel, exp_rel)):
    diff = con.execute(q.format(x, y)).fetchall()
    assert not diff, f"duckdb diff: {diff[:5]}"
n_exp = con.execute(f"SELECT count(*) FROM {exp_rel}").fetchone()[0]
assert con.execute(f"SELECT count(*) FROM {got_rel}").fetchone()[0] == n_exp
# Row order is preserved.
exp_ids = [r[0] for r in con.execute(f"SELECT id FROM {exp_rel} ORDER BY id").fetchall()]
t = pq.read_table(act)
assert t.column("id").to_pylist() == exp_ids, "row order differs"
pf = pq.ParquetFile(act)
for i in range(pf.num_row_groups):
    pf.read_row_group(i)
# Stats-driven pruning must not lose rows.
if exp_ids:
    lo, hi = exp_ids[len(exp_ids) // 3], exp_ids[2 * len(exp_ids) // 3]
    for w in (f"id BETWEEN {lo} AND {hi}", f"id = {lo}", f"id > {hi}", f"id < {lo}"):
        a = con.execute(f"SELECT count(*) FROM {got_rel} WHERE {w}").fetchone()[0]
        b = con.execute(f"SELECT count(*) FROM {exp_rel} WHERE {w}").fetchone()[0]
        assert a == b, (w, a, b)
