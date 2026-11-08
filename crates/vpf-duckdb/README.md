# vpf-duckdb — use a `.vpf` file as a DuckDB database

```sql
LOAD 'vpf.duckdb_extension';
ATTACH 'sales.vpf' AS sales (TYPE vpf);           -- creates the file if missing

CREATE TABLE sales.orders (id BIGINT NOT NULL, item VARCHAR, tags VARCHAR[]);
INSERT INTO sales.orders SELECT range, 'item' || range, ['a'] FROM range(1000);
UPDATE sales.orders SET item = 'gone' WHERE id < 10;
DELETE FROM sales.orders WHERE id % 2 = 0;
SELECT count(*) FROM sales.orders;

BEGIN; DELETE FROM sales.orders; ROLLBACK;        -- transactions
ATTACH 'events.vpf' AS ev (TYPE vpf);
CREATE TABLE ev.events AS SELECT * FROM 'events.parquet';
COPY sales.orders TO 'orders.parquet';
```

A `.vpf` file holds one table, so an attached file is a database with one table in its
`main` schema. Every DuckDB type that maps to Arrow/Parquet works, including DECIMAL,
TIMESTAMPTZ, LIST, STRUCT and MAP. Files written by the extension, the `vpf` CLI and the
Rust library are interchangeable.

Options: `ATTACH 'f.vpf' (TYPE vpf, READ_ONLY)` and `(TYPE vpf, CODEC 'zstd')` (codec for
new tables; default snappy). `METADATA_MODE 'lazy'` (default) resolves snapshot
layouts on demand; `METADATA_MODE 'eager'` persists each newly written snapshot's
footer and offset mapping in the same transaction:

```sql
ATTACH 'sales.vpf' AS sales (TYPE vpf, METADATA_MODE 'eager');
```

This is a per-attachment write policy, not a permanent file setting. Existing
persisted layouts are read in either mode; older uncached snapshots remain readable.
Eager trades extra write CPU/storage for cheaper read setup and retains each prepared
snapshot for time travel. Buffered snapshots also persist synthesized Parquet pages.
Schema/row-count lookups avoid full snapshot assembly in both modes, and each C API
handle caches one resolved snapshot with invalidation at write/transaction boundaries.

## How it works

The extension is a DuckDB storage extension (like the SQLite and Postgres extensions) in
one C++ file, [`src/vpf_extension.cpp`](src/vpf_extension.cpp). It calls the VPF C API
([`vpf-ffi`](../vpf-ffi/include/vpf.h)), which moves data as Arrow C arrays and streams.
DuckDB plans and executes the query; VPF stores the rows.

* **Scans** stream the snapshot with projection pushdown. DuckDB's `rowid` is the row's
  position in the snapshot.
* **Filter pushdown**: `col = | <> | < | <= | > | >= constant` (also prepared parameters,
  `BETWEEN`, `AND` and `IS [NOT] NULL`) on numeric, decimal, boolean, date, timestamp,
  string and blob columns are sent to `vpf_scan`, which skips row groups and pages whose
  statistics rule them out. DuckDB still applies every filter, so results are always exact;
  other predicates (`OR`, `LIKE`, functions of columns) just scan everything. `EXPLAIN`
  shows the pushed filters as `VPF Pruning`. A point lookup reads about one page (~20k rows).
* **INSERT** buffers up to 1M rows per call to `vpf_append`.
* **UPDATE / DELETE** collect the row ids DuckDB selects and apply them in one
  `vpf_update_rows` / `vpf_delete_rows` against the snapshot they were read from.
  With `UPDATE ... FROM`, the first matching source row wins.
* **Transactions**: a DuckDB transaction reads one pinned snapshot (repeatable reads).
  Its first write takes the file's write lock (other writers wait up to 30 s) and
  commit/rollback ends it. Other connections and processes see only committed data.
  Conflicting writers get a `TransactionException`.

Not supported (clear errors): `RETURNING`, `ON CONFLICT`, ALTER/DROP, indexes, more than
one table per file. Constraints other than NOT NULL are not enforced.

## Build

Needs a C++17 compiler, Rust, git and python3. The extension must be built for the exact
DuckDB version that loads it (default v2.0.0):

```sh
crates/vpf-duckdb/build.sh                         # -> crates/vpf-duckdb/build/vpf.duckdb_extension
DUCKDB_VERSION=v2.0.0 DUCKDB_PLATFORM=linux_amd64 crates/vpf-duckdb/build.sh
```

The script clones DuckDB's headers (or uses `DUCKDB_SRC`), builds `libvpf_ffi.a`, links
both into one shared library with Rust/SQLite symbols hidden, and appends DuckDB's
extension footer. Check your platform with `PRAGMA platform`.

The extension is unsigned, so enable `allow_unsigned_extensions`:

```sh
duckdb -unsigned -c "LOAD 'crates/vpf-duckdb/build/vpf.duckdb_extension'; ATTACH 't.vpf' (TYPE vpf); ..."
```

In Python, load `duckdb` with global symbols first (the extension links against DuckDB's
C++ symbols, which the Python module otherwise keeps private):

```python
import os, sys
sys.setdlopenflags(os.RTLD_GLOBAL | os.RTLD_NOW)
import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'crates/vpf-duckdb/build/vpf.duckdb_extension'")
```

## Test

```sh
VPF_BIN=target/debug/vpf python crates/vpf-duckdb/test/test_duckdb.py
```

Covers CRUD on flat and nested columns, NOT NULL, reopen and read-only attach,
transactions and isolation between connections, `UPDATE ... FROM`, COPY, CTAS, a type
round trip against an in-memory DuckDB table, concurrent writers, and files shared with
the CLI (`VPF_BIN`).

## Analytical benchmark

[`benches/analytical.py`](../../benches/analytical.py) compares this extension with
DuckDB reading Parquet and native SQLite on identical orders tables at multiple
sizes. See [`BENCHMARKS.md`](../../BENCHMARKS.md) for results, methodology and
reproduction commands.
Pass `--writes` to also measure durable fresh writes, appends, updates and deletes.
The read suite includes point/range lookups and simple equality, NULL, LIKE and OR
filters in addition to analytical queries. `--metadata-mode lazy|eager` compares
the two snapshot-preparation policies.
