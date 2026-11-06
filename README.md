# vpf — mutable virtual Parquet files

`vpf` stores Parquet data as **immutable pages** plus **SQLite metadata**, and serves every
snapshot as a **byte-addressable, standards-compliant Parquet file** that is assembled on the
fly. Updates, deletes, appends and schema changes are page-level copy-on-write: only the
pages that actually change are written, and every earlier version stays readable (time travel).

Any Parquet reader can consume a snapshot, either through the `ChunkReader` API (`VirtualFile`)
or after `vpf export`. The test suite checks the output with arrow-rs, PyArrow and DuckDB.

## How it works

```
store/
  meta.db    SQLite (WAL): files, schema versions, snapshots, pages, chunks,
             row groups, buffered batches, segments
  data.bin   append-only blob log of compressed Parquet pages (header + body)
```

* **Pages** are stored exactly as found in the source file (compressed, with their Thrift
  header), so importing a file and reading it back returns the original page bytes.
* A **chunk** is an ordered list of page ids plus column-chunk metadata (codec, encodings,
  statistics). A **row group** maps each leaf (column id, leaf index) to a chunk id.
* **Nested columns** (structs, lists, maps, any nesting) are one top-level column with one
  chunk per Parquet leaf. Every stored page starts at a record boundary (repetition
  level 0) and records its exact row count, so deletes and updates address pages by row
  range just as they do for flat columns.
* A **snapshot** is the list of visible **segments** (row groups or buffered batches), stored
  with `added_in` / `removed_in` snapshot ids. A snapshot never changes after it is
  committed.
* **Reading** a snapshot (`Store::open_snapshot`) builds a `VirtualFile`. It assigns virtual
  offsets to every page, rewrites each page header only where needed (for example dictionary
  offsets), synthesizes Parquet for buffered batches, and generates a fresh footer. Reads
  are served by range from the blob log.
* **Columns have stable ids**. Rename, reorder and drop only change metadata, and a chunk
  keeps belonging to its column across schema versions.

### Operations

| Operation | What is written |
|---|---|
| `import` | Every page of the source file, verbatim. |
| `append` (buffered) | One SQLite row with the rows in a compact columnar encoding. The rows are visible immediately as a synthesized row group. Synthesized row groups are cached. |
| `append` (direct, or auto with ≥ 65,536 rows) | A new row group of freshly encoded pages. |
| `flush` | Converts runs of buffered batches into stored row groups. |
| `delete WHERE p` | Untouched pages are reused by id. Each touched page is decoded, filtered and re-encoded in PLAIN, using the dictionary page from the original chunk. Row groups where every row matches are dropped. Batches are filtered. If no row matches, no snapshot is created. |
| `update SET c = lit WHERE p` | Only the touched pages of the assigned columns are rewritten. Every other chunk is reused. |
| `alter` add column | One constant chunk per distinct row-group size, shared between those row groups. |
| `alter` drop / rename / reorder | Metadata only. |
| `alter` widen (INT32→INT64, FLOAT→DOUBLE) and make-optional | Only the affected column's chunks are re-encoded. |
| time travel | `--at <snapshot>` on any read command. |

Buffered batches are stored conformed to the schema in effect when they were written. They
are adapted to newer schemas when read (defaults for added columns, widening, nullability).
Every write runs in one SQLite transaction. Page blobs are appended and synced before the
metadata commit, so a crash leaves at most unreferenced bytes in the blob log.

## Trade-offs and limitations

The rule behind these choices: **nothing may compromise basic reads or writes.** Anything
that would risk producing an invalid file, or that some readers might mis-handle, is left
out.

* **Nested types** (struct, list, map, legacy 2-level lists, bare repeated fields) are fully
  supported for import, read, append, delete, predicates on struct fields (`s.a > 3`),
  `IS [NOT] NULL` on any column, and alter add/drop/rename/reorder/make-optional. The
  limits are:
  * `UPDATE` can only assign `NULL` to a nested column, and only when the column is
    optional. There is no literal syntax for nested values.
  * Lists and maps cannot be compared in predicates, apart from `IS [NOT] NULL`.
  * `widen` is not supported on nested columns.
  * An added nested column must be optional, with a NULL default.
  * Imported chunks whose records span V1 data pages (allowed, but no common writer does
    it) are decoded and re-encoded at import, so every stored page starts a record. Their
    page bytes are therefore not verbatim.
  * Appended Arrow data is written with the stored Parquet schema. The levels are then
    checked to read back identically, and the append is rejected if they do not. Exotic
    legacy layouts can never be silently mis-encoded.
* **Statistics and page indexes.** Every data page stores its own statistics (min/max,
  null count, NaN count), computed by vpf from the decoded values. Every virtual file
  carries an **OffsetIndex** for each column chunk, and a **ColumnIndex** with a boundary
  order when all of the chunk's pages have statistics. Both are written at the start of
  the footer.
  * Statistics are recomputed whenever pages change. After a delete or update, a chunk's
    statistics are merged from its page statistics, so they stay exact, null count
    included. Unchanged pages reuse their stored statistics.
  * Import decodes each chunk once to compute page statistics. The source file's page
    statistics and column indexes are never trusted. Source chunk statistics are kept
    when present.
  * Values follow the Parquet column order: signed or unsigned integers; IEEE 754 total
    order for FLOAT, DOUBLE and FLOAT16, with NaNs only counted; signed decimals;
    bytewise order for strings and binary.
  * Byte-array bounds longer than 64 bytes are truncated (as parquet-rs does). The min
    becomes a prefix. The max is incremented at its last byte, or at its last character
    for UTF-8 values.
  * Some chunks get no ColumnIndex, though their OffsetIndex is always emitted:
    * INT96 and other types without a defined order;
    * chunks with a NaN-only page, because readers disagree on how NaN bounds compare;
    * pages that cannot be decoded for statistics at import (LZO);
    * pages from stores created before page statistics existed. Those stores are
      upgraded in place when opened.
  * When a chunk lacks page statistics, deletes fall back to keeping min/max as inexact
    bounds and dropping counts, and updates drop the statistics. Readers never see stale
    bounds.
* **Bloom filters are not emitted.**
* **Rewritten pages are PLAIN encoded,** even inside dictionary-encoded chunks. Mixing
  dictionary and plain pages in one chunk is valid Parquet. Readers fall back exactly as
  they do when a writer's dictionary overflows.
* **LZO** files can be imported and read, but pages are never re-encoded with LZO. Chunks
  that are rewritten use the file's fallback codec (SNAPPY).
* **`UPDATE SET` takes literals only** (no expressions), and the predicate language is a
  small SQL subset:
  * comparisons, `IS [NOT] NULL`, `[NOT] IN`, `[NOT] BETWEEN`, `[NOT] LIKE`
  * `AND`, `OR`, `NOT`
  * double-quoted identifiers
* **Widening** is limited to the lossless cases INT32 → INT64 (signed or plain) and
  FLOAT → DOUBLE.
* **No compaction or garbage collection.** Old snapshots and orphaned pages stay in the
  blob log forever. Long chains of small batches or small row groups are not merged, apart
  from what `flush` does.
* The `ARROW:schema` metadata of imported files (time zones, dictionary types, and so on)
  is kept and updated by `alter`. `pandas` metadata is dropped after the first schema
  change.

## CLI

```
cargo build --release
export VPF_STORE=./mystore            # or --store DIR

vpf import sales data.parquet
vpf create events id:int64:required name:string at:timestamp_us amount:decimal(12,2) --codec zstd
vpf create docs id:int64:required tags:list<string> attrs:map<string,int64> 'pt:struct<x:double!,y:list<int32>>'
vpf append sales --parquet more.parquet           # --mode auto|buffered|direct
echo '{"id": 1, "name": "x"}' | vpf append events --json -
vpf delete sales --where "region = 'EU' AND amount < 10"
vpf delete docs --where "pt.x > 2 OR tags IS NULL"
vpf update sales --set "status = 'void', amount = NULL" --where "id IN (3, 5, 8)"
vpf alter sales add-column level int64 --required --default 0
vpf alter sales rename-column amount total
vpf alter sales widen qty int64
vpf alter sales make-optional id
vpf alter sales reorder id total level ...
vpf alter sales drop-column legacy
vpf flush sales
vpf ls | snapshots sales | schema sales --at 3 | stats sales
vpf cat sales --limit 20 --format table|json|csv --at 5
vpf export sales out.parquet --at 5
```

Column types for `create` and `add-column`:
* `bool`, `int32`, `int64`, `float`, `double`
* `string`, `binary`, `date`
* `timestamp_ms`, `timestamp_us`, `timestamp_ns`
* `decimal(p,s)`
* `list<T>`, `map<K,V>`, `struct<name:T,...>`, nested freely. A `!` after a child type
  makes that child non-null (map keys are always non-null). A column def is
  `name:type[:required|:optional]`.

## Library

```rust
use vpf::store::Store;
use vpf::ops::append::AppendMode;

let store = Store::open("mystore")?;
store.import_file("t", "data.parquet")?;
store.append("t", &record_batch, AppendMode::Auto)?;
store.update_where("t", "flag = TRUE", "id < 10")?;
let vf = store.open_snapshot("t", None)?;   // impl parquet ChunkReader
let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(vf)?.build()?;
```

## Tests

```
cargo test                                   # Rust-only checks
python -m venv ~/.vpf-venv && ~/.vpf-venv/bin/pip install pyarrow duckdb
VPF_PYTHON=~/.vpf-venv/bin/python cargo test # also verifies with PyArrow and DuckDB
VPF_FUZZ_SEEDS=200 VPF_FUZZ_STEPS=1000 cargo test --release --test fuzz
```

What the suites cover:
* Byte-exact round trips of imported files, for every codec and for data page v1 and v2.
* Dictionary and plain chunks.
* Nested files from arrow-rs and PyArrow (structs, lists, maps, list<list>, required
  structs, tiny pages, non-compliant list names), legacy 2-level lists, and records split
  across pages. Each leaf is checked by decoding page by page and re-encoding.
* Every operation, checked against an Arrow reference model.
* Time travel to every historical snapshot.
* CLI workflows.
* A randomized fuzz test that mixes appends, deletes, updates, schema changes and flushes,
  on a schema with flat and nested columns.
