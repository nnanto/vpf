# vpf — mutable virtual Parquet files

A `.vpf` file holds **one table**, the way a SQLite file holds a database. It stores
Parquet data as **immutable pages** plus **metadata** in a single SQLite file, and serves every
snapshot as a **byte-addressable, standards-compliant Parquet file** that is assembled on the
fly. Updates, deletes, appends and schema changes are page-level copy-on-write: only the
pages that actually change are written, and every earlier version stays readable (time travel).

Any Parquet reader can consume a snapshot, either through the `ChunkReader` API (`VirtualFile`)
or after `vpf export`. The test suite checks the output with arrow-rs, PyArrow and DuckDB.

## How it works

```
sales.vpf    one SQLite database (WAL) holding one table:
  blobs        compressed Parquet pages (header + body), never modified once written
  metadata     table, schema versions, snapshots, pages, chunks, row groups,
               buffered batches, segments
```

Page bytes and metadata commit in the same SQLite transaction, so a failed write leaves
nothing behind. A file is a portable unit: copy it (with no writer active, or using
SQLite's backup API) and you have the table and its full history. Several readers and
one writer can use a file at a time.

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
  are served by range from the stored page blobs.
* **Columns have stable ids**. Rename, reorder and drop only change metadata, and a chunk
  keeps belonging to its column across schema versions.

### Snapshot metadata policy

Choose the policy per writer with `--metadata-mode lazy|eager`, the library's
`Store::set_metadata_mode(SnapshotMetadataMode::Eager)`, or DuckDB
`ATTACH 'sales.vpf' AS sales (TYPE vpf, METADATA_MODE 'eager')`.

**Lazy (default)** preserves the existing write costs and resolves virtual layouts
on demand. The C API caches one resolved snapshot per handle. **Eager** additionally
persists each new snapshot's serialized footer and virtual-offset mapping in
`snapshot_layouts`, atomically with the write. Reads use persisted layouts in either
mode, including after reopen; uncached older snapshots remain readable on demand.
Changing the mode affects subsequent writes and does not backfill history.

Appends load only the current snapshot header and schema, derive the new row count
from the parent snapshot, and use an indexed lookup for the next segment position.
In lazy mode, existing row groups, page metadata and buffered payloads are not loaded
on the append path. Eager mode still assembles the full snapshot layout before commit.

Eager shifts whole-snapshot metadata assembly onto writes and retains a layout for
each prepared snapshot, increasing write CPU and storage. Buffered snapshots also
persist their synthesized Parquet pages in the layout, so eager can be especially
expensive for frequent small appends. Stored page BLOBs remain immutable and are
referenced, not duplicated. Footer/layout failures abort the write; transaction
rollback discards uncommitted layouts alongside the corresponding data.

Snapshot ID/row-count and schema lookups do not load row groups or build footers in
either mode. Handle-local resolved-snapshot caches are cleared at write and
transaction boundaries, and current reads resolve the latest committed snapshot
before using a cached entry.

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

Deletes and updates evaluate predicates and rewrite pages one row group at a time,
discarding each group's row masks before processing the next. All groups still commit
atomically as one snapshot; a failure rolls back metadata for the entire operation.
Mask memory is bounded by the largest row group (including synthesized buffered runs),
not the total row count. Snapshot metadata and buffered data are still loaded in memory.

Mutation predicates use VPF-computed page statistics to skip impossible row ranges and
entire row groups, with offset indexes enabling the reader to skip excluded pages.
Pruning supports flat integer, boolean, string/binary and compatible temporal comparisons,
`IS [NOT] NULL`, `[NOT] IN`, `[NOT] BETWEEN`, and boolean combinations. Imported chunk
bounds are not used for pruning. Missing statistics, nested comparisons, floating-point
and decimal comparisons, `LIKE`, column-to-column comparisons and unsupported mappings
fall back to evaluation; pruning never assumes an overlapping range actually matches.

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

# vpf FILE COMMAND ...   (`create` and `import` create FILE; the table is named after its stem)
vpf sales.vpf import data.parquet
vpf sales.vpf import data.parquet --metadata-mode eager # prepare footer/layout on writes
vpf events.vpf create id:int64:required name:string at:timestamp_us amount:decimal(12,2) --codec zstd
vpf docs.vpf create id:int64:required tags:list<string> attrs:map<string,int64> 'pt:struct<x:double!,y:list<int32>>'
vpf sales.vpf append --parquet more.parquet           # --mode auto|buffered|direct
echo '{"id": 1, "name": "x"}' | vpf events.vpf append --json -
vpf sales.vpf delete --where "region = 'EU' AND amount < 10"
vpf docs.vpf delete --where "pt.x > 2 OR tags IS NULL"
vpf sales.vpf update --set "status = 'void', amount = NULL" --where "id IN (3, 5, 8)"
vpf sales.vpf alter add-column level int64 --required --default 0
vpf sales.vpf alter rename-column amount total
vpf sales.vpf alter widen qty int64
vpf sales.vpf alter make-optional id
vpf sales.vpf alter reorder id total level ...
vpf sales.vpf alter drop-column legacy
vpf sales.vpf flush
vpf sales.vpf info | snapshots | schema --at 3 | stats
vpf sales.vpf cat --limit 20 --format table|json|csv --at 5
vpf sales.vpf export out.parquet --at 5
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

let store = Store::open("t.vpf")?;            // one table per file
store.import_file("t", "data.parquet")?;
store.append("t", &record_batch, AppendMode::Auto)?;
store.update_where("t", "flag = TRUE", "id < 10")?;
let vf = store.open_snapshot("t", None)?;   // impl parquet ChunkReader
let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(vf)?.build()?;

store.begin()?;                              // explicit transaction: holds the write lock
store.delete_rows("t", base_snapshot, &[0, 5])?;
store.append("t", &more_rows, AppendMode::Auto)?;
store.commit()?;                             // or rollback(); others see nothing until commit
```

## DuckDB, C API and other engines

* [`vpf-ffi`](crates/vpf-ffi/include/vpf.h) is a small C API over the Arrow C data and
  stream interfaces: open, create, scan (with row ids and statistics-based filter
  pruning), append, update/delete by row id, and transactions. Any engine or language
  with Arrow support can bind to it.
* [`vpf-duckdb`](crates/vpf-duckdb/README.md) is a DuckDB storage extension built on it:
  `ATTACH 't.vpf' (TYPE vpf)`, then plain SQL `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
  `CREATE TABLE [AS]` and transactions. Simple `WHERE` filters skip row groups and pages.

## HTTP and FUSE adapters

Two separate workspace crates expose a directory of `.vpf` files (one table each, named
after the file stem) without exporting Parquet:

* [`vpf-http`](crates/vpf-http/README.md) serves snapshot-specific URLs with HTTP
  byte ranges, for DuckDB's native `httpfs` reader and other HTTP-aware readers.
  Resolve `/files/<name>/latest` once, then query
  `/files/<name>/snapshots/<id>.parquet` for query-wide consistency.
* [`vpf-fuse`](crates/vpf-fuse/README.md) provides a read-only local filesystem
  mount. Open handles pin a snapshot; later opens can see newer data.

```sh
cargo run -p vpf-http -- --dir ./tables --listen 127.0.0.1:8080
cargo test -p vpf-http -p vpf-fuse
```

See each crate's README for DuckDB queries, mounting prerequisites and laptop
instructions. The core `vpf` crate does not depend on either adapter.

## Tests

```
cargo test                                   # Rust-only checks
python -m venv ~/.vpf-venv && ~/.vpf-venv/bin/pip install pyarrow duckdb fsspec
VPF_PYTHON=~/.vpf-venv/bin/python cargo test # also verifies with PyArrow and DuckDB
VPF_FUZZ_SEEDS=200 VPF_FUZZ_STEPS=1000 cargo test --release --test fuzz
cargo test --workspace                       # core, C API and adapter Rust checks
crates/vpf-duckdb/build.sh && VPF_BIN=target/debug/vpf \
  ~/.vpf-venv/bin/python crates/vpf-duckdb/test/test_duckdb.py   # DuckDB extension
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
* DuckDB reads frozen and current snapshots directly through a test-only `fsspec`
  filesystem adapter, whose random-access reads call `VirtualFile::read_range`. No Parquet
  export or filesystem mount is involved. Run it alone with
  `VPF_PYTHON=~/.vpf-venv/bin/python cargo test --test duckdb_direct -- --nocapture`.
  This adapter is for testing, not a production filesystem integration.

## Benchmarks

```
cargo bench --bench parquet_io
cargo bench --bench parquet_io -- read/          # only reader comparisons
cargo bench --bench parquet_io -- write/         # only write comparisons
cargo bench --bench parquet_io -- append_stages/ # direct-append CPU breakdown
cargo bench --bench parquet_io -- append_scaling/ # one-row append vs segment count
cargo bench --bench parquet_io -- update/        # small update vs full rewrite
cargo bench --bench parquet_io -- --test         # correctness/smoke run, without timing
VPF_LARGE_BENCH=1 cargo bench --bench large_mutations -- --test # 1 GiB correctness/smoke run
VPF_LARGE_BENCH=1 cargo bench --bench large_mutations -- --noplot
```

`append_scaling` compares header-only buffered appends with a benchmark-only
whole-snapshot baseline at 1, 100 and 10,000 existing buffered segments. Each timed
append runs in an explicit transaction and rolls back afterward, keeping the fixture
fixed; timings include rollback but exclude transaction start and durable commit.
The baseline uses the same new position index, isolating the cost of loading the
whole snapshot and recounting its rows.

Example warm-cache release measurements (one row, 11 flat columns, lazy metadata):

| Existing buffered segments | Whole-snapshot baseline | Header-only append |
|---|---|---|
| 1 | 309 us | 242 us |
| 100 | 769 us | 271 us |
| 10,000 | 26.6 ms | 249 us |

The header-only path stays approximately flat in this fixture; the 10,000-segment
case is about 107 times faster. These are transaction-path timings, not durable
commit throughput, and will vary with hardware and workload.

The separate `large_mutations` suite generates an **at least 1 GiB on-disk Snappy
file**, using streamed 65,536-row groups and deterministic, incompressible 256-byte
binary payloads. It compares VPF updates with full Parquet rewrites for 100 clustered
rows in one group, one row in every group, and an unclustered bucket predicate that
cannot prune groups. Fixture size, row count and group count are printed on startup.
Each iteration starts from an independent copy of a checkpointed seed store; copying
and syncing that copy are outside the timed mutation. Both updated output and time
travel are checked by streaming comparisons, including unchanged payloads. Allow about **5 GiB of free
temporary disk space** and several minutes; temporary files are removed automatically.
Without `VPF_LARGE_BENCH=1`, this suite skips fixture generation even with `cargo bench`.
These are warm-cache elapsed-time measurements, not a peak-RAM or cold-I/O benchmark.
The byte throughput uses total fixture size for comparison; VPF does not read/rewrite
all those bytes on selective updates. This wide-row fixture does not model billions
of narrow rows.

The Criterion harness uses 80,000 deterministic rows with 11 flat columns, nulls,
dictionary encoding, and SNAPPY/ZSTD compression. It follows the public reader/writer
approach of parquet-rs's
[`arrow_reader`](https://github.com/apache/arrow-rs/blob/main/parquet/benches/arrow_reader.rs)
and [`parquet_round_trip`](https://github.com/apache/arrow-rs/blob/main/parquet/benches/parquet_round_trip.rs)
benchmarks; it is not a verbatim port or a ClickBench query benchmark.

* **Read:** the same imported pages scanned through parquet-rs from in-memory `Bytes`,
  a regular `File`, and a VPF `VirtualFile`, with all columns or just `id` and `name`.
  Reader construction, footer parsing, and decoding are timed; snapshot construction is
  measured separately as `open_snapshot`. Files are already open, and repeated scans
  use the warm OS cache, not cold-disk I/O.
* **Write:** parquet-rs `ArrowWriter` writes a fresh file and calls `sync_data`; VPF
  directly appends to a fresh, empty table using its normal blob sync and SQLite
  transaction. Both use the same data, codec, dictionary/statistics policy, page limits,
  and a single row group, and emit ColumnIndex/OffsetIndex page indexes. Their encoding
  pipelines and metadata work differ. SQLite retains the store's normal
  WAL/`synchronous=NORMAL` durability policy.
  `vpf_direct_append` uses the production Arrow fast path for this fixture.
  `vpf_physical_append` is a benchmark-only reproduction of the previous physical
  conversion/per-column encoding path using the same transaction APIs, settings,
  data and fresh destinations; its complete output is also verified before timing.
* **Import:** VPF stores already encoded pages. This is measured separately and is
  **not** an encoding/write-speed comparison. As on current `main`, import also decodes
  the chunks and computes its own page statistics.
* **Append stages:** separately measures Arrow-to-physical conversion, copying the
  converted rows into a row group, and encoding the final pages. These CPU-only
  measurements exclude SQLite and blob I/O, include result destruction, and are
  diagnostic rather than an exact additive decomposition of the end-to-end append.
  These are diagnostics for the physical compatibility path, not the Arrow fast
  path. Compatible direct appends no longer materialize physical-column vectors or
  copy them for row-group splitting.
* **Update:** changes `name` for one row or 100 clustered rows in the same 80,000-row
  file. VPF evaluates the predicate and rewrites one page of one column, preserving
  the original snapshot. The parquet-rs baseline streams and decodes the original
  file, evaluates the same predicate, applies the assignment with Arrow, and writes
  and syncs a complete replacement file. Both start with a fresh destination/base
  snapshot per iteration; importing the base into VPF is outside the timed update.
  Full output equality, affected-row counts, VPF's one-page rewrite and chunk reuse,
  and historical snapshot readability are checked before timing. Throughput here
  counts **updated rows**, not scanned rows. These are warm-cache, clustered updates,
  not a claim about arbitrary predicates or scattered updates.

Fixture generation, file/store/table creation, and temporary-directory cleanup are
outside the timed write operations. Each iteration gets a fresh destination, so store
growth and old snapshots do not accumulate. Full output equality is checked before
timing, including required page-index loading and the presence of both index types for
every column; timed scans use the reader's default page-index policy and check row counts.
Buffered append, deletes, nested data, cold-cache
reads, and concurrent workloads are not covered. Criterion reports timings and row
throughput, and saves baselines under `target/criterion`.

Compatible flat direct appends match columns by name, cast to the stored Arrow
types, fill missing nullable fields, and encode each row group once with ArrowWriter
using the stored Parquet schema. The resulting column pages are extracted from one
in-memory file, and page statistics are computed directly from Arrow slices.
The stored Parquet type tree must exactly match Arrow's mapping: nested layouts,
INT96, noncanonical decimal representations, dictionary-typed target schemas,
missing physical defaults and out-of-precision decimal values retain the physical
compatibility path. Actual validation/encoding errors are propagated, not retried
as a fallback. Buffered appends, flushes and page-level mutations also retain the
physical path. That path uses direct primitive conversion and shared Arrow
string/binary buffers, and borrows whole row groups when possible. Both append paths
retain the existing maximum of 1,048,576 rows per row group.

The separate [`benches/analytical.py`](benches/analytical.py) suite runs identical
analytical SQL against native SQLite, DuckDB reading Parquet and the DuckDB VPF
extension at 10K, 100K, 1M and 10M rows. It covers aggregations, filters, grouping,
top-N and distinct counts, with full-table equality checks before timing. This is
a single-threaded, warm-cache, unindexed SQLite comparison; it is not ClickBench.

Recorded measurements and reproduction commands are in [BENCHMARKS.md](BENCHMARKS.md).
