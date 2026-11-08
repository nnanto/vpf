# VPF benchmark results

## Latest: analytical SQL across SQLite, Parquet and VPF

The analytical suite runs on **`f931b1f` (latest main fetched for this run)**,
including the DuckDB filter-pushdown implementation. It compares **10,000,
100,000, 1,000,000 and 10,000,000 identical rows** in an ordinary SQLite table,
a Snappy Parquet file and a VPF file imported from that same Parquet file.

See [the per-query, size and storage summary](benches/results/analytical.md)
and [raw samples, exact SQL, results, query plans and environment](benches/results/analytical.json).
Earlier benchmarks below measure decoding or mutations, not analytical SQL.

Across the seven non-count workloads, VPF's summed median latency is **20.7 ms,
35.7 ms, 199.8 ms and 1,799.3 ms** at the four sizes. VPF is about **2.1x slower
than SQLite at 10K rows**, then **2.5x, 5.0x and 7.1x faster** at 100K, 1M and
10M rows. DuckDB on Parquet is faster than VPF at every size, by **2.2-2.6x** on
this workload score. At 10M rows, storage is **283.32 MiB for SQLite, 105.37 MiB
for Parquet and 110.06 MiB for VPF**.

Selective lookup and count remain notable gaps: the 100-row ID range takes
**95.1 ms on VPF versus 5.5 ms on Parquet** at 10M rows, despite confirmed VPF
pruning. `COUNT(*)` takes **96.5 ms versus 2.8 ms**. These measurements do not
establish the cause of that overhead.

### Workload and methodology

The deterministic orders table has seven columns: sorted `id`, interleaved string
`region` (32 values), `product_id` (4,096 values), integer `day` (365 values),
`quantity`, `revenue_cents` and nullable `discount_cents`. All numeric values are
integers, so sums and result comparisons are exact across engines. The generated
columns are correlated synthetic data, not a production dataset.

Eight SQL workloads cover `COUNT(*)`, multi-column full-scan aggregation, a
100-row contiguous ID range, interleaved filtered aggregation, regional grouping,
filtered daily grouping, top-20 products by revenue and `COUNT(DISTINCT product_id)`.
The identical SQL is executed through **native Python SQLite** and **DuckDB 2.0.0**
for both `read_parquet` and the VPF storage extension. VPF is scanned directly:
there is no timed export or materialization into a DuckDB table. This compares
engine-plus-storage combinations, not storage formats in isolation.

Each query has two warm-ups and seven measured executions. Tables report median
elapsed milliseconds, including SQL planning, execution and fetching the complete
result, with persistent connections and warm OS caches. Query/engine order is
shuffled reproducibly each repetition. Setup, import, attachment, full-table
verification, query-plan collection and cleanup are excluded. Every row is checked
for equality across the three fixtures before timing, and every measured result
is compared with SQLite's result. Raw samples are retained; these are medians,
not confidence intervals.

DuckDB uses **one thread** and a 512 MiB memory limit because VPF's scan currently
supports one thread. SQLite has a 64 MiB page-cache budget, mmap disabled, and
**no indexes, including no primary-key index**; this is an unindexed analytical
baseline, not an indexed point-lookup comparison. Snappy Parquet row groups target
65,536 rows, and VPF reuses the imported pages. Interleaved region/day predicates
cannot prune whole groups; the sorted ID range can. The recorded `EXPLAIN` plans
show the actual pushed filters. File sizes include any SQLite WAL/SHM sidecars
present while the read connections are open, but exclude temporary query spill
files. Each VPF fixture has only its imported snapshot, with no mutation history.

The size summary sums the seven non-`COUNT(*)` query medians with equal weights;
it is not a single timed transaction or a universal throughput score. `COUNT(*)`
is shown separately because metadata/rowid fast paths are not full column scans.
This shared-environment, warm-cache run does not measure cold-I/O, concurrent
readers, peak memory, joins, indexed SQLite, multithreaded DuckDB or mutations.

### Reproduction

```bash
# Use an isolated Python environment; the DuckDB version must match the extension.
python3 -m venv /tmp/vpf-analytics-venv
/tmp/vpf-analytics-venv/bin/python -m pip install duckdb==2.0.0
DUCKDB_VERSION=v2.0.0 crates/vpf-duckdb/build.sh
cargo build --release --bin vpf

/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --output benches/results/analytical.json

# Smaller correctness/reporting smoke run:
/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --sizes 1000 10000 --repeats 2 --warmups 1 --output /tmp/analytical-smoke.json
```

`--sizes`, `--repeats`, `--warmups`, `--extension` and `--vpf-bin` allow explicit
overrides. Fixtures are built and removed one size at a time. Results are saved
after each completed size, with Markdown tables beside the JSON output.

## Earlier: 1 GiB multi-row-group mutations

Recorded on **2026-11-07**, with the `large_mutations` benchmark added on top of
`b132172`. The Snappy input is **1,074,285,061 bytes** on disk, with **3,932,160 rows
in 60 row groups** of 65,536 rows. Columns are sorted `id`, an interleaved `bucket`
(`id % 1024`), `name`, and a deterministic pseudo-random 256-byte binary `payload`.
The payload is deliberately incompressible, so the target measures actual file size,
not uncompressed Arrow size. Generation and verification stream bounded batches.

```bash
VPF_LARGE_BENCH=1 cargo bench --bench large_mutations -- --test
VPF_LARGE_BENCH=1 cargo bench --bench large_mutations -- \
  --noplot --save-baseline large-mutations-1gib-synced
```

All six smoke cases passed. Timing uses 10 samples, flat sampling, a 1 s warm-up and
a 5 s measurement target; expensive cases extend collection to obtain 10 samples.
Times below are Criterion mean estimates and 95% confidence intervals, in milliseconds.

| Workload | Rows changed | Groups touched | VPF update (ms) | VPF 95% CI (ms) | Full rewrite (ms) | Rewrite 95% CI (ms) | Speedup |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 clustered rows | 100 | 1 | 16.606 | 16.359-16.900 | 3227.841 | 3207.823-3246.128 | 194.4x |
| One row per group | 60 | 60 | 293.100 | 291.355-294.923 | 3326.784 | 3293.035-3366.598 | 11.4x |
| Unclustered `bucket = 0` | 3840 | 60 | 749.935 | 747.452-752.814 | 3215.366 | 3186.616-3247.888 | 4.3x |

Both paths assign `name = 'changed'`. Clustered predicates can prune most ID pages;
the bucket predicate cannot prune row groups because every group contains every bucket.
VPF preserves payload chunks and all other unassigned columns by reference. Full
rewrites decode and re-encode every column, including the binary payload.

Each VPF iteration receives an independent copy of a closed, checkpointed seed store.
**Copying and syncing the copied metadata/blob files are excluded from timing**:
otherwise the mutation's blob sync also flushes the unrelated 1 GiB setup copy.
The measured VPF operation includes its normal blob sync and SQLite commit; full
rewrites include output sync. Their durability policies still differ as described below.
Before timing each workload, streaming equality checks validate the updated snapshot,
the original time-travel snapshot and the full-rewrite output. Row counts, touched-group
counts and reuse of unassigned chunks are asserted.

This is a warm-cache elapsed-time benchmark in a shared environment, not a cold-I/O,
peak-RAM or terabyte-scale measurement. The 256-byte payload favors page-level
copy-on-write over full rewrites; do not generalize these speedups to narrow-row files.
Throughput labels use total input file bytes, **not bytes actually processed by VPF**.
Temporary fixtures/copies are removed automatically; allow about 5 GiB of free space.
Without `VPF_LARGE_BENCH=1`, the separate suite skips generation.

## Earlier: direct Arrow append

Recorded on **2026-11-06**, with the Arrow append fast path applied on top of
`2a10e1d4af102fd08c27dac359b88db6947b779c`. The complete 36-case run finished at
`2026-11-06T16:31:55Z`. Environment, fixture and sampling match the earlier indexed
run documented below: 80,000 rows, 11 flat columns, 30 samples, 1 s warm-up and a
5 s measurement target. All intervals below are Criterion's 95% confidence intervals.

```bash
cargo test --release --quiet
cargo bench --bench parquet_io -- --test
cargo bench --bench parquet_io -- \
  --sample-size 30 --measurement-time 5 --warm-up-time 1 --noplot \
  --save-baseline arrow-append-2a10e1d
```

### Same-run fresh-write comparison

Timings are milliseconds. The physical-append baseline reproduces the previous
single-row-group implementation using public transaction/codec APIs in the
benchmark, including schema lookup, conversion, page encoding/statistics, persistence
and snapshot commit. Both VPF paths use the same schema, data, codec, row/page limits
and SQLite/blob durability policy. Each iteration gets a fresh empty table, and both
outputs are independently checked against the fixture. The parquet-rs file baseline
also emits page indexes and syncs the file.

| Codec | Operation | Estimate (ms) | 95% CI (ms) |
|---|---|---:|---:|
| Snappy | parquet-rs fresh file | 33.465 | 33.177-33.802 |
| Snappy | VPF Arrow direct append | 37.600 | 37.204-38.053 |
| Snappy | VPF physical append (previous path) | 56.102 | 55.877-56.380 |
| Snappy | VPF import | 31.685 | 31.502-31.945 |
| Zstd | parquet-rs fresh file | 38.238 | 37.800-38.748 |
| Zstd | VPF Arrow direct append | 43.152 | 42.385-44.097 |
| Zstd | VPF physical append (previous path) | 58.686 | 58.413-59.096 |
| Zstd | VPF import | 31.300 | 31.198-31.403 |

The fast path reduces append time by **33.0% for Snappy** and **26.5% for Zstd**
versus the same-run physical baseline. VPF fresh writes are now approximately
**12-13% slower** than parquet-rs here, rather than the earlier 1.6-1.8x gap.
Percentages compare point estimates, not confidence bounds.

Compatible direct appends encode a conformed Arrow row group once with the stored
Parquet schema. They extract its pages from one in-memory file, and compute page
statistics directly from Arrow slices without materializing physical value vectors
or definition-level vectors. Larger batches use zero-copy Arrow row-group slices.
Nested/legacy layouts, dictionary-typed target schemas, missing physical defaults
and out-of-precision decimal values retain the compatibility path. Buffered
append, flush and mutations retain the existing physical representation.

### Small updates, rerun

| Codec | Rows updated | Full rewrite (ms) | Rewrite 95% CI | VPF update (ms) | VPF 95% CI | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Snappy | 1 | 46.547 | 46.213-46.971 | 4.3642 | 4.1726-4.5993 | 10.67x |
| Snappy | 100 clustered | 49.228 | 48.029-50.962 | 4.3413 | 4.2834-4.4155 | 11.34x |
| Zstd | 1 | 52.193 | 51.857-52.537 | 4.7770 | 4.6941-4.8838 | 10.93x |
| Zstd | 100 clustered | 52.096 | 51.767-52.463 | 4.6167 | 4.5655-4.6796 | 11.28x |

These use the same one-row/100-clustered-row update workload described below.

### Reads, rerun

| Codec | Projection | Input | Estimate (ms) | 95% CI (ms) |
|---|---|---|---:|---:|
| Snappy | All columns | parquet-rs Bytes | 5.2165 | 5.1876-5.2451 |
| Snappy | All columns | parquet-rs File | 5.7523 | 5.7023-5.7942 |
| Snappy | All columns | VPF snapshot | 6.4226 | 6.2674-6.5946 |
| Snappy | id, name | parquet-rs Bytes | 1.1240 | 1.0686-1.2132 |
| Snappy | id, name | parquet-rs File | 1.1549 | 1.1480-1.1619 |
| Snappy | id, name | VPF snapshot | 1.2396 | 1.2367-1.2423 |
| Zstd | All columns | parquet-rs Bytes | 7.9152 | 7.5569-8.4806 |
| Zstd | All columns | parquet-rs File | 7.9184 | 7.8889-7.9552 |
| Zstd | All columns | VPF snapshot | 8.4491 | 8.4203-8.4975 |
| Zstd | id, name | parquet-rs Bytes | 1.4769 | 1.4664-1.4926 |
| Zstd | id, name | parquet-rs File | 1.5401 | 1.5386-1.5419 |
| Zstd | id, name | VPF snapshot | 1.6567 | 1.6514-1.6647 |

| Codec | Snapshot construction (us) | 95% CI (us) |
|---|---:|---:|
| Snappy | 337.47 | 336.15-339.30 |
| Zstd | 345.86 | 339.57-354.72 |

### Physical-path stage diagnostics, rerun

These stages are **not** performed by the compatible Arrow direct-append path.
They remain useful for buffered/legacy operations and page-level mutations.

| Codec | Stage | Estimate (ms) | 95% CI (ms) |
|---|---|---:|---:|
| Snappy | Arrow to physical values | 7.4721 | 7.0354-7.9824 |
| Snappy | Copy whole row group | 9.4737 | 9.0351-9.9620 |
| Snappy | Encode pages and compute page statistics | 39.581 | 39.101-40.258 |
| Zstd | Arrow to physical values | 7.2010 | 6.8476-7.8659 |
| Zstd | Copy whole row group | 8.9054 | 8.8504-8.9866 |
| Zstd | Encode pages and compute page statistics | 43.897 | 43.805-43.992 |

### Fast-path correctness

The complete Rust suite passed **75 tests**, and all **36 benchmark smoke cases**
passed. New checks compare direct Arrow statistics with the physical/Parquet oracle
across primitive, temporal, decimal and binary types, including unsigned extremes,
nulls, slices, NaNs, signed zero and long/truncated bounds. Fast-path eligibility,
casts, column ordering, null filling, required fields and decimal/legacy fallback
are checked. Existing tests cover multi-row-group splitting, page indexes,
mutations, schema changes and time travel. Optional PyArrow/DuckDB checks remain
unconfigured (`VPF_PYTHON` unset). Shared-environment and warm-cache limitations
described below still apply.

## Earlier indexed run, before direct Arrow append

Recorded on **2026-11-06**, after integrating `origin/main` at
`0a535393b2bf2c3b13fc68c2be246c3a54f5629a` and reapplying the direct-conversion,
shared-Arrow-buffer, and single-row-group append optimizations.
The supplementary Zstd write run completed at `2026-11-06T15:58:11Z`.

These results supersede the earlier pre-page-index measurements. Current `main`
computes page statistics on import and mutation and emits page indexes. The
parquet-rs baseline now also emits ColumnIndex and OffsetIndex using page statistics;
the older runs disabled those indexes. This is not a controlled before/after
comparison of the optimizations alone.

## Environment and reproduction

| Item | Value |
|---|---|
| CPU | AMD EPYC 9V74 80-Core Processor; 2 logical CPUs exposed |
| OS | Linux x86_64, kernel `6.12.8+` |
| Workspace filesystem | `stat -f` reports `ext2/ext3` |
| Rust | `rustc 1.99.1 (58cf8f9c3 2026-10-13)` |
| Cargo | `cargo 1.99.1 (5f94df478 2026-08-27)` |
| Arrow / parquet-rs | 60.1.0 |
| Criterion | 0.7.0 |
| Build | Optimized Cargo bench profile |
| Sampling | 30 samples, 1 s warm-up, 5 s measurement target per benchmark |

```bash
cargo test --release --quiet
cargo bench --bench parquet_io -- --test

# Complete recorded run:
cargo bench --bench parquet_io -- \
  --sample-size 30 --measurement-time 5 --warm-up-time 1 --noplot \
  --save-baseline main-0a53539-indexed

# Supplementary run of the anomalous Zstd write group:
cargo bench --bench parquet_io -- write/zstd/ \
  --sample-size 30 --measurement-time 5 --warm-up-time 1 --noplot \
  --save-baseline main-0a53539-indexed-repeat
```

Criterion saves the full samples and estimates under `target/criterion`, with the
baseline names above. Tables report Criterion's **point estimate and 95% confidence
interval**, not a median or an individual iteration.

## Workload and timing boundaries

The fixture contains 80,000 deterministic rows and 11 flat columns: required `id`,
nullable integers, strings, floating point, boolean, date, timestamp, decimal and
binary data. Both codecs use dictionary encoding, Parquet V1 pages, a single
80,000-row row group, a 20,000-row page limit and a 1 MiB page-size limit.
The projections select `id` and `name`.

Reads include reader construction, footer parsing and decoding. Files and snapshots
are already open; snapshot construction is measured separately. Repeated reads use
the warm OS cache. Timed scans use the reader's default page-index policy.

Fresh writes include encoding and `sync_data` for the parquet-rs file, or VPF's
normal blob sync and SQLite transaction. SQLite remains in WAL mode with
`synchronous=NORMAL`; the durability semantics are not identical to a standalone
file. Fixture generation, destination creation, VPF table creation and cleanup are
outside the timed writes. Each iteration has a fresh destination.

Updates assign `name = 'changed'` where `id = 1234`, or where
`id >= 1234 AND id < 1334`. Both cases affect one existing data page of one column.
VPF evaluates the predicate and commits a new snapshot. The full-rewrite baseline
streams and decodes the original file, evaluates the same predicate, changes the
Arrow batches, and encodes and syncs an entire replacement file. Importing the base
into VPF is setup, not timed update work. The original file/snapshot remains available
on both paths. These results do not describe scattered updates or arbitrary predicates.

Import reuses already encoded pages but also decodes chunks to compute VPF's page
statistics. It is not an Arrow-to-Parquet encoding comparison.

## Reads

All timings are milliseconds.

| Codec | Projection | Input | Estimate | 95% CI |
|---|---|---|---:|---:|
| Snappy | All columns | parquet-rs Bytes | 5.2257 | 5.2131-5.2400 |
| Snappy | All columns | parquet-rs File | 5.6797 | 5.6604-5.6966 |
| Snappy | All columns | VPF snapshot | 6.3294 | 6.3120-6.3479 |
| Snappy | id, name | parquet-rs Bytes | 1.0336 | 1.0316-1.0364 |
| Snappy | id, name | parquet-rs File | 1.1253 | 1.1197-1.1303 |
| Snappy | id, name | VPF snapshot | 1.2275 | 1.2192-1.2356 |
| Zstd | All columns | parquet-rs Bytes | 7.4334 | 7.4243-7.4434 |
| Zstd | All columns | parquet-rs File | 8.0293 | 7.9815-8.0908 |
| Zstd | All columns | VPF snapshot | 8.7895 | 8.5444-9.0583 |
| Zstd | id, name | parquet-rs Bytes | 1.5390 | 1.4712-1.6250 |
| Zstd | id, name | parquet-rs File | 1.5184 | 1.5153-1.5223 |
| Zstd | id, name | VPF snapshot | 1.6081 | 1.6052-1.6120 |

Snapshot construction is separate from the scans:

| Codec | Estimate (us) | 95% CI (us) |
|---|---:|---:|
| Snappy | 349.31 | 342.43-357.58 |
| Zstd | 337.76 | 335.80-339.66 |

## Fresh writes and import

All timings are milliseconds. This table preserves the complete first run,
including the anomalous Zstd append result.

| Codec | Operation | Estimate | 95% CI |
|---|---|---:|---:|
| Snappy | parquet-rs fresh file | 33.934 | 33.686-34.202 |
| Snappy | VPF direct append | 59.529 | 59.149-59.944 |
| Snappy | VPF import | 43.101 | 35.850-51.815 |
| Zstd | parquet-rs fresh file | 45.241 | 42.951-47.982 |
| Zstd | VPF direct append | 184.82 | 147.85-226.94 |
| Zstd | VPF import | 30.277 | 30.005-30.612 |

### Supplementary Zstd write run

The first Zstd append estimate was unusually slow and had a wide interval. Repeating
the entire Zstd write group, with unchanged code and settings, produced:

| Operation | Estimate (ms) | 95% CI (ms) |
|---|---:|---:|
| parquet-rs fresh file | 37.989 | 37.449-38.509 |
| VPF direct append | 60.560 | 59.549-61.639 |
| VPF import | 29.770 | 29.626-29.939 |

The discrepancy demonstrates run-to-run variability in this shared environment;
its exact cause was not profiled. Both results are retained rather than silently
replacing the first run. Stable measurements here put VPF direct appends at about
1.6-1.8 times the fresh-file write time.

## Small update versus full rewrite

All timings are milliseconds. Speedups divide the full-rewrite point estimate by
the VPF point estimate; they are not confidence bounds.

| Codec | Rows updated | Full rewrite | Full rewrite 95% CI | VPF update | VPF 95% CI | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Snappy | 1 | 44.089 | 43.660-44.574 | 4.2440 | 4.0546-4.4796 | 10.39x |
| Snappy | 100 clustered | 43.742 | 43.466-44.035 | 4.0895 | 3.9909-4.2637 | 10.70x |
| Zstd | 1 | 51.484 | 51.101-51.887 | 4.6048 | 4.5375-4.7011 | 11.18x |
| Zstd | 100 clustered | 50.236 | 49.991-50.501 | 4.5678 | 4.4971-4.6546 | 11.00x |

VPF reuses the ten unassigned column chunks and the untouched pages of the assigned
column. It preserves the original snapshot. This page reuse, rather than fresh-file
encoding speed, is the advantage exercised by these benchmarks.

## Append-stage diagnostics

All timings are milliseconds. These isolated CPU measurements include destruction
of their results and exclude SQLite/blob I/O. They are not an exact additive
decomposition of the end-to-end append.

| Codec | Stage | Estimate | 95% CI |
|---|---|---:|---:|
| Snappy | Arrow to physical values | 10.513 | 9.8037-11.528 |
| Snappy | Copy whole row group | 13.275 | 13.224-13.322 |
| Snappy | Encode pages and compute page statistics | 38.949 | 38.693-39.249 |
| Zstd | Arrow to physical values | 9.7862 | 9.7096-9.8728 |
| Zstd | Copy whole row group | 13.227 | 13.146-13.321 |
| Zstd | Encode pages and compute page statistics | 44.083 | 43.906-44.273 |

The whole-row-group copy is no longer performed by direct append for this fixture:
the converted columns are borrowed. Larger batches still need row-group splitting,
and current `main` also slices data when computing individual page statistics.
Common flat types no longer take the temporary-Parquet conversion round trip;
offset-based strings/binary and fixed-size binary share their Arrow value buffers.
Compatibility fallbacks and nested-layout validation remain in place.

## Correctness and limitations

The integrated Rust suite passed: **72 tests**, including the new conversion
oracle/ownership tests, large-append splitting, and current `main`'s page-index
and exact-statistics tests. All **34 benchmark smoke cases** passed.
`VPF_PYTHON` was unset, so optional PyArrow/DuckDB checks were not run.

Benchmark preflight checks compare complete decoded output for Bytes, File and VPF,
including projections, appends and updates. They require page-index loading and
verify that both index types exist for every column. Update checks also verify the
affected-row count, one rewritten page, ten reused chunks and the unchanged
historical snapshot.

This is a small, deterministic, flat, warm-cache workload on a shared environment,
not a dedicated performance machine or a full ClickBench workload. Cold-cache I/O,
concurrent clients, scattered updates, buffered appends, deletes and nested-data
performance are not measured. Confidence intervals describe within-run sampling;
the Zstd repeat shows that they do not capture all run-to-run variation.
