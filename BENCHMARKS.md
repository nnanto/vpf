# VPF benchmark results

## Latest: OR-aware pruning

The DuckDB extension now passes conservative AND/OR expressions to VPF: AND
intersects candidate row ranges, OR unions them, and DuckDB still evaluates the
exact predicate. Conjunction-only scans retain the original batched pruning
path. Unsupported OR branches disable pruning of that disjunction, not of
independent supported AND terms.

Rebuilt baseline **`d27b97a`** and the modified extension, then ran the unchanged
read/write harness sequentially in before/after eager and after/before lazy
order on **2026-11-08**. Both variants used the same release CLI, one DuckDB
thread, two read warm-ups/seven samples and one write warm-up/five samples.
All 14 read queries and four durable write workloads ran at every size, with
the harness's full-fixture, result and write verification.
[Samples, plans, binary hashes and environment](benches/results/or-pruning.json)
retain both the original observations and the follow-up measurements.

OR medians in milliseconds; other-read change excludes both `count` and `or_filter`:

| Rows | OR lazy before | OR lazy after | OR eager before | OR eager after | Other-read score change, lazy / eager |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 1.929 | 1.994 | 1.652 | 1.733 | +3.5% / +6.4% |
| 100,000 | 3.836 | 3.265 | 3.409 | 2.722 | +2.3% / -1.8% |
| 1,000,000 | 21.269 | 5.430 | 19.417 | 3.274 | -0.8% / -0.7% |
| 10,000,000 | 198.780 | 26.176 | 188.768 | 5.231 | +1.3% / +0.01% |

At 10M rows, OR is **7.6x faster in lazy mode and 36.1x faster in eager mode**;
eager is comparable to the same-run Parquet median of **5.399 ms**. Tiny tables
show no gain. The other-read score stayed within **-1.8% to +6.4%** across
all sizes/modes, without a material individual read slowdown (more than both
10% and 1 ms).

Two size/mode combinations exceeded that write slowdown threshold: all four eager
100K writes and the lazy 10M update. The eager 100K run also had large SQLite
and Parquet spikes. Repeating the affected cases in both before/after orders,
with two warm-ups and 11 samples per variant, did not reproduce those slowdowns:
the eager 100K write changes ranged from **-2.8% to +4.3%**.
The first lazy 10M update pair measured **59.828 ms before / 59.418 ms after**.
The reverse-order pair measured **332.214 ms before / 60.267 ms after**, with
renewed baseline spikes. These shared-environment timings show no reproducible
write regression, not a guaranteed latency bound. The original noisy samples
are retained rather than discarded.

Integration tests additionally verify same-page OR scans produce at most
25K candidate rows, distant two-branch scans at most 50K, and three branches
at most 75K on a 300K-row table. Nested AND/OR, unsupported branches, prepared
parameters, NULL/NaN, missing statistics, row IDs and mutations remain correct.

Reproduce each primary run with the existing harness, choosing the matching
baseline or modified `vpf.duckdb_extension` and metadata mode:

```sh
python benches/analytical.py --metadata-mode eager --writes \
  --extension /path/to/vpf.duckdb_extension --output /path/to/report.json
```

## SQL reads and durable writes baseline

The expanded [`benches/analytical.py`](benches/analytical.py) suite compares
**10K, 100K, 1M and 10M identical orders rows** across native SQLite, DuckDB on
Parquet, and DuckDB on VPF, in separate sequential lazy/eager runs.
The runtime is rebuilt from **`7f6cd4a`**, including the header-only append
optimization that avoids loading existing pages/buffers in lazy mode.
Full reports retain medians, raw samples, exact SQL, read plans, verification
counts, output sizes and environment, including the harness SHA-256:
[lazy tables](benches/results/read-write-lazy.md),
[lazy JSON](benches/results/read-write-lazy.json),
[eager tables](benches/results/read-write-eager.md),
[eager JSON](benches/results/read-write-eager.json).

### Results across sizes

Recorded on **2026-11-08**. Times are milliseconds, lower is better.
SQLite and Parquet columns below come from the eager run; each VPF column comes
from its own mode's run. Each linked report includes its same-run baselines.
The modes ran sequentially, so differences are not controlled stage attribution.

Read score: sum of the **13 non-count query medians**:

| Rows | SQLite, unindexed | Parquet / DuckDB | VPF lazy | VPF eager |
|---:|---:|---:|---:|---:|
| 10,000 | 13.223 | 14.275 | 27.744 | 24.110 |
| 100,000 | 110.116 | 27.118 | 52.275 | 45.595 |
| 1,000,000 | 1,205.770 | 116.934 | 249.466 | 212.921 |
| 10,000,000 | 15,380.458 | 992.305 | 2,205.224 | 1,876.973 |

At 10M rows, eager's score is **8.2x faster than unindexed SQLite** and
**1.9x slower than Parquet**. Eager's full-row point lookup takes **7.694 ms**
versus **5.391 ms Parquet** and **29.879 ms lazy VPF**. String prefix filtering
still takes **227.454 ms versus 87.130 ms Parquet**, and the two-ID `OR` takes
**180.460 ms versus 4.788 ms**. That baseline did not push pruning hints for
`LIKE` or `OR`; OR is addressed by the follow-up above, while LIKE remains a gap.

Durable write medians:

| Seed rows | Write | SQLite | Parquet / DuckDB | VPF lazy | VPF eager |
|---:|---|---:|---:|---:|---:|
| 10,000 | fresh | 5.340 | 4.373 | 14.077 | 17.433 |
| 10,000 | append | 2.390 | 5.656 | 5.771 | 6.363 |
| 10,000 | update | 2.831 | 5.328 | 9.646 | 8.599 |
| 10,000 | delete | 2.894 | 5.400 | 11.366 | 11.863 |
| 100,000 | fresh | 23.153 | 21.729 | 43.314 | 43.568 |
| 100,000 | append | 3.453 | 23.348 | 6.472 | 8.303 |
| 100,000 | update | 6.484 | 22.994 | 16.707 | 17.010 |
| 100,000 | delete | 6.571 | 23.823 | 36.344 | 35.906 |
| 1,000,000 | fresh | 211.787 | 145.397 | 256.636 | 288.759 |
| 1,000,000 | append | 8.405 | 154.508 | 13.936 | 23.303 |
| 1,000,000 | update | 56.030 | 149.573 | 21.261 | 25.751 |
| 1,000,000 | delete | 46.851 | 155.379 | 43.386 | 47.360 |
| 10,000,000 | fresh | 2,020.046 | 1,339.083 | 1,892.237 | 2,064.484 |
| 10,000,000 | append | 28.319 | 1,360.402 | 67.766 | 90.070 |
| 10,000,000 | update | 398.745 | 1,354.599 | 60.494 | 63.731 |
| 10,000,000 | delete | 385.282 | 1,437.278 | 83.573 | 86.121 |

At 10M rows, eager VPF's append/update/delete medians are **15.1x / 21.3x /
16.7x faster than complete Parquet rewrites**, but fresh VPF writes are **1.54x
slower**. Unindexed SQLite remains faster for appends; VPF's clustered
updates/deletes avoid SQLite's full predicate scan. Eager improves the read
score by about **15% relative to lazy** in these runs, but its 100K-row append
costs **90.070 ms versus 67.766 ms lazy**. This demonstrates the configurable
read/write tradeoff, not a universal winning mode.

### Read workloads

**Version 2 has 14 queries:** the eight analytical queries described below plus
a full-row midpoint ID lookup, an ordered full-row 100-ID range, numeric equality
(`product_id = 17`), `IS NULL`, string prefix `LIKE 'r1%'`, and a two-ID `OR`.
The score now sums **13 non-count query medians**, so it must not be compared
directly with the historical seven-query scores. Individual unchanged queries
remain comparable, subject to run-to-run noise.

The original deterministic seven-column data, unindexed SQLite table, imported
VPF read pages, one-thread configuration and warm-cache methodology are retained.
Reads have two warm-ups and seven measured repetitions; complete fixture equality
is checked before timing, and every query result is checked after execution.
In particular, these point lookups do **not** compare against indexed SQLite.

### Write workloads and timing boundaries

| Write | Logical operation |
|---|---|
| Fresh | Write all N rows into a new destination/table |
| Append | Add `max(1, N // 100)` rows, with IDs starting at N |
| Update | Set `revenue_cents = 12345` for up to 100 clustered midpoint IDs |
| Delete | Remove the same clustered midpoint ID range |

Each case has one warm-up and five measured repetitions, with reproducibly
shuffled workload/engine order. Every iteration receives a new destination or
an independent copy of a checkpointed seed. Input generation, copying/syncing
the seed, connection/extension/attachment setup, verification and cleanup are
excluded. Table creation, native SQL execution/commit and explicit durability
work are included.

SQLite uses native `INSERT SELECT` from a prebuilt SQLite input table, not timed
Python row ingestion. Parquet and VPF consume canonical Parquet input through
DuckDB. These are **engine-native workflows with identical logical data**, not
identical physical input formats or encoder-only measurements. SQLite uses
WAL/NORMAL; both SQLite and VPF finish each sample with a WAL checkpoint/TRUNCATE,
file fsync and directory fsync. Parquet writes a complete new file for append,
update and delete, then fsyncs it, atomically replaces the destination and fsyncs
the directory. This measures a single-file Parquet workflow, not partitioned
append or an external table format.

The JSON includes separate SQL and explicit durability stage samples.
**SQL time may already contain engine-internal sync**, so it is not CPU-only.
Stage samples sum to the total for each iteration; independently computed
stage medians need not sum to the total median. These boundaries provide an
explicit synced endpoint, not a crash-recovery test or identical internal
durability protocols.

The first warm-up verifies every output row against the expected canonical SQL,
checking ID bounds and comparing ordered 8,192-ID windows outside timing.
This avoids a global DuckDB verification sort and does not assume physical row order.
SQLite's unindexed output is instead sorted once and streamed through those
windows, avoiding a complete table scan per window.
Every iteration checks affected/emitted counts
and final row count. Output sizes include any WAL/SHM sidecars; VPF retains its
original snapshot after a mutation, unlike SQLite and a replaced Parquet file.

All Parquet/VPF writes explicitly use **Snappy**. Parquet COPY targets 65,536-row
groups; VPF CTAS uses the production auto-append policy, buffering batches below
65,536 rows and directly encoding larger batches. Thus fresh VPF outputs can
have different row-group/buffer layouts from the imported read seeds. We do not
force a flush or otherwise change production behavior to match Parquet's layout.
Eager mode prepares metadata on writes and reads persisted seed layouts;
lazy mode resolves seed layouts on demand.

These are shared-environment warm-cache elapsed-time medians, not confidence
intervals. They do not cover cold I/O, concurrent writers, joins, indexed
SQLite, multithreaded DuckDB, peak memory or production data distributions.

### Reproduction

Build/install the matching DuckDB extension and release CLI as described below,
then use separate output paths to preserve the historical read-only archives:

```bash
/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --writes --metadata-mode lazy --output benches/results/read-write-lazy.json
/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --writes --metadata-mode eager --output benches/results/read-write-eager.json

# Small correctness/reporting run:
/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --sizes 1 70000 --repeats 1 --warmups 1 --writes --write-repeats 1 \
  --metadata-mode eager --output /tmp/read-write-smoke.json

# Focused tests, including independent multi-row-group writes in both modes:
/tmp/vpf-analytics-venv/bin/python -m unittest discover \
  -s benches -p test_analytical.py -q
```

`--write-repeats` and `--write-warmups` control write sampling independently of
the read options. Omitting `--writes` runs only the expanded read suite.

## Earlier: configurable snapshot metadata preparation

Recorded on **2026-11-08**, using **`962146f`**, after the original analytical
baseline. Snapshot identity/row-count and schema lookups now avoid loading page
metadata. The C API caches one resolved snapshot per handle with invalidation at
write and transaction boundaries. **Lazy (default)** resolves layouts on demand;
**eager** additionally persists each new snapshot's footer and offset mapping
atomically on writes. Both modes can read existing persisted layouts after reopen.

The SQL, data, sizes, one-thread settings, warm-ups, repetition count and equality
checks match the baseline below. Lazy and eager were measured in separate sequential
runs; these shared-environment medians are not a controlled stage profile or
confidence intervals. Small timing differences can reflect run-to-run noise.

Summed medians of the seven non-`COUNT(*)` queries, in milliseconds:

| Rows | Previous VPF | Optimized lazy | Optimized eager | Eager reduction |
|---:|---:|---:|---:|---:|
| 10,000 | 20.717 | 14.918 | 12.845 | 38.0% |
| 100,000 | 35.664 | 28.215 | 25.585 | 28.3% |
| 1,000,000 | 199.769 | 155.766 | 138.075 | 30.9% |
| 10,000,000 | 1,799.336 | 1,349.484 | 1,237.958 | 31.2% |

At 10M rows, the 100-row ID range improves from **95.067 ms** to **25.405 ms
lazy / 6.462 ms eager** (3.7x / 14.7x faster). `COUNT(*)` improves from **96.463 ms**
to **33.597 ms / 12.472 ms**, without introducing count-aggregate pushdown.
Eager's same-run workload score is **10.7x faster than unindexed SQLite** and
**1.77x slower than DuckDB on Parquet**. Native Parquet remains faster overall.

The eager 10M fixture occupies **110.266 MiB**, versus **110.078 MiB** for lazy.
Observed fixture imports took **1.30 s lazy / 1.68 s eager**, including CLI startup.
Those are single setup observations, not repeated write measurements; they should
not be treated as a stable write-overhead estimate. Eager prepares whole-snapshot
metadata on writes, and buffered snapshots additionally persist synthesized
Parquet pages, so frequent small appends can have a larger CPU/storage cost.

Full per-query results, plans, samples, storage and environment:
[lazy tables](benches/results/analytical-lazy.md),
[lazy JSON](benches/results/analytical-lazy.json),
[eager tables](benches/results/analytical-eager.md),
[eager JSON](benches/results/analytical-eager.json).
The original [baseline JSON](benches/results/analytical.json) remains unchanged.

```bash
# These archived eight-query runs require the harness from aef8d37.
# The current harness adds six reads; use new output paths for current results.
# Build for the exact installed DuckDB version, as described below.
DUCKDB_VERSION=v2.0.0 crates/vpf-duckdb/build.sh
cargo build --release --bin vpf
/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --metadata-mode lazy --output benches/results/analytical-lazy.json
/tmp/vpf-analytics-venv/bin/python benches/analytical.py \
  --metadata-mode eager --output benches/results/analytical-eager.json
```

`--metadata-mode` configures the fixture's writer and DuckDB attachment. The
attachment setting is a write policy: even a lazy attachment reads a persisted
layout if one exists. The benchmark therefore builds separate fixtures for each
mode. The handle-local cache is not shared across DuckDB autocommit transactions;
persisted eager layouts avoid reconstruction across those transactions and reopens.

## Earlier: analytical SQL across SQLite, Parquet and VPF

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

The original harness was committed in `7123f79`, whose runtime code matches
`f931b1f`; use that revision to reproduce this archived baseline. Current builds
also support `--metadata-mode` and should use separate output names as above.
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
