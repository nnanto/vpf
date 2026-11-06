# Parquet I/O benchmark results

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
