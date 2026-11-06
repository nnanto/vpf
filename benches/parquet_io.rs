#[path = "../tests/common/mod.rs"]
mod common;

use std::fs::File;
use std::hint::black_box;
use std::time::Duration;

use arrow::array::{BooleanArray, RecordBatch, RecordBatchReader, Scalar, StringArray};
use arrow::compute::kernels::zip::zip;
use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::ChunkReader;
use tempfile::TempDir;
use vpf::ops::append::AppendMode;
use vpf::predicate::Expr;
use vpf::store::Store;

const ROWS: usize = 80_000;
const BATCH_SIZE: usize = 8192;

fn properties(codec: Compression) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(codec)
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false)
        .set_data_page_size_limit(vpf::codec::DATA_PAGE_SIZE)
        .set_data_page_row_count_limit(vpf::codec::DATA_PAGE_ROWS)
        .set_max_row_group_row_count(Some(ROWS))
        .build()
}

fn scan<R: ChunkReader + 'static>(source: R, projected: bool) -> usize {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(source).unwrap();
    if projected {
        let mask = ProjectionMask::roots(builder.parquet_schema(), [0, 2]);
        builder = builder.with_projection(mask);
    }
    let mut rows = 0;
    for batch in builder.with_batch_size(BATCH_SIZE).build().unwrap() {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        black_box(batch);
    }
    assert_eq!(rows, ROWS);
    rows
}

fn verify<R: ChunkReader + 'static>(source: R, projected: bool, expected: &RecordBatch) {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(source, options).unwrap();
    let metadata = builder.metadata();
    for rg in 0..metadata.num_row_groups() {
        let index = metadata.page_index_for_row_group(rg);
        for column in 0..metadata.row_group(rg).num_columns() {
            assert!(index.offset_index(column).is_some());
            assert!(index.column_index(column).is_some());
        }
    }
    if projected {
        let mask = ProjectionMask::roots(builder.parquet_schema(), [0, 2]);
        builder = builder.with_projection(mask);
    }
    let batches = builder
        .with_batch_size(BATCH_SIZE)
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(&common::concat(&batches), expected);
}

fn empty_store(batch: &RecordBatch, codec: Compression) -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("store")).unwrap();
    store
        .create_from_arrow("data", &batch.schema(), codec)
        .unwrap();
    (dir, store)
}

fn write_file(file: &mut File, batch: &RecordBatch, codec: Compression) {
    let mut writer =
        ArrowWriter::try_new(&mut *file, batch.schema(), Some(properties(codec))).unwrap();
    writer.write(black_box(batch)).unwrap();
    black_box(writer.close().unwrap());
    file.sync_data().unwrap();
}

fn update_batch(batch: &RecordBatch, predicate: &Expr) -> (RecordBatch, usize) {
    let mask = BooleanArray::from(predicate.matches(batch).unwrap());
    let rows_updated = mask.true_count();
    let mut columns = batch.columns().to_vec();
    let index = batch.schema().index_of("name").unwrap();
    columns[index] = zip(
        &mask,
        &Scalar::new(StringArray::from(vec!["changed"])),
        &columns[index],
    )
    .unwrap();
    (
        RecordBatch::try_new(batch.schema(), columns).unwrap(),
        rows_updated,
    )
}

fn rewrite_update(
    source: File,
    destination: &mut File,
    codec: Compression,
    predicate: &str,
) -> usize {
    let predicate = Expr::parse(predicate).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(source)
        .unwrap()
        .with_batch_size(BATCH_SIZE)
        .build()
        .unwrap();
    let mut writer =
        ArrowWriter::try_new(&mut *destination, reader.schema(), Some(properties(codec))).unwrap();
    let mut rows_updated = 0;
    for batch in reader {
        let (batch, count) = update_batch(&batch.unwrap(), &predicate);
        rows_updated += count;
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    destination.sync_data().unwrap();
    rows_updated
}

fn update_benchmarks(
    c: &mut Criterion,
    codec_name: &str,
    codec: Compression,
    batch: &RecordBatch,
    bytes: &Bytes,
    file: &File,
) {
    for (label, predicate, affected_rows) in [
        ("single_row", "id = 1234", 1),
        ("clustered_100_rows", "id >= 1234 AND id < 1334", 100),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("store")).unwrap();
        let imported = store.import("data", bytes).unwrap();
        let (expected, count) = update_batch(batch, &Expr::parse(predicate).unwrap());
        assert_eq!(count, affected_rows);
        let summary = store
            .update_where("data", "name = 'changed'", predicate)
            .unwrap();
        assert_eq!(summary.rows_updated, affected_rows as i64);
        assert_eq!(summary.pages.rewritten, 1);
        assert_eq!(summary.chunks_reused, batch.num_columns() - 1);
        verify(store.open_snapshot("data", None).unwrap(), false, &expected);
        verify(
            store
                .open_snapshot("data", Some(imported.snapshot_id))
                .unwrap(),
            false,
            batch,
        );
        let check_path = dir.path().join("rewrite.parquet");
        assert_eq!(
            rewrite_update(
                file.try_clone().unwrap(),
                &mut File::create(&check_path).unwrap(),
                codec,
                predicate,
            ),
            affected_rows,
        );
        verify(File::open(&check_path).unwrap(), false, &expected);

        let mut group = c.benchmark_group(format!("update/{codec_name}/{label}"));
        group.throughput(Throughput::Elements(affected_rows as u64));
        group.bench_function("parquet_full_rewrite", |b| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let destination = File::create(dir.path().join("updated.parquet")).unwrap();
                    (dir, file.try_clone().unwrap(), destination)
                },
                |(_, source, destination)| {
                    let count = rewrite_update(
                        source.try_clone().unwrap(),
                        destination,
                        codec,
                        black_box(predicate),
                    );
                    assert_eq!(count, affected_rows);
                    black_box(count);
                },
                BatchSize::PerIteration,
            )
        });
        group.bench_function("vpf_page_update", |b| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let store = Store::open(dir.path().join("store")).unwrap();
                    store.import("data", bytes).unwrap();
                    (dir, store)
                },
                |(_, store)| {
                    let summary = store
                        .update_where("data", "name = 'changed'", black_box(predicate))
                        .unwrap();
                    assert_eq!(summary.rows_updated, affected_rows as i64);
                    black_box(summary);
                },
                BatchSize::PerIteration,
            )
        });
        group.finish();
    }
}

fn parquet_io(c: &mut Criterion) {
    let batch = common::sample_batch(0, ROWS);
    for (codec_name, codec) in [
        ("snappy", Compression::SNAPPY),
        ("zstd", Compression::ZSTD(Default::default())),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let bytes = common::write_parquet(std::slice::from_ref(&batch), properties(codec));
        let path = dir.path().join("source.parquet");
        std::fs::write(&path, &bytes).unwrap();
        let file = File::open(&path).unwrap();
        let store = Store::open(dir.path().join("store")).unwrap();
        store.import("data", &bytes).unwrap();
        let snapshot = store.open_snapshot("data", None).unwrap();

        for projected in [false, true] {
            let expected = if projected {
                batch.project(&[0, 2]).unwrap()
            } else {
                batch.clone()
            };
            verify(bytes.clone(), projected, &expected);
            verify(file.try_clone().unwrap(), projected, &expected);
            verify(snapshot.clone(), projected, &expected);

            let label = if projected {
                "project_id_name"
            } else {
                "all_columns"
            };
            let mut group = c.benchmark_group(format!("read/{codec_name}/{label}"));
            group.throughput(Throughput::Elements(ROWS as u64));
            group.bench_function("parquet_bytes", |b| {
                b.iter(|| scan(black_box(bytes.clone()), projected))
            });
            group.bench_function("parquet_file", |b| {
                b.iter(|| scan(black_box(file.try_clone().unwrap()), projected))
            });
            group.bench_function("vpf_snapshot", |b| {
                b.iter(|| scan(black_box(snapshot.clone()), projected))
            });
            group.finish();
        }

        c.bench_function(&format!("open_snapshot/{codec_name}"), |b| {
            b.iter(|| black_box(store.open_snapshot("data", None).unwrap()))
        });

        // Check each write path before timing; decoding is not part of the write benchmark.
        let (_check_dir, check_store) = empty_store(&batch, codec);
        check_store
            .append("data", &batch, AppendMode::Direct)
            .unwrap();
        verify(
            check_store.open_snapshot("data", None).unwrap(),
            false,
            &batch,
        );
        let check_path = dir.path().join("check.parquet");
        write_file(&mut File::create(&check_path).unwrap(), &batch, codec);
        verify(File::open(&check_path).unwrap(), false, &batch);

        let schema = check_store
            .read(|conn| Ok(vpf::ops::snapshot::load_view(conn, "data", None)?.schema))
            .unwrap();
        let columns = vpf::convert::batch_to_columns(&schema, &batch).unwrap();
        let mut stages = c.benchmark_group(format!("append_stages/{codec_name}"));
        stages.throughput(Throughput::Elements(ROWS as u64));
        stages.bench_function("arrow_to_physical", |b| {
            b.iter(|| {
                black_box(vpf::convert::batch_to_columns(&schema, black_box(&batch)).unwrap())
            })
        });
        stages.bench_function("copy_row_group", |b| {
            b.iter(|| {
                black_box(
                    columns
                        .iter()
                        .map(|column| column.slice(0, ROWS))
                        .collect::<Vec<_>>(),
                )
            })
        });
        stages.bench_function("encode_pages", |b| {
            b.iter(|| {
                black_box(
                    vpf::ops::synth::encode_row_group(&schema, codec, black_box(&columns)).unwrap(),
                )
            })
        });
        stages.finish();

        let mut group = c.benchmark_group(format!("write/{codec_name}"));
        group.throughput(Throughput::Elements(ROWS as u64));
        group.bench_function("parquet_file", |b| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let file = File::create(dir.path().join("output.parquet")).unwrap();
                    (dir, file)
                },
                |(_, file)| {
                    write_file(file, &batch, codec);
                },
                BatchSize::PerIteration,
            )
        });
        group.bench_function("vpf_direct_append", |b| {
            b.iter_batched_ref(
                || empty_store(&batch, codec),
                |(_, store)| {
                    black_box(
                        store
                            .append("data", black_box(&batch), AppendMode::Direct)
                            .unwrap(),
                    );
                },
                BatchSize::PerIteration,
            )
        });
        group.bench_function("vpf_import", |b| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let store = Store::open(dir.path().join("store")).unwrap();
                    (dir, store)
                },
                |(_, store)| {
                    black_box(store.import("data", black_box(&bytes)).unwrap());
                },
                BatchSize::PerIteration,
            )
        });
        group.finish();
        update_benchmarks(c, codec_name, codec, &batch, &bytes, &file);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = parquet_io
}
criterion_main!(benches);
