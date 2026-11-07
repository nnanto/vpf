use std::fs::{File, copy};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    BinaryArray, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, Scalar, StringArray,
};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::ChunkReader;
use tempfile::TempDir;
use vpf::ops::snapshot::SegmentData;
use vpf::predicate::Expr;
use vpf::store::Store;

const TARGET_BYTES: u64 = 1 << 30;
const ROW_GROUP_ROWS: usize = 65_536;
const PAYLOAD_BYTES: usize = 256;
const READ_BATCH_ROWS: usize = 8192;
const BUCKETS: i64 = 1024;

fn properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_column_dictionary_enabled("payload".into(), false)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false)
        .set_data_page_size_limit(vpf::codec::DATA_PAGE_SIZE)
        .set_data_page_row_count_limit(vpf::codec::DATA_PAGE_ROWS)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build()
}

struct Fixture {
    _dir: TempDir,
    source: PathBuf,
    seed: PathBuf,
    rows: usize,
    groups: usize,
    size: u64,
    snapshot: i64,
}

impl Fixture {
    fn create() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.parquet");
        let seed = dir.path().join("seed");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bucket", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let mut file = File::create(&source).unwrap();
        let mut writer =
            ArrowWriter::try_new(&mut file, schema.clone(), Some(properties())).unwrap();
        let mut rows = 0usize;
        let mut groups = 0usize;
        let mut random = 0x1234_5678_9abc_def0u64;
        while (writer.bytes_written() as u64) < TARGET_BYTES {
            let ids: Vec<_> = (rows as i64..(rows + ROW_GROUP_ROWS) as i64).collect();
            let mut payload = vec![0u8; ROW_GROUP_ROWS * PAYLOAD_BYTES];
            // Deterministic SplitMix64 output prevents a nominal 1 GiB fixture
            // from collapsing into a tiny compressed file.
            for bytes in payload.chunks_exact_mut(8) {
                random = random.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let mut value = random;
                value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                bytes.copy_from_slice(&(value ^ (value >> 31)).to_le_bytes());
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from(
                        ids.iter().map(|i| i % BUCKETS).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|i| format!("name-{i}")),
                    )),
                    Arc::new(BinaryArray::from_iter_values(
                        payload.chunks_exact(PAYLOAD_BYTES),
                    )),
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
            rows += ROW_GROUP_ROWS;
            groups += 1;
        }
        writer.close().unwrap();
        file.sync_all().unwrap();
        let size = file.metadata().unwrap().len();
        assert!(
            size >= TARGET_BYTES,
            "fixture must be at least 1 GiB on disk"
        );
        assert!(groups >= 2, "fixture must have multiple row groups");
        let store = Store::open(&seed).unwrap();
        let imported = store.import_file("data", &source).unwrap();
        assert_eq!(imported.num_rows, rows as i64);
        assert_eq!(imported.row_groups, groups);
        verify(
            &source,
            store.open_snapshot("data", None).unwrap(),
            None,
            rows,
        );
        // Iterations copy a closed, checkpointed store, not a live WAL database.
        drop(store);
        let conn = rusqlite::Connection::open(seed.join("meta.db")).unwrap();
        let (busy, _, _): (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(busy, 0, "seed checkpoint must complete before copying");
        drop(conn);
        eprintln!(
            "large_mutations fixture: {size} bytes, {rows} rows, {groups} row groups, Snappy"
        );
        Self {
            _dir: dir,
            source,
            seed,
            rows,
            groups,
            size,
            snapshot: imported.snapshot_id,
        }
    }

    fn fresh_store(&self) -> (TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        copy(self.seed.join("meta.db"), dir.path().join("meta.db")).unwrap();
        copy(self.seed.join("data.bin"), dir.path().join("data.bin")).unwrap();
        // Do not charge the timed mutation's blob sync for dirty bytes written
        // by the untimed GiB seed copy.
        for name in ["meta.db", "data.bin"] {
            File::open(dir.path().join(name))
                .unwrap()
                .sync_all()
                .unwrap();
        }
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }
}

fn updated(batch: &RecordBatch, predicate: &Expr) -> (RecordBatch, usize) {
    let mask = BooleanArray::from(predicate.matches(batch).unwrap());
    let count = mask.true_count();
    let mut columns = batch.columns().to_vec();
    columns[2] = zip(
        &mask,
        &Scalar::new(StringArray::from(vec!["changed"])),
        &columns[2],
    )
    .unwrap();
    (
        RecordBatch::try_new(batch.schema(), columns).unwrap(),
        count,
    )
}

fn rewrite(source: &Path, destination: &mut File, predicate: &Expr) -> usize {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(source).unwrap())
        .unwrap()
        .with_batch_size(READ_BATCH_ROWS)
        .build()
        .unwrap();
    let mut writer =
        ArrowWriter::try_new(&mut *destination, reader.schema(), Some(properties())).unwrap();
    let mut count = 0;
    for batch in reader {
        let (batch, matches) = updated(&batch.unwrap(), predicate);
        writer.write(&batch).unwrap();
        count += matches;
    }
    writer.close().unwrap();
    destination.sync_all().unwrap();
    count
}

fn verify<R: ChunkReader + 'static>(
    source: &Path,
    actual: R,
    predicate: Option<&Expr>,
    rows: usize,
) {
    let mut expected = ParquetRecordBatchReaderBuilder::try_new(File::open(source).unwrap())
        .unwrap()
        .with_batch_size(READ_BATCH_ROWS)
        .build()
        .unwrap();
    let mut actual = ParquetRecordBatchReaderBuilder::try_new(actual)
        .unwrap()
        .with_batch_size(READ_BATCH_ROWS)
        .build()
        .unwrap();
    let mut count = 0;
    loop {
        match (expected.next(), actual.next()) {
            (Some(expected), Some(actual)) => {
                let expected = expected.unwrap();
                let expected =
                    predicate.map_or_else(|| expected.clone(), |p| updated(&expected, p).0);
                assert_eq!(actual.unwrap(), expected, "mismatch at row {count}");
                count += expected.num_rows();
            }
            (None, None) => break,
            _ => panic!("different numbers of batches at row {count}"),
        }
    }
    assert_eq!(count, rows);
}

fn check_update(fixture: &Fixture, predicate: &str, affected: usize, touched_groups: usize) {
    let (_dir, store) = fixture.fresh_store();
    let before = store.view("data", None).unwrap();
    let summary = store
        .update_where("data", "name = 'changed'", predicate)
        .unwrap();
    assert_eq!(summary.rows_updated, affected as i64);
    assert_eq!(summary.segments_rewritten, touched_groups);
    let after = store.view("data", None).unwrap();
    assert_eq!(after.segments.len(), fixture.groups);
    for (old, new) in before.segments.iter().zip(&after.segments) {
        let (SegmentData::RowGroup { chunks: old, .. }, SegmentData::RowGroup { chunks: new, .. }) =
            (old, new)
        else {
            panic!("expected stored row groups")
        };
        for column in [0, 1, 3] {
            assert_eq!(
                old[column].chunk_id, new[column].chunk_id,
                "unassigned column must be reused"
            );
        }
    }
    let predicate = Expr::parse(predicate).unwrap();
    verify(
        &fixture.source,
        store.open_snapshot("data", None).unwrap(),
        Some(&predicate),
        fixture.rows,
    );
    verify(
        &fixture.source,
        store.open_snapshot("data", Some(fixture.snapshot)).unwrap(),
        None,
        fixture.rows,
    );
}

fn large_mutations(c: &mut Criterion) {
    // A separate opt-in avoids generating/importing a GiB even when Criterion
    // filters out this suite during ordinary benchmark runs.
    match std::env::var("VPF_LARGE_BENCH") {
        Err(std::env::VarError::NotPresent) => {
            eprintln!("Skipping 1 GiB benchmark; set VPF_LARGE_BENCH=1 to enable.");
            return;
        }
        Ok(value) if value == "1" => {}
        other => panic!("VPF_LARGE_BENCH must be unset or 1, got {other:?}"),
    }
    let fixture = Fixture::create();
    let middle = fixture.rows / 2 + 1234;
    let scattered = (0..fixture.groups)
        .map(|g| (g * ROW_GROUP_ROWS + 1234).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    for (label, predicate, affected, touched_groups) in [
        (
            "clustered_100_rows",
            format!("id >= {middle} AND id < {}", middle + 100),
            100,
            1,
        ),
        (
            "one_row_per_group",
            format!("id IN ({scattered})"),
            fixture.groups,
            fixture.groups,
        ),
        (
            "unclustered_bucket",
            "bucket = 0".to_string(),
            fixture.rows / BUCKETS as usize,
            fixture.groups,
        ),
    ] {
        check_update(&fixture, &predicate, affected, touched_groups);
        let expr = Expr::parse(&predicate).unwrap();
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("updated.parquet");
            assert_eq!(
                rewrite(&fixture.source, &mut File::create(&path).unwrap(), &expr),
                affected
            );
            verify(
                &fixture.source,
                File::open(&path).unwrap(),
                Some(&expr),
                fixture.rows,
            );
        }
        let mut group = c.benchmark_group(format!("large_mutations/1gib/snappy/{label}"));
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Bytes(fixture.size));
        group.bench_function("vpf_page_update", |b| {
            b.iter_batched_ref(
                || fixture.fresh_store(),
                |(_, store)| {
                    let summary = store
                        .update_where("data", "name = 'changed'", black_box(&predicate))
                        .unwrap();
                    assert_eq!(summary.rows_updated, affected as i64);
                    assert_eq!(summary.segments_rewritten, touched_groups);
                    black_box(summary);
                },
                BatchSize::PerIteration,
            )
        });
        group.bench_function("parquet_full_rewrite", |b| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let file = File::create(dir.path().join("updated.parquet")).unwrap();
                    (dir, file)
                },
                |(_, file)| assert_eq!(black_box(rewrite(&fixture.source, file, &expr)), affected),
                BatchSize::PerIteration,
            )
        });
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    targets = large_mutations
}
criterion_main!(benches);
