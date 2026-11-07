#![allow(dead_code)]
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};

/// A flat batch with a wide spread of physical/logical types and nulls.
pub fn sample_batch(start: i64, n: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n as i64).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("i32", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("day", DataType::Date32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new("bin", DataType::Binary, true),
        Field::new("u16", DataType::UInt16, true),
    ]));
    let opt = |i: i64, m: i64| i % m != 0;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int32Array::from_iter(
                ids.iter().map(|&i| opt(i, 7).then_some((i * 3) as i32)),
            )),
            Arc::new(StringArray::from_iter(
                ids.iter()
                    .map(|&i| opt(i, 5).then(|| format!("name-{}", i % 97))),
            )),
            Arc::new(Float64Array::from_iter(
                ids.iter().map(|&i| opt(i, 11).then_some(i as f64 * 1.5)),
            )),
            Arc::new(Float32Array::from_iter(
                ids.iter().map(|&i| opt(i, 13).then_some(i as f32 / 4.0)),
            )),
            Arc::new(BooleanArray::from_iter(
                ids.iter().map(|&i| opt(i, 3).then_some(i % 2 == 0)),
            )),
            Arc::new(Date32Array::from_iter(
                ids.iter()
                    .map(|&i| opt(i, 17).then_some(18000 + (i % 1000) as i32)),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter(ids.iter().map(|&i| {
                opt(i, 19).then_some(1_600_000_000_000_000 + i * 1000)
            }))),
            Arc::new(
                Decimal128Array::from_iter(
                    ids.iter().map(|&i| opt(i, 23).then_some(i as i128 * 101)),
                )
                .with_precision_and_scale(12, 2)
                .unwrap(),
            ),
            Arc::new(BinaryArray::from_iter(ids.iter().map(|&i| {
                opt(i, 29).then(|| vec![(i % 256) as u8; (i % 5) as usize])
            }))),
            Arc::new(UInt16Array::from_iter(
                ids.iter()
                    .map(|&i| opt(i, 31).then_some((i % 65000) as u16 + 500)),
            )),
        ],
    )
    .unwrap()
}

pub fn codecs() -> Vec<Compression> {
    vec![
        Compression::UNCOMPRESSED,
        Compression::SNAPPY,
        Compression::GZIP(GzipLevel::default()),
        Compression::ZSTD(ZstdLevel::default()),
        Compression::LZ4_RAW,
        Compression::BROTLI(BrotliLevel::default()),
    ]
}

pub fn props(
    codec: Compression,
    v2: bool,
    dict: bool,
    page_rows: usize,
    rg_rows: usize,
) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(codec)
        .set_writer_version(if v2 {
            WriterVersion::PARQUET_2_0
        } else {
            WriterVersion::PARQUET_1_0
        })
        .set_dictionary_enabled(dict)
        .set_data_page_row_count_limit(page_rows)
        .set_write_batch_size(page_rows.min(1024))
        .set_max_row_group_row_count(Some(rg_rows))
        .build()
}

pub fn write_parquet(batches: &[RecordBatch], props: WriterProperties) -> Bytes {
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, batches[0].schema(), Some(props)).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    Bytes::from(buf)
}

pub fn read_parquet(bytes: Bytes) -> Vec<RecordBatch> {
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

pub fn concat(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// Python interpreter with pyarrow + duckdb, from `VPF_PYTHON` (tests using it are skipped if unset).
pub fn python() -> Option<String> {
    std::env::var("VPF_PYTHON").ok().filter(|p| !p.is_empty())
}

pub fn run_python(args: &[&str]) {
    let py = python().expect("VPF_PYTHON");
    let out = std::process::Command::new(&py).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "python {:?} failed:\n{}\n{}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

pub fn py_script(name: &str) -> String {
    format!("{}/tests/python/{}", env!("CARGO_MANIFEST_DIR"), name)
}

/// Reads all rows of a virtual file with the arrow reader.
pub fn read_virtual(vf: &vpf::layout::VirtualFile) -> RecordBatch {
    let batches: Vec<RecordBatch> =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(vf.clone())
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(vf.clone())
        .unwrap()
        .schema()
        .clone();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Applies an alter op to an Arrow batch: the reference model.
pub fn alter_model(b: &RecordBatch, op: &vpf::ops::alter::AlterOp) -> RecordBatch {
    let s = b.schema();
    let mut fields: Vec<Field> = s.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut cols = b.columns().to_vec();
    match op {
        vpf::ops::alter::AlterOp::AddColumn { spec, default } => {
            let ty = match (spec.physical, spec.logical.as_ref()) {
                _ if spec.is_nested() => {
                    let d = vpf::schema::build_descriptor("schema", std::slice::from_ref(spec))
                        .unwrap();
                    parquet::arrow::parquet_to_arrow_schema(&d, None)
                        .unwrap()
                        .field(0)
                        .data_type()
                        .clone()
                }
                (parquet::basic::Type::INT64, None) => DataType::Int64,
                (parquet::basic::Type::INT32, None) => DataType::Int32,
                (parquet::basic::Type::DOUBLE, None) => DataType::Float64,
                (parquet::basic::Type::BOOLEAN, None) => DataType::Boolean,
                (parquet::basic::Type::BYTE_ARRAY, Some(_)) => DataType::Utf8,
                (parquet::basic::Type::INT64, Some(_)) => {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                }
                (parquet::basic::Type::FIXED_LEN_BYTE_ARRAY, Some(_)) => {
                    DataType::Decimal128(spec.precision as u8, spec.scale as i8)
                }
                other => panic!("model lacks {other:?}"),
            };
            let one = match default {
                Some(l) => vpf::predicate::literal_array(l, &ty).unwrap(),
                None => new_null_array(&ty, 1),
            };
            let idx = UInt32Array::from(vec![0u32; b.num_rows()]);
            cols.push(arrow::compute::take(&one, &idx, None).unwrap());
            fields.push(Field::new(&spec.name, ty, spec.nullable));
        }
        vpf::ops::alter::AlterOp::DropColumn(n) => {
            let i = s.index_of(n).unwrap();
            fields.remove(i);
            cols.remove(i);
        }
        vpf::ops::alter::AlterOp::RenameColumn { from, to } => {
            let i = s.index_of(from).unwrap();
            fields[i] = fields[i].clone().with_name(to);
        }
        vpf::ops::alter::AlterOp::Reorder(order) => {
            let p: Vec<usize> = order.iter().map(|n| s.index_of(n).unwrap()).collect();
            fields = p.iter().map(|&i| fields[i].clone()).collect();
            cols = p.iter().map(|&i| cols[i].clone()).collect();
        }
        vpf::ops::alter::AlterOp::Widen { column, to } => {
            let i = s.index_of(column).unwrap();
            let ty = if *to == parquet::basic::Type::INT64 {
                DataType::Int64
            } else {
                DataType::Float64
            };
            cols[i] = arrow::compute::cast(&cols[i], &ty).unwrap();
            fields[i] = fields[i].clone().with_data_type(ty);
        }
        vpf::ops::alter::AlterOp::MakeOptional(n) => {
            let i = s.index_of(n).unwrap();
            fields[i] = fields[i].clone().with_nullable(true);
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}
