//! Page index (ColumnIndex/OffsetIndex) and statistics of virtual files, verified against
//! the actual pages and against statistics computed independently by parquet-rs.
mod common;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::*;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::basic::Compression;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use vpf::ops::append::AppendMode;
use vpf::store::Store;

/// Min/max of page `i` as plain-encoded bytes.
fn bounds(ci: &ColumnIndexMetaData, i: usize) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    match ci {
        ColumnIndexMetaData::BOOLEAN(x) => (x.min_value(i).map(|v| vec![*v as u8]), x.max_value(i).map(|v| vec![*v as u8])),
        ColumnIndexMetaData::INT32(x) => (x.min_value(i).map(|v| v.to_le_bytes().to_vec()), x.max_value(i).map(|v| v.to_le_bytes().to_vec())),
        ColumnIndexMetaData::INT64(x) => (x.min_value(i).map(|v| v.to_le_bytes().to_vec()), x.max_value(i).map(|v| v.to_le_bytes().to_vec())),
        ColumnIndexMetaData::FLOAT(x) => (x.min_value(i).map(|v| v.to_le_bytes().to_vec()), x.max_value(i).map(|v| v.to_le_bytes().to_vec())),
        ColumnIndexMetaData::DOUBLE(x) => (x.min_value(i).map(|v| v.to_le_bytes().to_vec()), x.max_value(i).map(|v| v.to_le_bytes().to_vec())),
        ColumnIndexMetaData::BYTE_ARRAY(x) | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(x) => {
            (x.min_value(i).map(<[u8]>::to_vec), x.max_value(i).map(<[u8]>::to_vec))
        }
        other => panic!("unexpected column index {other:?}"),
    }
}

/// Statistics of leaf `c` over `rows` of row group `rg`, computed by re-writing those rows
/// with parquet-rs.
fn truth(bytes: &Bytes, rg: usize, c: usize, rows: std::ops::Range<usize>) -> parquet::file::statistics::Statistics {
    let b = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let schema = b.parquet_schema().clone();
    let root = schema.get_column_root_idx(c);
    let sel = RowSelection::from(vec![RowSelector::skip(rows.start), RowSelector::select(rows.len())]);
    let batches: Vec<RecordBatch> = b
        .with_row_groups(vec![rg])
        .with_projection(ProjectionMask::roots(&schema, [root]))
        .with_row_selection(sel)
        .build()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let batch = concat(&batches);
    assert_eq!(batch.num_rows(), rows.len());
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_statistics_truncate_length(None)
        .set_dictionary_enabled(false)
        .build();
    let out = write_parquet(&[batch], props);
    let md = ParquetMetaDataReader::new().parse_and_finish(&out).unwrap();
    let path = schema.column(c).path().clone();
    let col = md.row_group(0).columns().iter().find(|x| *x.column_path() == path).expect("leaf in rewritten file");
    col.statistics().expect("parquet-rs statistics").clone()
}

#[derive(Debug, Default, PartialEq)]
struct Counts {
    chunks: usize,
    offset_indexes: usize,
    column_indexes: usize,
    pages: usize,
}

/// Verifies the page indexes of a Parquet file against its pages and its data.
fn verify(bytes: &Bytes) -> Counts {
    let md = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional).parse_and_finish(bytes).unwrap();
    let mut n = Counts::default();
    for rg in 0..md.num_row_groups() {
        let rgm = md.row_group(rg);
        let pi = md.page_index_for_row_group(rg);
        for c in 0..rgm.num_columns() {
            n.chunks += 1;
            let col = rgm.column(c);
            let Some(oi) = pi.offset_index(c) else { continue };
            n.offset_indexes += 1;
            // Locations are exactly the chunk's data pages.
            let (start, len) = col.byte_range();
            let chunk = bytes.slice(start as usize..(start + len) as usize);
            let pages: Vec<(i64, i32)> = vpf::thrift::split_pages(&chunk)
                .unwrap()
                .into_iter()
                .filter(|p| p.header.kind.is_data())
                .map(|p| (start as i64 + p.offset as i64, p.header.total_len() as i32))
                .collect();
            let locs = oi.page_locations();
            assert_eq!(locs.iter().map(|l| (l.offset, l.compressed_page_size)).collect::<Vec<_>>(), pages, "rg {rg} col {c}");
            assert_eq!(locs[0].first_row_index, 0);
            assert!(locs.windows(2).all(|w| w[0].first_row_index < w[1].first_row_index));
            assert!(locs.last().unwrap().first_row_index < rgm.num_rows());
            n.pages += locs.len();
            let Some(ci) = pi.column_index(c) else { continue };
            n.column_indexes += 1;
            assert_eq!(ci.num_pages() as usize, locs.len());
            let physical = col.column_type();
            for i in 0..locs.len() {
                let end = locs.get(i + 1).map_or(rgm.num_rows(), |l| l.first_row_index) as usize;
                let t = truth(bytes, rg, c, locs[i].first_row_index as usize..end);
                let ctx = format!("rg {rg} col {} page {i}", col.column_path());
                assert_eq!(ci.null_count(i), t.null_count_opt().map(|x| x as i64), "{ctx}: null count");
                if t.nan_count_opt().is_some() || ci.nan_count(i).is_some() {
                    assert_eq!(ci.nan_count(i), t.nan_count_opt().map(|x| x as i64), "{ctx}: nan count");
                }
                let (lo, hi) = bounds(ci, i);
                match (t.min_bytes_opt(), t.max_bytes_opt()) {
                    (None, None) => assert!(ci.is_null_page(i), "{ctx}: null page"),
                    (Some(tlo), Some(thi)) => {
                        assert!(!ci.is_null_page(i), "{ctx}");
                        let (lo, hi) = (lo.unwrap(), hi.unwrap());
                        let byte_array = matches!(physical, parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY);
                        if byte_array && tlo.len() > 64 {
                            assert!(lo.len() <= 64 && tlo.starts_with(&lo), "{ctx}: truncated min");
                        } else {
                            assert_eq!(lo, tlo, "{ctx}: min");
                        }
                        if byte_array && thi.len() > 64 {
                            // Untruncatable maxima (all 0xff prefix) are kept whole.
                            assert!(hi == thi || (hi.as_slice() > thi && hi.len() <= 64), "{ctx}: truncated max");
                        } else {
                            assert_eq!(hi, thi, "{ctx}: max");
                        }
                    }
                    _ => unreachable!(),
                }
            }
            // Chunk statistics agree with the data as well.
            let t = truth(bytes, rg, c, 0..rgm.num_rows() as usize);
            if let Some(s) = col.statistics() {
                if let Some(nc) = s.null_count_opt() {
                    assert_eq!(Some(nc), t.null_count_opt(), "rg {rg} col {c}: chunk null count");
                }
                if s.min_is_exact() && s.min_bytes_opt().is_some() {
                    assert_eq!(s.min_bytes_opt(), t.min_bytes_opt(), "rg {rg} col {c}: chunk min");
                }
                if s.max_is_exact() && s.max_bytes_opt().is_some() {
                    assert_eq!(s.max_bytes_opt(), t.max_bytes_opt(), "rg {rg} col {c}: chunk max");
                }
            }
        }
    }
    check_selection_reads(bytes);
    n
}

/// Row-selective reads that use the offset index return the same rows as plain reads.
fn check_selection_reads(bytes: &Bytes) {
    let all = concat(&read_parquet(bytes.clone()));
    let total = all.num_rows();
    if total == 0 {
        return;
    }
    for (skip, take) in [(0usize, 1usize), (37, 211), (total / 2, 3), (total.saturating_sub(5), 5)] {
        let skip = skip.min(total - 1);
        let take = take.min(total - skip);
        let mut sel = vec![RowSelector::skip(skip), RowSelector::select(take)];
        if skip + take < total {
            sel.push(RowSelector::skip(total - skip - take));
        }
        let opts = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let got: Vec<RecordBatch> = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes.clone(), opts)
            .unwrap()
            .with_row_selection(RowSelection::from(sel))
            .build()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let got = if got.is_empty() { all.slice(0, 0) } else { concat(&got) };
        assert_eq!(got, all.slice(skip, take), "selection {skip}+{take}");
    }
}

fn export(store: &Store, name: &str) -> Bytes {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("x.parquet");
    store.export(name, None, &p).unwrap();
    Bytes::from(std::fs::read(&p).unwrap())
}

fn py_check(bytes: &Bytes, expect_ci: bool) {
    if python().is_none() {
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("x.parquet");
    std::fs::write(&p, bytes).unwrap();
    run_python(&[&py_script("check_page_index.py"), p.to_str().unwrap(), if expect_ci { "1" } else { "0" }]);
}

fn full(n: &Counts) -> bool {
    n.chunks == n.offset_indexes && n.chunks == n.column_indexes
}

#[test]
fn imported_flat_files_get_exact_page_indexes() {
    for (codec, v2, dict) in [(Compression::SNAPPY, false, true), (Compression::ZSTD(Default::default()), true, false)] {
        let d = tempfile::tempdir().unwrap();
        let store = Store::open(d.path()).unwrap();
        let base = sample_batch(0, 2500);
        store.import("f", &write_parquet(&[base.clone()], props(codec, v2, dict, 100, 1000))).unwrap();
        let bytes = export(&store, "f");
        assert_eq!(concat(&read_parquet(bytes.clone())), base);
        let n = verify(&bytes);
        assert!(full(&n), "{n:?}");
        assert!(n.pages > 3 * base.num_columns() * 5, "multi-page chunks: {n:?}");
        // The in-memory metadata carries the same page index.
        let vf = store.open_snapshot("f", None).unwrap();
        assert!(vf.metadata().page_index().is_some());
        py_check(&bytes, true);
    }
}

#[test]
fn page_indexes_survive_mutations() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    store.import("f", &write_parquet(&[sample_batch(0, 3000)], props(Compression::SNAPPY, false, true, 100, 1500))).unwrap();
    let steps: Vec<Box<dyn Fn(&Store)>> = vec![
        Box::new(|s| {
            s.delete_where("f", "id = 7").unwrap();
        }),
        Box::new(|s| {
            s.delete_where("f", "name LIKE 'name-1%' AND score > 100").unwrap();
        }),
        Box::new(|s| {
            s.update_where("f", "name = 'changed', i32 = NULL", "id < 450").unwrap();
        }),
        Box::new(|s| {
            s.update_where("f", "score = 1000000000", "id = 2999").unwrap();
        }),
        Box::new(|s| {
            s.append("f", &sample_batch(5000, 120), AppendMode::Buffered).unwrap();
        }),
        Box::new(|s| {
            s.append("f", &sample_batch(9000, 700), AppendMode::Direct).unwrap();
        }),
        Box::new(|s| {
            s.flush("f").unwrap();
        }),
        Box::new(|s| {
            s.delete_where("f", "id >= 5000 AND id < 5010").unwrap();
        }),
        Box::new(|s| {
            s.delete_where("f", "id < 1450").unwrap();
        }),
    ];
    for (i, step) in steps.iter().enumerate() {
        step(&store);
        let bytes = export(&store, "f");
        let n = verify(&bytes);
        assert!(full(&n), "step {i}: {n:?}");
        if i % 3 == 2 {
            py_check(&bytes, true);
        }
    }
    // After mutations the touched chunks have exact statistics including null counts.
    let vf = store.open_snapshot("f", None).unwrap();
    for rg in vf.metadata().row_groups() {
        for col in rg.columns() {
            let s = col.statistics().unwrap();
            assert!(s.null_count_opt().is_some() && s.min_is_exact() && s.max_is_exact(), "{}", col.column_path());
        }
    }
}

#[test]
fn appended_and_altered_files() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let b = sample_batch(0, 1);
    store.create_from_arrow("f", &b.schema(), Compression::LZ4_RAW).unwrap();
    store.append("f", &sample_batch(0, 30_000), AppendMode::Direct).unwrap();
    store.append("f", &sample_batch(30_000, 10), AppendMode::Buffered).unwrap();
    let n = verify(&export(&store, "f"));
    assert!(full(&n), "{n:?}");
    let tree = parquet::arrow::ArrowSchemaConverter::new()
        .convert(&Schema::new(vec![Field::new("extra", DataType::Int64, true)]))
        .unwrap();
    let spec = vpf::schema::ColumnSpec::from_parquet_type(0, &tree.root_schema().get_fields()[0]).unwrap();
    store
        .alter("f", vpf::ops::alter::AlterOp::AddColumn { spec, default: Some(vpf::predicate::Literal::Num("42".into())) })
        .unwrap();
    store.update_where("f", "extra = 7", "id IN (0, 1000, 17000, 29999)").unwrap();
    let bytes = export(&store, "f");
    let n = verify(&bytes);
    assert!(full(&n), "{n:?}");
    py_check(&bytes, true);
}

#[test]
fn long_strings_are_truncated_and_floats_with_nan() {
    let n = 600usize;
    let s: Vec<Option<String>> = (0..n)
        .map(|i| (i % 9 != 0).then(|| format!("{}{}-{i:05}", if i % 2 == 0 { "é" } else { "z" }, "x".repeat(60 + i % 50))))
        .collect();
    let bin: Vec<Option<Vec<u8>>> = (0..n).map(|i| Some(vec![if i % 3 == 0 { 0xff } else { (i % 250) as u8 }; 70 + i % 7])).collect();
    // Rows 200..300 hold only NaN in `f`: that chunk gets no column index.
    let f: Vec<Option<f64>> = (0..n)
        .map(|i| match i {
            200..300 => Some(f64::NAN),
            _ if i % 5 == 0 => Some(f64::NAN),
            _ if i % 7 == 0 => None,
            _ => Some(if i % 2 == 0 { -0.0 } else { i as f64 - 300.0 }),
        })
        .collect();
    let g: Vec<Option<f32>> = (0..n).map(|i| Some(if i % 4 == 0 { f32::NAN } else { (i as f32).sin() })).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Binary, true),
        Field::new("f", DataType::Float64, true),
        Field::new("g", DataType::Float32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..n as i64)),
            Arc::new(StringArray::from(s)),
            Arc::new(BinaryArray::from_iter(bin)),
            Arc::new(Float64Array::from(f)),
            Arc::new(Float32Array::from(g)),
        ],
    )
    .unwrap();
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    store.import("f", &write_parquet(&[batch.clone()], props(Compression::UNCOMPRESSED, false, false, 100, 600))).unwrap();
    let bytes = export(&store, "f");
    let c = verify(&bytes);
    assert_eq!((c.chunks, c.offset_indexes, c.column_indexes), (5, 5, 4), "{c:?}");
    store.update_where("f", "f = 1.5", "id >= 200 AND id < 300").unwrap();
    store.delete_where("f", "id IN (1, 11, 21, 250, 251, 599) OR (id > 400 AND id < 420)").unwrap();
    let bytes = export(&store, "f");
    let c = verify(&bytes);
    assert!(full(&c), "NaN-only page rewritten: {c:?}");
    py_check(&bytes, true);
}

fn nested_batch(start: i64, n: usize) -> RecordBatch {
    let mut list = ListBuilder::new(Int64Builder::new());
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    let mut names = StringBuilder::new();
    for i in start..start + n as i64 {
        match i % 6 {
            0 => list.append_null(),
            1 => list.append(true),
            _ => {
                for j in 0..(i % 4) {
                    if (i + j) % 5 == 0 {
                        list.values().append_null()
                    } else {
                        list.values().append_value(i * 10 - j);
                    }
                }
                list.append(true);
            }
        }
        if i % 7 == 0 {
            map.append(false).unwrap();
        } else {
            for j in 0..(i % 3) {
                map.keys().append_value(format!("k{}", (i + j) % 13));
                map.values().append_option(((i + j) % 4 != 0).then_some((i * j) as i32));
            }
            map.append(true).unwrap();
        }
        names.append_option((i % 3 != 0).then(|| format!("n{}", i % 41)));
    }
    let names = names.finish();
    let st = StructArray::from(vec![
        (Arc::new(Field::new("name", DataType::Utf8, true)), Arc::new(names) as ArrayRef),
        (Arc::new(Field::new("k", DataType::Int64, false)), Arc::new(Int64Array::from_iter_values(start..start + n as i64)) as ArrayRef),
    ]);
    let ids = Int64Array::from_iter_values(start..start + n as i64);
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("l", Arc::new(list.finish()) as ArrayRef),
        ("m", Arc::new(map.finish()) as ArrayRef),
        ("st", Arc::new(st) as ArrayRef),
    ])
    .unwrap()
}

#[test]
fn nested_columns_page_indexes() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    for (v2, dict) in [(false, true), (true, false)] {
        let name = format!("n{v2}");
        let b = nested_batch(0, 2000);
        store.import(&name, &write_parquet(&[b.clone()], props(Compression::SNAPPY, v2, dict, 150, 1000))).unwrap();
        let n = verify(&export(&store, &name));
        assert!(full(&n), "{n:?}");
        store.delete_where(&name, "id IN (3, 20, 37, 999, 1000) OR (id >= 1200 AND id < 1350)").unwrap();
        store.update_where(&name, "id = -1", "id > 1500 AND id < 1520").unwrap();
        store.append(&name, &nested_batch(5000, 300), AppendMode::Buffered).unwrap();
        let bytes = export(&store, &name);
        let n = verify(&bytes);
        assert!(full(&n), "{n:?}");
        store.flush(&name).unwrap();
        let bytes2 = export(&store, &name);
        let n = verify(&bytes2);
        assert!(full(&n), "{n:?}");
        assert_eq!(concat(&read_parquet(bytes)), concat(&read_parquet(bytes2)));
    }
}

#[test]
fn stores_without_page_statistics_still_work() {
    let d = tempfile::tempdir().unwrap();
    let base = sample_batch(0, 2000);
    {
        let store = Store::open(d.path()).unwrap();
        store.import("f", &write_parquet(&[base.clone()], props(Compression::SNAPPY, false, true, 100, 1000))).unwrap();
    }
    // Simulate a store written before page statistics existed.
    {
        let conn = rusqlite::Connection::open(d.path().join("meta.db")).unwrap();
        conn.execute_batch("ALTER TABLE pages DROP COLUMN stats").unwrap();
    }
    let store = Store::open(d.path()).unwrap();
    let bytes = export(&store, "f");
    assert_eq!(concat(&read_parquet(bytes.clone())), base);
    let n = verify(&bytes);
    assert_eq!((n.offset_indexes, n.column_indexes), (n.chunks, 0), "{n:?}");
    py_check(&bytes, false);
    // Partially rewritten chunks mix old and new pages: still no column index, data intact.
    store.delete_where("f", "id = 5").unwrap();
    store.update_where("f", "name = 'q'", "id = 1500").unwrap();
    let bytes = export(&store, "f");
    let n = verify(&bytes);
    assert_eq!((n.offset_indexes, n.column_indexes), (n.chunks, 0), "{n:?}");
    let got = concat(&read_parquet(bytes));
    assert_eq!(got.num_rows(), 1999);
    // New data gets full page indexes.
    store.import("g", &write_parquet(&[base], props(Compression::SNAPPY, false, true, 100, 1000))).unwrap();
    assert!(full(&verify(&export(&store, "g"))));
}
