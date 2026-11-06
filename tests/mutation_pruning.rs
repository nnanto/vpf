mod common;

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

use arrow::array::*;
use arrow::compute::{filter_record_batch, kernels::zip::zip};
use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::ops::snapshot::SegmentData;
use vpf::predicate::{Expr, Literal, literal_array};
use vpf::store::Store;

#[test]
fn pruned_masks_match_arrow_across_pages_groups_and_buffers() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let base = sample_batch(0, 400);
    store.import("f", &write_parquet(&[base], props(Compression::SNAPPY, false, true, 17, 100))).unwrap();
    store.append("f", &sample_batch(400, 20), AppendMode::Buffered).unwrap();
    store.append("f", &sample_batch(420, 20), AppendMode::Buffered).unwrap();
    let view = store.view("f", None).unwrap();
    let all = sample_batch(0, 440);
    for predicate in [
        "TRUE", "FALSE", "id = 123", "123 = id", "id != 123", "id < 123", "id <= 123",
        "id > 123", "id >= 123", "123 < id", "123 >= id",
        "id BETWEEN 120 AND 160", "id NOT BETWEEN 120 AND 160",
        "id IN (1, 119, 401)", "id NOT IN (1, 119, 401)",
        "id IN (NULL, 123)", "id NOT IN (NULL, 123)",
        "NOT (id < 100 OR id >= 300)", "NOT (id > 100 AND id < 300)",
        "id = NULL", "NOT (id = NULL)", "i32 IS NULL", "i32 IS NOT NULL",
        "i32 != 6", "NOT (i32 IN (3, NULL))", "id > 99 AND i32 < 360",
        "name = 'name-5'", "name != 'name-5'", "name < 'name-5'",
        "name LIKE 'name-1%' AND id >= 200",
        "flag = TRUE", "flag IS NULL", "u16 > 700", "u16 <= 510",
        "day >= '2020-01-01'", "ts > '2020-01-01'", "bin = 'abc'",
        "amount > 100.00", "score >= 300", "f32 < 0", "id = i32",
    ] {
        let expr = Expr::parse(predicate).unwrap();
        let actual: Vec<_> = store.match_rows(&view, &expr).unwrap().into_iter().flatten().collect();
        assert_eq!(actual, expr.matches(&all).unwrap(), "{predicate}");
    }
    for predicate in ["id > 99999 AND missing = 1", "id > 99999 AND id = 'invalid'"] {
        let expr = Expr::parse(predicate).unwrap();
        assert!(store.match_rows(&view, &expr).is_err(), "{predicate}");
        assert!(store.delete_where("f", predicate).is_err(), "{predicate}");
    }
}

#[test]
fn extreme_values_truncated_bounds_and_missing_stats_are_conservative() {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let prefix = "x".repeat(80);
    let low = format!("{prefix}a");
    let high = format!("{prefix}z");
    let schema = Arc::new(Schema::new(vec![
        Field::new("signed", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("float", DataType::Float64, true),
        Field::new("decimal", DataType::Decimal128(20, 2), true),
        Field::new("empty", DataType::Int32, true),
    ]));
    let base = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(-1), None, Some(0), Some(i64::MAX), Some(1)])),
        Arc::new(UInt64Array::from(vec![Some(0), Some(u64::MAX), None, Some(1 << 63), Some(1), Some(2)])),
        Arc::new(UInt32Array::from(vec![Some(0), Some(u32::MAX), None, Some(1 << 31), Some(1), Some(2)])),
        Arc::new(StringArray::from(vec![Some(low.as_str()), Some(high.as_str()), None, Some("a"), Some("z"), Some("x")])),
        Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0), None, Some(f64::NAN), Some(f64::INFINITY), Some(f64::NEG_INFINITY)])),
        Arc::new(Decimal128Array::from(vec![Some(-100), Some(100), None, Some(1), Some(0), Some(200)]).with_precision_and_scale(20, 2).unwrap()),
        new_null_array(&DataType::Int32, 6),
    ]).unwrap();
    store.import("f", &write_parquet(&[base.clone()], props(Compression::SNAPPY, true, false, 2, 4))).unwrap();
    let mut predicates: Vec<String> = [
        "signed = -9223372036854775808", "signed >= 9223372036854775807", "signed < 0",
        "unsigned = 18446744073709551615", "unsigned > 9223372036854775807",
        "unsigned < 9223372036854775808", "u32 >= 2147483648", "u32 != 4294967295",
        "float = 0", "float != 0", "float > 1", "float < 0",
        "decimal > 1.00", "decimal < -1.00", "decimal > 999999999999999999.99",
        "empty = 1", "empty IS NOT NULL", "empty IS NULL", "NOT (empty IN (1, NULL))",
        "text < 'a'", "text > 'z'", "text IS NULL",
    ].into_iter().map(str::to_string).collect();
    for op in ["=", "!=", "<", "<=", ">", ">="] {
        predicates.push(format!("text {op} '{low}'"));
        predicates.push(format!("text {op} '{high}'"));
        predicates.push(format!("text {op} '{}'", "x".repeat(64)));
    }
    for missing in [false, true] {
        if missing {
            store.write(|t| { t.conn().execute("UPDATE pages SET stats = NULL", [])?; Ok(()) }).unwrap();
        }
        let view = store.view("f", None).unwrap();
        for predicate in &predicates {
            let expr = Expr::parse(predicate).unwrap();
            let actual: Vec<_> = store.match_rows(&view, &expr).unwrap().into_iter().flatten().collect();
            assert_eq!(actual, expr.matches(&base).unwrap(), "{predicate}, missing={missing}");
        }
    }
}

#[test]
fn pruning_skips_unreadable_pages_and_does_not_trust_source_chunk_stats() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let base = sample_batch(0, 300);
    store.import("f", &write_parquet(&[base.clone()], props(Compression::SNAPPY, false, false, 20, 100))).unwrap();
    let before = store.view("f", None).unwrap();
    let snapshot = before.snapshot.snapshot_id;
    // Make the source chunk bounds deliberately wrong. Pruning must use VPF's
    // page statistics, not those imported bounds.
    store.write(|t| {
        for seg in &before.segments {
            if let SegmentData::RowGroup { chunks, .. } = seg {
                let mut stats = chunks[0].stats.clone().unwrap();
                stats.min = Some(0i64.to_le_bytes().to_vec());
                stats.max = stats.min.clone();
                t.conn().execute("UPDATE chunks SET stats = ?1 WHERE chunk_id = ?2",
                    rusqlite::params![serde_json::to_string(&stats)?, chunks[0].chunk_id])?;
            }
        }
        Ok(())
    }).unwrap();
    let file = OpenOptions::new().write(true).open(dir.path().join("data.bin")).unwrap();
    let mut saved = Vec::new();
    for seg in &before.segments {
        let SegmentData::RowGroup { chunks, .. } = seg else { unreachable!() };
        let mut row = 0;
        for page in &chunks[0].pages {
            if !page.kind.is_data() { continue; }
            let end = row + page.num_rows;
            // Keep only the ID page containing 150 readable; all other pages
            // should be skipped, including pages in the matching row group.
            let start_id = match seg {
                SegmentData::RowGroup { seg, .. } => seg.pos * 100,
                _ => unreachable!(),
            };
            if !(start_id + row <= 150 && 150 < start_id + end) {
                saved.push((page.blob_offset, store.blobs().read(page.blob_offset, page.blob_len).unwrap()));
                file.write_all_at(&vec![0xff; page.blob_len as usize], page.blob_offset).unwrap();
            }
            row = end;
        }
    }
    let update = store.update_where("f", "name = 'changed'", "id = 150");
    let delete = store.delete_where("f", "id < 0");
    for (offset, bytes) in saved { file.write_all_at(&bytes, offset).unwrap(); }
    assert_eq!(update.unwrap().rows_updated, 1);
    assert_eq!(delete.unwrap().snapshot_id, None);
    let mut columns = base.columns().to_vec();
    let matches = BooleanArray::from((0..300).map(|i| i == 150).collect::<Vec<_>>());
    columns[2] = zip(&matches, &Scalar::new(literal_array(&Literal::Str("changed".into()), columns[2].data_type()).unwrap()), &columns[2]).unwrap();
    let expected = RecordBatch::try_new(base.schema(), columns).unwrap();
    assert_eq!(read_virtual(&store.open_snapshot("f", None).unwrap()), expected);
    assert_eq!(read_virtual(&store.open_snapshot("f", Some(snapshot)).unwrap()), base);
}

#[test]
fn later_group_failure_rolls_back_all_mutations_and_can_be_retried() {
    for update in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let base = sample_batch(0, 1000);
        store.import("f", &write_parquet(&[base.clone()], props(Compression::SNAPPY, false, true, 20, 100))).unwrap();
        let before = store.view("f", None).unwrap();
        let old_len = std::fs::metadata(dir.path().join("data.bin")).unwrap().len();
        let old_pages: i64 = store.read(|c| Ok(c.query_row("SELECT COUNT(*) FROM pages", [], |r| r.get(0))?)).unwrap();
        store.write(|t| {
            t.conn().execute_batch(
                "CREATE TRIGGER fail_sixth_group BEFORE INSERT ON row_groups
                 WHEN (SELECT COUNT(*) FROM row_groups) >= 15
                 BEGIN SELECT RAISE(ABORT, 'injected sixth group failure'); END;"
            )?;
            Ok(())
        }).unwrap();
        let error = if update {
            store.update_where("f", "name = 'changed'", "TRUE").unwrap_err()
        } else {
            store.delete_where("f", "i32 IS NOT NULL").unwrap_err()
        };
        assert!(error.to_string().contains("injected sixth group failure"));
        assert!(std::fs::metadata(dir.path().join("data.bin")).unwrap().len() > old_len);
        assert_eq!(store.view("f", None).unwrap().snapshot.snapshot_id, before.snapshot.snapshot_id);
        assert_eq!(store.read(|c| Ok(c.query_row("SELECT COUNT(*) FROM pages", [], |r| r.get::<_, i64>(0))?)).unwrap(), old_pages);
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(read_virtual(&store.open_snapshot("f", None).unwrap()), base);
        store.write(|t| { t.conn().execute_batch("DROP TRIGGER fail_sixth_group")?; Ok(()) }).unwrap();
        if update {
            assert_eq!(store.update_where("f", "name = 'changed'", "TRUE").unwrap().rows_updated, 1000);
        } else {
            let keep = BooleanArray::from(Expr::parse("i32 IS NULL").unwrap().matches(&base).unwrap());
            let expected = filter_record_batch(&base, &keep).unwrap();
            assert_eq!(store.delete_where("f", "i32 IS NOT NULL").unwrap().rows_deleted, (1000 - expected.num_rows()) as i64);
            assert_eq!(read_virtual(&store.open_snapshot("f", None).unwrap()), expected);
        }
        assert_eq!(read_virtual(&store.open_snapshot("f", Some(before.snapshot.snapshot_id)).unwrap()), base);
    }
}
