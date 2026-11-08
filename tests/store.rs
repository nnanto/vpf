mod common;

use std::sync::Arc;

use common::*;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescriptor;
use vpf::meta::{self, SegmentKind};
use vpf::schema::{ColumnSpec, build_descriptor, spec_for_type_name};
use vpf::stats::StoredStats;
use vpf::store::{BlobRead, NewChunk, Store};
use vpf::thrift::parse_page_header;

const MSG: &str = "
message spark_schema {
  required int64 id = 7;
  optional int96 legacy_ts;
  optional fixed_len_byte_array(16) uid (UUID);
  optional fixed_len_byte_array(9) d1 (DECIMAL(20,4));
  optional int32 d2 (DECIMAL(9,2));
  optional int64 d3 (DECIMAL(18,3));
  optional binary d4 (DECIMAL(30,2));
  optional int32 u8 (INTEGER(8,false));
  optional int64 u64 (INTEGER(64,false));
  optional int32 t (TIME(MILLIS,true));
  optional int64 tn (TIME(NANOS,false));
  optional int64 ts (TIMESTAMP(NANOS,false));
  optional binary j (JSON);
  optional binary e (ENUM);
  optional binary s (STRING);
  optional binary raw;
  required boolean b;
  optional float f;
  optional double g;
  optional int32 dt (DATE);
  optional fixed_len_byte_array(2) h (FLOAT16);
}";

#[test]
fn schema_roundtrips_through_sqlite() {
    let root = Arc::new(parse_message_type(MSG).unwrap());
    let descr = SchemaDescriptor::new(root.clone());
    let specs: Vec<ColumnSpec> = root
        .get_fields()
        .iter()
        .enumerate()
        .map(|(i, f)| ColumnSpec::from_parquet_type(i as i64 + 1, f).unwrap())
        .collect();
    let rebuilt = build_descriptor("spark_schema", &specs).unwrap();
    assert_eq!(rebuilt.root_schema(), descr.root_schema());

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("t.vpf")).unwrap();
    let kv = vec![parquet::file::metadata::KeyValue::new(
        "k".into(),
        "v".to_string(),
    )];
    let svid = store
        .write(|t| {
            let f = t.create_file("x", Compression::SNAPPY, Some("test"), 1)?;
            meta::insert_schema(t.conn(), f, None, "spark_schema", &specs, Some(&kv))
        })
        .unwrap();
    let loaded = store.read(|c| meta::load_schema(c, svid)).unwrap();
    assert_eq!(loaded.columns, specs);
    assert_eq!(
        loaded.key_value.as_ref().unwrap()[0].value.as_deref(),
        Some("v")
    );
    assert_eq!(
        loaded.descriptor().unwrap().root_schema(),
        descr.root_schema()
    );
}

const NESTED: &str = "message spark_schema {
    optional group s { required int32 a; optional binary b (STRING); optional group inner { optional double d; } }
    optional group l (LIST) { repeated group list { optional int64 element; } }
    required group m (MAP) { repeated group key_value { required binary key (STRING); optional group value (LIST) { repeated group list { optional fixed_len_byte_array(16) element (DECIMAL(30,2)); } } } }
    repeated int32 r = 7;
    optional group legacy (LIST) { repeated binary str (UTF8); }
    required int64 flat (TIMESTAMP(MICROS,true));
}";

#[test]
fn nested_schemas_roundtrip_through_sqlite() {
    let root = Arc::new(parse_message_type(NESTED).unwrap());
    let descr = SchemaDescriptor::new(root.clone());
    let specs: Vec<ColumnSpec> = root
        .get_fields()
        .iter()
        .enumerate()
        .map(|(i, f)| ColumnSpec::from_parquet_type(i as i64 + 1, f).unwrap())
        .collect();
    assert_eq!(specs.iter().filter(|s| s.is_nested()).count(), 5);
    assert!(
        specs[4].top_level_nullable()
            && specs[3].top_level_nullable()
            && !specs[2].top_level_nullable()
    );
    let rebuilt = build_descriptor("spark_schema", &specs).unwrap();
    assert_eq!(rebuilt.root_schema(), descr.root_schema());

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("t.vpf")).unwrap();
    let svid = store
        .write(|t| {
            let f = t.create_file("x", Compression::SNAPPY, None, 1)?;
            meta::insert_schema(t.conn(), f, None, "spark_schema", &specs, None)
        })
        .unwrap();
    let loaded = store.read(|c| meta::load_schema(c, svid)).unwrap();
    assert_eq!(loaded.columns, specs);
    assert_eq!(
        loaded.descriptor().unwrap().root_schema(),
        descr.root_schema()
    );

    let leaves = loaded.leaves().unwrap();
    assert_eq!(leaves.len(), descr.num_columns());
    for (i, l) in leaves.iter().enumerate() {
        let c = descr.column(i);
        assert_eq!(
            (l.max_def, l.max_rep),
            (c.max_def_level(), c.max_rep_level()),
            "{}",
            l.spec.name
        );
        assert_eq!(l.physical(), c.physical_type());
        assert_eq!(l.field, descr.get_column_root_idx(i));
        assert_eq!(l.spec.name, c.path().string());
        // The single-leaf descriptor used by the codec has the same levels and type.
        let d = l.descriptor().unwrap();
        assert_eq!(d.num_columns(), 1);
        let lc = d.column(0);
        assert_eq!(
            (lc.max_def_level(), lc.max_rep_level(), lc.physical_type()),
            (c.max_def_level(), c.max_rep_level(), c.physical_type())
        );
        assert_eq!(lc.type_length(), c.type_length());
    }
    let keys: Vec<(i64, i64)> = leaves.iter().map(|l| (l.column_id, l.leaf)).collect();
    assert_eq!(
        keys,
        vec![
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (3, 0),
            (3, 1),
            (4, 0),
            (5, 0),
            (6, 0)
        ]
    );

    // Renaming / making optional rebuilds the type tree.
    let mut s0 = specs[0].clone();
    s0.name = "renamed".into();
    s0.nullable = false;
    let t = s0.to_parquet_type().unwrap();
    assert_eq!(t.name(), "renamed");
    assert_eq!(
        t.get_basic_info().repetition(),
        parquet::basic::Repetition::REQUIRED
    );
    assert_eq!(t.get_fields().len(), 3);
}

#[test]
fn unsupported_nested_types_are_rejected() {
    let empty = Arc::new(
        parquet::schema::types::Type::group_type_builder("g")
            .with_repetition(parquet::basic::Repetition::OPTIONAL)
            .build()
            .unwrap(),
    );
    assert!(ColumnSpec::from_parquet_type(1, &empty).is_err());
}

#[test]
fn type_names_build_valid_columns() {
    for t in [
        "bool",
        "int32",
        "int64",
        "float",
        "double",
        "string",
        "binary",
        "date",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "decimal(10,2)",
        "decimal(38,0)",
        "decimal(1,1)",
    ] {
        let s = spec_for_type_name(1, "c", t, true).unwrap();
        build_descriptor("r", &[s]).unwrap();
    }
    assert!(spec_for_type_name(1, "c", "decimal(39,1)", true).is_err());
    assert!(spec_for_type_name(1, "c", "nope", true).is_err());
}

#[test]
fn statistics_roundtrip_all_types() {
    let bytes = write_parquet(
        &[sample_batch(0, 3000)],
        props(Compression::SNAPPY, false, true, 500, 3000),
    );
    let md = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();
    let mut n = 0;
    for col in md.row_group(0).columns() {
        let s = col.statistics().expect("stats");
        let stored = StoredStats::from_parquet(s);
        let json = serde_json::to_string(&stored).unwrap();
        let back: StoredStats = serde_json::from_str(&json).unwrap();
        assert_eq!(
            &back.to_parquet(col.column_type()).unwrap(),
            s,
            "{}",
            col.column_path()
        );
        let relaxed = back
            .after_row_removal()
            .to_parquet(col.column_type())
            .unwrap();
        assert_eq!(relaxed.null_count_opt(), None);
        assert!(!relaxed.min_is_exact() && !relaxed.max_is_exact());
        assert_eq!(relaxed.min_bytes_opt(), s.min_bytes_opt());
        n += 1;
    }
    assert_eq!(n, 11);
}

#[test]
fn write_txn_is_atomic_and_blobs_are_stable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("t.vpf")).unwrap();
    let bytes = write_parquet(
        &[sample_batch(0, 100)],
        props(Compression::UNCOMPRESSED, false, false, 100, 100),
    );
    let md = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();
    let (start, _) = md.row_group(0).column(0).byte_range();
    let h = parse_page_header(&bytes[start as usize..]).unwrap();
    let page = bytes[start as usize..start as usize + h.total_len()].to_vec();

    // Failed transaction: nothing visible afterwards.
    let r: vpf::Result<()> = store.write(|t| {
        t.create_file("f", Compression::SNAPPY, None, 1)?;
        t.put_page(&page, &h)?;
        Err(vpf::Error::Invalid("boom".into()))
    });
    assert!(r.is_err());
    assert!(store.read(|c| meta::list_files(c)).unwrap().is_empty());

    let (page_id, chunk_id, rg_id, fid, s1) = store
        .write(|t| {
            let fid = t.create_file("f", Compression::SNAPPY, None, 1)?;
            let page_id = t.put_page(&page, &h)?;
            let chunk_id = t.put_chunk(
                &NewChunk {
                    physical: parquet::basic::Type::INT64,
                    codec: Compression::UNCOMPRESSED,
                    encodings: vec![],
                    stats: None,
                },
                &[page_id],
            )?;
            let rg = t.put_row_group(100, &[((1, 0), chunk_id)])?;
            let s1 = t.new_snapshot(fid, 1, "test", None)?;
            t.append_segment(fid, SegmentKind::RowGroup, rg, s1)?;
            assert_eq!(t.finish_snapshot(fid, s1)?, 100);
            Ok((page_id, chunk_id, rg, fid, s1))
        })
        .unwrap();
    let chunk = store.read(|c| meta::load_chunk(c, chunk_id)).unwrap();
    assert_eq!(chunk.pages[0].page_id, page_id);
    assert_eq!(chunk.num_values, 100);
    assert_eq!(chunk.pages[0].num_rows, 100);
    let got = store
        .blobs()
        .read(chunk.pages[0].blob_offset, chunk.pages[0].blob_len)
        .unwrap();
    assert_eq!(got, page);
    // The aborted transaction's page bytes were rolled back with its metadata.
    assert_eq!(chunk.pages[0].blob_offset, 0);
    assert_eq!(
        store
            .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        1
    );

    // Replace + append segments in a second snapshot; visibility is per-snapshot.
    let s2 = store
        .write(|t| {
            let s2 = t.new_snapshot(fid, 1, "test2", None)?;
            let segs = meta::visible_segments(t.conn(), fid, s1)?;
            let rg2 = t.put_row_group(100, &[((1, 0), chunk_id)])?;
            t.replace_segment(fid, &segs[0], SegmentKind::RowGroup, rg2, s2)?;
            let b = t.put_batch(1, 5, b"payload")?;
            t.append_segment(fid, SegmentKind::Batch, b, s2)?;
            assert_eq!(t.finish_snapshot(fid, s2)?, 105);
            Ok(s2)
        })
        .unwrap();
    let v1 = store.read(|c| meta::visible_segments(c, fid, s1)).unwrap();
    let v2 = store.read(|c| meta::visible_segments(c, fid, s2)).unwrap();
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0].obj_id, rg_id);
    assert_eq!(v2.len(), 2);
    assert_eq!(v2[0].pos, v1[0].pos);
    assert_ne!(v2[0].obj_id, rg_id);
    assert_eq!(v2[1].kind, SegmentKind::Batch);
    let snaps = store.read(|c| meta::list_snapshots(c, fid)).unwrap();
    assert_eq!(
        snaps
            .iter()
            .map(|s| (s.num_rows, s.parent_id))
            .collect::<Vec<_>>(),
        vec![(100, None), (105, Some(s1))]
    );
    drop(store);
    // Reopen: persisted.
    let store = Store::open(dir.path().join("t.vpf")).unwrap();
    assert_eq!(
        store
            .read(|c| meta::load_file(c, "f"))
            .unwrap()
            .current_snapshot_id,
        Some(s2)
    );
}

#[test]
fn segment_position_lookup_is_bounded_and_added_to_older_stores() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.vpf");
    let store = Store::open(&path).unwrap();
    store
        .write(|t| {
            let fid = t.create_file("f", Compression::SNAPPY, None, 1)?;
            let mut insert = t.conn().prepare(
                "INSERT INTO segments (file_id, pos, kind, obj_id, added_in, removed_in)
                 VALUES (?1, ?2, 0, 1, 1, 2)",
            )?;
            for pos in 0..10_000 {
                insert.execute(rusqlite::params![fid, pos])?;
            }
            t.conn().execute_batch("DROP INDEX idx_segments_pos")?;
            Ok(())
        })
        .unwrap();
    drop(store);

    let store = Store::open(&path).unwrap();
    store
        .read(|c| {
            let mut statement =
                c.prepare("SELECT COALESCE(MAX(pos), -1) + 1 FROM segments WHERE file_id = ?1")?;
            let pos: i64 = statement.query_row([1], |r| r.get(0))?;
            assert_eq!(
                pos, 10_000,
                "removed segments still reserve their positions"
            );
            let steps = statement.get_status(rusqlite::StatementStatus::VmStep);
            assert!(steps < 50, "position lookup executed {steps} VM steps");
            Ok(())
        })
        .unwrap();
}
