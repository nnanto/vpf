mod common;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::ops::snapshot::SegmentData;
use vpf::schema::spec_for_type_name;
use vpf::store::Store;

fn rows_of(store: &Store, name: &str, snap: Option<i64>) -> RecordBatch {
    read_virtual(&store.open_snapshot(name, snap).unwrap())
}

fn kinds(store: &Store, name: &str) -> String {
    store
        .view(name, None)
        .unwrap()
        .segments
        .iter()
        .map(|s| match s {
            SegmentData::RowGroup { .. } => 'R',
            SegmentData::Batch { .. } => 'B',
        })
        .collect()
}

#[test]
fn buffered_and_direct_appends_are_visible_with_time_travel() {
    let d = tempfile::tempdir().unwrap();
    for (fi, codec) in codecs().into_iter().enumerate() {
        let name = format!("f{fi}");
        let store = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
        let base = sample_batch(0, 1000);
        let imp = store
            .import(
                &name,
                &write_parquet(&[base.clone()], props(codec, fi % 2 == 1, true, 100, 400)),
            )
            .unwrap();
        let mut expected = vec![base];
        let mut snaps = vec![(imp.snapshot_id, 1000)];
        let mut start = 1000;
        for (i, (n, mode)) in [
            (1, AppendMode::Buffered),
            (37, AppendMode::Auto),
            (500, AppendMode::Direct),
            (3, AppendMode::Buffered),
            (70_000, AppendMode::Auto),
            (250, AppendMode::Buffered),
        ]
        .into_iter()
        .enumerate()
        {
            let b = sample_batch(start, n);
            let s = store.append(&name, &b, mode).unwrap();
            assert_eq!(s.num_rows, n as i64);
            assert_eq!(
                s.buffered,
                mode == AppendMode::Buffered || (mode == AppendMode::Auto && n < 65_536),
                "{i}"
            );
            expected.push(b);
            start += n as i64;
            snaps.push((s.snapshot_id, start));
            // Immediately visible.
            assert_eq!(rows_of(&store, &name, None), concat(&expected));
        }
        assert_eq!(kinds(&store, &name), "RRRBBRBRB");
        let vf = store.open_snapshot(&name, None).unwrap();
        // import: 3 rgs; B B -> one synthesized rg; R; B; R; B
        assert_eq!(vf.metadata().num_row_groups(), 3 + 1 + 1 + 1 + 1 + 1);
        // Time travel: every snapshot sees exactly the prefix that existed.
        let all = concat(&expected);
        for (sid, rows) in &snaps {
            let got = rows_of(&store, &name, Some(*sid));
            assert_eq!(got, all.slice(0, *rows as usize), "snapshot {sid}");
            assert_eq!(
                store.view(&name, Some(*sid)).unwrap().snapshot.num_rows,
                *rows
            );
        }
        // Reads are deterministic (cache) and stable across store reopen.
        let bytes = vf.to_bytes().unwrap();
        assert_eq!(
            store
                .open_snapshot(&name, None)
                .unwrap()
                .to_bytes()
                .unwrap(),
            bytes
        );
        let again = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
        assert_eq!(
            again
                .open_snapshot(&name, None)
                .unwrap()
                .to_bytes()
                .unwrap(),
            bytes
        );
        if python().is_some() {
            let exp = d.path().join(format!("{name}.exp.parquet"));
            let act = d.path().join(format!("{name}.parquet"));
            std::fs::write(
                &exp,
                write_parquet(
                    &[all.clone()],
                    props(Compression::SNAPPY, false, false, 10_000, 1 << 20),
                )
                .to_vec(),
            )
            .unwrap();
            store.export(&name, None, &act).unwrap();
            run_python(&[
                &py_script("check_equal.py"),
                exp.to_str().unwrap(),
                act.to_str().unwrap(),
            ]);
        }
    }
}

#[test]
fn flush_materializes_batches_and_preserves_history() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("t.vpf")).unwrap();
    store
        .import(
            "f",
            &write_parquet(
                &[sample_batch(0, 300)],
                props(Compression::ZSTD(Default::default()), false, true, 50, 100),
            ),
        )
        .unwrap();
    let before = store.view("f", None).unwrap();
    let mut expected = vec![sample_batch(0, 300)];
    let mut s = 300;
    for n in [5, 10, 15] {
        expected.push(sample_batch(s, n));
        store
            .append("f", expected.last().unwrap(), AppendMode::Buffered)
            .unwrap();
        s += n as i64;
    }
    store
        .append("f", &sample_batch(s, 100), AppendMode::Direct)
        .unwrap();
    expected.push(sample_batch(s, 100));
    s += 100;
    store
        .append("f", &sample_batch(s, 7), AppendMode::Buffered)
        .unwrap();
    expected.push(sample_batch(s, 7));
    let pre_flush = store.view("f", None).unwrap().snapshot.snapshot_id;
    assert_eq!(kinds(&store, "f"), "RRRBBBRB");
    let fs = store.flush("f").unwrap().unwrap();
    assert_eq!(kinds(&store, "f"), "RRRRRR");
    assert_eq!(store.flush("f").unwrap(), None, "nothing left to flush");
    assert_eq!(rows_of(&store, "f", None), concat(&expected));
    assert_eq!(rows_of(&store, "f", Some(pre_flush)), concat(&expected));
    assert_eq!(
        store.view("f", Some(fs)).unwrap().snapshot.operation,
        "flush"
    );
    // Imported row groups keep their original chunks (no rewrite).
    let after = store.view("f", None).unwrap();
    for i in 0..3 {
        let (SegmentData::RowGroup { rg: a, .. }, SegmentData::RowGroup { rg: b, .. }) =
            (&before.segments[i], &after.segments[i])
        else {
            panic!()
        };
        assert_eq!(a.rg_id, b.rg_id);
    }
    // The pre-flush snapshot still serves its batches.
    assert_eq!(
        store
            .view("f", Some(pre_flush))
            .unwrap()
            .segments
            .iter()
            .filter(|s| matches!(s, SegmentData::Batch { .. }))
            .count(),
        4
    );
}

#[test]
fn create_from_scratch_and_conversion_rules() {
    let d = tempfile::tempdir().unwrap();
    let cols = vec![
        spec_for_type_name(0, "id", "int64", false).unwrap(),
        spec_for_type_name(0, "small", "int32", true).unwrap(),
        spec_for_type_name(0, "s", "string", true).unwrap(),
        spec_for_type_name(0, "amt", "decimal(20,3)", true).unwrap(),
        spec_for_type_name(0, "t", "timestamp_ms", true).unwrap(),
    ];
    for (i, codec) in codecs().into_iter().chain([Compression::LZO]).enumerate() {
        let name = format!("c{i}");
        let store = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
        let sid = store.create(&name, &cols, codec).unwrap();
        let empty = store.open_snapshot(&name, Some(sid)).unwrap();
        assert_eq!(empty.metadata().num_row_groups(), 0);
        assert_eq!(read_virtual(&empty).num_rows(), 0);

        // Input uses different-but-castable types and omits a nullable column.
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("small", DataType::Int8, true),
                Field::new("id", DataType::Int32, false),
                Field::new("amt", DataType::Decimal128(9, 1), true),
                Field::new("s", DataType::LargeUtf8, true),
            ])),
            vec![
                Arc::new(Int8Array::from(vec![Some(1), None, Some(-3)])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345), Some(-1), None])
                        .with_precision_and_scale(9, 1)
                        .unwrap(),
                ),
                Arc::new(LargeStringArray::from(vec![Some("a"), Some(""), None])),
            ],
        )
        .unwrap();
        for mode in [AppendMode::Buffered, AppendMode::Direct] {
            store.append(&name, &input, mode).unwrap();
        }
        let got = rows_of(&store, &name, None);
        assert_eq!(got.num_rows(), 6);
        assert_eq!(
            got.column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values()
                .to_vec(),
            vec![10, 20, 30, 10, 20, 30]
        );
        let amt = got
            .column(3)
            .as_primitive::<arrow::datatypes::Decimal128Type>();
        assert_eq!(amt.data_type(), &DataType::Decimal128(20, 3));
        assert_eq!(
            amt.iter().collect::<Vec<_>>(),
            vec![
                Some(1234500),
                Some(-100),
                None,
                Some(1234500),
                Some(-100),
                None
            ]
        );
        assert_eq!(got.column(4).null_count(), 6);
        assert_eq!(
            got.column(1)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .iter()
                .collect::<Vec<_>>()[..3],
            [Some(1), None, Some(-3)]
        );
        if codec == Compression::LZO {
            let vf = store.open_snapshot(&name, None).unwrap();
            assert_eq!(
                vf.metadata().row_group(0).column(0).compression(),
                Compression::SNAPPY
            );
        }
    }
    let store = Store::open(d.path().join("c0.vpf")).unwrap();
    let snap = store.view("c0", None).unwrap().snapshot.snapshot_id;
    let bad = |fields: Vec<Field>, arrays: Vec<ArrayRef>| {
        let b = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        for mode in [AppendMode::Buffered, AppendMode::Direct] {
            let e = store.append("c0", &b, mode).unwrap_err();
            eprintln!("{e}");
        }
    };
    // Missing required column.
    bad(
        vec![Field::new("small", DataType::Int32, true)],
        vec![Arc::new(Int32Array::from(vec![1]))],
    );
    // Unknown column.
    bad(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("nope", DataType::Int64, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    );
    // NULL into a required column.
    bad(
        vec![Field::new("id", DataType::Int64, true)],
        vec![Arc::new(Int64Array::from(vec![None]))],
    );
    // Lossy cast.
    bad(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("small", DataType::Int64, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![i64::MAX])),
        ],
    );
    // Decimal overflow.
    bad(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amt", DataType::Decimal128(38, 0), true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(
                Decimal128Array::from(vec![Some(i128::MAX / 10)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    );
    // Unparseable string.
    bad(
        vec![Field::new("id", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["x"]))],
    );
    // Empty append.
    assert!(
        store
            .append("c0", &sample_batch(0, 0), AppendMode::Auto)
            .is_err()
    );
    assert!(
        store
            .append("missing", &sample_batch(0, 1), AppendMode::Auto)
            .is_err()
    );
    // Failed appends leave no trace.
    assert_eq!(store.view("c0", None).unwrap().snapshot.snapshot_id, snap);
    assert_eq!(rows_of(&store, "c0", None).num_rows(), 6);

    // create_from_arrow uses the Arrow -> Parquet mapping.
    let schema = sample_batch(0, 1).schema();
    let store = Store::open(d.path().join("fa.vpf")).unwrap();
    store
        .create_from_arrow("fa", &schema, Compression::SNAPPY)
        .unwrap();
    for n in [3, 4] {
        store
            .append("fa", &sample_batch(0, n), AppendMode::Buffered)
            .unwrap();
    }
    let got = rows_of(&store, "fa", None);
    assert_eq!(got.num_rows(), 7);
    assert_eq!(
        got.column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .values()
            .to_vec(),
        vec![0, 1, 2, 0, 1, 2, 3]
    );
    // The embedded Arrow schema makes reads return exactly the original types.
    assert_eq!(got, concat(&[sample_batch(0, 3), sample_batch(0, 4)]));
}

#[test]
fn open_snapshot_is_isolated_from_later_appends() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("t.vpf")).unwrap();
    store
        .import(
            "f",
            &write_parquet(
                &[sample_batch(0, 100)],
                props(Compression::SNAPPY, false, true, 30, 100),
            ),
        )
        .unwrap();
    store
        .append("f", &sample_batch(100, 5), AppendMode::Buffered)
        .unwrap();
    let vf = store.open_snapshot("f", None).unwrap();
    let before = vf.to_bytes().unwrap();
    store
        .append("f", &sample_batch(105, 5), AppendMode::Buffered)
        .unwrap();
    store
        .append("f", &sample_batch(110, 500), AppendMode::Direct)
        .unwrap();
    store.flush("f").unwrap();
    assert_eq!(vf.to_bytes().unwrap(), before);
    assert_eq!(read_virtual(&vf), sample_batch(0, 105));
    assert_eq!(rows_of(&store, "f", None), sample_batch(0, 610));
}

#[test]
fn direct_append_splits_large_batches_at_row_group_limit() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("t.vpf")).unwrap();
    let n = vpf::ops::append::MAX_ROW_GROUP_ROWS + 17;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..n as i64)),
            Arc::new(Int32Array::from_iter(
                (0..n).map(|i| (i % 7 != 0).then_some(i as i32)),
            )),
        ],
    )
    .unwrap();
    store
        .create_from_arrow("f", &batch.schema(), Compression::SNAPPY)
        .unwrap();
    let summary = store.append("f", &batch, AppendMode::Direct).unwrap();
    assert_eq!(summary.num_rows, n as i64);
    let snapshot = store.open_snapshot("f", None).unwrap();
    assert_eq!(snapshot.metadata().num_row_groups(), 2);
    assert_eq!(snapshot.metadata().row_group(0).num_rows(), (n - 17) as i64);
    assert_eq!(snapshot.metadata().row_group(1).num_rows(), 17);
    assert_eq!(read_virtual(&snapshot), batch);
}

#[test]
fn pyarrow_fixture_appends_roundtrip() {
    if python().is_none() {
        eprintln!("skipping: VPF_PYTHON not set");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let fx = d.path().join("fx");
    std::fs::create_dir_all(&fx).unwrap();
    run_python(&[&py_script("gen_fixtures.py"), fx.to_str().unwrap()]);
    for name in [
        "v1_snappy",
        "v2_zstd",
        "gzip_nodict",
        "pageindex_lz4",
        "int96_brotli",
        "uncompressed_v2_crc",
    ] {
        let src = fx.join(format!("{name}.parquet"));
        let store = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
        let srcs = src.to_str().unwrap();
        store.import_file(name, &src).unwrap();
        let all = concat(&read_parquet(bytes::Bytes::from(
            std::fs::read(&src).unwrap(),
        )));
        store
            .append(name, &all.slice(10, 20), AppendMode::Buffered)
            .unwrap();
        store
            .append(name, &all.slice(100, 1), AppendMode::Buffered)
            .unwrap();
        store
            .append(name, &all.slice(0, 3000), AppendMode::Direct)
            .unwrap();
        store
            .append(name, &all.slice(4990, 10), AppendMode::Buffered)
            .unwrap();
        let want = concat(&[
            all.clone(),
            all.slice(10, 20),
            all.slice(100, 1),
            all.slice(0, 3000),
            all.slice(4990, 10),
        ]);
        assert_eq!(rows_of(&store, name, None), want, "{name}");
        let out = d.path().join(format!("{name}.out.parquet"));
        store.export(name, None, &out).unwrap();
        let parts = [
            format!("{srcs}:0:5000"),
            format!("{srcs}:10:20"),
            format!("{srcs}:100:1"),
            format!("{srcs}:0:3000"),
            format!("{srcs}:4990:10"),
        ];
        let mut args = vec![
            py_script("check_concat.py"),
            out.to_str().unwrap().to_string(),
        ];
        args.extend(parts);
        run_python(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    }
}
