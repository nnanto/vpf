mod common;

use arrow::array::*;
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::zip::zip;
use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::ops::snapshot::SegmentData;
use vpf::predicate::{Expr, literal_array, parse_assignments};
use vpf::store::Store;

fn rows_of(store: &Store, name: &str, snap: Option<i64>) -> RecordBatch {
    read_virtual(&store.open_snapshot(name, snap).unwrap())
}

fn mask(pred: &str, b: &RecordBatch) -> BooleanArray {
    BooleanArray::from(Expr::parse(pred).unwrap().matches(b).unwrap())
}

/// Reference implementation of UPDATE on an Arrow batch.
fn model_update(b: &RecordBatch, set: &str, pred: &str) -> RecordBatch {
    let m = mask(pred, b);
    let mut cols = b.columns().to_vec();
    for (col, lit) in parse_assignments(set).unwrap() {
        let i = b.schema().index_of(&col).unwrap();
        let v = literal_array(&lit, cols[i].data_type()).unwrap();
        cols[i] = zip(&m, &Scalar::new(v), &cols[i]).unwrap();
    }
    RecordBatch::try_new(b.schema(), cols).unwrap()
}

fn model_delete(b: &RecordBatch, pred: &str) -> RecordBatch {
    let m = mask(pred, b);
    filter_record_batch(b, &arrow::compute::not(&m).unwrap()).unwrap()
}

#[test]
fn single_cell_update_rewrites_one_page_of_one_column() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("t.vpf")).unwrap();
    let base = sample_batch(0, 3000);
    store
        .import(
            "f",
            &write_parquet(
                &[base.clone()],
                props(Compression::SNAPPY, false, true, 100, 3000),
            ),
        )
        .unwrap();
    let before = store.view("f", None).unwrap();
    let s = store
        .update_where("f", "name = 'changed'", "id = 1234")
        .unwrap();
    assert_eq!(s.rows_updated, 1);
    assert_eq!(s.pages.rewritten, 1);
    assert_eq!(s.chunks_reused, base.num_columns() - 1);
    let after = store.view("f", None).unwrap();
    let (SegmentData::RowGroup { chunks: c0, .. }, SegmentData::RowGroup { chunks: c1, .. }) =
        (&before.segments[0], &after.segments[0])
    else {
        panic!()
    };
    let name_idx = 2;
    for (i, (a, b)) in c0.iter().zip(c1).enumerate() {
        if i == name_idx {
            assert_ne!(a.chunk_id, b.chunk_id);
            let old: Vec<i64> = a.pages.iter().map(|p| p.page_id).collect();
            assert_eq!(
                b.pages.iter().filter(|p| !old.contains(&p.page_id)).count(),
                1
            );
            assert_eq!(b.pages.len(), a.pages.len());
            let st = b.stats.as_ref().expect("stats recomputed on updated chunk");
            assert!(st.min_exact && st.max_exact && st.null_count.is_some());
        } else {
            assert_eq!(a.chunk_id, b.chunk_id, "column {i} chunk reused");
        }
    }
    assert_eq!(
        rows_of(&store, "f", None),
        model_update(&base, "name = 'changed'", "id = 1234")
    );
    assert_eq!(
        rows_of(&store, "f", Some(before.snapshot.snapshot_id)),
        base
    );
    let vf = store.open_snapshot("f", None).unwrap();
    let st = vf
        .metadata()
        .row_group(0)
        .column(name_idx)
        .statistics()
        .unwrap();
    assert_eq!(st.min_bytes_opt(), Some(&b"changed"[..]));
    assert_eq!(st.max_bytes_opt(), Some(&b"name-96"[..]));
    assert!(st.min_is_exact() && st.max_is_exact());
    assert!(
        vf.metadata()
            .row_group(0)
            .column(0)
            .statistics()
            .unwrap()
            .min_is_exact()
    );
}

#[test]
fn updates_and_deletes_match_model() {
    let ops: Vec<(&str, &str)> = vec![
        ("U", "name = 'zzz', score = NULL|id < 50"),
        ("U", "i32 = -1|i32 IS NULL"),
        ("D", "id BETWEEN 100 AND 180"),
        (
            "U",
            "amount = 99999.99, flag = true, day = '2024-02-29'|name = 'name-5' OR id >= 2390",
        ),
        (
            "U",
            "ts = '2001-01-01T00:00:00', bin = 'abc', u16 = 65535|id IN (1, 2399, 2200)",
        ),
        ("U", "f32 = 0.25|TRUE"),
        ("U", "id = 0|id = 7"),
        ("D", "name = 'zzz'"),
        ("U", "name = NULL|id > 2000"),
    ];
    let d = tempfile::tempdir().unwrap();
    let mut fi = 0;
    for codec in codecs() {
        for (v2, dict) in [(false, true), (true, false), (true, true)] {
            let name = format!("f{fi}");
            fi += 1;
            let store = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
            store
                .import(
                    &name,
                    &write_parquet(&[sample_batch(0, 1800)], props(codec, v2, dict, 64, 600)),
                )
                .unwrap();
            store
                .append(&name, &sample_batch(1800, 200), AppendMode::Buffered)
                .unwrap();
            store
                .append(&name, &sample_batch(2000, 300), AppendMode::Direct)
                .unwrap();
            store
                .append(&name, &sample_batch(2300, 100), AppendMode::Buffered)
                .unwrap();
            let mut model = sample_batch(0, 2400);
            let mut history = Vec::new();
            for (k, op) in &ops {
                let (sid, n) = if *k == "U" {
                    let (set, pred) = op.split_once('|').unwrap();
                    let want = mask(pred, &model).true_count();
                    model = model_update(&model, set, pred);
                    let s = store.update_where(&name, set, pred).unwrap();
                    (s.snapshot_id, (s.rows_updated as usize, want))
                } else {
                    let want = mask(op, &model).true_count();
                    model = model_delete(&model, op);
                    let s = store.delete_where(&name, op).unwrap();
                    (s.snapshot_id, (s.rows_deleted as usize, want))
                };
                assert_eq!(n.0, n.1, "{op}");
                assert_eq!(
                    rows_of(&store, &name, None),
                    model,
                    "{name} {codec} v2={v2} dict={dict} {op}"
                );
                history.push((sid.unwrap(), model.clone()));
            }
            for (sid, want) in &history {
                assert_eq!(&rows_of(&store, &name, Some(*sid)), want);
            }
            store.flush(&name).unwrap();
            assert_eq!(rows_of(&store, &name, None), model);
        }
    }
}

#[test]
fn update_validation() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("t.vpf")).unwrap();
    store
        .import(
            "f",
            &write_parquet(
                &[sample_batch(0, 100)],
                props(Compression::SNAPPY, false, true, 10, 100),
            ),
        )
        .unwrap();
    store
        .append("f", &sample_batch(100, 10), AppendMode::Buffered)
        .unwrap();
    let cur = store.view("f", None).unwrap().snapshot.snapshot_id;
    for (set, pred) in [
        ("id = NULL", "id = 1"),
        ("id = 'x'", "id = 1"),
        ("nope = 1", "id = 1"),
        ("id = 1, id = 2", "id = 1"),
        ("id = i32", "id = 1"),
        ("u16 = 70000", "id = 1"),
        ("", "id = 1"),
        ("id 1", "id = 1"),
        ("id = 1", "nope = 1"),
        ("amount = 1e30", "id = 1"),
    ] {
        assert!(
            store.update_where("f", set, pred).is_err(),
            "{set} / {pred}"
        );
    }
    assert_eq!(store.view("f", None).unwrap().snapshot.snapshot_id, cur);
    let s = store.update_where("f", "id = 5", "id > 1000").unwrap();
    assert_eq!((s.rows_updated, s.snapshot_id), (0, None));
    // Updating only buffered rows leaves stored row groups untouched.
    let before = store.view("f", None).unwrap();
    let s = store
        .update_where("f", "name = 'buffered'", "id >= 100")
        .unwrap();
    assert_eq!((s.rows_updated, s.pages.written), (10, 0));
    let after = store.view("f", None).unwrap();
    let rg_ids = |v: &vpf::ops::snapshot::SnapshotView| {
        v.segments
            .iter()
            .filter_map(|s| match s {
                SegmentData::RowGroup { rg, .. } => Some(rg.rg_id),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(rg_ids(&before), rg_ids(&after));
    let got = rows_of(&store, "f", None);
    let names = got.column(2).as_string::<i32>();
    assert!((100..110).all(|i| names.value(i) == "buffered"));
}

#[test]
fn pyarrow_fixture_updates_cross_checked_with_duckdb() {
    if python().is_none() {
        eprintln!("skipping: VPF_PYTHON not set");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let fx = d.path().join("fx");
    std::fs::create_dir_all(&fx).unwrap();
    run_python(&[&py_script("gen_fixtures.py"), fx.to_str().unwrap()]);
    let stmts: Vec<(&str, &str, &str)> = vec![
        ("U", "cat = 'purple', name = NULL", "id < 300"),
        ("U", "price = 12345678.91", "id BETWEEN 1600 AND 1800"),
        ("D", "", "price IS NULL AND id > 4500"),
        ("U", "wide = 123456789012345678901234567890", "id = 4"),
        (
            "U",
            "ts = '1999-12-31 23:59:59.123456789', tstz = '2000-01-01T00:00:00Z'",
            "id IN (5, 4999, 1700)",
        ),
        ("U", "fixed = 'abcd', u = 255, b = false", "big LIKE 'x7%'"),
        (
            "U",
            "d = '1970-01-01', t = '12:34:56.789', f = 1.5",
            "id >= 4900",
        ),
    ];
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
        store.import_file(name, &src).unwrap();
        let mut args = vec![
            py_script("check_update.py"),
            String::new(),
            src.to_str().unwrap().into(),
        ];
        for (k, set, pred) in &stmts {
            if *k == "U" {
                let s = store.update_where(name, set, pred).unwrap();
                assert!(s.rows_updated > 0, "{name}: {set} {pred}");
                args.push(format!("UPDATE SET {set} WHERE {pred}"));
            } else {
                assert!(store.delete_where(name, pred).unwrap().rows_deleted > 0);
                args.push(format!("DELETE WHERE {pred}"));
            }
        }
        let out = d.path().join(format!("{name}.out.parquet"));
        store.export(name, None, &out).unwrap();
        args[1] = out.to_str().unwrap().into();
        run_python(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    }
}
