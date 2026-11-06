mod common;

use arrow::array::*;
use arrow::compute::filter_record_batch;
use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::ops::rewrite::page_row_ranges;
use vpf::ops::snapshot::SegmentData;
use vpf::predicate::Expr;
use vpf::store::Store;

fn rows_of(store: &Store, name: &str, snap: Option<i64>) -> RecordBatch {
    read_virtual(&store.open_snapshot(name, snap).unwrap())
}

fn mask(pred: &str, b: &RecordBatch) -> Vec<bool> {
    Expr::parse(pred).unwrap().matches(b).unwrap()
}

fn without(b: &RecordBatch, m: &[bool]) -> RecordBatch {
    filter_record_batch(b, &BooleanArray::from(m.iter().map(|x| !x).collect::<Vec<_>>())).unwrap()
}

#[test]
fn predicate_parsing_and_evaluation() {
    let b = sample_batch(0, 3000);
    let ids: Vec<i64> = (0..3000).collect();
    let opt = |i: i64, m: i64| i % m != 0;
    let check = |pred: &str, f: &dyn Fn(i64) -> bool| {
        let want: Vec<bool> = ids.iter().map(|&i| f(i)).collect();
        assert_eq!(mask(pred, &b), want, "{pred}");
    };
    check("id = 5", &|i| i == 5);
    check("5 = id", &|i| i == 5);
    check("5 < id AND id <= 9", &|i| i > 5 && i <= 9);
    check("\"id\" <> 0 and \"name\" IS NULL", &|i| i != 0 && !opt(i, 5));
    check("i32 > 100", &|i| opt(i, 7) && i * 3 > 100);
    check("i32 IS NULL", &|i| !opt(i, 7));
    check("NOT i32 IS NULL", &|i| opt(i, 7));
    check("i32 is not null or id < 3", &|i| opt(i, 7) || i < 3);
    check("NOT (i32 > 30)", &|i| opt(i, 7) && i * 3 <= 30);
    check("id IN (1, 2, 2999, 5000)", &|i| [1, 2, 2999].contains(&i));
    check("id NOT IN (1, 2)", &|i| i != 1 && i != 2);
    check("id IN (1, NULL)", &|i| i == 1);
    check("id NOT IN (1, NULL)", &|_| false);
    check("name = 'name-3'", &|i| opt(i, 5) && i % 97 == 3);
    check("name LIKE 'name-1_'", &|i| opt(i, 5) && (10..20).contains(&(i % 97)));
    check("name NOT LIKE 'name-%'", &|_| false);
    check("score < 10.5", &|i| opt(i, 11) && (i as f64) * 1.5 < 10.5);
    check("f32 >= 1e2", &|i| opt(i, 13) && i as f32 / 4.0 >= 100.0);
    check("flag = true", &|i| opt(i, 3) && i % 2 == 0);
    check("day = '2019-04-14'", &|i| opt(i, 17) && i % 1000 == 0);
    check("day BETWEEN '2019-04-14' AND '2019-04-15'", &|i| opt(i, 17) && (0..=1).contains(&(i % 1000)));
    check("ts > '2020-09-13T12:26:40.002'", &|i| opt(i, 19) && i > 2);
    check("amount = 1.01", &|i| opt(i, 23) && i == 1);
    check("amount BETWEEN -1 AND 2.02", &|i| opt(i, 23) && i <= 2);
    check("u16 = 501", &|i| opt(i, 31) && i == 1);
    check("TRUE", &|_| true);
    check("false OR id = 0", &|i| i == 0);
    check("i32 = NULL", &|_| false);
    check("id = id", &|_| true);
    check("i32 < id", &|_| false);
    check("(id < 10 OR id > 2990) AND NOT id IN (0, 2999)", &|i| (i < 10 || i > 2990) && i != 0 && i != 2999);
}

#[test]
fn predicate_errors() {
    let b = sample_batch(0, 10);
    for bad in [
        "", "id", "id =", "id = 1 AND", "(id = 1", "id = 1)", "id IN ()", "id IN (name)", "id NOT NULL",
        "id LIKE 5", "1 = 1", "id = 'x", "id @ 1", "AND = 1", "id = 1 id = 2", "id BETWEEN 1 2",
    ] {
        let r = Expr::parse(bad).and_then(|e| e.matches(&b));
        assert!(r.is_err(), "{bad:?} should fail");
    }
    for bad in ["nope = 1", "id = 'abc'", "day = 'not a date'", "u16 = -1", "id = 1.5"] {
        let e = Expr::parse(bad).unwrap();
        assert!(e.matches(&b).is_err(), "{bad:?} should fail at evaluation");
    }
    assert_eq!(Expr::parse("\"we\"\"ird\" = 'it''s'").unwrap().columns().into_iter().collect::<Vec<_>>(), vec!["we\"ird".to_string()]);
}

fn data_page_ids(store: &Store, name: &str, snap: Option<i64>) -> Vec<Vec<Vec<i64>>> {
    // [segment][column] -> data page ids
    store
        .view(name, snap)
        .unwrap()
        .segments
        .iter()
        .filter_map(|s| match s {
            SegmentData::RowGroup { chunks, .. } => Some(
                chunks.iter().map(|c| c.pages.iter().filter(|p| p.kind.is_data()).map(|p| p.page_id).collect()).collect(),
            ),
            _ => None,
        })
        .collect()
}

#[test]
fn single_row_delete_rewrites_one_page_per_column() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let base = sample_batch(0, 4000);
    store.import("f", &write_parquet(&[base.clone()], props(Compression::SNAPPY, false, true, 100, 2000))).unwrap();
    let ncols = base.num_columns();
    let before = data_page_ids(&store, "f", None);
    let pages_per_rg: Vec<usize> = before.iter().map(|rg| rg.iter().map(|c| c.len()).sum()).collect();
    let v0 = store.view("f", None).unwrap().snapshot.snapshot_id;

    let s = store.delete_where("f", "id = 2345").unwrap();
    assert_eq!(s.rows_deleted, 1);
    assert_eq!((s.segments_removed, s.segments_rewritten), (0, 1));
    assert_eq!(s.pages.rewritten, ncols);
    assert_eq!(s.pages.dropped, 0);
    assert_eq!(s.pages.reused, pages_per_rg[1] - ncols);
    let after = data_page_ids(&store, "f", None);
    assert_eq!(after[0], before[0], "untouched row group keeps every page");
    for (c, (old, new)) in before[1].iter().zip(&after[1]).enumerate() {
        let shared = new.iter().filter(|p| old.contains(p)).count();
        assert_eq!(shared, old.len() - 1, "column {c}: all but one page reused");
    }
    assert_eq!(rows_of(&store, "f", None), without(&base, &mask("id = 2345", &base)));
    // Old snapshot unaffected.
    assert_eq!(rows_of(&store, "f", Some(v0)), base);

    // Deleting exactly the rows of the first page of every column drops pages without
    // writing any. Page boundaries are taken from the stored layout.
    let view = store.view("f", None).unwrap();
    let SegmentData::RowGroup { chunks, .. } = &view.segments[0] else { panic!() };
    let first = page_row_ranges(&chunks[0]).into_iter().flatten().next().unwrap();
    let aligned = chunks.iter().all(|c| page_row_ranges(c).into_iter().flatten().next().unwrap() == first);
    let s = store.delete_where("f", &format!("id < {}", first.end)).unwrap();
    if aligned {
        assert_eq!((s.pages.dropped, s.pages.rewritten, s.pages.written), (ncols, 0, 0));
    }
    // Whole row group removal touches no pages.
    let s = store.delete_where("f", "id >= 2000").unwrap();
    assert_eq!((s.segments_removed, s.pages.written), (1, 0));
    // No match: no snapshot.
    let cur = store.view("f", None).unwrap().snapshot.snapshot_id;
    assert_eq!(store.delete_where("f", "id > 1000000").unwrap().snapshot_id, None);
    assert_eq!(store.view("f", None).unwrap().snapshot.snapshot_id, cur);
    // Errors leave the file untouched.
    assert!(store.delete_where("f", "nope = 1").is_err());
    assert!(store.delete_where("f", "id = 'x'").is_err());
    assert_eq!(store.view("f", None).unwrap().snapshot.snapshot_id, cur);
    // Relaxed statistics after row removal.
    let vf = store.open_snapshot("f", None).unwrap();
    let st = vf.metadata().row_group(0).column(0).statistics().unwrap();
    assert!(!st.min_is_exact() && !st.max_is_exact());
    assert_eq!(st.null_count_opt(), None);
}

#[test]
fn deletes_match_model_across_formats_and_segment_kinds() {
    let preds = [
        "id = 7",
        "i32 IS NULL",
        "name LIKE 'name-1%' AND score > 100",
        "id BETWEEN 1500 AND 2600",
        "flag = false OR amount < 3",
        "id >= 0 AND id < 40",
    ];
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let mut fi = 0;
    for codec in codecs() {
        for v2 in [false, true] {
            for dict in [false, true] {
                let name = format!("f{fi}");
                fi += 1;
                let base = sample_batch(0, 2000);
                store.import(&name, &write_parquet(&[base.clone()], props(codec, v2, dict, 64, 700))).unwrap();
                store.append(&name, &sample_batch(2000, 300), AppendMode::Buffered).unwrap();
                store.append(&name, &sample_batch(2300, 10), AppendMode::Buffered).unwrap();
                store.append(&name, &sample_batch(2310, 500), AppendMode::Direct).unwrap();
                store.append(&name, &sample_batch(2810, 90), AppendMode::Buffered).unwrap();
                let mut model = sample_batch(0, 2900);
                let mut history = vec![(store.view(&name, None).unwrap().snapshot.snapshot_id, model.clone())];
                for p in preds {
                    let m = mask(p, &model);
                    let s = store.delete_where(&name, p).unwrap();
                    assert_eq!(s.rows_deleted as usize, m.iter().filter(|x| **x).count(), "{p}");
                    model = without(&model, &m);
                    assert_eq!(rows_of(&store, &name, None), model, "{name} {codec} v2={v2} dict={dict} {p}");
                    if let Some(sid) = s.snapshot_id {
                        history.push((sid, model.clone()));
                    }
                }
                for (sid, want) in &history {
                    assert_eq!(&rows_of(&store, &name, Some(*sid)), want);
                }
                store.flush(&name).unwrap();
                assert_eq!(rows_of(&store, &name, None), model);
                if python().is_some() && fi % 4 == 1 {
                    let src = d.path().join(format!("{name}.src.parquet"));
                    let out = d.path().join(format!("{name}.parquet"));
                    std::fs::write(&src, write_parquet(&[sample_batch(0, 2900)], props(Compression::SNAPPY, false, false, 10_000, 100_000)).to_vec()).unwrap();
                    store.export(&name, None, &out).unwrap();
                    let mut args = vec![py_script("check_delete.py"), out.to_str().unwrap().into(), src.to_str().unwrap().into()];
                    args.extend(preds.iter().map(|p| p.to_string()));
                    run_python(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                }
            }
        }
    }
}

#[test]
fn delete_everything_then_append() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    store.import("f", &write_parquet(&[sample_batch(0, 500)], props(Compression::ZSTD(Default::default()), true, true, 50, 200))).unwrap();
    store.append("f", &sample_batch(500, 5), AppendMode::Buffered).unwrap();
    let s = store.delete_where("f", "TRUE").unwrap();
    assert_eq!((s.rows_deleted, s.segments_removed, s.pages.written), (505, 4, 0));
    let vf = store.open_snapshot("f", None).unwrap();
    assert_eq!(vf.metadata().num_row_groups(), 0);
    assert_eq!(read_virtual(&vf).num_rows(), 0);
    assert_eq!(store.view("f", None).unwrap().snapshot.num_rows, 0);
    store.append("f", &sample_batch(9, 3), AppendMode::Buffered).unwrap();
    assert_eq!(rows_of(&store, "f", None), sample_batch(9, 3));
}

#[test]
fn pyarrow_fixture_deletes_cross_checked_with_duckdb() {
    if python().is_none() {
        eprintln!("skipping: VPF_PYTHON not set");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let fx = d.path().join("fx");
    std::fs::create_dir_all(&fx).unwrap();
    run_python(&[&py_script("gen_fixtures.py"), fx.to_str().unwrap()]);
    let store = Store::open(d.path().join("s")).unwrap();
    let preds = [
        "id = 17",
        "cat = 'red' AND id < 2000",
        "name IS NULL AND id > 4000",
        "price > 30.5 AND price < 31",
        "tstz >= '2021-01-03T00:00:00Z' AND tstz < '2021-01-03T06:00:00Z'",
        "big LIKE 'x45%'",
        "id BETWEEN 1650 AND 1750",
    ];
    for name in ["v1_snappy", "v2_zstd", "gzip_nodict", "pageindex_lz4", "int96_brotli", "uncompressed_v2_crc"] {
        let src = fx.join(format!("{name}.parquet"));
        store.import_file(name, &src).unwrap();
        for p in preds {
            let s = store.delete_where(name, p).unwrap();
            assert!(s.rows_deleted > 0, "{name}: {p}");
        }
        let out = d.path().join(format!("{name}.out.parquet"));
        store.export(name, None, &out).unwrap();
        let mut args = vec![py_script("check_delete.py"), out.to_str().unwrap().into(), src.to_str().unwrap().into()];
        args.extend(preds.iter().map(|p| p.to_string()));
        run_python(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    }
}
