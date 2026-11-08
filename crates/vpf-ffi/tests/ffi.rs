//! Round trips through the C API, called the way a C host would.

use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use arrow::record_batch::RecordBatchReader;
use vpf_ffi::*;

fn ok(rc: i32) {
    let msg = unsafe { CStr::from_ptr(vpf_last_error()) };
    assert_eq!(rc, VPF_OK, "{}", msg.to_string_lossy());
}

fn open(path: &Path) -> *mut VpfFile {
    let path = CString::new(path.to_str().unwrap()).unwrap();
    let mut f = ptr::null_mut();
    ok(unsafe { vpf_open(path.as_ptr(), &mut f) });
    f
}

fn struct_schema(schema: &Schema) -> FFI_ArrowSchema {
    FFI_ArrowSchema::try_from(DataType::Struct(schema.fields().clone())).unwrap()
}

fn people() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

fn create(f: *mut VpfFile, name: &str) {
    let name = CString::new(name).unwrap();
    ok(unsafe { vpf_create(f, name.as_ptr(), &struct_schema(&people()), ptr::null()) });
}

fn arrays(schema: Schema, columns: Vec<ArrayRef>) -> (FFI_ArrowSchema, FFI_ArrowArray) {
    let ffi_schema = struct_schema(&schema);
    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
    (
        ffi_schema,
        FFI_ArrowArray::new(&StructArray::from(batch).to_data()),
    )
}

fn append(f: *mut VpfFile, ids: Vec<i64>, names: Vec<Option<&str>>) -> i32 {
    let (schema, mut array) = arrays(
        people(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    );
    let mut n = 0;
    unsafe { vpf_append(f, &schema, &mut array, 1, &mut n) }
}

fn snapshot(f: *mut VpfFile) -> (i64, i64) {
    let (mut id, mut rows) = (0, 0);
    ok(unsafe { vpf_snapshot(f, &mut id, &mut rows) });
    (id, rows)
}

fn scan(f: *mut VpfFile, snapshot: i64, columns: &[i64]) -> Vec<RecordBatch> {
    let mut stream = FFI_ArrowArrayStream::empty();
    ok(unsafe {
        vpf_scan(
            f,
            snapshot,
            columns.as_ptr(),
            columns.len(),
            ptr::null(),
            0,
            2,
            &mut stream,
        )
    });
    RecordBatchStreamReader::try_new(stream)
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}

/// (row id, id, name) of the current snapshot.
fn rows(f: *mut VpfFile) -> Vec<(i64, i64, Option<String>)> {
    let mut out = vec![];
    for b in scan(f, -1, &[VPF_ROWID, 0, 1]) {
        let rowid = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let id = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let name = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            let n = (!name.is_null(i)).then(|| name.value(i).to_string());
            out.push((rowid.value(i), id.value(i), n));
        }
    }
    out
}

#[test]
fn create_append_scan_update_delete() {
    let dir = tempfile::tempdir().unwrap();
    let f = open(&dir.path().join("t.vpf"));
    let mut name = ptr::null_mut();
    ok(unsafe { vpf_table_name(f, &mut name) });
    assert!(name.is_null());

    create(f, "people");
    ok(unsafe { vpf_table_name(f, &mut name) });
    assert_eq!(unsafe { CStr::from_ptr(name) }.to_str().unwrap(), "people");
    unsafe { vpf_free_string(name) };
    assert_eq!(snapshot(f).1, 0);
    assert_eq!(rows(f), vec![]);

    let mut schema = FFI_ArrowSchema::empty();
    ok(unsafe { vpf_schema(f, -1, &mut schema) });
    let DataType::Struct(fields) = DataType::try_from(&schema).unwrap() else {
        panic!("not a struct")
    };
    assert_eq!(fields[0].name(), "id");
    assert!(!fields[0].is_nullable() && fields[1].is_nullable());

    ok(append(f, vec![1, 2, 3], vec![Some("a"), None, Some("c")]));
    let (base, count) = snapshot(f);
    assert_eq!(count, 3);
    let a = Some("a".to_string());
    assert_eq!(
        rows(f),
        vec![(0, 1, a.clone()), (1, 2, None), (2, 3, Some("c".into()))]
    );

    let (values, mut array) = arrays(
        Schema::new(vec![Field::new("name", DataType::Utf8, true)]),
        vec![Arc::new(StringArray::from(vec!["B"]))],
    );
    let mut n = 0;
    ok(unsafe { vpf_update_rows(f, base, [1i64].as_ptr(), 1, &values, &mut array, 1, &mut n) });
    assert_eq!(n, 1);

    let base = snapshot(f).0;
    ok(unsafe { vpf_delete_rows(f, base, [0i64, 0].as_ptr(), 2, &mut n) });
    assert_eq!(n, 1);
    assert_eq!(
        rows(f),
        vec![(0, 2, Some("B".into())), (1, 3, Some("c".into()))]
    );

    // Older snapshots stay readable.
    let old: usize = scan(f, base, &[0]).iter().map(|b| b.num_rows()).sum();
    assert_eq!(old, 3);
    unsafe { vpf_close(f) };
}

#[test]
fn transactions_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.vpf");
    let f = open(&path);
    create(f, "t");
    ok(append(f, vec![1], vec![Some("a")]));

    ok(unsafe { vpf_begin(f) });
    ok(append(f, vec![2], vec![Some("b")]));
    assert_eq!(snapshot(f).1, 2);
    let g = open(&path);
    assert_eq!(
        snapshot(g).1,
        1,
        "uncommitted rows are invisible to other handles"
    );
    ok(unsafe { vpf_rollback(f) });
    assert_eq!(snapshot(f).1, 1);

    ok(unsafe { vpf_begin(f) });
    ok(append(f, vec![3], vec![None]));
    ok(unsafe { vpf_commit(f) });
    assert_eq!(snapshot(g).1, 2);

    // Errors (including caught panics from malformed input) leave the file usable.
    let (_, mut array) = arrays(
        people(),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["d"])),
        ],
    );
    let mismatched = struct_schema(&Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let mut n = 0;
    assert_eq!(
        unsafe { vpf_append(f, &mismatched, &mut array, 1, &mut n) },
        VPF_ERROR
    );
    assert!(
        !unsafe { CStr::from_ptr(vpf_last_error()) }
            .to_bytes()
            .is_empty()
    );
    assert_ne!(
        unsafe { vpf_delete_rows(f, 999, [0i64].as_ptr(), 1, &mut n) },
        VPF_OK
    );
    let name = CString::new("again").unwrap();
    let rc = unsafe { vpf_create(f, name.as_ptr(), &struct_schema(&people()), ptr::null()) };
    assert_eq!(rc, VPF_ERROR, "a file holds one table");
    assert_eq!(snapshot(f).1, 2);

    unsafe {
        vpf_close(g);
        vpf_close(f);
    }
}

const OPS: [i32; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

#[test]
fn metadata_modes_keep_cached_scans_fresh_and_existing_streams_pinned() {
    for mode in [VPF_METADATA_LAZY, VPF_METADATA_EAGER] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vpf");
        let f = open(&path);
        ok(unsafe { vpf_set_metadata_mode(f, mode) });
        assert_eq!(unsafe { vpf_set_metadata_mode(f, 42) }, VPF_ERROR);
        create(f, "t");
        ok(append(f, vec![1], vec![Some("a")]));
        let base = snapshot(f).0;
        assert_eq!(rows(f), vec![(0, 1, Some("a".into()))]);
        let g = open(&path);
        ok(unsafe { vpf_set_metadata_mode(g, mode) });

        let mut stream = FFI_ArrowArrayStream::empty();
        ok(unsafe { vpf_scan(f, base, ptr::null(), 0, ptr::null(), 0, 1, &mut stream) });
        let mut schema = FFI_ArrowSchema::empty();
        ok(unsafe { vpf_schema(f, base, &mut schema) });
        let reader = RecordBatchStreamReader::try_new(stream).unwrap();
        assert_eq!(
            DataType::try_from(&schema).unwrap(),
            DataType::Struct(reader.schema().fields().clone())
        );
        ok(append(g, vec![2], vec![Some("b")]));
        assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 1);
        assert_eq!(
            rows(f).len(),
            2,
            "current scans see another handle's commit"
        );

        ok(unsafe { vpf_begin(f) });
        ok(append(f, vec![3], vec![Some("rolled-back")]));
        let uncommitted = snapshot(f).0;
        assert_eq!(rows(f).last().unwrap().1, 3);
        assert_eq!(rows(g).len(), 2);
        ok(unsafe { vpf_rollback(f) });
        ok(append(f, vec![4], vec![Some("committed")]));
        assert_eq!(
            snapshot(f).0,
            uncommitted,
            "SQLite reuses rolled-back snapshot ids"
        );
        assert_eq!(rows(f).last().unwrap(), &(2, 4, Some("committed".into())));
        assert_eq!(rows(g), rows(f));
        assert_eq!(
            scan(f, base, &[0])
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1
        );
        unsafe {
            vpf_close(g);
            vpf_close(f);
        }
    }
}

/// Scans `columns` with filters `(column, op, value)`.
fn scan_filtered(
    f: *mut VpfFile,
    columns: &[i64],
    filters: &[(i64, i32, Option<ArrayRef>)],
) -> RecordBatch {
    scan_filters(f, columns, filters, false)
}

fn scan_filters(
    f: *mut VpfFile,
    columns: &[i64],
    filters: &[(i64, i32, Option<ArrayRef>)],
    expression: bool,
) -> RecordBatch {
    let mut owned: Vec<(FFI_ArrowSchema, FFI_ArrowArray)> = filters
        .iter()
        .map(|(_, _, v)| match v {
            Some(v) => {
                let field = Field::new("v", v.data_type().clone(), true);
                arrays(Schema::new(vec![field]), vec![v.clone()])
            }
            None => (FFI_ArrowSchema::empty(), FFI_ArrowArray::empty()),
        })
        .collect();
    let c: Vec<VpfFilter> = filters
        .iter()
        .zip(owned.iter_mut())
        .map(|((col, op, v), (s, a))| VpfFilter {
            column: *col,
            op: *op,
            schema: if v.is_some() {
                s as *const _
            } else {
                ptr::null()
            },
            value: if v.is_some() {
                a as *mut _
            } else {
                ptr::null_mut()
            },
        })
        .collect();
    let mut stream = FFI_ArrowArrayStream::empty();
    let scan = if expression { vpf_scan_expr } else { vpf_scan };
    ok(unsafe {
        scan(
            f,
            -1,
            columns.as_ptr(),
            columns.len(),
            c.as_ptr(),
            c.len(),
            0,
            &mut stream,
        )
    });
    assert!(owned.iter().all(|(_, a)| a.is_released()), "values moved");
    let reader = RecordBatchStreamReader::try_new(stream).unwrap();
    let schema = reader.schema();
    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}

/// SQL-style comparison (NaN largest and equal to itself, -0 = 0); `None` for NULL.
fn sql_match<T: PartialOrd>(x: Option<T>, op: i32, v: &T, nan: impl Fn(&T) -> bool) -> bool {
    use std::cmp::Ordering::*;
    let Some(x) = x else { return op == 7 };
    let ord = match (nan(&x), nan(v)) {
        (true, true) => Equal,
        (true, false) => Greater,
        (false, true) => Less,
        _ => x.partial_cmp(v).unwrap(),
    };
    match op {
        1 => ord == Equal,
        2 => ord != Equal,
        3 => ord == Less,
        4 => ord != Greater,
        5 => ord == Greater,
        6 => ord != Less,
        7 => false,
        _ => true,
    }
}

/// Checks that a filtered scan returns every matching row, with correct row ids and values.
fn check_superset(
    f: *mut VpfFile,
    all: &RecordBatch,
    filters: &[(i64, i32, Option<ArrayRef>)],
    matches: impl Fn(usize) -> bool,
) -> usize {
    check_expression_superset(f, all, filters, matches, false)
}

fn check_expression_superset(
    f: *mut VpfFile,
    all: &RecordBatch,
    filters: &[(i64, i32, Option<ArrayRef>)],
    matches: impl Fn(usize) -> bool,
    expression: bool,
) -> usize {
    let got = scan_filters(f, &[VPF_ROWID, 0, 1], filters, expression);
    let ids = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let returned: Vec<usize> = ids.values().iter().map(|&r| r as usize).collect();
    assert!(returned.windows(2).all(|w| w[0] < w[1]), "row ids sorted");
    for (i, &r) in returned.iter().enumerate() {
        for c in 1..3 {
            assert_eq!(
                got.column(c).slice(i, 1).to_data(),
                all.column(c - 1).slice(r, 1).to_data(),
                "value of row {r}"
            );
        }
    }
    for r in 0..all.num_rows() {
        if matches(r) {
            assert!(
                returned.binary_search(&r).is_ok(),
                "row {r} missing for {filters:?}"
            );
        }
    }
    returned.len()
}

#[test]
fn filtered_scans_prune_and_keep_matches() {
    let dir = tempfile::tempdir().unwrap();
    let f = open(&dir.path().join("t.vpf"));
    create(f, "t");
    // Three large sorted row groups, then small buffered appends.
    for g in 0..3 {
        let ids: Vec<i64> = (g * 70_000..(g + 1) * 70_000).collect();
        let names = ids
            .iter()
            .map(|i| (i % 7 != 0).then(|| format!("n{i:07}")))
            .collect::<Vec<_>>();
        ok(append(f, ids, names.iter().map(|n| n.as_deref()).collect()));
    }
    ok(append(
        f,
        vec![5, 300_000, -1],
        vec![None, Some("zz"), Some("a")],
    ));
    // Mutations keep statistics exact.
    let (snap, _) = snapshot(f);
    ok(unsafe { vpf_delete_rows(f, snap, [10i64, 150_000].as_ptr(), 2, ptr::null_mut()) });

    let all = {
        let b = scan(f, -1, &[0, 1]);
        concat_batches(&b[0].schema(), &b).unwrap()
    };
    let id = all
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    let name = all
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let total = all.num_rows();

    // Point lookups read about one page.
    let n = check_superset(
        f,
        &all,
        &[(0, 1, Some(Arc::new(Int64Array::from(vec![100_005]))))],
        |r| id.value(r) == 100_005,
    );
    assert!(n <= 25_000, "point lookup scanned {n} rows");
    let n = check_superset(
        f,
        &all,
        &[(1, 1, Some(Arc::new(StringArray::from(vec!["n0200001"]))))],
        |r| name.is_valid(r) && name.value(r) == "n0200001",
    );
    assert!(n <= 25_000, "string lookup scanned {n} rows");
    // No match anywhere.
    assert_eq!(
        check_superset(
            f,
            &all,
            &[(0, 5, Some(Arc::new(Int64Array::from(vec![1_000_000]))))],
            |_| false
        ),
        0
    );
    // Projections without data columns still produce the right row ids.
    let filter = [(
        0,
        6,
        Some(Arc::new(Int64Array::from(vec![209_990])) as ArrayRef),
    )];
    let got = scan_filtered(f, &[VPF_ROWID], &filter);
    let with_data = scan_filtered(f, &[VPF_ROWID, 0], &filter);
    assert_eq!(got.column(0), with_data.column(0));
    assert!(got.num_rows() < 25_000, "scanned {}", got.num_rows());
    let r = got.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for row in 0..total {
        if id.value(row) >= 209_990 {
            assert!(r.values().contains(&(row as i64)));
        }
    }

    for op in OPS {
        for v in [
            -5i64, 0, 5, 69_999, 70_000, 139_999, 150_000, 209_999, 300_000, 400_000,
        ] {
            let value = (op < 7).then(|| Arc::new(Int64Array::from(vec![v])) as ArrayRef);
            check_superset(f, &all, &[(0, op, value)], |r| {
                sql_match(id.is_valid(r).then(|| id.value(r)), op, &v, |_| false)
            });
        }
        for v in ["", "a", "n0000000", "n0100000", "n0209999", "zz", "zzz"] {
            let value = (op < 7).then(|| Arc::new(StringArray::from(vec![v])) as ArrayRef);
            check_superset(f, &all, &[(1, op, value)], |r| {
                sql_match(name.is_valid(r).then(|| name.value(r)), op, &v, |_| false)
            });
        }
    }
    // ANDed filters intersect.
    let lo: ArrayRef = Arc::new(Int64Array::from(vec![60_000]));
    let hi: ArrayRef = Arc::new(Int64Array::from(vec![80_000]));
    let n = check_superset(f, &all, &[(0, 6, Some(lo)), (0, 3, Some(hi))], |r| {
        (60_000..80_000).contains(&id.value(r))
    });
    assert!(n < 70_000, "range scanned {n} rows");

    let eq = |v| (0, 1, Some(Arc::new(Int64Array::from(vec![v])) as ArrayRef));
    let or = (0, VPF_OR, None);
    let and = (0, VPF_AND, None);
    for (a, b, limit) in [
        (100_005, 100_007, 25_000),
        (5, 200_001, 50_000),
        (-1, 400_000, 3),
    ] {
        let n = check_expression_superset(
            f,
            &all,
            &[eq(a), eq(b), or.clone()],
            |r| id.value(r) == a || id.value(r) == b,
            true,
        );
        assert!(n <= limit, "OR scanned {n} rows");
    }
    let expr = [
        eq(5),
        eq(100_005),
        or.clone(),
        (
            0,
            6,
            Some(Arc::new(Int64Array::from(vec![100_000])) as ArrayRef),
        ),
        and.clone(),
    ];
    let n = check_expression_superset(
        f,
        &all,
        &expr,
        |r| (id.value(r) == 5 || id.value(r) == 100_005) && id.value(r) >= 100_000,
        true,
    );
    assert!(n <= 25_000);
    let expr = [
        eq(5),
        (1, 8, None),
        and,
        (
            1,
            1,
            Some(Arc::new(StringArray::from(vec!["zz"])) as ArrayRef),
        ),
        or.clone(),
    ];
    check_expression_superset(
        f,
        &all,
        &expr,
        |r| name.is_valid(r) && (id.value(r) == 5 || name.value(r) == "zz"),
        true,
    );
    // An OR with a broad null branch must not lose rows outside the equality's pages.
    check_expression_superset(
        f,
        &all,
        &[eq(100_005), (1, 7, None), or.clone()],
        |r| id.value(r) == 100_005 || name.is_null(r),
        true,
    );
    let got = scan_filters(f, &[VPF_ROWID], &[eq(5), eq(200_001), or.clone()], true);
    let with_data = scan_filters(f, &[VPF_ROWID, 0], &[eq(5), eq(200_001), or], true);
    assert_eq!(got.column(0), with_data.column(0));
    assert_eq!(
        scan_filters(f, &[VPF_ROWID, 0], &[], true),
        scan_filtered(f, &[VPF_ROWID, 0], &[]),
    );

    // Values of another type are cast to the column type.
    let v: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![5]));
    check_superset(f, &all, &[(0, 1, Some(v))], |r| id.value(r) == 5);

    // Bad filters are errors (and values are still released).
    let mut stream = FFI_ArrowArrayStream::empty();
    let bad = [VpfFilter {
        column: 9,
        op: 7,
        schema: ptr::null(),
        value: ptr::null_mut(),
    }];
    let cols = [0i64];
    assert_eq!(
        unsafe { vpf_scan(f, -1, cols.as_ptr(), 1, bad.as_ptr(), 1, 0, &mut stream) },
        VPF_ERROR
    );
    let (s, mut a) = arrays(
        Schema::new(vec![Field::new("v", DataType::Int64, true)]),
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    let bad = [VpfFilter {
        column: 0,
        op: 42,
        schema: &s,
        value: &mut a,
    }];
    assert_eq!(
        unsafe { vpf_scan(f, -1, cols.as_ptr(), 1, bad.as_ptr(), 1, 0, &mut stream) },
        VPF_ERROR
    );
    assert!(a.is_released());

    for ops in [
        vec![VPF_OR],
        vec![7, VPF_AND],
        vec![7, 8],
        vec![7, 8, VPF_OR, VPF_AND],
    ] {
        let bad: Vec<_> = ops
            .into_iter()
            .map(|op| VpfFilter {
                column: 0,
                op,
                schema: ptr::null(),
                value: ptr::null_mut(),
            })
            .collect();
        assert_eq!(
            unsafe {
                vpf_scan_expr(
                    f,
                    -1,
                    cols.as_ptr(),
                    1,
                    bad.as_ptr(),
                    bad.len(),
                    0,
                    &mut stream,
                )
            },
            VPF_ERROR,
        );
    }
    let (s, mut a) = arrays(
        Schema::new(vec![Field::new("v", DataType::Int64, true)]),
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    let bad = [VpfFilter {
        column: 0,
        op: VPF_OR,
        schema: &s,
        value: &mut a,
    }];
    assert_eq!(
        unsafe { vpf_scan_expr(f, -1, cols.as_ptr(), 1, bad.as_ptr(), 1, 0, &mut stream) },
        VPF_ERROR,
    );
    assert!(a.is_released());
    unsafe { vpf_close(f) };
}

#[test]
fn float_filters_follow_sql_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let f = open(&dir.path().join("f.vpf"));
    let schema = Schema::new(vec![
        Field::new("x", DataType::Float64, true),
        Field::new("tag", DataType::Utf8, true),
    ]);
    let name = CString::new("f").unwrap();
    ok(unsafe { vpf_create(f, name.as_ptr(), &struct_schema(&schema), ptr::null()) });
    let groups: Vec<Vec<Option<f64>>> = vec![
        (0..70_000).map(|i| Some(i as f64 - 100.0)).collect(),
        (0..70_000)
            .map(|i| Some(if i == 5 { f64::NAN } else { 1e6 + i as f64 }))
            .collect(),
        (0..70_000)
            .map(|i| Some(if i % 2 == 0 { -0.0 } else { 0.0 }))
            .collect(),
        vec![None, Some(f64::NAN), Some(-1.0)],
    ];
    for g in groups {
        let tags: Vec<Option<String>> = g.iter().map(|x| x.map(|x| format!("{x}"))).collect();
        let (s, mut a) = arrays(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(g)),
                Arc::new(StringArray::from(tags)),
            ],
        );
        let mut n = 0;
        ok(unsafe { vpf_append(f, &s, &mut a, 1, &mut n) });
    }
    let all = {
        let b = scan(f, -1, &[0, 1]);
        concat_batches(&b[0].schema(), &b).unwrap()
    };
    let x = all
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .clone();
    for op in OPS {
        for v in [
            -0.0,
            0.0,
            -100.0,
            -101.0,
            1e6,
            2e6,
            69_899.0,
            f64::NAN,
            f64::INFINITY,
        ] {
            let value = (op < 7).then(|| Arc::new(Float64Array::from(vec![v])) as ArrayRef);
            check_superset(f, &all, &[(0, op, value)], |r| {
                sql_match(x.is_valid(r).then(|| x.value(r)), op, &v, |x| x.is_nan())
            });
        }
    }
    // Zeros of either sign match each other; only the zero group is read.
    let n = check_superset(
        f,
        &all,
        &[(0, 1, Some(Arc::new(Float64Array::from(vec![-0.0]))))],
        |r| x.is_valid(r) && x.value(r) == 0.0,
    );
    assert!(n <= 70_000 + 25_000, "zero lookup scanned {n} rows");
    let filters = [
        (
            0,
            1,
            Some(Arc::new(Float64Array::from(vec![-0.0])) as ArrayRef),
        ),
        (
            0,
            1,
            Some(Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef),
        ),
        (0, VPF_OR, None),
    ];
    check_expression_superset(
        f,
        &all,
        &filters,
        |r| x.is_valid(r) && (x.value(r) == 0.0 || x.value(r).is_nan()),
        true,
    );
    unsafe { vpf_close(f) };
}
