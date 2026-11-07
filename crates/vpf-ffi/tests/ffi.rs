//! Round trips through the C API, called the way a C host would.

use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
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
    ok(unsafe { vpf_scan(f, snapshot, columns.as_ptr(), columns.len(), 2, &mut stream) });
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
