//! C API for VPF files (see `include/vpf.h`). Data crosses the boundary through the Arrow C
//! data and stream interfaces, so any Arrow-speaking engine can read and write VPF files.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StructArray};
use arrow::compute::{CastOptions, cast_with_options, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use vpf::layout::VirtualFile;
use vpf::ops::append::AppendMode;
use vpf::store::Store;
use vpf::{Error, Result};

pub const VPF_OK: c_int = 0;
pub const VPF_ERROR: c_int = 1;
pub const VPF_CONFLICT: c_int = 2;
pub const VPF_ROWID: i64 = -1;

/// An open `.vpf` file.
pub struct VpfFile {
    store: Store,
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_error(msg: String) {
    let msg = CString::new(msg.replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = msg);
}

/// Runs `f`, turning errors and panics into a status code and `vpf_last_error`.
fn guard(f: impl FnOnce() -> Result<()>) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => VPF_OK,
        Ok(Err(e)) => {
            let code = if matches!(e, Error::Conflict(_)) {
                VPF_CONFLICT
            } else {
                VPF_ERROR
            };
            set_error(e.to_string());
            code
        }
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            set_error(format!("internal error: {msg}"));
            VPF_ERROR
        }
    }
}

fn invalid(msg: &str) -> Error {
    Error::Invalid(msg.to_string())
}

unsafe fn file<'a>(f: *mut VpfFile) -> Result<&'a VpfFile> {
    unsafe { f.as_ref() }.ok_or_else(|| invalid("null vpf_file"))
}

unsafe fn str_arg<'a>(s: *const c_char, what: &str) -> Result<&'a str> {
    if s.is_null() {
        return Err(invalid(&format!("{what} is null")));
    }
    unsafe { CStr::from_ptr(s) }
        .to_str()
        .map_err(|_| invalid(&format!("{what} is not UTF-8")))
}

unsafe fn slice<'a, T>(p: *const T, n: usize) -> Result<&'a [T]> {
    if n == 0 {
        return Ok(&[]);
    }
    if p.is_null() {
        return Err(invalid("null array argument"));
    }
    Ok(unsafe { std::slice::from_raw_parts(p, n) })
}

fn out<T>(p: *mut T, v: T) {
    if !p.is_null() {
        unsafe { p.write(v) };
    }
}

impl VpfFile {
    fn table(&self) -> Result<String> {
        self.store
            .table()?
            .ok_or_else(|| Error::NotFound("the file has no table yet".into()))
    }

    fn snapshot(&self, id: i64) -> Result<(String, VirtualFile, ArrowReaderMetadata)> {
        let name = self.table()?;
        let vf = self.store.open_snapshot(&name, (id >= 0).then_some(id))?;
        let md =
            ArrowReaderMetadata::try_new(vf.metadata().clone(), ArrowReaderOptions::default())?;
        Ok((name, vf, md))
    }

    /// The table's Arrow schema, as scans produce it.
    fn schema(&self) -> Result<(String, SchemaRef)> {
        let (name, _, md) = self.snapshot(-1)?;
        Ok((name, md.schema().clone()))
    }
}

/// Takes ownership of the caller's arrays (they are released when dropped).
unsafe fn take(arrays: *mut FFI_ArrowArray, n: usize) -> Vec<FFI_ArrowArray> {
    if arrays.is_null() {
        return Vec::new();
    }
    (0..n)
        .map(|i| unsafe { std::ptr::replace(arrays.add(i), FFI_ArrowArray::empty()) })
        .collect()
}

/// Imports struct arrays as record batches.
unsafe fn import(
    schema: *const FFI_ArrowSchema,
    owned: Vec<FFI_ArrowArray>,
) -> Result<Vec<RecordBatch>> {
    let schema = unsafe { schema.as_ref() }.ok_or_else(|| invalid("null schema"))?;
    owned
        .into_iter()
        .map(|a| {
            if a.num_children() != schema.children().count() {
                return Err(invalid("arrays do not match the schema"));
            }
            let data = unsafe { from_ffi(a, schema) }?;
            if !matches!(data.data_type(), DataType::Struct(_)) {
                return Err(invalid("arrays must be struct arrays"));
            }
            Ok(RecordBatch::from(StructArray::from(data)))
        })
        .collect()
}

/// Concatenates batches and casts each column to the table field it maps to.
fn conform(batches: &[RecordBatch], fields: &[Arc<Field>]) -> Result<RecordBatch> {
    let first = batches.first().ok_or_else(|| invalid("no rows"))?;
    let all = concat_batches(&first.schema(), batches)?;
    let opts = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = all
        .columns()
        .iter()
        .zip(fields)
        .map(|(c, f)| {
            if c.data_type() == f.data_type() {
                Ok(c.clone())
            } else {
                cast_with_options(c, f.data_type(), &opts)
                    .map_err(|e| Error::Invalid(format!("column '{}': {e}", f.name())))
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let schema = Schema::new(
        fields
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    );
    Ok(RecordBatch::try_new_with_options(
        Arc::new(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(all.num_rows())),
    )?)
}

fn row_ids(rows: &[i64]) -> Result<Vec<u64>> {
    rows.iter()
        .map(|&r| u64::try_from(r).map_err(|_| invalid("negative row id")))
        .collect()
}

/// A snapshot scan with projection and an optional row-id column.
struct Scan {
    schema: SchemaRef,
    inner: Option<ParquetRecordBatchReader>,
    /// Output column -> column of `inner`'s batches (`None` = row id).
    map: Vec<Option<usize>>,
    next_row: i64,
    total: i64,
    batch_rows: usize,
    _file: VirtualFile,
}

impl Iterator for Scan {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (n, batch) = match &mut self.inner {
            Some(r) => match r.next()? {
                Ok(b) => (b.num_rows(), Some(b)),
                Err(e) => return Some(Err(e)),
            },
            None => {
                if self.next_row >= self.total {
                    return None;
                }
                (
                    (self.total - self.next_row).min(self.batch_rows as i64) as usize,
                    None,
                )
            }
        };
        let start = self.next_row;
        self.next_row += n as i64;
        let mut rowid: Option<ArrayRef> = None;
        let columns = self
            .map
            .iter()
            .map(|m| match m {
                Some(i) => batch.as_ref().unwrap().column(*i).clone(),
                None => rowid
                    .get_or_insert_with(|| {
                        Arc::new(Int64Array::from_iter_values(start..start + n as i64))
                    })
                    .clone(),
            })
            .collect();
        Some(RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        ))
    }
}

impl RecordBatchReader for Scan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vpf_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

/// # Safety
/// `s` must come from this library (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// # Safety
/// `path` must be a NUL-terminated string and `out` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_open(path: *const c_char, out_file: *mut *mut VpfFile) -> c_int {
    guard(|| {
        let path = unsafe { str_arg(path, "path") }?;
        if out_file.is_null() {
            return Err(invalid("null out pointer"));
        }
        let store = Store::open(path)?;
        out(out_file, Box::into_raw(Box::new(VpfFile { store })));
        Ok(())
    })
}

/// # Safety
/// `f` must come from `vpf_open` (or be null) and not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_close(f: *mut VpfFile) {
    if !f.is_null() {
        let f = unsafe { Box::from_raw(f) };
        if f.store.in_transaction() {
            let _ = f.store.rollback();
        }
    }
}

/// # Safety
/// `f` must be a valid handle and `name` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_table_name(f: *mut VpfFile, name: *mut *mut c_char) -> c_int {
    guard(|| {
        let f = unsafe { file(f) }?;
        let v = match f.store.table()? {
            Some(n) => CString::new(n).map_err(|_| invalid("bad name"))?.into_raw(),
            None => std::ptr::null_mut(),
        };
        out(name, v);
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle; out pointers may be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_snapshot(f: *mut VpfFile, id: *mut i64, num_rows: *mut i64) -> c_int {
    guard(|| {
        let f = unsafe { file(f) }?;
        let v = f.store.view(&f.table()?, None)?;
        out(id, v.snapshot.snapshot_id);
        out(num_rows, v.snapshot.num_rows);
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle and `out_schema` point to writable memory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_schema(
    f: *mut VpfFile,
    snapshot_id: i64,
    out_schema: *mut FFI_ArrowSchema,
) -> c_int {
    guard(|| {
        let f = unsafe { file(f) }?;
        if out_schema.is_null() {
            return Err(invalid("null out pointer"));
        }
        let (_, _, md) = f.snapshot(snapshot_id)?;
        let s = FFI_ArrowSchema::try_from(md.schema().as_ref())?;
        out(out_schema, s);
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle, `name` NUL-terminated, `schema` a valid Arrow schema.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_create(
    f: *mut VpfFile,
    name: *const c_char,
    schema: *const FFI_ArrowSchema,
    codec: *const c_char,
) -> c_int {
    guard(|| {
        let f = unsafe { file(f) }?;
        let name = unsafe { str_arg(name, "name") }?;
        let schema = unsafe { schema.as_ref() }.ok_or_else(|| invalid("null schema"))?;
        let schema = Schema::try_from(schema)?;
        let codec = if codec.is_null() {
            "zstd"
        } else {
            unsafe { str_arg(codec, "codec") }?
        };
        f.store
            .create_from_arrow(name, &schema, vpf::meta::parse_codec(codec)?)?;
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle, `columns` hold `n_columns` entries (or be null), and
/// `out_stream` point to writable memory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_scan(
    f: *mut VpfFile,
    snapshot_id: i64,
    columns: *const i64,
    n_columns: usize,
    batch_rows: usize,
    out_stream: *mut FFI_ArrowArrayStream,
) -> c_int {
    guard(|| {
        let f = unsafe { file(f) }?;
        if out_stream.is_null() {
            return Err(invalid("null out pointer"));
        }
        let (_, vf, md) = f.snapshot(snapshot_id)?;
        let table = md.schema().clone();
        let columns: Vec<i64> = if columns.is_null() {
            (0..table.fields().len() as i64).collect()
        } else {
            unsafe { slice(columns, n_columns) }?.to_vec()
        };
        let mut wanted: Vec<usize> = Vec::new();
        for &c in &columns {
            if c == VPF_ROWID {
                continue;
            }
            let c = usize::try_from(c)
                .ok()
                .filter(|&c| c < table.fields().len())
                .ok_or_else(|| invalid(&format!("column index {c} out of range")))?;
            wanted.push(c);
        }
        wanted.sort_unstable();
        wanted.dedup();
        let fields: Vec<Field> = columns
            .iter()
            .map(|&c| {
                if c == VPF_ROWID {
                    Field::new("rowid", DataType::Int64, false)
                } else {
                    table.field(c as usize).clone()
                }
            })
            .collect();
        let map = columns
            .iter()
            .map(|&c| (c != VPF_ROWID).then(|| wanted.binary_search(&(c as usize)).unwrap()))
            .collect();
        let batch_rows = if batch_rows == 0 { 2048 } else { batch_rows };
        let total = vf.metadata().file_metadata().num_rows();
        let inner = if wanted.is_empty() {
            None
        } else {
            let b = ParquetRecordBatchReaderBuilder::new_with_metadata(vf.clone(), md);
            let mask = ProjectionMask::roots(b.parquet_schema(), wanted.iter().copied());
            Some(
                b.with_projection(mask)
                    .with_batch_size(batch_rows)
                    .build()?,
            )
        };
        let scan = Scan {
            schema: Arc::new(Schema::new(fields)),
            inner,
            map,
            next_row: 0,
            total,
            batch_rows,
            _file: vf,
        };
        out(out_stream, FFI_ArrowArrayStream::new(Box::new(scan)));
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle, `schema` a valid Arrow schema and `arrays` hold `n_arrays`
/// valid struct arrays (ownership moves to this function).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_append(
    f: *mut VpfFile,
    schema: *const FFI_ArrowSchema,
    arrays: *mut FFI_ArrowArray,
    n_arrays: usize,
    rows_appended: *mut u64,
) -> c_int {
    let batches = unsafe { take(arrays, n_arrays) };
    guard(|| {
        let f = unsafe { file(f) }?;
        let batches: Vec<RecordBatch> = unsafe { import(schema, batches) }?
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect();
        if batches.is_empty() {
            out(rows_appended, 0);
            return Ok(());
        }
        let (name, table) = f.schema()?;
        if batches[0].num_columns() != table.fields().len() {
            return Err(Error::Invalid(format!(
                "expected {} columns, got {}",
                table.fields().len(),
                batches[0].num_columns()
            )));
        }
        let batch = conform(&batches, table.fields())?;
        f.store.append(&name, &batch, AppendMode::Auto)?;
        out(rows_appended, batch.num_rows() as u64);
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle and `rows` hold `n_rows` ids.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_delete_rows(
    f: *mut VpfFile,
    base: i64,
    rows: *const i64,
    n_rows: usize,
    rows_deleted: *mut u64,
) -> c_int {
    guard(|| {
        let f = unsafe { file(f) }?;
        let rows = row_ids(unsafe { slice(rows, n_rows) }?)?;
        let s = f.store.delete_rows(&f.table()?, base, &rows)?;
        out(rows_deleted, s.rows_deleted as u64);
        Ok(())
    })
}

/// # Safety
/// As `vpf_delete_rows`, plus `schema`/`arrays` as in `vpf_append`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_update_rows(
    f: *mut VpfFile,
    base: i64,
    rows: *const i64,
    n_rows: usize,
    schema: *const FFI_ArrowSchema,
    arrays: *mut FFI_ArrowArray,
    n_arrays: usize,
    rows_updated: *mut u64,
) -> c_int {
    let batches = unsafe { take(arrays, n_arrays) };
    guard(|| {
        let f = unsafe { file(f) }?;
        let rows = row_ids(unsafe { slice(rows, n_rows) }?)?;
        let batches = unsafe { import(schema, batches) }?;
        if rows.is_empty() {
            out(rows_updated, 0);
            return Ok(());
        }
        let (name, table) = f.schema()?;
        let fields = batches
            .first()
            .ok_or_else(|| invalid("no values"))?
            .schema()
            .fields()
            .iter()
            .map(|c| {
                table
                    .field_with_name(c.name())
                    .map(|t| Arc::new(t.clone()))
                    .map_err(|_| Error::NotFound(format!("column '{}'", c.name())))
            })
            .collect::<Result<Vec<_>>>()?;
        let values = conform(&batches, &fields)?;
        let s = f.store.update_rows(&name, base, &rows, &values)?;
        out(rows_updated, s.rows_updated as u64);
        Ok(())
    })
}

/// # Safety
/// `f` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_begin(f: *mut VpfFile) -> c_int {
    guard(|| unsafe { file(f) }?.store.begin())
}

/// # Safety
/// `f` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_commit(f: *mut VpfFile) -> c_int {
    guard(|| unsafe { file(f) }?.store.commit())
}

/// # Safety
/// `f` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpf_rollback(f: *mut VpfFile) -> c_int {
    guard(|| unsafe { file(f) }?.store.rollback())
}
