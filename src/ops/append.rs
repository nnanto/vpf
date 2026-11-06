//! Creating empty files, appending rows (buffered or direct) and flushing buffers.

use std::borrow::Cow;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::{ARROW_SCHEMA_META_KEY, ArrowSchemaConverter, encode_arrow_schema};
use parquet::file::metadata::KeyValue;
use parquet::basic::Compression;

use crate::convert::batch_to_columns;
use crate::error::{Error, Result};
use crate::meta::{self, SegmentKind};
use crate::ops::snapshot::{SegmentData, load_view};
use crate::ops::synth::{self, SynthRowGroup};
use crate::schema::{ColumnSpec, Leaf, SchemaVersion};
use crate::store::{NewChunk, Store, Txn};
use crate::values::{ColumnData, encode_batch};

/// Appends of at least this many rows are written as row groups in `Auto` mode.
pub const DIRECT_APPEND_ROWS: usize = 65_536;
/// Maximum rows per row group written by direct appends and flushes.
pub const MAX_ROW_GROUP_ROWS: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendMode {
    /// Buffer small appends, write large appends directly.
    #[default]
    Auto,
    /// Store rows in the SQLite append buffer; pages are synthesized on read.
    Buffered,
    /// Encode rows into new row groups immediately.
    Direct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSummary {
    pub snapshot_id: i64,
    pub num_rows: i64,
    pub buffered: bool,
}

impl Store {
    /// Creates an empty file with the given columns (column ids are assigned by the store).
    pub fn create(&self, name: &str, columns: &[ColumnSpec], codec: Compression) -> Result<i64> {
        self.create_with_metadata(name, columns, codec, None)
    }

    fn create_with_metadata(
        &self,
        name: &str,
        columns: &[ColumnSpec],
        codec: Compression,
        key_value: Option<Vec<KeyValue>>,
    ) -> Result<i64> {
        self.write(|t| {
            let file_id = t.create_file(name, codec, Some(concat!("vpf version ", env!("CARGO_PKG_VERSION"))), 1)?;
            let first = t.alloc_column_ids(file_id, columns.len() as i64)?;
            let specs: Vec<ColumnSpec> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnSpec { column_id: first + i as i64, ..c.clone() })
                .collect();
            for s in &specs {
                if s.is_nested() && s.default_value.is_some() {
                    return Err(Error::Unsupported(format!("default value for nested column '{}'", s.name)));
                }
                if let Some(d) = &s.default_value {
                    let v = crate::values::decode_value(d)?;
                    ColumnData::repeat(s.physical, s.nullable, Some(&v), 1)?;
                }
            }
            let svid = meta::insert_schema(t.conn(), file_id, None, "schema", &specs, key_value.as_deref())?;
            let sid = t.new_snapshot(file_id, svid, "create", None)?;
            t.finish_snapshot(file_id, sid)?;
            Ok(sid)
        })
    }

    /// Creates an empty file from an Arrow schema. The Arrow schema is embedded (as Arrow's
    /// writer does) so readers see the original Arrow types and field names.
    pub fn create_from_arrow(&self, name: &str, schema: &Schema, codec: Compression) -> Result<i64> {
        let descr = ArrowSchemaConverter::new().convert(schema)?;
        let specs = descr
            .root_schema()
            .get_fields()
            .iter()
            .map(|f| ColumnSpec::from_parquet_type(0, f))
            .collect::<Result<Vec<_>>>()?;
        let kv = vec![KeyValue::new(ARROW_SCHEMA_META_KEY.to_string(), encode_arrow_schema(schema))];
        self.create_with_metadata(name, &specs, codec, Some(kv))
    }

    /// Appends rows, creating a new snapshot in which they are immediately visible.
    pub fn append(&self, name: &str, batch: &RecordBatch, mode: AppendMode) -> Result<AppendSummary> {
        if batch.num_rows() == 0 {
            return Err(Error::Invalid("cannot append an empty batch".into()));
        }
        self.write(|t| {
            let view = load_view(t.conn(), name, None)?;
            let cols = batch_to_columns(&view.schema, batch)?;
            let n = batch.num_rows();
            let direct = match mode {
                AppendMode::Auto => n >= DIRECT_APPEND_ROWS,
                AppendMode::Buffered => false,
                AppendMode::Direct => true,
            };
            let file_id = view.file.file_id;
            let svid = view.schema.schema_version_id;
            let sid = t.new_snapshot(file_id, svid, "append", Some(&format!("{n} rows")))?;
            if direct {
                let codec = synth::write_codec(&view.file);
                let mut start = 0;
                while start < n {
                    let len = MAX_ROW_GROUP_ROWS.min(n - start);
                    let part = if len == n {
                        Cow::Borrowed(cols.as_slice())
                    } else {
                        Cow::Owned(cols.iter().map(|c| c.slice(start, len)).collect::<Vec<_>>())
                    };
                    let rg = synth::encode_row_group(&view.schema, codec, &part)?;
                    let rg_id = store_row_group(t, &view.schema, &rg)?;
                    t.append_segment(file_id, SegmentKind::RowGroup, rg_id, sid)?;
                    start += len;
                }
            } else {
                let keyed: Vec<((i64, i64), ColumnData)> =
                    view.schema.leaves()?.iter().map(|l| (l.column_id, l.leaf)).zip(cols).collect();
                let payload = encode_batch(n, &keyed)?;
                let bid = t.put_batch(svid, n as i64, &payload)?;
                t.append_segment(file_id, SegmentKind::Batch, bid, sid)?;
            }
            t.finish_snapshot(file_id, sid)?;
            Ok(AppendSummary { snapshot_id: sid, num_rows: n as i64, buffered: !direct })
        })
    }

    /// Materializes all buffered batches into stored row groups. Returns the new snapshot,
    /// or `None` if there was nothing to flush.
    pub fn flush(&self, name: &str) -> Result<Option<i64>> {
        self.write(|t| {
            let view = load_view(t.conn(), name, None)?;
            let runs = synth::batch_runs(&view.segments, |s| match s {
                SegmentData::Batch { batch, .. } => Some(batch.num_rows),
                _ => None,
            });
            if runs.is_empty() {
                return Ok(None);
            }
            let file_id = view.file.file_id;
            let sid = t.new_snapshot(file_id, view.schema.schema_version_id, "flush", None)?;
            let codec = synth::write_codec(&view.file);
            for run in runs {
                let segs = &view.segments[run];
                let batches: Vec<_> = segs
                    .iter()
                    .map(|s| match s {
                        SegmentData::Batch { batch, .. } => batch,
                        _ => unreachable!(),
                    })
                    .collect();
                let rg = synth::synthesize(&view.schema, codec, &batches)?;
                let rg_id = store_row_group(t, &view.schema, &rg)?;
                t.replace_segment(file_id, segs[0].segment(), SegmentKind::RowGroup, rg_id, sid)?;
                for s in &segs[1..] {
                    t.remove_segment(s.segment().segment_id, sid)?;
                }
            }
            t.finish_snapshot(file_id, sid)?;
            Ok(Some(sid))
        })
    }
}

/// Persists encoded chunks as pages, chunks and a row group object.
pub(crate) fn store_row_group(t: &mut Txn, schema: &SchemaVersion, rg: &SynthRowGroup) -> Result<i64> {
    let leaves = schema.leaves()?;
    if leaves.len() != rg.chunks.len() {
        return Err(Error::Corrupt("leaf count mismatch".into()));
    }
    let mut chunk_ids = Vec::with_capacity(rg.chunks.len());
    for (leaf, chunk) in leaves.iter().zip(&rg.chunks) {
        chunk_ids.push(((leaf.column_id, leaf.leaf), store_chunk(t, leaf, chunk)?));
    }
    t.put_row_group(rg.num_rows, &chunk_ids)
}

pub(crate) fn store_chunk(t: &mut Txn, leaf: &Leaf, chunk: &crate::codec::EncodedChunk) -> Result<i64> {
    if chunk.page_rows.len() != chunk.pages.len() {
        return Err(Error::Corrupt("page row counts missing".into()));
    }
    let page_ids = chunk
        .pages
        .iter()
        .zip(&chunk.page_rows)
        .zip(&chunk.page_stats)
        .map(|(((b, h), &rows), st)| t.put_page_rows(b, h, rows, st.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    t.put_chunk(
        &NewChunk {
            physical: leaf.physical(),
            codec: chunk.codec,
            encodings: chunk.encodings.clone(),
            stats: chunk.stats.clone(),
        },
        &page_ids,
    )
}
