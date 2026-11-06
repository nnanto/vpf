//! Synthesizing Parquet row groups from buffered batches at read time.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::SchemaDescriptor;

use crate::codec::{DATA_PAGE_ROWS, DATA_PAGE_SIZE, EncodedChunk, encode_column};
use crate::convert::conform;
use crate::error::{Error, Result};
use crate::meta::{BatchRow, FileRow};
use crate::schema::SchemaVersion;
use crate::stats::{PageStats, StoredStats};
use crate::thrift::split_pages;
use crate::values::{ColumnData, decode_batch};

/// Upper bound on rows coalesced into one synthesized row group.
pub const MAX_SYNTH_ROWS: i64 = 1 << 20;
const CACHE_ENTRIES: usize = 64;

/// Row group synthesized from a run of consecutive batches.
#[derive(Debug)]
pub struct SynthRowGroup {
    pub num_rows: i64,
    /// One encoded chunk per leaf column, in leaf order.
    pub chunks: Vec<EncodedChunk>,
}

type Key = (Vec<i64>, i64);

/// Small FIFO cache of synthesized row groups keyed by (batch ids, schema version).
/// Batches and schema versions are immutable, so entries never go stale.
#[derive(Default)]
pub struct SynthCache {
    inner: Mutex<(HashMap<Key, Arc<SynthRowGroup>>, VecDeque<Key>)>,
}

impl SynthCache {
    pub fn get_or_build(
        &self,
        key: Key,
        build: impl FnOnce() -> Result<SynthRowGroup>,
    ) -> Result<Arc<SynthRowGroup>> {
        if let Some(v) = self.inner.lock().unwrap().0.get(&key) {
            return Ok(v.clone());
        }
        let v = Arc::new(build()?);
        let mut g = self.inner.lock().unwrap();
        if g.0.insert(key.clone(), v.clone()).is_none() {
            g.1.push_back(key);
            while g.1.len() > CACHE_ENTRIES {
                let old = g.1.pop_front().unwrap();
                g.0.remove(&old);
            }
        }
        Ok(v)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Codec used for pages vpf writes into a file (LZO cannot be written).
pub fn write_codec(file: &FileRow) -> Compression {
    match file.default_codec {
        Compression::LZO => Compression::SNAPPY,
        c => c,
    }
}

/// Decodes and conforms a batch to `schema` (one column per leaf column).
pub fn batch_columns(schema: &SchemaVersion, batch: &BatchRow) -> Result<Vec<ColumnData>> {
    let (n, cols) = decode_batch(&batch.payload)?;
    if n as i64 != batch.num_rows {
        return Err(crate::Error::Corrupt(format!("batch {} row count mismatch", batch.batch_id)));
    }
    conform(schema, n, cols)
}

/// Encodes per-leaf data as one row group.
pub fn encode_row_group(schema: &SchemaVersion, codec: Compression, cols: &[ColumnData]) -> Result<SynthRowGroup> {
    let num_rows = cols.first().map(|c| c.num_rows()).unwrap_or(0) as i64;
    let leaves = schema.leaves()?;
    if leaves.len() != cols.len() {
        return Err(crate::Error::Corrupt("leaf count mismatch".into()));
    }
    let chunks = leaves
        .iter()
        .zip(cols)
        .map(|(leaf, data)| encode_column(leaf, codec, data, true))
        .collect::<Result<_>>()?;
    Ok(SynthRowGroup { num_rows, chunks })
}

/// Encodes a compatible, conformed flat Arrow row group once, then extracts its
/// column pages. Page statistics are computed from Arrow slices, not decoded pages.
pub(crate) fn encode_arrow_row_group(schema: &SchemaVersion, codec: Compression, batch: &RecordBatch) -> Result<SynthRowGroup> {
    let descriptor = schema.descriptor()?;
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(codec)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_offset_index_disabled(true)
        .set_data_page_size_limit(DATA_PAGE_SIZE)
        .set_data_page_row_count_limit(DATA_PAGE_ROWS)
        .set_max_row_group_row_count(None)
        .build();
    let options = ArrowWriterOptions::new()
        .with_properties(props)
        .with_parquet_schema(SchemaDescriptor::new(descriptor.root_schema_ptr()));
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut buffer, batch.schema(), options)?;
    writer.write(batch)?;
    writer.close()?;
    let bytes = Bytes::from(buffer);
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
    if metadata.num_row_groups() != 1
        || metadata.row_group(0).num_rows() != batch.num_rows() as i64
        || metadata.file_metadata().schema_descr().root_schema() != descriptor.root_schema()
    {
        return Err(Error::Corrupt("Arrow append encoder produced an unexpected layout".into()));
    }
    let leaves = schema.leaves()?;
    if leaves.len() != batch.num_columns() {
        return Err(Error::Corrupt("Arrow append requires one flat leaf per column".into()));
    }
    let chunks = leaves.iter().zip(metadata.row_group(0).columns()).zip(batch.columns()).map(|((leaf, column), array)| {
        let (start, len) = column.byte_range();
        let chunk = bytes.slice(start as usize..(start + len) as usize);
        let mut pages = Vec::new();
        let mut page_rows = Vec::new();
        let mut page_stats = Vec::new();
        let mut row = 0usize;
        let stat_type = leaf.stat_type()?;
        for page in split_pages(&chunk)? {
            let rows = if page.header.kind.is_data() {
                usize::try_from(page.header.num_values).map_err(|_| Error::Corrupt("negative append page row count".into()))?
            } else { 0 };
            let end = row.checked_add(rows).filter(|&end| end <= array.len())
                .ok_or_else(|| Error::Corrupt("append pages exceed row group".into()))?;
            let stats = if page.header.kind.is_data() {
                PageStats::compute_arrow(&stat_type, array.slice(row, rows).as_ref(), leaf.spec.type_length.max(0) as usize)?
            } else { None };
            page_stats.push(stats);
            page_rows.push(rows as i64);
            pages.push((chunk.slice(page.offset..page.offset + page.header.total_len()), page.header));
            row = end;
        }
        if row != batch.num_rows() || column.num_values() != row as i64 {
            return Err(Error::Corrupt("append page rows do not match batch".into()));
        }
        Ok(EncodedChunk {
            pages, page_rows, page_stats,
            codec: column.compression(),
            encodings: column.encodings().collect(),
            stats: column.statistics().map(StoredStats::from_parquet),
            num_values: column.num_values(),
        })
    }).collect::<Result<Vec<_>>>()?;
    Ok(SynthRowGroup { num_rows: batch.num_rows() as i64, chunks })
}

/// Synthesizes one row group from consecutive batches.
pub fn synthesize(schema: &SchemaVersion, codec: Compression, batches: &[&BatchRow]) -> Result<SynthRowGroup> {
    let mut acc: Option<Vec<ColumnData>> = None;
    for b in batches {
        let cols = batch_columns(schema, b)?;
        match &mut acc {
            None => acc = Some(cols),
            Some(a) => {
                for (x, y) in a.iter_mut().zip(&cols) {
                    x.append(y)?;
                }
            }
        }
    }
    encode_row_group(schema, codec, &acc.unwrap_or_default())
}

/// Groups consecutive batch segments into runs of at most [`MAX_SYNTH_ROWS`] rows.
/// Returns, for each segment index, the run it starts (if any).
pub fn batch_runs<T>(items: &[T], is_batch: impl Fn(&T) -> Option<i64>) -> Vec<std::ops::Range<usize>> {
    let mut runs = Vec::new();
    let mut i = 0;
    while i < items.len() {
        if let Some(mut rows) = is_batch(&items[i]) {
            let start = i;
            i += 1;
            while i < items.len() {
                match is_batch(&items[i]) {
                    Some(r) if rows + r <= MAX_SYNTH_ROWS => {
                        rows += r;
                        i += 1;
                    }
                    _ => break,
                }
            }
            runs.push(start..i);
        } else {
            i += 1;
        }
    }
    runs
}
