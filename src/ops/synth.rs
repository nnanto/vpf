//! Synthesizing Parquet row groups from buffered batches at read time.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use parquet::basic::Compression;

use crate::codec::{EncodedChunk, encode_column};
use crate::convert::conform;
use crate::error::Result;
use crate::meta::{BatchRow, FileRow};
use crate::schema::SchemaVersion;
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
