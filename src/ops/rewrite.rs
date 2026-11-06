//! Shared machinery for predicate-driven mutations: row matching over a snapshot and
//! page-level copy-on-write of column chunks.

use std::ops::Range;

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, Encoding};

use crate::codec::{RawPage, decode_pages, encode_column};
use crate::error::{Error, Result};
use crate::meta::ChunkRow;
use crate::ops::append::store_chunk;
use crate::ops::snapshot::SnapshotView;
use crate::predicate::Expr;
use crate::schema::Leaf;
use crate::stats::StoredStats;
use crate::store::{NewChunk, Store, Txn};
use crate::values::ColumnData;

/// Page reuse accounting for a mutation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageCounts {
    /// Data pages referenced unchanged by the new snapshot.
    pub reused: usize,
    /// Data pages decoded and replaced by newly encoded pages.
    pub rewritten: usize,
    /// Data pages removed because none of their rows survived.
    pub dropped: usize,
    /// Pages newly written.
    pub written: usize,
}

impl Store {
    /// Evaluates `expr` over a snapshot; returns one mask (`true` = match) per segment.
    pub fn match_rows(&self, view: &SnapshotView, expr: &Expr) -> Result<Vec<Vec<bool>>> {
        let mut idx = expr
            .columns()
            .iter()
            .map(|c| {
                let root = crate::predicate::root_column(c, |n| view.schema.column_by_name(n).is_some());
                view.schema.column_index(root)
            })
            .collect::<Result<Vec<_>>>()?;
        if idx.is_empty() && !view.schema.columns.is_empty() {
            idx.push(0);
        }
        let (vf, sources) = self.assemble(view)?;
        let md = ArrowReaderMetadata::load(&vf, ArrowReaderOptions::default())?;
        let mut out = vec![Vec::new(); view.segments.len()];
        for (rg, src) in sources.into_iter().enumerate() {
            let b = ParquetRecordBatchReaderBuilder::new_with_metadata(vf.clone(), md.clone());
            let mask = ProjectionMask::roots(b.parquet_schema(), idx.iter().copied());
            let reader = b.with_row_groups(vec![rg]).with_projection(mask).with_batch_size(64 * 1024).build()?;
            let mut m = Vec::new();
            for batch in reader {
                m.extend(expr.matches(&batch?)?);
            }
            let mut off = 0;
            for si in src {
                let n = view.segments[si].num_rows() as usize;
                out[si] = m.get(off..off + n).ok_or_else(|| Error::Corrupt("row count mismatch".into()))?.to_vec();
                off += n;
            }
            if off != m.len() {
                return Err(Error::Corrupt("row count mismatch".into()));
            }
        }
        Ok(out)
    }
}

/// Row ranges covered by each page of a chunk (`None` for non-data pages).
pub fn page_row_ranges(chunk: &ChunkRow) -> Vec<Option<Range<usize>>> {
    let mut row = 0usize;
    chunk
        .pages
        .iter()
        .map(|p| {
            p.kind.is_data().then(|| {
                let r = row..row + p.num_rows as usize;
                row = r.end;
                r
            })
        })
        .collect()
}

/// Page-level copy-on-write of one column chunk.
///
/// Pages whose row range is not `touched` are reused by reference. Each touched data page is
/// decoded (with the chunk's dictionary page), passed to `transform` together with its row
/// range, and re-encoded as PLAIN pages with the chunk's codec; pages left empty are dropped.
/// LZO chunks (which cannot be written) are re-encoded as a whole with `fallback_codec`.
/// Returns the chunk to reference: the original one if nothing was touched.
#[allow(clippy::too_many_arguments)]
pub fn rewrite_chunk(
    t: &mut Txn,
    leaf: &Leaf,
    chunk: &ChunkRow,
    fallback_codec: Compression,
    touched: &dyn Fn(Range<usize>) -> bool,
    transform: &dyn Fn(ColumnData, Range<usize>) -> Result<ColumnData>,
    stats: Option<StoredStats>,
    counts: &mut PageCounts,
) -> Result<i64> {
    if leaf.physical() != chunk.physical {
        return Err(Error::Corrupt(format!("chunk {} does not match column '{}'", chunk.chunk_id, leaf.spec.name)));
    }
    let ranges = page_row_ranges(chunk);
    let data_pages = ranges.iter().flatten().count();
    if !ranges.iter().flatten().any(|r| touched(r.clone())) {
        counts.reused += data_pages;
        return Ok(chunk.chunk_id);
    }
    if chunk.codec == Compression::LZO {
        let raw = chunk.pages.iter().map(|p| RawPage::load(t.blobs(), p)).collect::<Result<Vec<_>>>()?;
        let total = ranges.iter().flatten().last().map(|r| r.end).unwrap_or(0);
        let data = transform(decode_pages(leaf, chunk.codec, &raw)?, 0..total)?;
        let mut enc = encode_column(leaf, fallback_codec, &data, true)?;
        enc.stats = stats;
        counts.rewritten += data_pages;
        counts.written += enc.pages.len();
        return store_chunk(t, leaf, &enc);
    }
    let dict = chunk
        .pages
        .iter()
        .filter(|p| !p.kind.is_data())
        .map(|p| RawPage::load(t.blobs(), p))
        .collect::<Result<Vec<_>>>()?;
    let mut encodings: Vec<Encoding> = chunk.encodings.clone();
    let mut page_ids = Vec::with_capacity(chunk.pages.len());
    let mut data_out = 0;
    for (p, r) in chunk.pages.iter().zip(ranges) {
        let r = match r {
            Some(r) if touched(r.clone()) => r,
            Some(_) => {
                counts.reused += 1;
                data_out += 1;
                page_ids.push(p.page_id);
                continue;
            }
            None => {
                page_ids.push(p.page_id);
                continue;
            }
        };
        let mut pages = dict.clone();
        pages.push(RawPage::load(t.blobs(), p)?);
        let data = decode_pages(leaf, chunk.codec, &pages)?;
        if data.num_rows() != r.len() {
            return Err(Error::Corrupt(format!("page of chunk {} holds {} rows, expected {}", chunk.chunk_id, data.num_rows(), r.len())));
        }
        let new = transform(data, r)?;
        if new.num_rows() == 0 {
            counts.dropped += 1;
            continue;
        }
        counts.rewritten += 1;
        let enc = encode_column(leaf, chunk.codec, &new, false)?;
        for e in &enc.encodings {
            if !encodings.contains(e) {
                encodings.push(*e);
            }
        }
        for ((b, h), &rows) in enc.pages.iter().zip(&enc.page_rows) {
            page_ids.push(t.put_page_rows(b, h, rows)?);
            counts.written += 1;
            data_out += h.kind.is_data() as usize;
        }
    }
    if data_out == 0 {
        return Err(Error::Invalid("page rewrite would leave a column chunk without rows".into()));
    }
    t.put_chunk(&NewChunk { physical: chunk.physical, codec: chunk.codec, encodings, stats }, &page_ids)
}
