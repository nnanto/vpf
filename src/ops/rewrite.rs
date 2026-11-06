//! Shared machinery for predicate-driven mutations: row matching over a snapshot and
//! page-level copy-on-write of column chunks.

use std::ops::Range;

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    RowSelection, RowSelector,
};
use parquet::basic::{Compression, Encoding};

use crate::codec::{RawPage, decode_pages, encode_column};
use crate::error::{Error, Result};
use crate::meta::ChunkRow;
use crate::ops::append::store_chunk;
use crate::ops::pruning::PruningExpr;
use crate::ops::snapshot::{SegmentData, SnapshotView};
use crate::predicate::Expr;
use crate::schema::Leaf;
use crate::stats::{PageStats, StoredStats, merge_pages};
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
        let mut out = vec![Vec::new(); view.segments.len()];
        self.for_each_matching_segment(view, expr, |si, mask| {
            out[si] = mask.to_vec();
            Ok(())
        })?;
        for (mask, segment) in out.iter_mut().zip(&view.segments) {
            if mask.is_empty() { mask.resize(segment.num_rows() as usize, false); }
        }
        Ok(out)
    }

    /// Evaluates and consumes masks one row group at a time against the frozen view.
    /// Segments proven not to match are omitted; the callback may write within the
    /// caller's transaction without changing the input snapshot.
    pub(crate) fn for_each_matching_segment(
        &self,
        view: &SnapshotView,
        expr: &Expr,
        mut consume: impl FnMut(usize, &[bool]) -> Result<()>,
    ) -> Result<()> {
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
        let md = ArrowReaderMetadata::try_new(vf.metadata().clone(), ArrowReaderOptions::default())?;
        let pruning = PruningExpr::compile(expr, &view.schema, md.schema())?;
        for (rg, src) in sources.into_iter().enumerate() {
            let rows = vf.metadata().row_group(rg).num_rows() as usize;
            let ranges = match &view.segments[src.start] {
                SegmentData::RowGroup { chunks, .. } => candidate_ranges(&pruning, chunks, rows)?,
                SegmentData::Batch { .. } => vec![0..rows],
            };
            if ranges.is_empty() {
                continue;
            }
            let b = ParquetRecordBatchReaderBuilder::new_with_metadata(vf.clone(), md.clone());
            let projection = ProjectionMask::roots(b.parquet_schema(), idx.iter().copied());
            let mut selectors = Vec::new();
            let mut end = 0;
            for r in &ranges {
                if r.start > end { selectors.push(RowSelector::skip(r.start - end)); }
                selectors.push(RowSelector::select(r.len()));
                end = r.end;
            }
            if end < rows { selectors.push(RowSelector::skip(rows - end)); }
            let reader = b.with_row_groups(vec![rg]).with_projection(projection)
                .with_row_selection(RowSelection::from(selectors))
                .with_batch_size(64 * 1024).build()?;
            let mut m = vec![false; rows];
            let mut selected = ranges.iter().flat_map(|r| r.clone());
            for batch in reader {
                for matches in expr.matches(&batch?)? {
                    let row = selected.next().ok_or_else(|| Error::Corrupt("row count mismatch".into()))?;
                    m[row] = matches;
                }
            }
            if selected.next().is_some() {
                return Err(Error::Corrupt("row count mismatch".into()));
            }
            let mut off = 0;
            for si in src {
                let n = view.segments[si].num_rows() as usize;
                let mask = m.get(off..off + n).ok_or_else(|| Error::Corrupt("row count mismatch".into()))?;
                if mask.iter().any(|v| *v) { consume(si, mask)?; }
                off += n;
            }
            if off != m.len() {
                return Err(Error::Corrupt("row count mismatch".into()));
            }
        }
        Ok(())
    }
}

fn candidate_ranges(pruning: &PruningExpr, chunks: &[ChunkRow], rows: usize) -> Result<Vec<Range<usize>>> {
    let mut columns = Vec::new();
    pruning.columns(&mut columns);
    columns.sort_unstable();
    columns.dedup();
    let mut boundaries = vec![0, rows];
    let mut pages = Vec::new();
    for column in columns {
        let mut end = 0usize;
        let mut spans = Vec::new();
        for p in &chunks[column].pages {
            if !p.kind.is_data() { continue; }
            let n = usize::try_from(p.num_rows).map_err(|_| Error::Corrupt("negative page row count".into()))?;
            end = end.checked_add(n).ok_or_else(|| Error::Corrupt("page row count overflow".into()))?;
            boundaries.push(end);
            spans.push((end, p.stats.as_ref(), n as u64));
        }
        if end != rows { return Err(Error::Corrupt("chunk page row count mismatch".into())); }
        pages.push((column, spans));
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    let mut ranges: Vec<Range<usize>> = Vec::new();
    for pair in boundaries.windows(2) {
        let start = pair[0];
        let page = |column| {
            let (_, spans) = pages.iter().find(|(c, _)| *c == column)?;
            let i = spans.partition_point(|(end, _, _)| *end <= start);
            let (_, stats, n) = spans.get(i)?;
            stats.map(|s| (s, *n))
        };
        if pruning.may_match(&page) {
            if let Some(last) = ranges.last_mut() && last.end == start {
                last.end = pair[1];
            } else {
                ranges.push(start..pair[1]);
            }
        }
    }
    Ok(ranges)
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
        let enc = encode_column(leaf, fallback_codec, &data, true)?;
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
    let mut out_stats: Vec<Option<PageStats>> = Vec::new();
    for (p, r) in chunk.pages.iter().zip(ranges) {
        let r = match r {
            Some(r) if touched(r.clone()) => r,
            Some(_) => {
                counts.reused += 1;
                data_out += 1;
                out_stats.push(p.stats.clone());
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
        for (((b, h), &rows), st) in enc.pages.iter().zip(&enc.page_rows).zip(&enc.page_stats) {
            page_ids.push(t.put_page_rows(b, h, rows, st.as_ref())?);
            counts.written += 1;
            if h.kind.is_data() {
                data_out += 1;
                out_stats.push(st.clone());
            }
        }
    }
    if data_out == 0 {
        return Err(Error::Invalid("page rewrite would leave a column chunk without rows".into()));
    }
    // Exact chunk statistics from the pages when all of them have statistics.
    let stats = match out_stats.into_iter().collect::<Option<Vec<_>>>() {
        Some(ps) => Some(merge_pages(&leaf.stat_type()?, &ps)),
        None => stats,
    };
    t.put_chunk(&NewChunk { physical: chunk.physical, codec: chunk.codec, encodings, stats }, &page_ids)
}
