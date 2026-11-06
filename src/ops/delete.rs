//! Physical row deletion by predicate with page-level copy-on-write.

use crate::error::Result;
use crate::meta::SegmentKind;
use crate::ops::rewrite::{PageCounts, rewrite_chunk};
use crate::ops::snapshot::{SegmentData, load_view};
use crate::ops::synth::write_codec;
use crate::predicate::Expr;
use crate::store::Store;
use crate::values::{decode_batch, encode_batch};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteSummary {
    /// New snapshot, or `None` if no row matched.
    pub snapshot_id: Option<i64>,
    pub rows_deleted: i64,
    /// Row groups / batches removed entirely.
    pub segments_removed: usize,
    /// Row groups / batches rewritten.
    pub segments_rewritten: usize,
    pub pages: PageCounts,
}

impl Store {
    /// Physically deletes rows matching `predicate` from the current snapshot.
    pub fn delete_where(&self, name: &str, predicate: &str) -> Result<DeleteSummary> {
        let expr = Expr::parse(predicate)?;
        self.write(|t| {
            let view = load_view(t.conn(), name, None)?;
            let mut s = DeleteSummary::default();
            let file_id = view.file.file_id;
            let codec = write_codec(&view.file);
            let leaves = view.schema.leaves()?;
            self.for_each_matching_segment(&view, &expr, |si, mask| {
                let seg = &view.segments[si];
                let n_del = mask.iter().filter(|x| **x).count();
                let sid = match s.snapshot_id {
                    Some(sid) => sid,
                    None => {
                        let sid = t.new_snapshot(file_id, view.schema.schema_version_id, "delete", None)?;
                        s.snapshot_id = Some(sid);
                        sid
                    }
                };
                s.rows_deleted += n_del as i64;
                let seg_row = seg.segment();
                if n_del == mask.len() {
                    t.remove_segment(seg_row.segment_id, sid)?;
                    s.segments_removed += 1;
                    return Ok(());
                }
                s.segments_rewritten += 1;
                let keep: Vec<bool> = mask.iter().map(|m| !m).collect();
                let remaining = (mask.len() - n_del) as i64;
                match seg {
                    SegmentData::RowGroup { chunks, .. } => {
                        let mut ids = Vec::with_capacity(chunks.len());
                        for (leaf, chunk) in leaves.iter().zip(chunks) {
                            let id = rewrite_chunk(
                                t,
                                leaf,
                                chunk,
                                codec,
                                &|r| mask[r].iter().any(|x| *x),
                                &|data, r| Ok(data.filter(&keep[r])),
                                chunk.stats.as_ref().map(|st| st.after_row_removal()),
                                &mut s.pages,
                            )?;
                            ids.push(((leaf.column_id, leaf.leaf), id));
                        }
                        let rg = t.put_row_group(remaining, &ids)?;
                        t.replace_segment(file_id, seg_row, SegmentKind::RowGroup, rg, sid)?;
                    }
                    SegmentData::Batch { batch, .. } => {
                        let (_, cols) = decode_batch(&batch.payload)?;
                        let cols: Vec<_> = cols.into_iter().map(|(id, c)| (id, c.filter(&keep))).collect();
                        let payload = encode_batch(remaining as usize, &cols)?;
                        let b = t.put_batch(batch.schema_version_id, remaining, &payload)?;
                        t.replace_segment(file_id, seg_row, SegmentKind::Batch, b, sid)?;
                    }
                }
                Ok(())
            })?;
            if let Some(sid) = s.snapshot_id {
                t.conn().execute(
                    "UPDATE snapshots SET summary = ?1 WHERE snapshot_id = ?2",
                    rusqlite::params![format!("{} rows where {predicate}", s.rows_deleted), sid],
                )?;
                t.finish_snapshot(file_id, sid)?;
            }
            Ok(s)
        })
    }
}
