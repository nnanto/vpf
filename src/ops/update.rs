//! In-place updates by predicate: only pages of the assigned columns that contain matching
//! rows are rewritten; all other pages and column chunks are reused.

use crate::convert::{array_to_column, arrow_schema};
use crate::error::{Error, Result};
use crate::meta::SegmentKind;
use crate::ops::rewrite::{PageCounts, rewrite_chunk};
use crate::ops::snapshot::{SegmentData, load_view};
use crate::ops::synth::{batch_columns, write_codec};
use crate::predicate::{Expr, Literal, literal_array, parse_assignments};
use crate::store::Store;
use crate::values::{Values, encode_batch};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateSummary {
    /// New snapshot, or `None` if no row matched.
    pub snapshot_id: Option<i64>,
    pub rows_updated: i64,
    pub segments_rewritten: usize,
    /// Column chunks reused without any change (unassigned columns and untouched chunks).
    pub chunks_reused: usize,
    pub pages: PageCounts,
}

impl Store {
    /// `UPDATE name SET assignments WHERE predicate`, e.g. `set = "a = 1, b = NULL"`.
    pub fn update_where(&self, name: &str, set: &str, predicate: &str) -> Result<UpdateSummary> {
        let assignments = parse_assignments(set)?;
        let expr = Expr::parse(predicate)?;
        self.write(|t| {
            let view = load_view(t.conn(), name, None)?;
            let arrow = arrow_schema(&view.schema)?;
            // Column index -> physical value (None = NULL).
            let mut values: Vec<(usize, Option<Values>)> = Vec::new();
            for (col, lit) in &assignments {
                let i = view.schema.column_index(col)?;
                let spec = &view.schema.columns[i];
                if spec.is_nested() {
                    if *lit != Literal::Null {
                        return Err(Error::Unsupported(format!("nested column '{col}' can only be set to NULL")));
                    }
                    if !spec.nullable {
                        return Err(Error::Invalid(format!("cannot set NULL in required column '{col}'")));
                    }
                    values.push((i, None));
                    continue;
                }
                let arr = literal_array(lit, arrow.field(i).data_type())?;
                let data = array_to_column(spec, arrow.field(i).data_type(), &arr)?;
                if *lit != Literal::Null && data.defs.as_ref().is_some_and(|d| d[0] == 0) {
                    return Err(Error::Invalid(format!("cannot convert {lit:?} for column '{col}'")));
                }
                values.push((i, (data.values.len() == 1).then_some(data.values)));
            }
            let masks = self.match_rows(&view, &expr)?;
            let rows_updated: i64 = masks.iter().map(|m| m.iter().filter(|x| **x).count() as i64).sum();
            let mut s = UpdateSummary { rows_updated, ..Default::default() };
            if rows_updated == 0 {
                return Ok(s);
            }
            let file_id = view.file.file_id;
            let sid = t.new_snapshot(
                file_id,
                view.schema.schema_version_id,
                "update",
                Some(&format!("{rows_updated} rows: SET {set} WHERE {predicate}")),
            )?;
            let codec = write_codec(&view.file);
            let leaves = view.schema.leaves()?;
            for (seg, mask) in view.segments.iter().zip(&masks) {
                if !mask.iter().any(|x| *x) {
                    continue;
                }
                s.segments_rewritten += 1;
                match seg {
                    SegmentData::RowGroup { rg, chunks, seg: seg_row } => {
                        let mut ids = Vec::with_capacity(chunks.len());
                        for (leaf, chunk) in leaves.iter().zip(chunks) {
                            let key = (leaf.column_id, leaf.leaf);
                            let Some((_, value)) = values.iter().find(|(i, _)| *i == leaf.field) else {
                                s.chunks_reused += 1;
                                ids.push((key, chunk.chunk_id));
                                continue;
                            };
                            let id = rewrite_chunk(
                                t,
                                leaf,
                                chunk,
                                codec,
                                &|r| mask[r].iter().any(|x| *x),
                                &|data, r| {
                                    let rows: Vec<usize> =
                                        r.clone().filter(|&i| mask[i]).map(|i| i - r.start).collect();
                                    data.set_rows(&rows, value.as_ref())
                                },
                                None,
                                &mut s.pages,
                            )?;
                            ids.push((key, id));
                        }
                        let new_rg = t.put_row_group(rg.num_rows, &ids)?;
                        t.replace_segment(file_id, seg_row, SegmentKind::RowGroup, new_rg, sid)?;
                    }
                    SegmentData::Batch { batch, seg: seg_row } => {
                        let mut cols = batch_columns(&view.schema, batch)?;
                        let rows: Vec<usize> = (0..mask.len()).filter(|&i| mask[i]).collect();
                        for (leaf, col) in leaves.iter().zip(cols.iter_mut()) {
                            if let Some((_, value)) = values.iter().find(|(i, _)| *i == leaf.field) {
                                *col = col.set_rows(&rows, value.as_ref())?;
                            }
                        }
                        let keyed: Vec<_> = leaves.iter().map(|l| (l.column_id, l.leaf)).zip(cols).collect();
                        let payload = encode_batch(mask.len(), &keyed)?;
                        let b = t.put_batch(view.schema.schema_version_id, batch.num_rows, &payload)?;
                        t.replace_segment(file_id, seg_row, SegmentKind::Batch, b, sid)?;
                    }
                }
            }
            t.finish_snapshot(file_id, sid)?;
            s.snapshot_id = Some(sid);
            Ok(s)
        })
    }
}
