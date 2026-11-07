//! In-place updates by predicate: only pages of the assigned columns that contain matching
//! rows are rewritten; all other pages and column chunks are reused.

use parquet::basic::Compression;

use crate::convert::{array_to_column, arrow_schema};
use crate::error::{Error, Result};
use crate::meta::SegmentKind;
use crate::ops::rewrite::{PageCounts, rewrite_chunk};
use crate::ops::snapshot::{SegmentData, SnapshotView, load_view};
use crate::ops::synth::{batch_columns, write_codec};
use crate::predicate::{Expr, Literal, literal_array, parse_assignments};
use crate::schema::Leaf;
use crate::store::{Store, Txn};
use crate::values::{ColumnData, Values, encode_batch};

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
                        return Err(Error::Unsupported(format!(
                            "nested column '{col}' can only be set to NULL"
                        )));
                    }
                    if !spec.nullable {
                        return Err(Error::Invalid(format!(
                            "cannot set NULL in required column '{col}'"
                        )));
                    }
                    values.push((i, None));
                    continue;
                }
                let arr = literal_array(lit, arrow.field(i).data_type())?;
                let data = array_to_column(spec, arrow.field(i).data_type(), &arr)?;
                if *lit != Literal::Null && data.defs.as_ref().is_some_and(|d| d[0] == 0) {
                    return Err(Error::Invalid(format!(
                        "cannot convert {lit:?} for column '{col}'"
                    )));
                }
                values.push((i, (data.values.len() == 1).then_some(data.values)));
            }
            let mut s = UpdateSummary::default();
            let file_id = view.file.file_id;
            let codec = write_codec(&view.file);
            let leaves = view.schema.leaves()?;
            let assign: Vec<Option<LeafAssign>> = leaves
                .iter()
                .map(|l| {
                    values
                        .iter()
                        .find(|(i, _)| *i == l.field)
                        .map(|(_, v)| LeafAssign::Literal(v.clone()))
                })
                .collect();
            self.for_each_matching_segment(&view, &expr, |si, mask| {
                update_in_segment(t, &view, &leaves, codec, &mut s, si, mask, &assign)
            })?;
            if let Some(sid) = s.snapshot_id {
                t.conn().execute(
                    "UPDATE snapshots SET summary = ?1 WHERE snapshot_id = ?2",
                    rusqlite::params![
                        format!("{} rows: SET {set} WHERE {predicate}", s.rows_updated),
                        sid
                    ],
                )?;
                t.finish_snapshot(file_id, sid)?;
            }
            Ok(s)
        })
    }
}

/// New content for one leaf column of the rows being updated.
#[derive(Debug, Clone)]
pub(crate) enum LeafAssign {
    /// The same value for every updated row (`None` = NULL).
    Literal(Option<Values>),
    /// One row per updated row of the segment, in row order.
    Rows(ColumnData),
}

impl LeafAssign {
    /// Applies to `rows` (indexes into `data`), which are the updated rows numbered from
    /// `first` among all updated rows of the segment.
    fn apply(&self, data: &ColumnData, rows: &[usize], first: usize) -> Result<ColumnData> {
        match self {
            LeafAssign::Literal(v) => data.set_rows(rows, v.as_ref()),
            LeafAssign::Rows(c) => data.replace_rows(rows, &c.slice(first, rows.len())),
        }
    }
}

/// Rewrites the rows flagged in `mask` of segment `si` with `assign` (one entry per leaf;
/// `None` keeps the leaf), creating the update snapshot on first use.
#[allow(clippy::too_many_arguments)]
pub(crate) fn update_in_segment(
    t: &mut Txn,
    view: &SnapshotView,
    leaves: &[Leaf],
    codec: Compression,
    s: &mut UpdateSummary,
    si: usize,
    mask: &[bool],
    assign: &[Option<LeafAssign>],
) -> Result<()> {
    let file_id = view.file.file_id;
    let seg = &view.segments[si];
    let n = mask.iter().filter(|x| **x).count();
    if n == 0 {
        return Ok(());
    }
    let sid = match s.snapshot_id {
        Some(sid) => sid,
        None => {
            let sid = t.new_snapshot(file_id, view.schema.schema_version_id, "update", None)?;
            s.snapshot_id = Some(sid);
            sid
        }
    };
    s.rows_updated += n as i64;
    s.segments_rewritten += 1;
    // before[i] = number of updated rows preceding row i.
    let mut before = Vec::with_capacity(mask.len() + 1);
    let mut acc = 0;
    for m in mask {
        before.push(acc);
        acc += *m as usize;
    }
    before.push(acc);
    match seg {
        SegmentData::RowGroup {
            rg,
            chunks,
            seg: seg_row,
        } => {
            let mut ids = Vec::with_capacity(chunks.len());
            for ((leaf, chunk), a) in leaves.iter().zip(chunks).zip(assign) {
                let key = (leaf.column_id, leaf.leaf);
                let Some(a) = a else {
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
                        let rows: Vec<usize> = r
                            .clone()
                            .filter(|&i| mask[i])
                            .map(|i| i - r.start)
                            .collect();
                        a.apply(&data, &rows, before[r.start])
                    },
                    None,
                    &mut s.pages,
                )?;
                ids.push((key, id));
            }
            let new_rg = t.put_row_group(rg.num_rows, &ids)?;
            t.replace_segment(file_id, seg_row, SegmentKind::RowGroup, new_rg, sid)?;
        }
        SegmentData::Batch {
            batch,
            seg: seg_row,
        } => {
            let mut cols = batch_columns(&view.schema, batch)?;
            let rows: Vec<usize> = (0..mask.len()).filter(|&i| mask[i]).collect();
            for (col, a) in cols.iter_mut().zip(assign) {
                if let Some(a) = a {
                    *col = a.apply(col, &rows, 0)?;
                }
            }
            let keyed: Vec<_> = leaves
                .iter()
                .map(|l| (l.column_id, l.leaf))
                .zip(cols)
                .collect();
            let payload = encode_batch(mask.len(), &keyed)?;
            let b = t.put_batch(view.schema.schema_version_id, batch.num_rows, &payload)?;
            t.replace_segment(file_id, seg_row, SegmentKind::Batch, b, sid)?;
        }
    }
    Ok(())
}
