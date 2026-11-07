//! Row-addressed mutations for SQL engines: rows are identified by their position in a
//! snapshot's virtual file (0-based, in file order), and the write fails with
//! [`Error::Conflict`] unless that snapshot is still current.

use arrow::array::{RecordBatch, UInt64Array};
use arrow::compute::take_record_batch;

use crate::convert::array_to_leaves;
use crate::error::{Error, Result};
use crate::ops::delete::{DeleteSummary, delete_in_segment};
use crate::ops::snapshot::{SnapshotView, load_view};
use crate::ops::synth::write_codec;
use crate::ops::update::{LeafAssign, UpdateSummary, update_in_segment};
use crate::store::{Store, Txn};
use crate::values::ColumnData;

/// Loads the current view and checks it is `base`.
fn current_view(t: &Txn, name: &str, base: i64) -> Result<SnapshotView> {
    let view = load_view(t.conn(), name, None)?;
    if view.snapshot.snapshot_id != base {
        return Err(Error::Conflict(format!(
            "table '{name}' changed: rows refer to snapshot {base}, current is {}",
            view.snapshot.snapshot_id
        )));
    }
    Ok(view)
}

/// Sorted positions; duplicates and out-of-range rows are errors.
fn check_sorted(rows: &[u64], num_rows: i64) -> Result<()> {
    if rows.windows(2).any(|w| w[0] >= w[1]) {
        return Err(Error::Invalid("row ids must be unique".into()));
    }
    if let Some(&last) = rows.last()
        && last >= num_rows as u64
    {
        return Err(Error::Invalid(format!(
            "row id {last} out of range ({num_rows} rows)"
        )));
    }
    Ok(())
}

/// Calls `f(segment, mask, range)` for every segment containing a row of `rows` (sorted),
/// where `range` indexes the rows of that segment within `rows`.
fn per_segment(
    view: &SnapshotView,
    rows: &[u64],
    mut f: impl FnMut(usize, &[bool], std::ops::Range<usize>) -> Result<()>,
) -> Result<()> {
    let mut start = 0u64;
    let mut i = 0;
    for (si, seg) in view.segments.iter().enumerate() {
        let n = seg.num_rows() as u64;
        let first = i;
        while i < rows.len() && rows[i] < start + n {
            i += 1;
        }
        if i > first {
            let mut mask = vec![false; n as usize];
            for &r in &rows[first..i] {
                mask[(r - start) as usize] = true;
            }
            f(si, &mask, first..i)?;
        }
        start += n;
    }
    Ok(())
}

impl Store {
    /// Deletes the given row positions of snapshot `base`.
    pub fn delete_rows(&self, name: &str, base: i64, rows: &[u64]) -> Result<DeleteSummary> {
        let mut rows = rows.to_vec();
        rows.sort_unstable();
        rows.dedup();
        self.write(|t| {
            let view = current_view(t, name, base)?;
            check_sorted(&rows, view.snapshot.num_rows)?;
            let mut s = DeleteSummary::default();
            let codec = write_codec(&view.file);
            let leaves = view.schema.leaves()?;
            per_segment(&view, &rows, |si, mask, _| {
                delete_in_segment(t, &view, &leaves, codec, &mut s, si, mask)
            })?;
            if let Some(sid) = s.snapshot_id {
                t.conn().execute(
                    "UPDATE snapshots SET summary = ?1 WHERE snapshot_id = ?2",
                    rusqlite::params![format!("{} rows by id", s.rows_deleted), sid],
                )?;
                t.finish_snapshot(view.file.file_id, sid)?;
            }
            Ok(s)
        })
    }

    /// Sets the columns of `values` (matched by name) on the given row positions of
    /// snapshot `base`; row `rows[i]` receives row `i` of `values`.
    pub fn update_rows(
        &self,
        name: &str,
        base: i64,
        rows: &[u64],
        values: &RecordBatch,
    ) -> Result<UpdateSummary> {
        if values.num_rows() != rows.len() {
            return Err(Error::Invalid("one value row is needed per row id".into()));
        }
        let mut order: Vec<u64> = (0..rows.len() as u64).collect();
        order.sort_unstable_by_key(|&i| rows[i as usize]);
        let sorted: Vec<u64> = order.iter().map(|&i| rows[i as usize]).collect();
        let values = take_record_batch(values, &UInt64Array::from(order))?;
        self.write(|t| {
            let view = current_view(t, name, base)?;
            check_sorted(&sorted, view.snapshot.num_rows)?;
            let leaves = view.schema.leaves()?;
            // Leaf index -> new values for all updated rows (in row order).
            let mut by_leaf: Vec<Option<ColumnData>> = vec![None; leaves.len()];
            let batch_schema = values.schema();
            for (i, f) in batch_schema.fields().iter().enumerate() {
                let field = view.schema.column_index(f.name())?;
                if batch_schema
                    .fields()
                    .iter()
                    .skip(i + 1)
                    .any(|g| g.name() == f.name())
                {
                    return Err(Error::Invalid(format!("duplicate column '{}'", f.name())));
                }
                let cols = array_to_leaves(&view.schema, field, values.column(i))?;
                let positions: Vec<usize> = (0..leaves.len())
                    .filter(|&l| leaves[l].field == field)
                    .collect();
                for (pos, c) in positions.into_iter().zip(cols) {
                    by_leaf[pos] = Some(c);
                }
            }
            let mut s = UpdateSummary::default();
            let codec = write_codec(&view.file);
            per_segment(&view, &sorted, |si, mask, range| {
                let assign: Vec<Option<LeafAssign>> = by_leaf
                    .iter()
                    .map(|c| {
                        c.as_ref()
                            .map(|c| LeafAssign::Rows(c.slice(range.start, range.len())))
                    })
                    .collect();
                update_in_segment(t, &view, &leaves, codec, &mut s, si, mask, &assign)
            })?;
            if let Some(sid) = s.snapshot_id {
                t.conn().execute(
                    "UPDATE snapshots SET summary = ?1 WHERE snapshot_id = ?2",
                    rusqlite::params![format!("{} rows by id", s.rows_updated), sid],
                )?;
                t.finish_snapshot(view.file.file_id, sid)?;
            }
            Ok(s)
        })
    }
}
