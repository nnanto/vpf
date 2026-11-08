//! Statistics-based pruning for filtered scans.
//!
//! Pruning is conservative: a row range is dropped only when the Parquet statistics of the
//! snapshot (row-group and page min/max/null/NaN counts, as any Parquet reader sees them)
//! prove that no row in it can match. Unknown statistics keep the rows.

use std::ops::Range;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Scalar, UInt64Array};
use arrow::compute::kernels::boolean::{and_kleene, not};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Schema};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::ParquetMetaData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
}

/// `column <op> value`; `value` is a one-element array (unused for null checks).
pub struct Filter {
    pub column: usize,
    pub op: Op,
    pub value: Option<ArrayRef>,
}

pub enum Step {
    Filter(usize),
    And,
    Or,
}

/// Evaluates a validated postfix expression over conservative candidate ranges.
pub fn expression_ranges(
    md: &ParquetMetaData,
    schema: &Schema,
    filters: &[Filter],
    steps: &[Step],
) -> Vec<Range<u64>> {
    // Keep the existing batched pruning path for conjunction-only scans.
    if !steps.iter().any(|s| matches!(s, Step::Or)) {
        return ranges(md, schema, filters);
    }
    let mut stack = Vec::new();
    for step in steps {
        match step {
            Step::Filter(i) => stack.push(ranges(md, schema, &filters[*i..=*i])),
            Step::And | Step::Or => {
                let b = stack.pop().expect("validated expression");
                let a = stack.pop().expect("validated expression");
                stack.push(match step {
                    Step::And => intersect(&a, &b),
                    _ => union(&a, &b),
                });
            }
        }
    }
    stack.pop().expect("validated expression")
}

/// Statistics of a set of row groups or pages, one entry each.
struct Stats {
    mins: ArrayRef,
    maxes: ArrayRef,
    nulls: UInt64Array,
    nans: UInt64Array,
    rows: Vec<u64>,
}

/// Sorted, disjoint row ranges of the file that may hold rows matching every filter.
pub fn ranges(md: &ParquetMetaData, schema: &Schema, filters: &[Filter]) -> Vec<Range<u64>> {
    let groups = md.row_groups();
    let converters: Vec<Option<StatisticsConverter>> = filters
        .iter()
        .map(|f| converter(md, schema, f.column))
        .collect();
    let all: Vec<usize> = (0..groups.len()).collect();
    let mut keep_group = vec![true; groups.len()];
    for (f, conv) in filters.iter().zip(&converters) {
        let Some(conv) = conv else { continue };
        let stats = (|| {
            Some(Stats {
                mins: conv.row_group_mins(groups).ok()?,
                maxes: conv.row_group_maxes(groups).ok()?,
                nulls: conv.row_group_null_counts(groups).ok()?,
                nans: conv.row_group_nan_counts(groups).ok()?,
                rows: groups.iter().map(|g| g.num_rows() as u64).collect(),
            })
        })();
        if let Some(stats) = stats {
            for (k, m) in keep_group.iter_mut().zip(may_match(f, &stats)) {
                *k &= m;
            }
        }
    }

    let mut out: Vec<Range<u64>> = Vec::new();
    let mut start = 0u64;
    for (g, group) in groups.iter().enumerate() {
        let rows = group.num_rows() as u64;
        if keep_group[g] {
            let mut keep = vec![0..rows];
            for (f, conv) in filters.iter().zip(&converters) {
                if let Some(pages) = conv
                    .as_ref()
                    .and_then(|c| page_ranges(md, c, f, &all[g..=g]))
                {
                    keep = intersect(&keep, &pages);
                }
            }
            for r in keep {
                push(&mut out, r.start + start..r.end + start);
            }
        }
        start += rows;
    }
    out
}

fn converter<'a>(
    md: &'a ParquetMetaData,
    schema: &'a Schema,
    column: usize,
) -> Option<StatisticsConverter<'a>> {
    let field = schema.fields().get(column)?;
    if field.data_type().is_nested() {
        return None;
    }
    let conv =
        StatisticsConverter::try_new(field.name(), schema, md.file_metadata().schema_descr())
            .ok()?
            .with_missing_null_counts_as_zero(false)
            .with_column_orders(md.file_metadata().column_orders().map(|o| o.as_slice()));
    conv.parquet_column_index().map(|_| conv)
}

/// Row ranges of row group `group[0]` (relative to it) whose pages may match `f`;
/// `None` if the page index cannot tell.
fn page_ranges(
    md: &ParquetMetaData,
    conv: &StatisticsConverter,
    f: &Filter,
    group: &[usize],
) -> Option<Vec<Range<u64>>> {
    let index = md.page_index()?.as_ref();
    let counts = conv
        .data_page_row_counts(index, md.row_groups(), group)
        .ok()??;
    if counts.null_count() > 0 {
        return None;
    }
    let stats = Stats {
        mins: conv.data_page_mins(index, group).ok()?,
        maxes: conv.data_page_maxes(index, group).ok()?,
        nulls: conv.data_page_null_counts(index, group).ok()?,
        nans: conv.data_page_nan_counts(index, group).ok()?,
        rows: counts.values().to_vec(),
    };
    if stats.mins.len() != stats.rows.len() {
        return None;
    }
    let mut out = Vec::new();
    let mut start = 0;
    for (rows, keep) in stats.rows.iter().zip(may_match(f, &stats)) {
        if keep {
            push(&mut out, start..start + rows);
        }
        start += rows;
    }
    Some(out)
}

/// For each entry of `s`, whether it may hold a row matching `f`.
fn may_match(f: &Filter, s: &Stats) -> Vec<bool> {
    let n = s.rows.len();
    let known = |a: &UInt64Array, i: usize| a.is_valid(i).then(|| a.value(i));
    let all_null = |i: usize| known(&s.nulls, i) == Some(s.rows[i]);
    match f.op {
        Op::IsNull => (0..n).map(|i| known(&s.nulls, i) != Some(0)).collect(),
        Op::IsNotNull => (0..n).map(|i| !all_null(i)).collect(),
        _ => {
            let bounds = f.value.as_ref().and_then(|v| compare(f.op, s, v));
            (0..n)
                .map(|i| {
                    // A comparison never matches NULL.
                    !all_null(i) && bounds.as_ref().is_none_or(|b| b.is_null(i) || b.value(i))
                })
                .collect()
        }
    }
}

/// For comparisons: whether the [min, max] bounds admit a match (null = unknown).
fn compare(op: Op, s: &Stats, value: &ArrayRef) -> Option<BooleanArray> {
    let ty = s.mins.data_type();
    let strict = CastOptions {
        safe: false,
        ..Default::default()
    };
    let value = cast_with_options(value, ty, &strict).ok()?;
    if value.len() != 1 || value.is_null(0) {
        return None;
    }
    if matches!(
        ty,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        return compare_floats(op, s, &value);
    }
    let v = Scalar::new(value);
    let (mins, maxes) = (&s.mins, &s.maxes);
    match op {
        Op::Eq => and_kleene(&lt_eq(mins, &v).ok()?, &gt_eq(maxes, &v).ok()?).ok(),
        Op::Ne => not(&and_kleene(&eq(mins, &v).ok()?, &eq(maxes, &v).ok()?).ok()?).ok(),
        Op::Lt => lt(mins, &v).ok(),
        Op::Le => lt_eq(mins, &v).ok(),
        Op::Gt => gt(maxes, &v).ok(),
        Op::Ge => gt_eq(maxes, &v).ok(),
        Op::IsNull | Op::IsNotNull => None,
    }
}

/// Float comparisons use IEEE semantics (-0 = 0). Min/max exclude NaN, and SQL engines
/// order NaN above every number, so `>`, `>=` and `<>` can only prune NaN-free entries.
fn compare_floats(op: Op, s: &Stats, value: &ArrayRef) -> Option<BooleanArray> {
    let f64s = |a: &ArrayRef| {
        cast_with_options(a, &DataType::Float64, &CastOptions::default())
            .ok()
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>().cloned())
    };
    let (mins, maxes, v) = (f64s(&s.mins)?, f64s(&s.maxes)?, f64s(value)?.value(0));
    if v.is_nan() {
        return None;
    }
    let nan_free = |i: usize| s.nans.is_valid(i) && s.nans.value(i) == 0;
    Some(
        (0..mins.len())
            .map(|i| {
                if mins.is_null(i) || maxes.is_null(i) {
                    return None;
                }
                let (lo, hi) = (mins.value(i), maxes.value(i));
                let ordered = match op {
                    Op::Eq => return Some(lo <= v && v <= hi),
                    Op::Lt => return Some(lo < v),
                    Op::Le => return Some(lo <= v),
                    Op::Gt => hi > v,
                    Op::Ge => hi >= v,
                    Op::Ne => !(lo == v && hi == v),
                    Op::IsNull | Op::IsNotNull => return None,
                };
                Some(ordered || !nan_free(i))
            })
            .collect(),
    )
}

/// Appends `r` to sorted ranges, merging with the last one when adjacent.
fn push(out: &mut Vec<Range<u64>>, r: Range<u64>) {
    if r.is_empty() {
        return;
    }
    match out.last_mut() {
        Some(last) if last.end == r.start => last.end = r.end,
        _ => out.push(r),
    }
}

fn intersect(a: &[Range<u64>], b: &[Range<u64>]) -> Vec<Range<u64>> {
    let (mut i, mut j, mut out) = (0, 0, Vec::new());
    while i < a.len() && j < b.len() {
        let r = a[i].start.max(b[j].start)..a[i].end.min(b[j].end);
        push(&mut out, r);
        if a[i].end < b[j].end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn union(a: &[Range<u64>], b: &[Range<u64>]) -> Vec<Range<u64>> {
    let (mut i, mut j, mut out) = (0, 0, Vec::<Range<u64>>::new());
    while i < a.len() || j < b.len() {
        let r = if j == b.len() || (i < a.len() && a[i].start <= b[j].start) {
            let r = a[i].clone();
            i += 1;
            r
        } else {
            let r = b[j].clone();
            j += 1;
            r
        };
        if let Some(last) = out.last_mut().filter(|last| last.end >= r.start) {
            last.end = last.end.max(r.end);
        } else {
            push(&mut out, r);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersect_ranges() {
        assert_eq!(intersect(&[0..10, 20..30], &[5..25]), vec![5..10, 20..25]);
        assert_eq!(intersect(&[0..10], &[10..20]), vec![]);
        assert_eq!(intersect(&[0..10], &[0..3, 3..10]), vec![0..10]);
    }

    #[test]
    fn union_ranges() {
        assert_eq!(union(&[0..10, 20..30], &[5..25]), vec![0..30]);
        assert_eq!(union(&[0..10], &[10..20]), vec![0..20]);
        assert_eq!(union(&[0..30], &[5..10, 20..25]), vec![0..30]);
        assert_eq!(union(&[0..10], &[20..30]), vec![0..10, 20..30]);
        assert_eq!(union(&[], &[5..10]), vec![5..10]);
        assert_eq!(union(&[], &[]), vec![]);
    }

    #[test]
    fn unknown_statistics_keep_or_branch() {
        use std::sync::Arc;

        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::Field;
        use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("unknown", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..1000)),
                Arc::new(StringArray::from(vec!["x"; 1000])),
            ],
        )
        .unwrap();
        let file = tempfile::tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_column_statistics_enabled("unknown".into(), EnabledStatistics::None)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let filters = [
            Filter {
                column: 0,
                op: Op::Eq,
                value: Some(Arc::new(Int64Array::from(vec![5]))),
            },
            Filter {
                column: 1,
                op: Op::Eq,
                value: Some(Arc::new(StringArray::from(vec!["x"]))),
            },
        ];
        assert_eq!(
            ranges(reader.metadata(), &schema, &filters[..1]),
            vec![0..100]
        );
        assert_eq!(
            expression_ranges(
                reader.metadata(),
                &schema,
                &filters,
                &[Step::Filter(0), Step::Filter(1), Step::Or]
            ),
            vec![0..1000],
        );
    }
}
