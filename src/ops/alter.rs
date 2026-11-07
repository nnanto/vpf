//! Schema evolution. Every change creates a new schema version and a snapshot atomically.
//!
//! * drop / rename / reorder: metadata only, all pages reused.
//! * add column: one constant chunk is encoded per distinct row group size and shared.
//! * widen (INT32 -> INT64, FLOAT -> DOUBLE) and required -> optional: the affected column's
//!   chunks are re-encoded; all other chunks are reused.
//!
//! Nested columns support add (NULL / empty only), drop, rename, reorder and
//! required -> optional (every leaf gains one definition level); widening is flat-only.
//!
//! Buffered batches are never rewritten; they are conformed to the snapshot's schema on read.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use parquet::arrow::{ARROW_SCHEMA_META_KEY, encode_arrow_schema, parquet_to_arrow_schema};
use parquet::basic::{Compression, ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::KeyValue;

use crate::codec::{RawPage, decode_pages, encode_column};
use crate::convert::{array_to_column, arrow_schema, conform_leaf};
use crate::error::{Error, Result};
use crate::meta::{self, SegmentKind};
use crate::ops::append::store_chunk;
use crate::ops::snapshot::{SegmentData, load_view};
use crate::ops::synth::write_codec;
use crate::predicate::{Literal, literal_array};
use crate::schema::{ColumnSpec, build_descriptor};
use crate::store::Store;
use crate::values::{ColumnData, decode_value, encode_value};

#[derive(Debug, Clone, PartialEq)]
pub enum AlterOp {
    /// Adds a column at the end. `spec.column_id` and `spec.default_value` are ignored;
    /// existing rows get `default` (NULL if `None`; required columns need a default).
    AddColumn {
        spec: ColumnSpec,
        default: Option<Literal>,
    },
    DropColumn(String),
    RenameColumn {
        from: String,
        to: String,
    },
    /// New column order; must be a permutation of all column names.
    Reorder(Vec<String>),
    /// Lossless widening: INT32 -> INT64 or FLOAT -> DOUBLE.
    Widen {
        column: String,
        to: PhysicalType,
    },
    /// REQUIRED -> OPTIONAL.
    MakeOptional(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterSummary {
    pub snapshot_id: i64,
    pub schema_version_id: i64,
    pub row_groups_rewritten: usize,
    pub chunks_written: usize,
}

enum Rewrite {
    None,
    Add(usize),
    Recode(usize),
}

impl Store {
    pub fn alter(&self, name: &str, op: AlterOp) -> Result<AlterSummary> {
        self.write(|t| {
            let view = load_view(t.conn(), name, None)?;
            let old = &view.schema;
            let file_id = view.file.file_id;
            let mut cols = old.columns.clone();
            let mut fields: Vec<FieldRef> = arrow_schema(old)?.fields().iter().cloned().collect();
            let idx = |cols: &[ColumnSpec], n: &str| {
                cols.iter()
                    .position(|c| c.name == n)
                    .ok_or_else(|| Error::NotFound(format!("column '{n}'")))
            };
            let (rewrite, desc) = match &op {
                AlterOp::AddColumn { spec, default } => {
                    if spec.name.is_empty() {
                        return Err(Error::Invalid("empty column name".into()));
                    }
                    if cols.iter().any(|c| c.name == spec.name) {
                        return Err(Error::Invalid(format!(
                            "column '{}' already exists",
                            spec.name
                        )));
                    }
                    let mut spec = ColumnSpec {
                        column_id: 0,
                        default_value: None,
                        ..spec.clone()
                    };
                    if spec.is_nested() {
                        if default.as_ref().is_some_and(|l| *l != Literal::Null) {
                            return Err(Error::Unsupported(format!(
                                "default value for nested column '{}'",
                                spec.name
                            )));
                        }
                        if !spec.top_level_nullable() {
                            return Err(Error::Invalid(format!(
                                "required nested column '{}' cannot be added to existing rows",
                                spec.name
                            )));
                        }
                    }
                    let field = parquet_to_arrow_schema(
                        build_descriptor("s", std::slice::from_ref(&spec))?.as_ref(),
                        None,
                    )?
                    .field(0)
                    .clone();
                    if let Some(lit) = default.as_ref().filter(|l| **l != Literal::Null) {
                        let arr = literal_array(lit, field.data_type())?;
                        let data = array_to_column(&spec, field.data_type(), &arr)?;
                        spec.default_value = Some(encode_value(&data.values)?);
                    }
                    if !spec.top_level_nullable() && spec.default_value.is_none() {
                        return Err(Error::Invalid(format!(
                            "required column '{}' needs a default",
                            spec.name
                        )));
                    }
                    spec.column_id = t.alloc_column_ids(file_id, 1)?;
                    let desc = format!("add column {}", spec.name);
                    cols.push(spec);
                    fields.push(Arc::new(field));
                    (Rewrite::Add(cols.len() - 1), desc)
                }
                AlterOp::DropColumn(n) => {
                    let i = idx(&cols, n)?;
                    cols.remove(i);
                    fields.remove(i);
                    (Rewrite::None, format!("drop column {n}"))
                }
                AlterOp::RenameColumn { from, to } => {
                    let i = idx(&cols, from)?;
                    if to.is_empty() {
                        return Err(Error::Invalid("empty column name".into()));
                    }
                    cols[i].name = to.clone();
                    fields[i] = Arc::new(fields[i].as_ref().clone().with_name(to));
                    (Rewrite::None, format!("rename column {from} to {to}"))
                }
                AlterOp::Reorder(order) => {
                    let mut seen = std::collections::HashSet::new();
                    let perm = order
                        .iter()
                        .map(|n| {
                            if !seen.insert(n) {
                                return Err(Error::Invalid(format!("column '{n}' listed twice")));
                            }
                            idx(&cols, n)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if perm.len() != cols.len() {
                        return Err(Error::Invalid(
                            "reorder must list every column exactly once".into(),
                        ));
                    }
                    cols = perm.iter().map(|&i| cols[i].clone()).collect();
                    fields = perm.iter().map(|&i| fields[i].clone()).collect();
                    (
                        Rewrite::None,
                        format!("reorder columns {}", order.join(", ")),
                    )
                }
                AlterOp::Widen { column, to } => {
                    let i = idx(&cols, column)?;
                    let c = &mut cols[i];
                    if c.is_nested() {
                        return Err(Error::Unsupported(format!(
                            "widening nested column '{column}'"
                        )));
                    }
                    let new_ty = widen_spec(c, *to)?;
                    fields[i] = Arc::new(fields[i].as_ref().clone().with_data_type(new_ty));
                    (Rewrite::Recode(i), format!("widen column {column} to {to}"))
                }
                AlterOp::MakeOptional(n) => {
                    let i = idx(&cols, n)?;
                    if cols[i].top_level_nullable() {
                        return Err(Error::Invalid(format!("column '{n}' is already optional")));
                    }
                    cols[i].nullable = true;
                    fields[i] = Arc::new(fields[i].as_ref().clone().with_nullable(true));
                    (Rewrite::Recode(i), format!("make column {n} optional"))
                }
            };
            let kv = evolve_key_value(old.key_value.as_deref(), &old.root_name, &cols, fields)?;
            let svid = meta::insert_schema(
                t.conn(),
                file_id,
                Some(old.schema_version_id),
                &old.root_name,
                &cols,
                kv.as_deref(),
            )?;
            let schema = meta::load_schema(t.conn(), svid)?;
            let sid = t.new_snapshot(file_id, svid, "alter", Some(&desc))?;
            let mut summary = AlterSummary {
                snapshot_id: sid,
                schema_version_id: svid,
                row_groups_rewritten: 0,
                chunks_written: 0,
            };
            let codec = write_codec(&view.file);
            let leaves = schema.leaves()?;
            let old_leaves = old.leaves()?;
            let mut shared: HashMap<(i64, i64), i64> = HashMap::new();
            for seg in &view.segments {
                let SegmentData::RowGroup {
                    seg: seg_row,
                    rg,
                    chunks,
                } = seg
                else {
                    continue;
                };
                let field = match rewrite {
                    Rewrite::None => continue,
                    Rewrite::Add(i) | Rewrite::Recode(i) => i,
                };
                let spec = &schema.columns[field];
                let mut new_chunks: HashMap<(i64, i64), i64> = HashMap::new();
                for leaf in leaves.iter().filter(|l| l.field == field) {
                    let key = (leaf.column_id, leaf.leaf);
                    let id = match rewrite {
                        Rewrite::Add(_) => match shared.get(&(rg.num_rows, leaf.leaf)) {
                            Some(id) => *id,
                            None => {
                                let n = rg.num_rows as usize;
                                let data = if spec.is_nested() {
                                    ColumnData::nulls(
                                        leaf.physical(),
                                        leaf.max_def,
                                        leaf.max_rep,
                                        n,
                                    )?
                                } else {
                                    let default = spec
                                        .default_value
                                        .as_deref()
                                        .map(decode_value)
                                        .transpose()?;
                                    ColumnData::repeat(
                                        spec.physical,
                                        spec.nullable,
                                        default.as_ref(),
                                        n,
                                    )?
                                };
                                let enc = encode_column(leaf, codec, &data, true)?;
                                let id = store_chunk(t, leaf, &enc)?;
                                summary.chunks_written += 1;
                                shared.insert((rg.num_rows, leaf.leaf), id);
                                id
                            }
                        },
                        _ => {
                            let old_i = old_leaves
                                .iter()
                                .position(|l| (l.column_id, l.leaf) == key)
                                .ok_or_else(|| Error::Corrupt(format!("missing leaf {key:?}")))?;
                            let chunk = &chunks[old_i];
                            let raw = chunk
                                .pages
                                .iter()
                                .map(|p| RawPage::load(&t.blobs(), p))
                                .collect::<Result<Vec<_>>>()?;
                            let data = conform_leaf(
                                decode_pages(&old_leaves[old_i], chunk.codec, &raw)?,
                                leaf,
                                spec.is_nested(),
                            )?;
                            let c = if chunk.codec == Compression::LZO {
                                codec
                            } else {
                                chunk.codec
                            };
                            let enc = encode_column(leaf, c, &data, true)?;
                            summary.chunks_written += 1;
                            store_chunk(t, leaf, &enc)?
                        }
                    };
                    new_chunks.insert(key, id);
                }
                let ids: Vec<((i64, i64), i64)> = leaves
                    .iter()
                    .map(|l| {
                        let key = (l.column_id, l.leaf);
                        new_chunks
                            .get(&key)
                            .or_else(|| rg.chunks.get(&key))
                            .map(|id| (key, *id))
                            .ok_or_else(|| {
                                Error::Corrupt(format!(
                                    "row group {} lacks column {}",
                                    rg.rg_id, l.spec.name
                                ))
                            })
                    })
                    .collect::<Result<_>>()?;
                let new_rg = t.put_row_group(rg.num_rows, &ids)?;
                t.replace_segment(file_id, seg_row, SegmentKind::RowGroup, new_rg, sid)?;
                summary.row_groups_rewritten += 1;
            }
            t.finish_snapshot(file_id, sid)?;
            Ok(summary)
        })
    }
}

/// Applies a lossless widening to a column spec; returns the Arrow type readers will see.
fn widen_spec(c: &mut ColumnSpec, to: PhysicalType) -> Result<DataType> {
    let bad = || {
        Error::Unsupported(format!(
            "widening column '{}' from {} to {to}",
            c.name, c.physical
        ))
    };
    let ty = match (c.physical, to) {
        (PhysicalType::INT32, PhysicalType::INT64) => {
            match &c.logical {
                None => {}
                Some(LogicalType::Integer(i)) if i.is_signed => {
                    c.logical = Some(LogicalType::integer(64, true))
                }
                _ => return Err(bad()),
            }
            c.converted = match c.converted {
                ConvertedType::NONE => ConvertedType::NONE,
                ConvertedType::INT_8 | ConvertedType::INT_16 | ConvertedType::INT_32 => {
                    ConvertedType::INT_64
                }
                _ => return Err(bad()),
            };
            DataType::Int64
        }
        (PhysicalType::FLOAT, PhysicalType::DOUBLE)
            if c.logical.is_none() && c.converted == ConvertedType::NONE =>
        {
            DataType::Float64
        }
        _ => return Err(bad()),
    };
    if let Some(d) = &c.default_value {
        let v = ColumnData::flat(decode_value(d)?, None).widen(to)?;
        c.default_value = Some(encode_value(&v.values)?);
    }
    c.physical = to;
    Ok(ty)
}

/// Carries key-value metadata to a new schema version: the embedded Arrow schema is
/// rewritten to match (or dropped if it cannot be), and pandas metadata is dropped.
fn evolve_key_value(
    kv: Option<&[KeyValue]>,
    root_name: &str,
    cols: &[ColumnSpec],
    fields: Vec<FieldRef>,
) -> Result<Option<Vec<KeyValue>>> {
    let Some(kv) = kv else { return Ok(None) };
    let descr = build_descriptor(root_name, cols)?;
    let mut out = Vec::new();
    for k in kv {
        match k.key.as_str() {
            "pandas" => {}
            ARROW_SCHEMA_META_KEY => {
                let fields: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
                let new = KeyValue::new(k.key.clone(), encode_arrow_schema(&Schema::new(fields)));
                let check = parquet_to_arrow_schema(descr.as_ref(), Some(&vec![new.clone()]));
                if check.is_ok_and(|s| s.fields().len() == cols.len()) {
                    out.push(new);
                }
            }
            _ => out.push(k.clone()),
        }
    }
    Ok(Some(out))
}
