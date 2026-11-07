//! Conservative predicate pruning using VPF-computed flat-column page statistics.

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::Type as PhysicalType;

use crate::error::Result;
use crate::predicate::{CmpOp, Expr, Operand, literal_array};
use crate::schema::SchemaVersion;
use crate::stats::{PageStats, StatOrder, StatType};

pub(super) enum PruningExpr {
    Unknown,
    Const(bool),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Compare {
        column: usize,
        op: CmpOp,
        value: Vec<u8>,
        ty: StatType,
    },
    IsNull {
        column: usize,
        negated: bool,
    },
}

impl PruningExpr {
    pub(super) fn compile(expr: &Expr, schema: &SchemaVersion, arrow: &Schema) -> Result<Self> {
        // Validate every branch before pruning can hide an invalid predicate.
        expr.matches(&RecordBatch::new_empty(std::sync::Arc::new(arrow.clone())))?;
        Self::build(expr, schema, arrow, false)
    }

    fn build(expr: &Expr, schema: &SchemaVersion, arrow: &Schema, negated: bool) -> Result<Self> {
        let leaves = schema.leaves()?;
        let flat_column = |name: &str| {
            let field = schema
                .columns
                .iter()
                .position(|c| c.name == name && !c.is_nested())?;
            let column = leaves.iter().position(|l| l.field == field)?;
            Some((field, column))
        };
        Ok(match expr {
            Expr::Const(v) => Self::Const(*v ^ negated),
            Expr::Not(e) => Self::build(e, schema, arrow, !negated)?,
            Expr::And(a, b) | Expr::Or(a, b) => {
                let a = Box::new(Self::build(a, schema, arrow, negated)?);
                let b = Box::new(Self::build(b, schema, arrow, negated)?);
                if matches!(expr, Expr::And(..)) ^ negated {
                    Self::And(a, b)
                } else {
                    Self::Or(a, b)
                }
            }
            Expr::IsNull(Operand::Column(name), not) => match flat_column(name) {
                Some((_, column)) => Self::IsNull {
                    column,
                    negated: *not ^ negated,
                },
                None => Self::Unknown,
            },
            Expr::Cmp(a, op, b) => {
                let (name, lit, op) = match (a, b) {
                    (Operand::Column(name), Operand::Lit(lit)) => (name, lit, *op),
                    (Operand::Lit(lit), Operand::Column(name)) => (name, lit, flip(*op)),
                    _ => return Ok(Self::Unknown),
                };
                let Some((field, column)) = flat_column(name) else {
                    return Ok(Self::Unknown);
                };
                let mut data_type = arrow.field(field).data_type();
                if let DataType::Dictionary(_, value) = data_type {
                    data_type = value;
                }
                let literal = literal_array(lit, data_type)?;
                if literal.is_null(0) {
                    return Ok(Self::Const(false));
                }
                let spec = &schema.columns[field];
                let ty = StatType::new(spec.physical, spec.logical.as_ref(), spec.converted);
                // Floating-point/decimal ordering and legacy Arrow mappings need a
                // separate proof; keep scanning them rather than risk false negatives.
                let compatible = match spec.physical {
                    PhysicalType::BOOLEAN => matches!(data_type, DataType::Boolean),
                    PhysicalType::INT32 => matches!(
                        data_type,
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::Date32
                            | DataType::Time32(_)
                    ),
                    PhysicalType::INT64 => matches!(
                        data_type,
                        DataType::Int64
                            | DataType::UInt64
                            | DataType::Time64(_)
                            | DataType::Timestamp(_, _)
                            | DataType::Duration(_)
                    ),
                    PhysicalType::BYTE_ARRAY => matches!(
                        data_type,
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Utf8View
                            | DataType::Binary
                            | DataType::LargeBinary
                            | DataType::BinaryView
                    ),
                    _ => false,
                };
                if !compatible || ty.order == StatOrder::Undefined {
                    return Ok(Self::Unknown);
                }
                let generated =
                    ArrowSchemaConverter::new().convert(&Schema::new(vec![Field::new(
                        &spec.name,
                        data_type.clone(),
                        spec.nullable,
                    )]))?;
                let generated = generated.column(0);
                if generated.physical_type() != spec.physical
                    || generated.logical_type_ref() != spec.logical.as_ref()
                    || generated.converted_type() != spec.converted
                {
                    return Ok(Self::Unknown);
                }
                let Some(stats) = PageStats::compute_arrow(&ty, literal.as_ref(), 0)? else {
                    return Ok(Self::Unknown);
                };
                match (stats.min, stats.max) {
                    (Some(value), Some(max))
                        if stats.min_exact && stats.max_exact && value == max =>
                    {
                        Self::Compare {
                            column,
                            op: if negated { negate(op) } else { op },
                            value,
                            ty,
                        }
                    }
                    _ => Self::Unknown,
                }
            }
            Expr::In(operand, items, not) => {
                let negated = negated ^ *not;
                let mut out = Self::Const(negated);
                for item in items {
                    let e = Expr::Cmp(operand.clone(), CmpOp::Eq, Operand::Lit(item.clone()));
                    let next = Self::build(&e, schema, arrow, negated)?;
                    out = if negated {
                        Self::And(Box::new(out), Box::new(next))
                    } else {
                        Self::Or(Box::new(out), Box::new(next))
                    };
                }
                out
            }
            _ => Self::Unknown,
        })
    }

    pub(super) fn columns(&self, out: &mut Vec<usize>) {
        match self {
            Self::Compare { column, .. } | Self::IsNull { column, .. } => out.push(*column),
            Self::And(a, b) | Self::Or(a, b) => {
                a.columns(out);
                b.columns(out);
            }
            _ => {}
        }
    }

    pub(super) fn may_match<'a>(
        &self,
        page: &impl Fn(usize) -> Option<(&'a PageStats, u64)>,
    ) -> bool {
        match self {
            Self::Unknown => true,
            Self::Const(v) => *v,
            Self::And(a, b) => a.may_match(page) && b.may_match(page),
            Self::Or(a, b) => a.may_match(page) || b.may_match(page),
            Self::IsNull { column, negated } => page(*column).is_none_or(|(s, rows)| {
                if *negated {
                    s.null_count < rows
                } else {
                    s.null_count > 0
                }
            }),
            Self::Compare {
                column,
                op,
                value,
                ty,
            } => page(*column).is_none_or(|(s, rows)| {
                if s.null_count == rows {
                    return false;
                }
                let (Some(min), Some(max)) = (&s.min, &s.max) else {
                    return true;
                };
                let lo = ty.cmp(min, value);
                let hi = ty.cmp(max, value);
                match op {
                    CmpOp::Eq => !lo.is_gt() && !hi.is_lt(),
                    CmpOp::Ne => !(lo.is_eq() && hi.is_eq() && s.min_exact && s.max_exact),
                    CmpOp::Lt => lo.is_lt(),
                    CmpOp::Le => !lo.is_gt(),
                    CmpOp::Gt => hi.is_gt(),
                    CmpOp::Ge => !hi.is_lt(),
                }
            }),
        }
    }
}

fn flip(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        other => other,
    }
}

fn negate(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Eq => CmpOp::Ne,
        CmpOp::Ne => CmpOp::Eq,
        CmpOp::Lt => CmpOp::Ge,
        CmpOp::Le => CmpOp::Gt,
        CmpOp::Gt => CmpOp::Le,
        CmpOp::Ge => CmpOp::Lt,
    }
}
