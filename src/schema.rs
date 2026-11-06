//! Flat schema model persisted in SQLite, with stable column ids.

use std::sync::Arc;

use parquet::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType,
};
use parquet::file::metadata::KeyValue;
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor, Type, TypePtr};
use serde_json::{Value, json};

use crate::error::{Error, Result};

/// A leaf column of a flat schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    /// Stable identity, preserved across renames/reorders; never reused within a file.
    pub column_id: i64,
    pub name: String,
    pub physical: PhysicalType,
    pub type_length: i32,
    pub nullable: bool,
    pub logical: Option<LogicalType>,
    pub converted: ConvertedType,
    pub precision: i32,
    pub scale: i32,
    pub field_id: Option<i32>,
    /// Encoded single value (see `values::encode_default`) used to fill rows that predate
    /// the column. `None` means NULL.
    pub default_value: Option<Vec<u8>>,
}

impl ColumnSpec {
    pub fn max_def(&self) -> i16 {
        self.nullable as i16
    }

    pub fn repetition(&self) -> Repetition {
        if self.nullable { Repetition::OPTIONAL } else { Repetition::REQUIRED }
    }

    pub fn to_parquet_type(&self) -> Result<TypePtr> {
        let mut b = Type::primitive_type_builder(&self.name, self.physical)
            .with_repetition(self.repetition())
            .with_converted_type(self.converted)
            .with_logical_type(self.logical.clone())
            .with_id(self.field_id);
        if self.physical == PhysicalType::FIXED_LEN_BYTE_ARRAY {
            b = b.with_length(self.type_length);
        }
        if self.converted == ConvertedType::DECIMAL || matches!(self.logical, Some(LogicalType::Decimal(_))) {
            b = b.with_precision(self.precision).with_scale(self.scale);
        }
        Ok(Arc::new(b.build()?))
    }

    /// Builds a spec from a primitive leaf of a parquet schema; rejects nested/repeated fields.
    pub fn from_parquet_type(column_id: i64, t: &Type) -> Result<Self> {
        let Type::PrimitiveType { basic_info, physical_type, type_length, scale, precision } = t else {
            return Err(Error::Unsupported(format!(
                "nested column '{}' (only flat schemas are supported)",
                t.name()
            )));
        };
        let nullable = match basic_info.repetition() {
            Repetition::REQUIRED => false,
            Repetition::OPTIONAL => true,
            Repetition::REPEATED => {
                return Err(Error::Unsupported(format!("repeated column '{}'", t.name())));
            }
        };
        let logical = basic_info.logical_type_ref().cloned();
        if let Some(l) = &logical {
            logical_to_json(l)?; // validates support
        }
        Ok(Self {
            column_id,
            name: basic_info.name().to_string(),
            physical: *physical_type,
            type_length: *type_length,
            nullable,
            logical,
            converted: basic_info.converted_type(),
            precision: *precision,
            scale: *scale,
            field_id: basic_info.has_id().then(|| basic_info.id()),
            default_value: None,
        })
    }
}

/// An immutable schema version of a virtual file.
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    pub schema_version_id: i64,
    pub root_name: String,
    pub columns: Vec<ColumnSpec>,
    pub key_value: Option<Vec<KeyValue>>,
}

impl SchemaVersion {
    pub fn descriptor(&self) -> Result<SchemaDescPtr> {
        build_descriptor(&self.root_name, &self.columns)
    }

    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSpec> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| Error::NotFound(format!("column '{name}'")))
    }
}

pub fn build_descriptor(root_name: &str, columns: &[ColumnSpec]) -> Result<SchemaDescPtr> {
    let fields = columns.iter().map(|c| c.to_parquet_type()).collect::<Result<Vec<_>>>()?;
    let root = Type::group_type_builder(root_name).with_fields(fields).build()?;
    Ok(Arc::new(SchemaDescriptor::new(Arc::new(root))))
}

fn unit_str(u: &TimeUnit) -> &'static str {
    match u {
        TimeUnit::MILLIS => "MILLIS",
        TimeUnit::MICROS => "MICROS",
        TimeUnit::NANOS => "NANOS",
    }
}

fn unit_from(s: &str) -> Result<TimeUnit> {
    Ok(match s {
        "MILLIS" => TimeUnit::MILLIS,
        "MICROS" => TimeUnit::MICROS,
        "NANOS" => TimeUnit::NANOS,
        _ => return Err(Error::Corrupt(format!("bad time unit {s}"))),
    })
}

pub fn logical_to_json(l: &LogicalType) -> Result<Value> {
    Ok(match l {
        LogicalType::String => json!({"type": "STRING"}),
        LogicalType::Enum => json!({"type": "ENUM"}),
        LogicalType::Date => json!({"type": "DATE"}),
        LogicalType::Unknown => json!({"type": "UNKNOWN"}),
        LogicalType::Json => json!({"type": "JSON"}),
        LogicalType::Bson => json!({"type": "BSON"}),
        LogicalType::Uuid => json!({"type": "UUID"}),
        LogicalType::Float16 => json!({"type": "FLOAT16"}),
        LogicalType::Decimal(d) => json!({"type": "DECIMAL", "scale": d.scale, "precision": d.precision}),
        LogicalType::Time(t) => json!({"type": "TIME", "utc": t.is_adjusted_to_u_t_c, "unit": unit_str(&t.unit)}),
        LogicalType::Timestamp(t) => {
            json!({"type": "TIMESTAMP", "utc": t.is_adjusted_to_u_t_c, "unit": unit_str(&t.unit)})
        }
        LogicalType::Integer(i) => json!({"type": "INTEGER", "bits": i.bit_width, "signed": i.is_signed}),
        other => return Err(Error::Unsupported(format!("logical type {other:?}"))),
    })
}

pub fn logical_from_json(v: &Value) -> Result<LogicalType> {
    let bad = || Error::Corrupt(format!("bad logical type json {v}"));
    let ty = v.get("type").and_then(Value::as_str).ok_or_else(bad)?;
    let int = |k: &str| v.get(k).and_then(Value::as_i64).ok_or_else(bad);
    let boolean = |k: &str| v.get(k).and_then(Value::as_bool).ok_or_else(bad);
    let unit = || unit_from(v.get("unit").and_then(Value::as_str).ok_or_else(bad)?);
    Ok(match ty {
        "STRING" => LogicalType::String,
        "ENUM" => LogicalType::Enum,
        "DATE" => LogicalType::Date,
        "UNKNOWN" => LogicalType::Unknown,
        "JSON" => LogicalType::Json,
        "BSON" => LogicalType::Bson,
        "UUID" => LogicalType::Uuid,
        "FLOAT16" => LogicalType::Float16,
        "DECIMAL" => LogicalType::decimal(int("scale")? as i32, int("precision")? as i32),
        "TIME" => LogicalType::time(boolean("utc")?, unit()?),
        "TIMESTAMP" => LogicalType::timestamp(boolean("utc")?, unit()?),
        "INTEGER" => LogicalType::integer(int("bits")? as i8, boolean("signed")?),
        _ => return Err(bad()),
    })
}

/// User-facing column type names for `add column`.
pub fn spec_for_type_name(column_id: i64, name: &str, ty: &str, nullable: bool) -> Result<ColumnSpec> {
    let mut spec = ColumnSpec {
        column_id,
        name: name.to_string(),
        physical: PhysicalType::INT32,
        type_length: -1,
        nullable,
        logical: None,
        converted: ConvertedType::NONE,
        precision: -1,
        scale: -1,
        field_id: None,
        default_value: None,
    };
    let lower = ty.trim().to_ascii_lowercase();
    let (physical, logical) = match lower.as_str() {
        "bool" | "boolean" => (PhysicalType::BOOLEAN, None),
        "int32" | "int" => (PhysicalType::INT32, None),
        "int64" | "bigint" | "long" => (PhysicalType::INT64, None),
        "float" | "float32" => (PhysicalType::FLOAT, None),
        "double" | "float64" => (PhysicalType::DOUBLE, None),
        "string" | "utf8" => (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)),
        "binary" | "bytes" => (PhysicalType::BYTE_ARRAY, None),
        "date" => (PhysicalType::INT32, Some(LogicalType::Date)),
        "timestamp_ms" => (PhysicalType::INT64, Some(LogicalType::timestamp(true, TimeUnit::MILLIS))),
        "timestamp_us" | "timestamp" => {
            (PhysicalType::INT64, Some(LogicalType::timestamp(true, TimeUnit::MICROS)))
        }
        "timestamp_ns" => (PhysicalType::INT64, Some(LogicalType::timestamp(true, TimeUnit::NANOS))),
        s if s.starts_with("decimal(") && s.ends_with(')') => {
            let inner = &s[8..s.len() - 1];
            let mut parts = inner.split(',').map(|p| p.trim().parse::<i32>());
            let (Some(Ok(p)), Some(Ok(sc)), None) = (parts.next(), parts.next(), parts.next()) else {
                return Err(Error::Invalid(format!("bad decimal type '{ty}'")));
            };
            if !(1..=38).contains(&p) || sc < 0 || sc > p {
                return Err(Error::Invalid(format!("bad decimal precision/scale '{ty}'")));
            }
            spec.precision = p;
            spec.scale = sc;
            spec.converted = ConvertedType::DECIMAL;
            spec.type_length = decimal_flba_len(p);
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, Some(LogicalType::decimal(sc, p)))
        }
        _ => return Err(Error::Invalid(format!("unknown column type '{ty}'"))),
    };
    spec.physical = physical;
    if spec.converted == ConvertedType::NONE {
        spec.converted = ConvertedType::from(logical.clone());
    }
    spec.logical = logical;
    Ok(spec)
}

/// Minimum bytes for a two's-complement decimal of the given precision.
pub fn decimal_flba_len(precision: i32) -> i32 {
    let mut len = 1;
    while 10f64.powi(precision) > 2f64.powi(8 * len - 1) {
        len += 1;
    }
    len
}
