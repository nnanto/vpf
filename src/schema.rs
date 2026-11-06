//! Schema model persisted in SQLite, with stable column ids.
//!
//! A schema version is a list of top-level fields ([`ColumnSpec`]). A flat field is a single
//! primitive leaf; a nested field (group, LIST, MAP, or a top-level repeated field) keeps its
//! full Parquet type tree and maps to one or more leaf columns ([`Leaf`]).

use std::sync::Arc;

use parquet::basic::{
    ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType,
};
use parquet::file::metadata::KeyValue;
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor, Type, TypePtr};
use serde_json::{Value, json};

use crate::error::{Error, Result};

/// A top-level field. For flat fields the primitive attributes describe the single leaf;
/// for nested fields they are unused and `nested` holds the Parquet type tree.
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
    /// Type tree of a nested field. Its own name/repetition are superseded by `name` and
    /// `nullable` (unless it is REPEATED).
    pub nested: Option<TypePtr>,
}

impl ColumnSpec {
    pub fn is_nested(&self) -> bool {
        self.nested.is_some()
    }

    /// Builds a nested field spec from a Parquet type.
    pub fn nested(column_id: i64, t: TypePtr) -> Result<Self> {
        validate_nested(&t)?;
        let info = t.get_basic_info();
        Ok(Self {
            column_id,
            name: info.name().to_string(),
            physical: PhysicalType::BYTE_ARRAY,
            type_length: -1,
            nullable: info.repetition() == Repetition::OPTIONAL,
            logical: None,
            converted: ConvertedType::NONE,
            precision: -1,
            scale: -1,
            field_id: info.has_id().then(|| info.id()),
            default_value: None,
            nested: Some(t),
        })
    }

    pub fn max_def(&self) -> i16 {
        self.nullable as i16
    }

    /// True if a row may lack this field: an optional field (NULL) or a top-level repeated
    /// field (no elements).
    pub fn top_level_nullable(&self) -> bool {
        self.nullable || self.nested.as_ref().is_some_and(|t| t.get_basic_info().repetition() == Repetition::REPEATED)
    }

    pub fn repetition(&self) -> Repetition {
        if self.nullable { Repetition::OPTIONAL } else { Repetition::REQUIRED }
    }

    pub fn to_parquet_type(&self) -> Result<TypePtr> {
        if let Some(t) = &self.nested {
            let rep = match t.get_basic_info().repetition() {
                Repetition::REPEATED => Repetition::REPEATED,
                _ => self.repetition(),
            };
            return rebuild_type(t, &self.name, rep, self.field_id);
        }
        self.primitive_type(self.repetition())
    }

    fn primitive_type(&self, repetition: Repetition) -> Result<TypePtr> {
        let mut b = Type::primitive_type_builder(&self.name, self.physical)
            .with_repetition(repetition)
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

    /// Builds a spec from a top-level field of a parquet schema (flat or nested).
    pub fn from_parquet_type(column_id: i64, t: &TypePtr) -> Result<Self> {
        if t.is_group() || t.get_basic_info().repetition() == Repetition::REPEATED {
            return Self::nested(column_id, t.clone());
        }
        Self::from_primitive(column_id, t)
    }

    /// Builds a spec describing a primitive (leaf) type; REPEATED leaves count as required.
    pub fn from_primitive(column_id: i64, t: &Type) -> Result<Self> {
        let Type::PrimitiveType { basic_info, physical_type, type_length, scale, precision } = t else {
            return Err(Error::Invalid(format!("'{}' is not a primitive type", t.name())));
        };
        let nullable = basic_info.repetition() == Repetition::OPTIONAL;
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
            nested: None,
        })
    }
}

/// One leaf column of a schema version.
#[derive(Debug, Clone)]
pub struct Leaf {
    pub column_id: i64,
    /// Index of the leaf within its field (0 for flat fields).
    pub leaf: i64,
    /// Index of the field in `SchemaVersion::columns`.
    pub field: usize,
    /// Primitive description; for nested leaves the name is the dotted path.
    pub spec: ColumnSpec,
    pub max_def: i16,
    pub max_rep: i16,
}

impl Leaf {
    /// A flat field's single leaf.
    pub fn flat(spec: &ColumnSpec) -> Self {
        Leaf { column_id: spec.column_id, leaf: 0, field: 0, spec: spec.clone(), max_def: spec.max_def(), max_rep: 0 }
    }

    pub fn physical(&self) -> PhysicalType {
        self.spec.physical
    }

    /// True for a leaf whose levels are those of a flat column (at most one definition
    /// level, no repetition), i.e. one level per row.
    pub fn is_flat(&self) -> bool {
        self.max_rep == 0 && self.max_def <= 1
    }

    /// A single-leaf schema with the same physical type and level structure as this leaf,
    /// used to encode/decode its pages in isolation.
    pub fn descriptor(&self) -> Result<SchemaDescPtr> {
        if self.spec.nested.is_none() && self.max_rep == 0 && self.max_def == self.spec.max_def() {
            return build_descriptor("m", std::slice::from_ref(&self.spec));
        }
        let mut t = self.spec.primitive_type(Repetition::REQUIRED)?;
        for i in 0..self.max_def {
            let rep = if i < self.max_rep { Repetition::REPEATED } else { Repetition::OPTIONAL };
            t = Arc::new(Type::group_type_builder(&format!("l{i}")).with_repetition(rep).with_fields(vec![t]).build()?);
        }
        let root = Type::group_type_builder("m").with_fields(vec![t]).build()?;
        Ok(Arc::new(SchemaDescriptor::new(Arc::new(root))))
    }
}

/// Copies a type with a new name, repetition and field id.
pub fn rebuild_type(t: &TypePtr, name: &str, repetition: Repetition, id: Option<i32>) -> Result<TypePtr> {
    let info = t.get_basic_info();
    let out = match t.as_ref() {
        Type::GroupType { fields, .. } => Type::group_type_builder(name)
            .with_repetition(repetition)
            .with_converted_type(info.converted_type())
            .with_logical_type(info.logical_type_ref().cloned())
            .with_id(id)
            .with_fields(fields.clone())
            .build()?,
        Type::PrimitiveType { physical_type, type_length, scale, precision, .. } => {
            let mut b = Type::primitive_type_builder(name, *physical_type)
                .with_repetition(repetition)
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_id(id)
                .with_length(*type_length);
            if *precision > 0 {
                b = b.with_precision(*precision).with_scale(*scale);
            }
            b.build()?
        }
    };
    Ok(Arc::new(out))
}

fn validate_nested(t: &Type) -> Result<()> {
    if let Some(l) = t.get_basic_info().logical_type_ref() {
        logical_to_json(l)?;
    }
    match t {
        Type::GroupType { fields, .. } => {
            if fields.is_empty() {
                return Err(Error::Unsupported(format!("empty group '{}'", t.name())));
            }
            fields.iter().try_for_each(|f| validate_nested(f))
        }
        Type::PrimitiveType { .. } => Ok(()),
    }
}

fn repetition_str(r: Repetition) -> &'static str {
    match r {
        Repetition::REQUIRED => "REQUIRED",
        Repetition::OPTIONAL => "OPTIONAL",
        Repetition::REPEATED => "REPEATED",
    }
}

/// Serializes a Parquet type tree.
pub fn type_to_json(t: &Type) -> Result<Value> {
    let info = t.get_basic_info();
    let mut v = json!({
        "name": info.name(),
        "repetition": repetition_str(info.repetition()),
        "converted": info.converted_type().to_string(),
    });
    if let Some(l) = info.logical_type_ref() {
        v["logical"] = logical_to_json(l)?;
    }
    if info.has_id() {
        v["id"] = json!(info.id());
    }
    match t {
        Type::GroupType { fields, .. } => {
            v["fields"] = Value::Array(fields.iter().map(|f| type_to_json(f)).collect::<Result<_>>()?);
        }
        Type::PrimitiveType { physical_type, type_length, scale, precision, .. } => {
            v["physical"] = json!(physical_type.to_string());
            v["length"] = json!(type_length);
            v["precision"] = json!(precision);
            v["scale"] = json!(scale);
        }
    }
    Ok(v)
}

pub fn type_from_json(v: &Value) -> Result<TypePtr> {
    let bad = || Error::Corrupt(format!("bad type json {v}"));
    let s = |k: &str| v.get(k).and_then(Value::as_str).ok_or_else(bad);
    let int = |k: &str| v.get(k).and_then(Value::as_i64).map(|x| x as i32).ok_or_else(bad);
    let name = s("name")?;
    let repetition: Repetition = s("repetition")?.parse()?;
    let converted: ConvertedType = s("converted")?.parse()?;
    let logical = v.get("logical").map(logical_from_json).transpose()?;
    let id = v.get("id").and_then(Value::as_i64).map(|x| x as i32);
    let t = if let Some(fields) = v.get("fields") {
        let fields = fields.as_array().ok_or_else(bad)?.iter().map(type_from_json).collect::<Result<Vec<_>>>()?;
        Type::group_type_builder(name)
            .with_repetition(repetition)
            .with_converted_type(converted)
            .with_logical_type(logical)
            .with_id(id)
            .with_fields(fields)
            .build()?
    } else {
        let physical: PhysicalType = s("physical")?.parse()?;
        let mut b = Type::primitive_type_builder(name, physical)
            .with_repetition(repetition)
            .with_converted_type(converted)
            .with_logical_type(logical)
            .with_id(id)
            .with_length(int("length")?);
        if int("precision")? > 0 {
            b = b.with_precision(int("precision")?).with_scale(int("scale")?);
        }
        b.build()?
    };
    Ok(Arc::new(t))
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

    /// All leaf columns in schema order.
    pub fn leaves(&self) -> Result<Vec<Leaf>> {
        let d = self.descriptor()?;
        let mut out = Vec::with_capacity(d.num_columns());
        let mut per_field = vec![0i64; self.columns.len()];
        for i in 0..d.num_columns() {
            let c = d.column(i);
            let f = d.get_column_root_idx(i);
            let col = &self.columns[f];
            let spec = if col.is_nested() {
                let mut s = ColumnSpec::from_primitive(col.column_id, c.self_type())?;
                s.name = c.path().string();
                s
            } else {
                col.clone()
            };
            out.push(Leaf {
                column_id: col.column_id,
                leaf: per_field[f],
                field: f,
                spec,
                max_def: c.max_def_level(),
                max_rep: c.max_rep_level(),
            });
            per_field[f] += 1;
        }
        Ok(out)
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
        LogicalType::List => json!({"type": "LIST"}),
        LogicalType::Map => json!({"type": "MAP"}),
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
        "LIST" => LogicalType::List,
        "MAP" => LogicalType::Map,
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
        nested: None,
    };
    let lower = ty.trim().to_ascii_lowercase();
    if ["list<", "map<", "struct<"].iter().any(|p| lower.starts_with(p)) {
        return nested_spec_for_type_name(column_id, name, ty, nullable);
    }
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

/// Builds a nested column from `list<T>`, `map<K,V>` or `struct<a:T,b:T!>` (`!` marks a
/// non-null child; map keys are always non-null). Uses Arrow's standard Parquet layouts.
fn nested_spec_for_type_name(column_id: i64, name: &str, ty: &str, nullable: bool) -> Result<ColumnSpec> {
    let mut p = TypeParser { s: ty, pos: 0 };
    let dt = p.ty()?;
    p.skip_ws();
    if p.pos != ty.len() {
        return Err(Error::Invalid(format!("trailing input in type '{ty}' at {}", p.pos)));
    }
    let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(name, dt, nullable)]);
    let tree = parquet::arrow::ArrowSchemaConverter::new().convert(&schema)?;
    ColumnSpec::from_parquet_type(column_id, &tree.root_schema().get_fields()[0])
}

struct TypeParser<'a> {
    s: &'a str,
    pos: usize,
}

impl TypeParser<'_> {
    fn skip_ws(&mut self) {
        while self.s[self.pos..].starts_with(char::is_whitespace) {
            self.pos += 1;
        }
    }

    fn err(&self, what: &str) -> Error {
        Error::Invalid(format!("bad type '{}': expected {what} at {}", self.s, self.pos))
    }

    fn eat(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.s[self.pos..].starts_with(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, c: char) -> Result<()> {
        if self.eat(c) { Ok(()) } else { Err(self.err(&format!("'{c}'"))) }
    }

    fn ident(&mut self) -> Result<&str> {
        self.skip_ws();
        let start = self.pos;
        while self.s[self.pos..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
            self.pos += 1;
        }
        if start == self.pos { Err(self.err("a name")) } else { Ok(&self.s[start..self.pos]) }
    }

    /// A type followed by an optional `!` (non-null marker).
    fn child(&mut self) -> Result<(arrow::datatypes::DataType, bool)> {
        let t = self.ty()?;
        Ok((t, !self.eat('!')))
    }

    fn ty(&mut self) -> Result<arrow::datatypes::DataType> {
        use arrow::datatypes::{DataType, Field, Fields, TimeUnit as AU};
        let id = self.ident()?.to_ascii_lowercase();
        Ok(match id.as_str() {
            "list" => {
                self.expect('<')?;
                let (t, n) = self.child()?;
                self.expect('>')?;
                DataType::List(Arc::new(Field::new("element", t, n)))
            }
            "map" => {
                self.expect('<')?;
                let k = self.ty()?;
                self.eat('!');
                self.expect(',')?;
                let (v, n) = self.child()?;
                self.expect('>')?;
                let entries = Fields::from(vec![Field::new("key", k, false), Field::new("value", v, n)]);
                DataType::Map(Arc::new(Field::new("key_value", DataType::Struct(entries), false)), false)
            }
            "struct" => {
                self.expect('<')?;
                let mut fields = Vec::new();
                loop {
                    let name = self.ident()?.to_string();
                    self.expect(':')?;
                    let (t, n) = self.child()?;
                    fields.push(Field::new(name, t, n));
                    if !self.eat(',') {
                        break;
                    }
                }
                self.expect('>')?;
                DataType::Struct(Fields::from(fields))
            }
            "bool" | "boolean" => DataType::Boolean,
            "int32" | "int" => DataType::Int32,
            "int64" | "bigint" | "long" => DataType::Int64,
            "float" | "float32" => DataType::Float32,
            "double" | "float64" => DataType::Float64,
            "string" | "utf8" => DataType::Utf8,
            "binary" | "bytes" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp_ms" => DataType::Timestamp(AU::Millisecond, Some("UTC".into())),
            "timestamp_us" | "timestamp" => DataType::Timestamp(AU::Microsecond, Some("UTC".into())),
            "timestamp_ns" => DataType::Timestamp(AU::Nanosecond, Some("UTC".into())),
            "decimal" => {
                self.expect('(')?;
                let p = self.ident()?.parse::<u8>().map_err(|_| self.err("precision"))?;
                self.expect(',')?;
                let sc = self.ident()?.parse::<i8>().map_err(|_| self.err("scale"))?;
                self.expect(')')?;
                if !(1..=38).contains(&p) || sc < 0 || sc as u8 > p {
                    return Err(Error::Invalid(format!("bad decimal precision/scale in '{}'", self.s)));
                }
                DataType::Decimal128(p, sc)
            }
            other => return Err(Error::Invalid(format!("unknown type '{other}' in '{}'", self.s))),
        })
    }
}

/// Splits `name:type[:required|:optional]`, where `type` may contain ':' inside `<...>`.
pub fn split_column_def(def: &str) -> Result<(&str, &str, bool)> {
    let bad = || Error::Invalid(format!("column must be name:type[:required|:optional]: '{def}'"));
    let (name, rest) = def.split_once(':').ok_or_else(bad)?;
    let mut depth = 0i32;
    let mut last_top = None;
    for (i, c) in rest.char_indices() {
        match c {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ':' if depth == 0 => last_top = Some(i),
            _ => {}
        }
    }
    let (ty, required) = match last_top {
        None => (rest, false),
        Some(i) => match &rest[i + 1..] {
            "required" => (&rest[..i], true),
            "optional" => (&rest[..i], false),
            m => return Err(Error::Invalid(format!("bad column modifier '{m}' in '{def}'"))),
        },
    };
    if name.is_empty() || ty.is_empty() {
        return Err(bad());
    }
    Ok((name, ty, required))
}

/// Minimum bytes for a two's-complement decimal of the given precision.
pub fn decimal_flba_len(precision: i32) -> i32 {
    let mut len = 1;
    while 10f64.powi(precision) > 2f64.powi(8 * len - 1) {
        len += 1;
    }
    len
}
