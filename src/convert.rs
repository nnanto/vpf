//! Converting Arrow record batches into physical column values that conform to a
//! virtual file's schema, and conforming stored values to newer schema versions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type,
    DurationSecondType, DurationMillisecondType, DurationMicrosecondType, DurationNanosecondType,
    Field, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Schema, TimeUnit, Time32SecondType, Time32MillisecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType,
    TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter, parquet_to_arrow_schema};
use parquet::basic::{Compression, ConvertedType, LogicalType, Type as PhysicalType};
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

use crate::codec::{read_column, read_leaf};
use crate::error::{Error, Result};
use crate::schema::{ColumnSpec, Leaf, SchemaVersion, build_descriptor};
use crate::values::{ColumnData, LeafKey, Values, decode_value};

/// The Arrow schema readers see for a schema version.
pub fn arrow_schema(schema: &SchemaVersion) -> Result<Schema> {
    let descr = schema.descriptor()?;
    Ok(parquet_to_arrow_schema(&descr, schema.key_value.as_ref())?)
}

/// The column's default fill value (`None` = NULL).
pub fn default_values(spec: &ColumnSpec) -> Result<Option<Values>> {
    spec.default_value.as_deref().map(decode_value).transpose()
}

/// Converts a record batch into one [`ColumnData`] per leaf column (leaf order).
/// Columns are matched by name; schema columns absent from the batch receive their default
/// (or NULL / empty for nested fields). Unknown batch columns, lossy casts and NULLs in
/// required columns are errors.
pub fn batch_to_columns(schema: &SchemaVersion, batch: &RecordBatch) -> Result<Vec<ColumnData>> {
    let target = arrow_schema(schema)?;
    if target.fields().len() != schema.columns.len() {
        return Err(Error::Corrupt("arrow schema does not match column count".into()));
    }
    validate_batch_columns(schema, batch)?;
    let n = batch.num_rows();
    let leaves = schema.leaves()?;
    let mut out = Vec::with_capacity(leaves.len());
    for (fi, (spec, field)) in schema.columns.iter().zip(target.fields()).enumerate() {
        let field_leaves: Vec<&Leaf> = leaves.iter().filter(|l| l.field == fi).collect();
        let missing = || Error::Invalid(format!("missing value for required column '{}'", spec.name));
        let cols = match (batch.column_by_name(&spec.name), spec.is_nested()) {
            (Some(arr), false) => vec![array_to_column(spec, field.data_type(), arr)?],
            (Some(arr), true) => nested_to_leaves(spec, field, arr, &field_leaves)?,
            (None, false) => vec![
                ColumnData::repeat(spec.physical, spec.nullable, default_values(spec)?.as_ref(), n)
                    .map_err(|_| missing())?,
            ],
            (None, true) => field_leaves
                .iter()
                .map(|l| missing_nested(spec, l, n))
                .collect::<Result<_>>()
                .map_err(|_| missing())?,
        };
        for c in &cols {
            if c.num_rows() != n {
                return Err(Error::Corrupt(format!("column '{}' converted to {} rows", spec.name, c.num_rows())));
            }
        }
        out.extend(cols);
    }
    Ok(out)
}

fn validate_batch_columns(schema: &SchemaVersion, batch: &RecordBatch) -> Result<()> {
    let bs = batch.schema();
    for (i, f) in bs.fields().iter().enumerate() {
        if schema.column_by_name(f.name()).is_none() {
            return Err(Error::Invalid(format!("unknown column '{}'", f.name())));
        }
        if bs.fields().iter().skip(i + 1).any(|g| g.name() == f.name()) {
            return Err(Error::Invalid(format!("duplicate column '{}'", f.name())));
        }
    }
    Ok(())
}

/// Conforms flat batches that the Arrow writer can encode with exactly the stored type
/// tree. Other layouts retain the physical-column compatibility path.
pub(crate) fn batch_for_arrow_append(schema: &SchemaVersion, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
    if schema.columns.iter().any(|c| c.is_nested() || c.physical == PhysicalType::INT96) {
        return Ok(None);
    }
    let target = Arc::new(arrow_schema(schema)?);
    if target.fields().iter().any(|f| !matches!(f.data_type(),
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 |
        DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16 |
        DataType::UInt32 | DataType::UInt64 | DataType::Float16 | DataType::Float32 |
        DataType::Float64 | DataType::Date32 | DataType::Date64 | DataType::Time32(_) |
        DataType::Time64(_) | DataType::Timestamp(_, _) | DataType::Duration(_) |
        DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) |
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Binary |
        DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_)
    )) {
        return Ok(None);
    }
    let stored = schema.descriptor()?;
    let generated = ArrowSchemaConverter::new().convert(&target)?;
    if generated.root_schema().get_fields() != stored.root_schema().get_fields() {
        return Ok(None);
    }
    // Physical defaults can have legacy representations; leave their filling to the
    // compatibility path rather than inventing a second physical-to-Arrow converter.
    if schema.columns.iter().any(|c| c.default_value.is_some() && batch.column_by_name(&c.name).is_none()) {
        return Ok(None);
    }
    validate_batch_columns(schema, batch)?;
    let arrays = schema.columns.iter().zip(target.fields()).map(|(spec, field)| {
        let ctx = |e| Error::Invalid(format!("column '{}': {e}", spec.name));
        let arr = match batch.column_by_name(&spec.name) {
            Some(arr) => cast_with_options(arr, field.data_type(), &cast_opts()).map_err(ctx)?,
            None if spec.nullable => arrow::array::new_null_array(field.data_type(), batch.num_rows()),
            None => return Err(Error::Invalid(format!("missing value for required column '{}'", spec.name))),
        };
        if !spec.nullable && arr.logical_null_count() != 0 {
            return Err(Error::Invalid(format!("NULL in required column '{}'", spec.name)));
        }
        Ok(arr)
    }).collect::<Result<Vec<_>>>()?;
    for array in &arrays {
        let valid = match array.data_type() {
            DataType::Decimal32(p, _) => {
                let limit = 10i128.pow(*p as u32);
                array.as_primitive::<Decimal32Type>().iter().flatten().all(|v| (v as i128) > -limit && (v as i128) < limit)
            }
            DataType::Decimal64(p, _) => {
                let limit = 10i128.pow(*p as u32);
                array.as_primitive::<Decimal64Type>().iter().flatten().all(|v| (v as i128) > -limit && (v as i128) < limit)
            }
            DataType::Decimal128(p, _) => {
                let limit = 10i128.pow(*p as u32);
                array.as_primitive::<Decimal128Type>().iter().flatten().all(|v| v > -limit && v < limit)
            }
            _ => true,
        };
        // Arrow arrays may carry values outside their declared decimal precision.
        // Keep the existing physical-coercion semantics for such arrays.
        if !valid {
            return Ok(None);
        }
    }
    Ok(Some(RecordBatch::try_new(target, arrays)?))
}

/// Fill for an absent nested field: NULL (optional) or empty (repeated) in every row.
fn missing_nested(spec: &ColumnSpec, leaf: &Leaf, n: usize) -> Result<ColumnData> {
    if !spec.top_level_nullable() {
        return Err(Error::Invalid(format!("missing value for required column '{}'", spec.name)));
    }
    ColumnData::nulls(leaf.physical(), leaf.max_def, leaf.max_rep, n)
}

/// Shreds one nested Arrow array into the leaf columns of `spec`, using the Arrow writer with
/// the stored Parquet type so levels match the stored layout exactly. The written data is
/// read back through the Arrow reader and compared, rejecting layouts the writer cannot
/// produce faithfully.
fn nested_to_leaves(spec: &ColumnSpec, field: &Field, arr: &ArrayRef, leaves: &[&Leaf]) -> Result<Vec<ColumnData>> {
    let ctx = |e: Error| Error::Invalid(format!("column '{}': {e}", spec.name));
    let arr = cast_with_options(arr, field.data_type(), &cast_opts()).map_err(|e| ctx(e.into()))?;
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let rb = RecordBatch::try_new(schema.clone(), vec![arr.clone()]).map_err(|e| ctx(e.into()))?;
    let descr = build_descriptor("m", std::slice::from_ref(spec))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let opts = ArrowWriterOptions::new()
        .with_properties(props)
        .with_parquet_schema(parquet::schema::types::SchemaDescriptor::new(descr.root_schema_ptr()));
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new_with_options(&mut buf, schema, opts).map_err(|e| ctx(e.into()))?;
    w.write(&rb).map_err(|e| ctx(e.into()))?;
    w.close().map_err(|e| ctx(e.into()))?;
    let bytes = Bytes::from(buf);

    let back = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.build()?.collect::<Result<Vec<_>, _>>()?;
    let back: Vec<ArrayRef> = back.iter().map(|b| b.column(0).clone()).collect();
    let back = arrow::compute::concat(&back.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
    let back = cast_with_options(&back, field.data_type(), &cast_opts()).map_err(|e| ctx(e.into()))?;
    if back.to_data() != arr.to_data() {
        return Err(Error::Unsupported(format!(
            "column '{}': its stored nested layout cannot be written from Arrow data faithfully",
            spec.name
        )));
    }

    let reader = SerializedFileReader::new(bytes)?;
    if reader.metadata().file_metadata().schema_descr().num_columns() != leaves.len() {
        return Err(Error::Corrupt("nested write produced an unexpected leaf count".into()));
    }
    let mut out: Vec<ColumnData> =
        leaves.iter().map(|l| ColumnData::empty_leaf(l.physical(), l.max_def, l.max_rep)).collect();
    for i in 0..reader.num_row_groups() {
        let rg = reader.get_row_group(i)?;
        let rows = rg.metadata().num_rows() as usize;
        for (j, l) in leaves.iter().enumerate() {
            let c = read_leaf(rg.get_column_reader(j)?, l.physical(), l.max_def, l.max_rep, Some(rows))?;
            out[j].append(&c)?;
        }
    }
    Ok(out)
}

fn cast_opts() -> CastOptions<'static> {
    CastOptions { safe: false, ..Default::default() }
}

/// Converts one Arrow array to the physical representation of `spec`.
pub fn array_to_column(spec: &ColumnSpec, target: &DataType, arr: &ArrayRef) -> Result<ColumnData> {
    let ctx = |e: Error| Error::Invalid(format!("column '{}': {e}", spec.name));
    let arr = cast_with_options(arr, target, &cast_opts()).map_err(|e| ctx(e.into()))?;
    let data = if spec.physical == PhysicalType::INT96 {
        let tz = match target {
            DataType::Timestamp(_, tz) => tz.clone(),
            _ => None,
        };
        let ns = cast_with_options(&arr, &DataType::Timestamp(TimeUnit::Nanosecond, tz), &cast_opts())
            .map_err(|e| ctx(e.into()))?;
        let p = ns.as_primitive::<TimestampNanosecondType>();
        ColumnData::flat(
            Values::I96(p.iter().flatten().map(nanos_to_int96).collect()),
            Some(p.iter().map(|v| v.is_some() as i16).collect()),
        )
    } else {
        let written = match direct_physical(&arr).map_err(ctx)? {
            Some(data) => data,
            None => roundtrip_physical(&arr).map_err(ctx)?,
        };
        coerce_physical(written, spec).map_err(ctx)?
    };
    data.with_nullability(spec.nullable).map_err(ctx)
}

fn direct_physical(arr: &ArrayRef) -> Result<Option<ColumnData>> {
    macro_rules! primitive {
        ($ty:ty, $variant:ident, $convert:expr) => {
            Values::$variant(arr.as_primitive::<$ty>().iter().flatten().map($convert).collect())
        };
    }
    macro_rules! byte_array {
        ($array:expr) => {{
            let array = $array;
            let bytes = Bytes::from_owner(array.values().clone());
            let offsets = array.value_offsets();
            Values::Bytes(
                (0..array.len())
                    .filter(|&i| array.is_valid(i))
                    .map(|i| bytes.slice(offsets[i] as usize..offsets[i + 1] as usize).into())
                    .collect(),
            )
        }};
    }
    let values = match arr.data_type() {
        DataType::Null => return Ok(Some(ColumnData::flat(Values::I32(Vec::new()), Some(vec![0; arr.len()])))),
        DataType::Boolean => Values::Bool(arr.as_boolean().iter().flatten().collect()),
        DataType::Int8 => primitive!(Int8Type, I32, |v| v as i32),
        DataType::Int16 => primitive!(Int16Type, I32, |v| v as i32),
        DataType::Int32 => primitive!(Int32Type, I32, |v| v),
        DataType::Int64 => primitive!(Int64Type, I64, |v| v),
        DataType::UInt8 => primitive!(UInt8Type, I32, |v| v as i32),
        DataType::UInt16 => primitive!(UInt16Type, I32, |v| v as i32),
        // Parquet stores unsigned integers using the same bits in signed physical values.
        DataType::UInt32 => primitive!(UInt32Type, I32, |v| v as i32),
        DataType::UInt64 => primitive!(UInt64Type, I64, |v| v as i64),
        DataType::Float16 => primitive!(Float16Type, Fixed, |v| v.to_le_bytes().to_vec().into()),
        DataType::Float32 => primitive!(Float32Type, F32, |v| v),
        DataType::Float64 => primitive!(Float64Type, F64, |v| v),
        DataType::Date32 => primitive!(Date32Type, I32, |v| v),
        DataType::Date64 => primitive!(Date64Type, I64, |v| v),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType, I32, |v| v),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType, I32, |v| v),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondType, I64, |v| v),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType, I64, |v| v),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType, I64, |v| v),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType, I64, |v| v),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType, I64, |v| v),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType, I64, |v| v),
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType, I64, |v| v),
        DataType::Duration(TimeUnit::Millisecond) => primitive!(DurationMillisecondType, I64, |v| v),
        DataType::Duration(TimeUnit::Microsecond) => primitive!(DurationMicrosecondType, I64, |v| v),
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondType, I64, |v| v),
        DataType::Decimal32(_, _) => primitive!(Decimal32Type, I32, |v| v),
        DataType::Decimal64(precision, _) if *precision <= 9 => primitive!(Decimal64Type, I32, |v| v as i32),
        DataType::Decimal64(_, _) => primitive!(Decimal64Type, I64, |v| v),
        DataType::Decimal128(precision, _) if *precision <= 9 => primitive!(Decimal128Type, I32, |v| v as i32),
        DataType::Decimal128(precision, _) if *precision <= 18 => primitive!(Decimal128Type, I64, |v| v as i64),
        DataType::Decimal128(_, _) => {
            let schema = Schema::new(vec![Field::new("c", arr.data_type().clone(), true)]);
            let descriptor = ArrowSchemaConverter::new().convert(&schema)?;
            let len = descriptor.column(0).type_length() as usize;
            primitive!(Decimal128Type, Fixed, |v: i128| v.to_be_bytes()[16 - len..].to_vec().into())
        }
        DataType::Utf8 => byte_array!(arr.as_string::<i32>()),
        DataType::LargeUtf8 => byte_array!(arr.as_string::<i64>()),
        DataType::Utf8View => Values::Bytes(
            arr.as_string_view().iter().flatten().map(|v| v.as_bytes().to_vec().into()).collect(),
        ),
        DataType::Binary => byte_array!(arr.as_binary::<i32>()),
        DataType::LargeBinary => byte_array!(arr.as_binary::<i64>()),
        DataType::BinaryView => Values::Bytes(
            arr.as_binary_view().iter().flatten().map(|v| v.to_vec().into()).collect(),
        ),
        DataType::FixedSizeBinary(_) => {
            let array = arr.as_fixed_size_binary();
            let bytes = Bytes::from_owner(array.values().clone());
            let width = array.value_size();
            Values::Fixed(
                (0..array.len())
                    .filter(|&i| array.is_valid(i))
                    .map(|i| ByteArray::from(bytes.slice(i * width..(i + 1) * width)).into())
                    .collect(),
            )
        }
        _ => return Ok(None),
    };
    let defs = (0..arr.len()).map(|i| i16::from(arr.is_valid(i))).collect();
    Ok(Some(ColumnData::flat(values, Some(defs))))
}

/// Writes an array with the Arrow writer and reads back its physical values.
fn roundtrip_physical(arr: &ArrayRef) -> Result<ColumnData> {
    let schema = Arc::new(Schema::new(vec![Field::new("c", arr.data_type().clone(), true)]));
    let rb = RecordBatch::try_new(schema.clone(), vec![arr.clone()])?;
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
    w.write(&rb)?;
    w.close()?;
    let reader = SerializedFileReader::new(Bytes::from(buf))?;
    let md = reader.metadata();
    let leaf = md.file_metadata().schema_descr().column(0);
    if md.file_metadata().schema_descr().num_columns() != 1 || leaf.max_rep_level() != 0 {
        return Err(Error::Unsupported(format!("nested arrow type {}", arr.data_type())));
    }
    let physical = leaf.physical_type();
    let mut out = ColumnData::empty(physical, true);
    for i in 0..reader.num_row_groups() {
        let rg = reader.get_row_group(i)?;
        let rows = rg.metadata().num_rows() as usize;
        out.append(&read_column(rg.get_column_reader(0)?, physical, true, rows)?)?;
    }
    if out.num_rows() != arr.len() {
        return Err(Error::Corrupt("arrow round trip row count mismatch".into()));
    }
    Ok(out)
}

fn is_decimal(spec: &ColumnSpec) -> bool {
    spec.converted == ConvertedType::DECIMAL || matches!(spec.logical, Some(LogicalType::Decimal { .. }))
}

/// Adapts physically different but logically equal encodings (decimals) to `spec`.
fn coerce_physical(data: ColumnData, spec: &ColumnSpec) -> Result<ColumnData> {
    let from = data.values.physical();
    if from == spec.physical {
        if from == PhysicalType::FIXED_LEN_BYTE_ARRAY {
            if let Values::Fixed(v) = &data.values {
                if v.iter().any(|b| b.len() as i32 != spec.type_length) {
                    if is_decimal(spec) {
                        return decimal_convert(&data, spec);
                    }
                    return Err(Error::Invalid(format!("expected {}-byte values", spec.type_length)));
                }
            }
        }
        return Ok(data);
    }
    if is_decimal(spec) {
        return decimal_convert(&data, spec);
    }
    Err(Error::Invalid(format!("cannot store {from} values in a {} column", spec.physical)))
}

fn decimal_convert(data: &ColumnData, spec: &ColumnSpec) -> Result<ColumnData> {
    let unscaled: Vec<i128> = match &data.values {
        Values::I32(v) => v.iter().map(|&x| x as i128).collect(),
        Values::I64(v) => v.iter().map(|&x| x as i128).collect(),
        Values::Fixed(v) => v.iter().map(|b| be_to_i128(b.data())).collect::<Result<_>>()?,
        Values::Bytes(v) => v.iter().map(|b| be_to_i128(b.data())).collect::<Result<_>>()?,
        other => return Err(Error::Invalid(format!("cannot convert {} to decimal", other.physical()))),
    };
    let overflow = || Error::Invalid("decimal value out of range".into());
    let values = match spec.physical {
        PhysicalType::INT32 => {
            Values::I32(unscaled.iter().map(|&x| i32::try_from(x).map_err(|_| overflow())).collect::<Result<_>>()?)
        }
        PhysicalType::INT64 => {
            Values::I64(unscaled.iter().map(|&x| i64::try_from(x).map_err(|_| overflow())).collect::<Result<_>>()?)
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Values::Fixed(
            unscaled
                .iter()
                .map(|&x| i128_to_be(x, spec.type_length as usize).map(FixedLenByteArray::from).ok_or_else(overflow))
                .collect::<Result<_>>()?,
        ),
        PhysicalType::BYTE_ARRAY => Values::Bytes(
            unscaled.iter().map(|&x| minimal_be(x).into()).collect(),
        ),
        t => return Err(Error::Invalid(format!("decimal stored as {t}"))),
    };
    Ok(ColumnData { values, ..data.clone() })
}

fn be_to_i128(b: &[u8]) -> Result<i128> {
    if b.is_empty() || b.len() > 16 {
        return Err(Error::Invalid(format!("decimal of {} bytes", b.len())));
    }
    let neg = b[0] & 0x80 != 0;
    let mut buf = [if neg { 0xff } else { 0 }; 16];
    buf[16 - b.len()..].copy_from_slice(b);
    Ok(i128::from_be_bytes(buf))
}

fn i128_to_be(x: i128, len: usize) -> Option<Vec<u8>> {
    let full = x.to_be_bytes();
    if len >= 16 {
        let mut out = vec![if x < 0 { 0xff } else { 0 }; len - 16];
        out.extend_from_slice(&full);
        return Some(out);
    }
    let out = full[16 - len..].to_vec();
    (be_to_i128(&out).ok()? == x).then_some(out)
}

fn minimal_be(x: i128) -> Vec<u8> {
    (1..=16).find_map(|l| i128_to_be(x, l)).expect("16 bytes always fit")
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;
const JULIAN_UNIX_EPOCH: i64 = 2_440_588;

pub fn nanos_to_int96(ns: i64) -> Int96 {
    let day = ns.div_euclid(NANOS_PER_DAY) + JULIAN_UNIX_EPOCH;
    let nod = ns.rem_euclid(NANOS_PER_DAY) as u64;
    let mut v = Int96::new();
    v.set_data(nod as u32, (nod >> 32) as u32, day as u32);
    v
}

/// Conforms stored leaf values (written under an older or equal schema version) to
/// `schema`, returning one column per leaf: missing columns get defaults (NULL/empty for
/// nested fields), widened columns are converted and nullability is adapted. Dropped
/// columns are ignored.
pub fn conform(schema: &SchemaVersion, num_rows: usize, mut cols: Vec<(LeafKey, ColumnData)>) -> Result<Vec<ColumnData>> {
    schema
        .leaves()?
        .iter()
        .map(|leaf| {
            let spec = &schema.columns[leaf.field];
            let data = match cols.iter().position(|(k, _)| *k == (leaf.column_id, leaf.leaf)) {
                Some(i) => {
                    let (_, c) = cols.swap_remove(i);
                    if c.num_rows() != num_rows {
                        return Err(Error::Corrupt("batch column length mismatch".into()));
                    }
                    conform_leaf(c, leaf, spec.is_nested())?
                }
                None if spec.is_nested() => ColumnData::nulls(leaf.physical(), leaf.max_def, leaf.max_rep, num_rows)?,
                None => ColumnData::repeat(spec.physical, spec.nullable, default_values(spec)?.as_ref(), num_rows)?,
            };
            Ok(data)
        })
        .collect()
}

/// Adapts one stored leaf to the leaf's current type (widening, nullability).
pub fn conform_leaf(c: ColumnData, leaf: &Leaf, nested: bool) -> Result<ColumnData> {
    if !nested {
        return c.widen(leaf.physical())?.with_nullability(leaf.spec.nullable);
    }
    match leaf.max_def - c.max_def {
        0 => Ok(c),
        1 => Ok(c.raise_defs()),
        _ => Err(Error::Corrupt(format!("stored levels do not fit column '{}'", leaf.spec.name))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    #[test]
    fn direct_flat_conversion_matches_parquet_writer() {
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(5)),
            Arc::new(BooleanArray::from(vec![None, Some(false), Some(true), Some(false), None])),
            Arc::new(Int8Array::from(vec![None, Some(i8::MIN), Some(0), Some(i8::MAX), None])),
            Arc::new(Int16Array::from(vec![None, Some(i16::MIN), Some(0), Some(i16::MAX), None])),
            Arc::new(Int32Array::from(vec![None, Some(i32::MIN), Some(0), Some(i32::MAX), None])),
            Arc::new(Int64Array::from(vec![None, Some(i64::MIN), Some(0), Some(i64::MAX), None])),
            Arc::new(UInt8Array::from(vec![None, Some(0), Some(127), Some(u8::MAX), None])),
            Arc::new(UInt16Array::from(vec![None, Some(0), Some(32767), Some(u16::MAX), None])),
            Arc::new(UInt32Array::from(vec![None, Some(0), Some(i32::MAX as u32 + 1), Some(u32::MAX), None])),
            Arc::new(UInt64Array::from(vec![None, Some(0), Some(i64::MAX as u64 + 1), Some(u64::MAX), None])),
            Arc::new(Float32Array::from(vec![None, Some(-0.0), Some(f32::MIN), Some(f32::INFINITY), None])),
            Arc::new(Float64Array::from(vec![None, Some(-0.0), Some(f64::MIN), Some(f64::INFINITY), None])),
            Arc::new(StringArray::from(vec![None, Some(""), Some("short"), Some("a value longer than a view"), None])),
            Arc::new(LargeStringArray::from(vec![None, Some(""), Some("short"), Some("a value longer than a view"), None])),
            Arc::new(StringViewArray::from(vec![None, Some(""), Some("short"), Some("a value longer than a view"), None])),
            Arc::new(BinaryArray::from(vec![None, Some(&b""[..]), Some(&b"\x00\xff"[..]), Some(&b"long binary value"[..]), None])),
            Arc::new(LargeBinaryArray::from(vec![None, Some(&b""[..]), Some(&b"\x00\xff"[..]), Some(&b"long binary value"[..]), None])),
            Arc::new(BinaryViewArray::from(vec![None, Some(&b""[..]), Some(&b"\x00\xff"[..]), Some(&b"long binary value"[..]), None])),
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![None, Some([0, 255]), Some([128, 0]), Some([0, 0]), None].into_iter(), 2,
            ).unwrap()),
            Arc::new(Decimal32Array::from(vec![None, Some(-12345), Some(0), Some(12345), None])
                .with_precision_and_scale(8, 2).unwrap()),
            Arc::new(Decimal64Array::from(vec![None, Some(-12345), Some(0), Some(12345), None])
                .with_precision_and_scale(8, 2).unwrap()),
            Arc::new(Decimal64Array::from(vec![None, Some(-12345678901), Some(0), Some(12345678901), None])
                .with_precision_and_scale(18, 2).unwrap()),
        ];
        macro_rules! temporal {
            ($($ty:ty),+ $(,)?) => {
                $(arrays.push(Arc::new(PrimitiveArray::<$ty>::from(vec![None, Some(-1), Some(0), Some(1234), None])));)+
            };
        }
        temporal!(
            Date32Type, Date64Type, Time32SecondType, Time32MillisecondType,
            Time64MicrosecondType, Time64NanosecondType, TimestampSecondType,
            TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType,
            DurationSecondType, DurationMillisecondType, DurationMicrosecondType,
            DurationNanosecondType,
        );
        arrays.push(Arc::new(TimestampNanosecondArray::from(vec![None, Some(-1), Some(0), Some(1234), None])
            .with_timezone("America/New_York")));
        let source: ArrayRef = Arc::new(Float32Array::from(vec![None, Some(-1.5), Some(-0.0), Some(1.5), None]));
        arrays.push(cast_with_options(&source, &DataType::Float16, &cast_opts()).unwrap());
        for precision in [1, 8, 9, 10, 18, 19, 20, 28, 38] {
            let value = 10i128.pow(precision as u32) - 1;
            arrays.push(Arc::new(Decimal128Array::from(vec![None, Some(-value), Some(0), Some(value), None])
                .with_precision_and_scale(precision, 0).unwrap()));
        }
        for arr in arrays {
            for arr in [arr.clone(), arr.slice(1, 3), arr.slice(0, 0), new_null_array(arr.data_type(), 3)] {
                let got = direct_physical(&arr).unwrap().expect("direct conversion");
                let expected = roundtrip_physical(&arr).unwrap();
                assert_eq!(got, expected, "{}", arr.data_type());
                let schema = Schema::new(vec![Field::new("c", arr.data_type().clone(), true)]);
                let descriptor = ArrowSchemaConverter::new().convert(&schema).unwrap();
                let column = descriptor.column(0);
                let stat_type = crate::stats::StatType::of_column(&column);
                assert_eq!(
                    crate::stats::PageStats::compute_arrow(&stat_type, arr.as_ref(), column.type_length().max(0) as usize).unwrap(),
                    crate::stats::PageStats::compute(&stat_type, &got),
                    "Arrow page statistics: {}", arr.data_type(),
                );
            }
        }
    }

    #[test]
    fn direct_conversion_preserves_float_bits() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![
            None, Some(-0.0), Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        ]));
        let Values::F64(values) = direct_physical(&arr).unwrap().unwrap().values else { panic!() };
        assert_eq!(values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(), vec![(-0.0f64).to_bits(), 0x7ff8_0000_0000_0042]);
    }

    #[test]
    fn byte_conversion_owns_sliced_arrow_buffers() {
        let data = {
            let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("unused"), None, Some("kept"), Some("also unused")]));
            direct_physical(&arr.slice(1, 2)).unwrap().unwrap()
        };
        assert_eq!(data.defs, Some(vec![0, 1]));
        let Values::Bytes(values) = data.values else { panic!() };
        assert_eq!(values[0].data(), b"kept");
    }

    #[test]
    fn dictionary_conversion_keeps_roundtrip_fallback() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("first").unwrap();
        builder.append_null();
        builder.append("first").unwrap();
        builder.append("second").unwrap();
        let arr: ArrayRef = Arc::new(builder.finish());
        assert!(direct_physical(&arr).unwrap().is_none());
        let schema = Schema::new(vec![Field::new("c", arr.data_type().clone(), true)]);
        let descriptor = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let spec = ColumnSpec::from_parquet_type(0, &descriptor.root_schema().get_fields()[0]).unwrap();
        assert_eq!(
            array_to_column(&spec, arr.data_type(), &arr).unwrap(),
            roundtrip_physical(&arr).unwrap(),
        );
    }

    #[test]
    fn direct_decimals_preserve_legacy_physical_coercion() {
        let arr: ArrayRef = Arc::new(Decimal128Array::from(vec![None, Some(-12345), Some(0), Some(12345)])
            .with_precision_and_scale(8, 2).unwrap());
        for (physical, len) in [
            (PhysicalType::INT32, 0), (PhysicalType::INT64, 0),
            (PhysicalType::BYTE_ARRAY, 0), (PhysicalType::FIXED_LEN_BYTE_ARRAY, 4),
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, 16),
        ] {
            let mut spec = crate::schema::spec_for_type_name(0, "c", "decimal(8,2)", true).unwrap();
            spec.physical = physical;
            spec.type_length = len;
            let expected = coerce_physical(roundtrip_physical(&arr).unwrap(), &spec).unwrap();
            assert_eq!(array_to_column(&spec, arr.data_type(), &arr).unwrap(), expected);
        }
    }

    fn stored_schema(schema: &Schema) -> SchemaVersion {
        let descriptor = ArrowSchemaConverter::new().convert(schema).unwrap();
        SchemaVersion {
            schema_version_id: 1,
            root_name: descriptor.root_schema().name().into(),
            columns: descriptor.root_schema().get_fields().iter().enumerate()
                .map(|(i, field)| ColumnSpec::from_parquet_type(i as i64, field).unwrap()).collect(),
            key_value: Some(vec![parquet::file::metadata::KeyValue::new(
                parquet::arrow::ARROW_SCHEMA_META_KEY.into(),
                parquet::arrow::encode_arrow_schema(schema),
            )]),
        }
    }

    #[test]
    fn arrow_append_conforms_names_casts_nulls_and_uses_legacy_fallbacks() {
        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("optional", DataType::Int32, true),
        ]);
        let schema = stored_schema(&target);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int32, true),
        ])), vec![
            Arc::new(LargeStringArray::from(vec![Some("x"), None])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ]).unwrap();
        let conformed = batch_for_arrow_append(&schema, &batch).unwrap().expect("canonical flat fast path");
        assert_eq!(conformed.schema().fields(), target.fields());
        assert_eq!(conformed.column(2).null_count(), 2);
        assert_eq!(batch_to_columns(&schema, &conformed).unwrap(), batch_to_columns(&schema, &batch).unwrap());

        for names in [vec!["name"], vec!["id", "unknown"], vec!["id", "id"]] {
            let invalid = RecordBatch::try_new(
                Arc::new(Schema::new(names.iter().map(|name| Field::new(*name, DataType::Utf8, true)).collect::<Vec<_>>())),
                names.iter().map(|_| Arc::new(StringArray::from(vec!["1", "2"])) as ArrayRef).collect(),
            ).unwrap();
            assert!(batch_for_arrow_append(&schema, &invalid).is_err());
        }

        let mut default_schema = schema.clone();
        default_schema.columns[2].default_value = Some(crate::values::encode_value(&Values::I32(vec![42])).unwrap());
        assert!(batch_for_arrow_append(&default_schema, &batch).unwrap().is_none());
        let mut legacy = schema.clone();
        legacy.columns[0].physical = PhysicalType::INT96;
        assert!(batch_for_arrow_append(&legacy, &batch).unwrap().is_none());
        let nested = Schema::new(vec![Field::new("nested",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))), true)]);
        assert!(batch_for_arrow_append(&stored_schema(&nested), &batch).unwrap().is_none());

        for arrays in [
            vec![Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef, Arc::new(StringArray::from(vec!["a", "b"]))],
            vec![Arc::new(StringArray::from(vec!["bad", "2"])) as ArrayRef, Arc::new(StringArray::from(vec!["a", "b"]))],
        ] {
            let input_schema = Arc::new(Schema::new(vec![
                Field::new("id", arrays[0].data_type().clone(), true),
                Field::new("name", DataType::Utf8, false),
            ]));
            let invalid = RecordBatch::try_new(input_schema, arrays).unwrap();
            assert!(batch_for_arrow_append(&schema, &invalid).is_err());
        }
    }

    #[test]
    fn out_of_precision_decimals_use_physical_compatibility_path() {
        let schema = Schema::new(vec![Field::new("decimal", DataType::Decimal128(8, 0), true)]);
        let stored = stored_schema(&schema);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(Decimal128Array::from(vec![Some(i32::MAX as i128 + 1), Some(1)])
                .with_precision_and_scale(8, 0).unwrap()),
        ]).unwrap();
        assert!(batch_for_arrow_append(&stored, &batch).unwrap().is_none());
        let mut legacy = stored.clone();
        legacy.columns[0].physical = PhysicalType::INT64;
        assert!(batch_for_arrow_append(&legacy, &batch).unwrap().is_none());
    }

    #[test]
    fn arrow_statistics_match_physical_statistics_for_nan_and_long_bounds() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![None, Some(f64::NAN), Some(-0.0), Some(0.0), Some(f64::INFINITY)])),
            Arc::new(Float32Array::from(vec![None, Some(f32::NAN), Some(-0.0), Some(0.0), Some(f32::NEG_INFINITY)])),
            Arc::new(Float64Array::from(vec![Some(f64::NAN), None, Some(f64::NAN)])),
            Arc::new(StringArray::from(vec![Some("é".repeat(80)), Some("z".repeat(100)), None])),
            Arc::new(BinaryArray::from_iter(vec![Some(vec![255; 100]), Some(vec![128; 100]), None])),
        ];
        for arr in arrays {
            let descriptor = ArrowSchemaConverter::new().convert(&Schema::new(vec![Field::new("c", arr.data_type().clone(), true)])).unwrap();
            let column = descriptor.column(0);
            let stat_type = crate::stats::StatType::of_column(&column);
            assert_eq!(
                crate::stats::PageStats::compute_arrow(&stat_type, arr.as_ref(), column.type_length().max(0) as usize).unwrap(),
                crate::stats::PageStats::compute(&stat_type, &roundtrip_physical(&arr).unwrap()),
            );
        }
    }
}
