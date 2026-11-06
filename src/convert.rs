//! Converting Arrow record batches into physical column values that conform to a
//! virtual file's schema, and conforming stored values to newer schema versions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampNanosecondType};
use bytes::Bytes;
use parquet::arrow::{ArrowWriter, parquet_to_arrow_schema};
use parquet::basic::{Compression, ConvertedType, LogicalType, Type as PhysicalType};
use parquet::data_type::{FixedLenByteArray, Int96};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

use crate::codec::read_column;
use crate::error::{Error, Result};
use crate::schema::{ColumnSpec, SchemaVersion};
use crate::values::{ColumnData, Values, decode_value};

/// The Arrow schema readers see for a schema version.
pub fn arrow_schema(schema: &SchemaVersion) -> Result<Schema> {
    let descr = schema.descriptor()?;
    Ok(parquet_to_arrow_schema(&descr, schema.key_value.as_ref())?)
}

/// The column's default fill value (`None` = NULL).
pub fn default_values(spec: &ColumnSpec) -> Result<Option<Values>> {
    spec.default_value.as_deref().map(decode_value).transpose()
}

/// Converts a record batch into one [`ColumnData`] per schema column (schema order).
/// Columns are matched by name; schema columns absent from the batch receive their default
/// (or NULL). Unknown batch columns, lossy casts and NULLs in required columns are errors.
pub fn batch_to_columns(schema: &SchemaVersion, batch: &RecordBatch) -> Result<Vec<ColumnData>> {
    let target = arrow_schema(schema)?;
    if target.fields().len() != schema.columns.len() {
        return Err(Error::Corrupt("arrow schema does not match column count".into()));
    }
    let bs = batch.schema();
    for (i, f) in bs.fields().iter().enumerate() {
        if schema.column_by_name(f.name()).is_none() {
            return Err(Error::Invalid(format!("unknown column '{}'", f.name())));
        }
        if bs.fields().iter().skip(i + 1).any(|g| g.name() == f.name()) {
            return Err(Error::Invalid(format!("duplicate column '{}'", f.name())));
        }
    }
    let n = batch.num_rows();
    schema
        .columns
        .iter()
        .zip(target.fields())
        .map(|(spec, field)| {
            let col = match batch.column_by_name(&spec.name) {
                Some(arr) => array_to_column(spec, field.data_type(), arr),
                None => ColumnData::repeat(spec.physical, spec.nullable, default_values(spec)?.as_ref(), n)
                    .map_err(|_| Error::Invalid(format!("missing value for required column '{}'", spec.name))),
            }?;
            debug_assert_eq!(col.num_rows(), n);
            Ok(col)
        })
        .collect()
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
        ColumnData {
            values: Values::I96(p.iter().flatten().map(nanos_to_int96).collect()),
            defs: Some(p.iter().map(|v| v.is_some() as i16).collect()),
        }
    } else {
        let written = roundtrip_physical(&arr).map_err(ctx)?;
        coerce_physical(written, spec).map_err(ctx)?
    };
    data.with_nullability(spec.nullable).map_err(ctx)
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
    Ok(ColumnData { values, defs: data.defs.clone() })
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

/// Conforms stored values (keyed by column id, written under an older or equal schema
/// version) to `schema`: missing columns get defaults, widened columns are converted and
/// nullability is adapted. Dropped columns are ignored.
pub fn conform(schema: &SchemaVersion, num_rows: usize, mut cols: Vec<(i64, ColumnData)>) -> Result<Vec<ColumnData>> {
    schema
        .columns
        .iter()
        .map(|spec| {
            let data = match cols.iter().position(|(id, _)| *id == spec.column_id) {
                Some(i) => {
                    let (_, c) = cols.swap_remove(i);
                    if c.num_rows() != num_rows {
                        return Err(Error::Corrupt("batch column length mismatch".into()));
                    }
                    c.widen(spec.physical)?.with_nullability(spec.nullable)?
                }
                None => ColumnData::repeat(spec.physical, spec.nullable, default_values(spec)?.as_ref(), num_rows)?,
            };
            Ok(data)
        })
        .collect()
}
