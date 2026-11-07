//! Persisted form of column-chunk statistics.

use std::cmp::Ordering;

use arrow::array::{Array, AsArray};
use arrow::datatypes::*;
use parquet::basic::{
    BoundaryOrder, ColumnOrder, ConvertedType, LogicalType, SortOrder, Type as PhysicalType,
};
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::statistics::{Statistics, ValueStatistics};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::values::{ColumnData, Values};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredStats {
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    #[serde(default)]
    pub nan_count: Option<u64>,
    pub min_exact: bool,
    pub max_exact: bool,
    pub deprecated: bool,
}

impl StoredStats {
    pub fn from_parquet(s: &Statistics) -> Self {
        Self {
            min: s.min_bytes_opt().map(<[u8]>::to_vec),
            max: s.max_bytes_opt().map(<[u8]>::to_vec),
            null_count: s.null_count_opt(),
            distinct_count: s.distinct_count_opt(),
            nan_count: s.nan_count_opt(),
            min_exact: s.min_is_exact(),
            max_exact: s.max_is_exact(),
            deprecated: s.is_min_max_deprecated(),
        }
    }

    /// Statistics remaining valid after rows are removed: min/max become (conservative)
    /// bounds; counts can no longer be trusted.
    pub fn after_row_removal(&self) -> Self {
        Self {
            null_count: None,
            distinct_count: None,
            nan_count: None,
            min_exact: false,
            max_exact: false,
            ..self.clone()
        }
    }

    pub fn to_parquet(&self, physical: PhysicalType) -> Result<Statistics> {
        fn fixed<const N: usize>(b: &[u8]) -> Result<[u8; N]> {
            b.try_into().map_err(|_| {
                Error::Corrupt(format!("statistic has {} bytes, expected {N}", b.len()))
            })
        }
        macro_rules! vs {
            ($variant:ident, $conv:expr) => {{
                let conv = $conv;
                let min = self.min.as_deref().map(&conv).transpose()?;
                let max = self.max.as_deref().map(&conv).transpose()?;
                Statistics::$variant(
                    ValueStatistics::new(
                        min,
                        max,
                        self.distinct_count,
                        self.null_count,
                        self.deprecated,
                    )
                    .with_min_is_exact(self.min_exact)
                    .with_max_is_exact(self.max_exact)
                    .with_nan_count(self.nan_count),
                )
            }};
        }
        Ok(match physical {
            PhysicalType::BOOLEAN => vs!(Boolean, |b: &[u8]| -> Result<bool> {
                Ok(fixed::<1>(b)?[0] != 0)
            }),
            PhysicalType::INT32 => vs!(Int32, |b: &[u8]| -> Result<i32> {
                Ok(i32::from_le_bytes(fixed(b)?))
            }),
            PhysicalType::INT64 => vs!(Int64, |b: &[u8]| -> Result<i64> {
                Ok(i64::from_le_bytes(fixed(b)?))
            }),
            PhysicalType::FLOAT => vs!(Float, |b: &[u8]| -> Result<f32> {
                Ok(f32::from_le_bytes(fixed(b)?))
            }),
            PhysicalType::DOUBLE => vs!(Double, |b: &[u8]| -> Result<f64> {
                Ok(f64::from_le_bytes(fixed(b)?))
            }),
            PhysicalType::INT96 => vs!(Int96, |b: &[u8]| -> Result<Int96> {
                let a: [u8; 12] = fixed(b)?;
                let w = |i: usize| u32::from_le_bytes(a[i * 4..i * 4 + 4].try_into().unwrap());
                let mut v = Int96::new();
                v.set_data(w(0), w(1), w(2));
                Ok(v)
            }),
            PhysicalType::BYTE_ARRAY => vs!(ByteArray, |b: &[u8]| -> Result<ByteArray> {
                Ok(ByteArray::from(b.to_vec()))
            }),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                vs!(FixedLenByteArray, |b: &[u8]| -> Result<FixedLenByteArray> {
                    Ok(FixedLenByteArray::from(ByteArray::from(b.to_vec())))
                })
            }
        })
    }
}

/// How min/max values of a leaf column are ordered (from its Parquet column order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatOrder {
    Signed,
    Unsigned,
    /// IEEE 754 total order (FLOAT, DOUBLE, FLOAT16); NaNs are excluded from min/max.
    Total,
    /// No usable order: no page statistics are kept.
    Undefined,
}

/// The parts of a leaf type that determine how statistics are computed and compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatType {
    pub physical: PhysicalType,
    pub order: StatOrder,
    /// FLOAT16 stored as 2-byte FIXED_LEN_BYTE_ARRAY.
    pub float16: bool,
    /// Byte arrays that must stay valid UTF-8 when truncated.
    pub utf8: bool,
}

/// Maximum length of byte-array min/max values in page statistics (as in parquet-rs).
pub const TRUNCATE_LEN: usize = 64;

impl StatType {
    pub fn new(
        physical: PhysicalType,
        logical: Option<&LogicalType>,
        converted: ConvertedType,
    ) -> Self {
        let float16 = logical == Some(&LogicalType::Float16);
        let order = match ColumnOrder::column_order_for_type(logical, converted, physical) {
            ColumnOrder::IEEE_754_TOTAL_ORDER => StatOrder::Total,
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED) => StatOrder::Signed,
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED) => StatOrder::Unsigned,
            _ => StatOrder::Undefined,
        };
        let order = match (order, physical) {
            (StatOrder::Total, PhysicalType::FIXED_LEN_BYTE_ARRAY) if !float16 => {
                StatOrder::Undefined
            }
            (_, PhysicalType::INT96) => StatOrder::Undefined,
            (o, _) => o,
        };
        let utf8 = matches!(
            logical,
            Some(LogicalType::String | LogicalType::Enum | LogicalType::Json)
        ) || matches!(
            converted,
            ConvertedType::UTF8 | ConvertedType::ENUM | ConvertedType::JSON
        );
        Self {
            physical,
            order,
            float16,
            utf8,
        }
    }

    pub fn of_column(c: &parquet::schema::types::ColumnDescriptor) -> Self {
        Self::new(c.physical_type(), c.logical_type_ref(), c.converted_type())
    }

    /// Compares two encoded (plain, as in statistics) values.
    pub fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        fn int<const N: usize>(b: &[u8]) -> [u8; N] {
            let mut out = [0u8; N];
            let n = b.len().min(N);
            out[..n].copy_from_slice(&b[..n]);
            out
        }
        match (self.physical, self.order) {
            (PhysicalType::BOOLEAN, _) => a.cmp(b),
            (PhysicalType::INT32, StatOrder::Unsigned) => {
                u32::from_le_bytes(int(a)).cmp(&u32::from_le_bytes(int(b)))
            }
            (PhysicalType::INT32, _) => i32::from_le_bytes(int(a)).cmp(&i32::from_le_bytes(int(b))),
            (PhysicalType::INT64, StatOrder::Unsigned) => {
                u64::from_le_bytes(int(a)).cmp(&u64::from_le_bytes(int(b)))
            }
            (PhysicalType::INT64, _) => i64::from_le_bytes(int(a)).cmp(&i64::from_le_bytes(int(b))),
            (PhysicalType::FLOAT, _) => {
                f32::from_le_bytes(int(a)).total_cmp(&f32::from_le_bytes(int(b)))
            }
            (PhysicalType::DOUBLE, _) => {
                f64::from_le_bytes(int(a)).total_cmp(&f64::from_le_bytes(int(b)))
            }
            (_, StatOrder::Total) => f16_key(a).cmp(&f16_key(b)),
            (_, StatOrder::Signed) => signed_be_cmp(a, b),
            _ => a.cmp(b),
        }
    }
}

/// Total-order key of a little-endian IEEE half float.
fn f16_key(b: &[u8]) -> i16 {
    let bits = i16::from_le_bytes([
        b.first().copied().unwrap_or(0),
        b.get(1).copied().unwrap_or(0),
    ]);
    if bits < 0 { bits ^ 0x7fff } else { bits }
}

fn f16_is_nan(b: &[u8]) -> bool {
    let bits = u16::from_le_bytes([b[0], b[1]]);
    bits & 0x7c00 == 0x7c00 && bits & 0x03ff != 0
}

/// Compares big-endian two's-complement integers of possibly different lengths.
fn signed_be_cmp(a: &[u8], b: &[u8]) -> Ordering {
    let neg = |x: &[u8]| x.first().is_some_and(|&v| v & 0x80 != 0);
    let (na, nb) = (neg(a), neg(b));
    if na != nb {
        return if na {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let pad = if na { 0xffu8 } else { 0 };
    let n = a.len().max(b.len());
    let at = |x: &[u8], i: usize| {
        if i + x.len() >= n {
            x[i + x.len() - n]
        } else {
            pad
        }
    };
    (0..n)
        .map(|i| at(a, i).cmp(&at(b, i)))
        .find(|o| o.is_ne())
        .unwrap_or(Ordering::Equal)
}

/// Statistics of one data page, as needed for the column index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStats {
    /// Min/max over non-null values (plain encoded); `None` iff the page holds no values.
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    /// Levels below the maximum definition level (nulls and empty or null parents).
    pub null_count: u64,
    /// Number of NaN values (floating-point columns only).
    pub nan_count: Option<u64>,
    pub min_exact: bool,
    pub max_exact: bool,
}

impl PageStats {
    pub fn is_null_page(&self) -> bool {
        self.min.is_none()
    }

    /// Computes flat-page statistics directly from Arrow without building physical
    /// value vectors. Used only after checking the Arrow/stored Parquet type mapping.
    pub(crate) fn compute_arrow(
        t: &StatType,
        arr: &dyn Array,
        width: usize,
    ) -> Result<Option<Self>> {
        if t.order == StatOrder::Undefined {
            return Ok(None);
        }
        macro_rules! primitive {
            ($ty:ty, $convert:expr, $encode:expr) => {{
                let convert = $convert;
                let encode = $encode;
                minmax(
                    arr.as_primitive::<$ty>().iter().flatten().map(convert),
                    |a, b| a.cmp(b),
                )
                .map(|(lo, hi)| (encode(lo), encode(hi)))
            }};
        }
        let mut nan_count = None;
        macro_rules! float {
            ($ty:ty, $encode:expr) => {{
                let mut count = 0;
                let bounds = minmax(
                    arr.as_primitive::<$ty>().iter().flatten().filter(|v| {
                        if v.is_nan() {
                            count += 1;
                            false
                        } else {
                            true
                        }
                    }),
                    |a, b| a.total_cmp(b),
                );
                nan_count = Some(count);
                if bounds.is_none() && count > 0 {
                    return Ok(None);
                }
                let encode = $encode;
                bounds.map(|(lo, hi)| (encode(lo), encode(hi)))
            }};
        }
        macro_rules! bytes {
            ($values:expr) => {
                minmax($values, |a, b| t.cmp(a, b)).map(|(lo, hi)| (lo.to_vec(), hi.to_vec()))
            };
        }
        let i32_bytes = |v: i32| v.to_le_bytes().to_vec();
        let i64_bytes = |v: i64| v.to_le_bytes().to_vec();
        let decimal_bytes = |v: i128| match t.physical {
            PhysicalType::INT32 => (v as i32).to_le_bytes().to_vec(),
            PhysicalType::INT64 => (v as i64).to_le_bytes().to_vec(),
            _ => v.to_be_bytes()[16 - width..].to_vec(),
        };
        let bounds = match arr.data_type() {
            DataType::Null => None,
            DataType::Boolean => minmax(arr.as_boolean().iter().flatten(), |a, b| a.cmp(b))
                .map(|(a, b)| (vec![a as u8], vec![b as u8])),
            DataType::Int8 => primitive!(Int8Type, |v| v as i32, i32_bytes),
            DataType::Int16 => primitive!(Int16Type, |v| v as i32, i32_bytes),
            DataType::Int32 => primitive!(Int32Type, |v| v, i32_bytes),
            DataType::Int64 => primitive!(Int64Type, |v| v, i64_bytes),
            DataType::UInt8 => {
                primitive!(UInt8Type, |v| v as u32, |v: u32| v.to_le_bytes().to_vec())
            }
            DataType::UInt16 => {
                primitive!(UInt16Type, |v| v as u32, |v: u32| v.to_le_bytes().to_vec())
            }
            DataType::UInt32 => primitive!(UInt32Type, |v| v, |v: u32| v.to_le_bytes().to_vec()),
            DataType::UInt64 => primitive!(UInt64Type, |v| v, |v: u64| v.to_le_bytes().to_vec()),
            DataType::Float16 => float!(
                Float16Type,
                |v: <Float16Type as ArrowPrimitiveType>::Native| v.to_le_bytes().to_vec()
            ),
            DataType::Float32 => float!(Float32Type, |v: f32| v.to_le_bytes().to_vec()),
            DataType::Float64 => float!(Float64Type, |v: f64| v.to_le_bytes().to_vec()),
            DataType::Date32 => primitive!(Date32Type, |v| v, i32_bytes),
            DataType::Date64 => primitive!(Date64Type, |v| v, i64_bytes),
            DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType, |v| v, i32_bytes),
            DataType::Time32(TimeUnit::Millisecond) => {
                primitive!(Time32MillisecondType, |v| v, i32_bytes)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive!(Time64MicrosecondType, |v| v, i64_bytes)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                primitive!(Time64NanosecondType, |v| v, i64_bytes)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                primitive!(TimestampSecondType, |v| v, i64_bytes)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                primitive!(TimestampMillisecondType, |v| v, i64_bytes)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                primitive!(TimestampMicrosecondType, |v| v, i64_bytes)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                primitive!(TimestampNanosecondType, |v| v, i64_bytes)
            }
            DataType::Duration(TimeUnit::Second) => {
                primitive!(DurationSecondType, |v| v, i64_bytes)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                primitive!(DurationMillisecondType, |v| v, i64_bytes)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                primitive!(DurationMicrosecondType, |v| v, i64_bytes)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                primitive!(DurationNanosecondType, |v| v, i64_bytes)
            }
            DataType::Decimal32(_, _) => primitive!(Decimal32Type, |v| v as i128, decimal_bytes),
            DataType::Decimal64(_, _) => primitive!(Decimal64Type, |v| v as i128, decimal_bytes),
            DataType::Decimal128(_, _) => primitive!(Decimal128Type, |v| v, decimal_bytes),
            DataType::Utf8 => bytes!(arr.as_string::<i32>().iter().flatten().map(str::as_bytes)),
            DataType::LargeUtf8 => {
                bytes!(arr.as_string::<i64>().iter().flatten().map(str::as_bytes))
            }
            DataType::Utf8View => bytes!(arr.as_string_view().iter().flatten().map(str::as_bytes)),
            DataType::Binary => bytes!(arr.as_binary::<i32>().iter().flatten()),
            DataType::LargeBinary => bytes!(arr.as_binary::<i64>().iter().flatten()),
            DataType::BinaryView => bytes!(arr.as_binary_view().iter().flatten()),
            DataType::FixedSizeBinary(_) => bytes!(arr.as_fixed_size_binary().iter().flatten()),
            other => {
                return Err(Error::Unsupported(format!(
                    "Arrow page statistics for {other}"
                )));
            }
        };
        Ok(Self::from_bounds(
            t,
            bounds,
            arr.logical_null_count() as u64,
            nan_count,
        ))
    }

    /// Computes the statistics of a page holding `data`; `None` if the column type has no
    /// defined order.
    pub fn compute(t: &StatType, data: &ColumnData) -> Option<PageStats> {
        if t.order == StatOrder::Undefined {
            return None;
        }
        let levels = data.defs.as_ref().map_or(data.values.len(), Vec::len);
        let null_count = (levels - data.values.len()) as u64;
        let mut nan_count = None;
        let enc: Option<(Vec<u8>, Vec<u8>)> = match &data.values {
            Values::Bool(v) => minmax(v.iter().copied(), |a, b| a.cmp(b))
                .map(|(a, b)| (vec![a as u8], vec![b as u8])),
            Values::I32(v) if t.order == StatOrder::Unsigned => {
                minmax(v.iter().map(|&x| x as u32), |a, b| a.cmp(b))
                    .map(|(a, b)| (a.to_le_bytes().to_vec(), b.to_le_bytes().to_vec()))
            }
            Values::I32(v) => minmax(v.iter().copied(), |a, b| a.cmp(b))
                .map(|(a, b)| (a.to_le_bytes().to_vec(), b.to_le_bytes().to_vec())),
            Values::I64(v) if t.order == StatOrder::Unsigned => {
                minmax(v.iter().map(|&x| x as u64), |a, b| a.cmp(b))
                    .map(|(a, b)| (a.to_le_bytes().to_vec(), b.to_le_bytes().to_vec()))
            }
            Values::I64(v) => minmax(v.iter().copied(), |a, b| a.cmp(b))
                .map(|(a, b)| (a.to_le_bytes().to_vec(), b.to_le_bytes().to_vec())),
            Values::F32(v) => {
                nan_count = Some(v.iter().filter(|x| x.is_nan()).count() as u64);
                float_minmax(
                    v,
                    |x| x.is_nan(),
                    f32::total_cmp,
                    |x| x.to_le_bytes().to_vec(),
                )
            }
            Values::F64(v) => {
                nan_count = Some(v.iter().filter(|x| x.is_nan()).count() as u64);
                float_minmax(
                    v,
                    |x| x.is_nan(),
                    f64::total_cmp,
                    |x| x.to_le_bytes().to_vec(),
                )
            }
            Values::Fixed(v) if t.float16 => {
                if v.iter().any(|b| b.len() != 2) {
                    return None;
                }
                nan_count = Some(v.iter().filter(|b| f16_is_nan(b.data())).count() as u64);
                float_minmax(
                    v,
                    |b| f16_is_nan(b.data()),
                    |a, b| f16_key(a.data()).cmp(&f16_key(b.data())),
                    |b| b.data().to_vec(),
                )
            }
            Values::Fixed(v) => minmax(v.iter().map(|b| b.data()), |a, b| t.cmp(a, b))
                .map(|(a, b)| (a.to_vec(), b.to_vec())),
            Values::Bytes(v) => minmax(v.iter().map(|b| b.data()), |a, b| t.cmp(a, b))
                .map(|(a, b)| (a.to_vec(), b.to_vec())),
            Values::I96(_) => return None,
        };
        Self::from_bounds(t, enc, null_count, nan_count)
    }

    fn from_bounds(
        t: &StatType,
        enc: Option<(Vec<u8>, Vec<u8>)>,
        null_count: u64,
        nan_count: Option<u64>,
    ) -> Option<Self> {
        let mut s = PageStats {
            min_exact: true,
            max_exact: true,
            null_count,
            nan_count,
            min: enc.as_ref().map(|e| e.0.clone()),
            max: enc.map(|e| e.1),
        };
        // NaN-only pages get no statistics: readers disagree on how NaN bounds compare.
        if s.min.as_deref().is_some_and(|m| is_nan_bytes(t, m)) {
            return None;
        }
        if t.order == StatOrder::Unsigned
            && matches!(
                t.physical,
                PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
            )
        {
            s.truncate(t.utf8);
        }
        Some(s)
    }

    /// Shortens long byte-array bounds: the min to a prefix, the max to a prefix whose last
    /// byte (or character, for UTF-8) is incremented. Both remain valid bounds.
    fn truncate(&mut self, utf8: bool) {
        if let Some(m) = &mut self.min
            && m.len() > TRUNCATE_LEN
        {
            let mut n = TRUNCATE_LEN;
            if utf8 {
                while n > 0 && (m[n] & 0xc0) == 0x80 {
                    n -= 1;
                }
            }
            m.truncate(n);
            self.min_exact = false;
        }
        if let Some(m) = &mut self.max
            && m.len() > TRUNCATE_LEN
            && let Some(t) = if utf8 {
                truncate_utf8_max(m)
            } else {
                truncate_binary_max(m)
            }
        {
            *m = t;
            self.max_exact = false;
        }
    }
}

fn truncate_binary_max(v: &[u8]) -> Option<Vec<u8>> {
    let mut out = v[..TRUNCATE_LEN].to_vec();
    while let Some(last) = out.pop() {
        if last < 0xff {
            out.push(last + 1);
            return Some(out);
        }
    }
    None
}

fn truncate_utf8_max(v: &[u8]) -> Option<Vec<u8>> {
    let s = std::str::from_utf8(v).ok()?;
    let mut cut = TRUNCATE_LEN;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    let mut chars: Vec<char> = s[..cut].chars().collect();
    while let Some(c) = chars.pop() {
        let next = (c as u32 + 1..=char::MAX as u32).find_map(char::from_u32);
        if let Some(n) = next {
            chars.push(n);
            return Some(chars.into_iter().collect::<String>().into_bytes());
        }
    }
    None
}

fn minmax<T: Copy>(
    mut it: impl Iterator<Item = T>,
    cmp: impl Fn(&T, &T) -> Ordering,
) -> Option<(T, T)> {
    let first = it.next()?;
    Some(it.fold((first, first), |(lo, hi), x| {
        (
            if cmp(&x, &lo).is_lt() { x } else { lo },
            if cmp(&x, &hi).is_gt() { x } else { hi },
        )
    }))
}

/// Min/max under total order ignoring NaN; all-NaN input yields (NaN, NaN).
fn float_minmax<T>(
    v: &[T],
    is_nan: impl Fn(&T) -> bool,
    cmp: impl Fn(&T, &T) -> Ordering,
    enc: impl Fn(&T) -> Vec<u8>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let first = v.first()?;
    let mut lo: Option<&T> = None;
    let mut hi: Option<&T> = None;
    for x in v.iter().filter(|x| !is_nan(x)) {
        if lo.is_none_or(|l| cmp(x, l).is_lt()) {
            lo = Some(x);
        }
        if hi.is_none_or(|h| cmp(x, h).is_gt()) {
            hi = Some(x);
        }
    }
    Some(match (lo, hi) {
        (Some(l), Some(h)) => (enc(l), enc(h)),
        _ => (enc(first), enc(first)),
    })
}

/// Chunk statistics derived from the statistics of all its data pages.
pub fn merge_pages<'a>(
    t: &StatType,
    pages: impl IntoIterator<Item = &'a PageStats>,
) -> StoredStats {
    let mut out = StoredStats {
        min: None,
        max: None,
        null_count: Some(0),
        distinct_count: None,
        nan_count: (t.order == StatOrder::Total).then_some(0),
        min_exact: true,
        max_exact: true,
        deprecated: false,
    };
    let mut nan_only = true;
    for p in pages {
        out.null_count = out.null_count.map(|n| n + p.null_count);
        out.nan_count = match (out.nan_count, p.nan_count) {
            (Some(a), Some(b)) => Some(a + b),
            _ => None,
        };
        let (Some(pmin), Some(pmax)) = (&p.min, &p.max) else {
            continue;
        };
        // Under total order a page min/max of NaN means the page holds only NaNs.
        let page_nan_only = t.order == StatOrder::Total && is_nan_bytes(t, pmin);
        if page_nan_only {
            if out.min.is_none() {
                out.min = Some(pmin.clone());
                out.max = Some(pmax.clone());
            }
            continue;
        }
        if nan_only {
            nan_only = false;
            out.min = None;
            out.max = None;
        }
        pick(
            t,
            &mut out.min,
            &mut out.min_exact,
            pmin,
            p.min_exact,
            Ordering::Less,
        );
        pick(
            t,
            &mut out.max,
            &mut out.max_exact,
            pmax,
            p.max_exact,
            Ordering::Greater,
        );
    }
    out
}

fn is_nan_bytes(t: &StatType, b: &[u8]) -> bool {
    match t.physical {
        PhysicalType::FLOAT => b.len() == 4 && f32::from_le_bytes(b.try_into().unwrap()).is_nan(),
        PhysicalType::DOUBLE => b.len() == 8 && f64::from_le_bytes(b.try_into().unwrap()).is_nan(),
        _ => t.float16 && b.len() == 2 && f16_is_nan(b),
    }
}

fn pick(
    t: &StatType,
    cur: &mut Option<Vec<u8>>,
    exact: &mut bool,
    v: &[u8],
    v_exact: bool,
    want: Ordering,
) {
    match cur {
        None => {
            *cur = Some(v.to_vec());
            *exact = v_exact;
        }
        Some(c) => match t.cmp(v, c) {
            o if o == want => {
                *cur = Some(v.to_vec());
                *exact = v_exact;
            }
            Ordering::Equal => *exact |= v_exact,
            _ => {}
        },
    }
}

/// Boundary order of a column index: whether page minimums and maximums (ignoring null
/// pages) are both non-decreasing or both non-increasing.
pub fn boundary_order(t: &StatType, pages: &[&PageStats]) -> BoundaryOrder {
    let bounds: Vec<(&[u8], &[u8])> = pages
        .iter()
        .filter_map(|p| Some((p.min.as_deref()?, p.max.as_deref()?)))
        .collect();
    let asc = bounds
        .windows(2)
        .all(|w| t.cmp(w[0].0, w[1].0).is_le() && t.cmp(w[0].1, w[1].1).is_le());
    let desc = bounds
        .windows(2)
        .all(|w| t.cmp(w[0].0, w[1].0).is_ge() && t.cmp(w[0].1, w[1].1).is_ge());
    if asc {
        BoundaryOrder::ASCENDING
    } else if desc {
        BoundaryOrder::DESCENDING
    } else {
        BoundaryOrder::UNORDERED
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{IntType, LogicalType as L};
    use parquet::data_type::{ByteArray, FixedLenByteArray};

    fn ty(p: PhysicalType, l: Option<L>, c: ConvertedType) -> StatType {
        StatType::new(p, l.as_ref(), c)
    }
    fn flat(values: Values, nulls: usize) -> ColumnData {
        let n = values.len();
        let mut defs = vec![1i16; n];
        defs.extend(std::iter::repeat_n(0, nulls));
        ColumnData::flat(values, Some(defs))
    }

    #[test]
    fn orders_follow_parquet_column_orders() {
        use PhysicalType as P;
        assert_eq!(
            ty(P::INT32, None, ConvertedType::NONE).order,
            StatOrder::Signed
        );
        assert_eq!(
            ty(
                P::INT32,
                Some(L::Integer(IntType {
                    bit_width: 16,
                    is_signed: false
                })),
                ConvertedType::NONE
            )
            .order,
            StatOrder::Unsigned
        );
        assert_eq!(
            ty(P::INT64, None, ConvertedType::UINT_64).order,
            StatOrder::Unsigned
        );
        assert_eq!(
            ty(P::DOUBLE, None, ConvertedType::NONE).order,
            StatOrder::Total
        );
        assert_eq!(
            ty(
                P::FIXED_LEN_BYTE_ARRAY,
                Some(L::Float16),
                ConvertedType::NONE
            )
            .order,
            StatOrder::Total
        );
        assert_eq!(
            ty(P::FIXED_LEN_BYTE_ARRAY, None, ConvertedType::DECIMAL).order,
            StatOrder::Signed
        );
        assert_eq!(
            ty(P::FIXED_LEN_BYTE_ARRAY, None, ConvertedType::INTERVAL).order,
            StatOrder::Undefined
        );
        assert_eq!(
            ty(P::INT96, None, ConvertedType::NONE).order,
            StatOrder::Undefined
        );
        assert_eq!(
            ty(P::BYTE_ARRAY, None, ConvertedType::NONE).order,
            StatOrder::Unsigned
        );
        assert!(ty(P::BYTE_ARRAY, Some(L::String), ConvertedType::UTF8).utf8);
    }

    #[test]
    fn integers_signed_and_unsigned() {
        let s = ty(PhysicalType::INT32, None, ConvertedType::NONE);
        let u = ty(PhysicalType::INT32, None, ConvertedType::UINT_32);
        let d = flat(Values::I32(vec![5, -3, 7, -1]), 2);
        let ps = PageStats::compute(&s, &d).unwrap();
        assert_eq!(
            (ps.min.unwrap(), ps.max.unwrap(), ps.null_count),
            (
                (-3i32).to_le_bytes().to_vec(),
                7i32.to_le_bytes().to_vec(),
                2
            )
        );
        let pu = PageStats::compute(&u, &d).unwrap();
        // -3 as u32 is the largest; 5 the smallest.
        assert_eq!(
            (pu.min.unwrap(), pu.max.unwrap()),
            (5i32.to_le_bytes().to_vec(), (-1i32).to_le_bytes().to_vec())
        );
        assert!(pu.nan_count.is_none() && pu.min_exact && pu.max_exact);
        let u64t = ty(PhysicalType::INT64, None, ConvertedType::UINT_64);
        assert!(
            u64t.cmp(&(-1i64).to_le_bytes(), &1i64.to_le_bytes())
                .is_gt()
        );
        assert!(s.cmp(&(-1i32).to_le_bytes(), &1i32.to_le_bytes()).is_lt());
    }

    #[test]
    fn null_pages_and_nested_null_counts() {
        let t = ty(PhysicalType::INT64, None, ConvertedType::NONE);
        let ps = PageStats::compute(&t, &flat(Values::I64(vec![]), 4)).unwrap();
        assert!(ps.is_null_page() && ps.max.is_none() && ps.null_count == 4);
        // list<int64>: [[1, null], [], null, [9]] -> defs 3,2,1,0,3 with max_def 3.
        let d = ColumnData {
            values: Values::I64(vec![1, 9]),
            defs: Some(vec![3, 2, 1, 0, 3]),
            reps: Some(vec![0, 1, 0, 0, 0]),
            max_def: 3,
        };
        let ps = PageStats::compute(&t, &d).unwrap();
        assert_eq!(
            (ps.null_count, ps.min.unwrap(), ps.max.unwrap()),
            (3, 1i64.to_le_bytes().to_vec(), 9i64.to_le_bytes().to_vec())
        );
    }

    #[test]
    fn floats_total_order_nan_and_zero() {
        let t = ty(PhysicalType::DOUBLE, None, ConvertedType::NONE);
        let ps = PageStats::compute(
            &t,
            &flat(Values::F64(vec![0.0, f64::NAN, -0.0, 2.5, -1.0]), 1),
        )
        .unwrap();
        assert_eq!(ps.min.unwrap(), (-1.0f64).to_le_bytes().to_vec());
        assert_eq!(ps.max.unwrap(), 2.5f64.to_le_bytes().to_vec());
        assert_eq!(ps.nan_count, Some(1));
        let z = PageStats::compute(&t, &flat(Values::F64(vec![0.0, -0.0]), 0)).unwrap();
        assert_eq!(
            (z.min.unwrap(), z.max.unwrap()),
            (
                (-0.0f64).to_le_bytes().to_vec(),
                0.0f64.to_le_bytes().to_vec()
            )
        );
        // NaN-only pages get no statistics.
        assert!(PageStats::compute(&t, &flat(Values::F64(vec![f64::NAN]), 1)).is_none());
        let f = ty(PhysicalType::FLOAT, None, ConvertedType::NONE);
        let p =
            PageStats::compute(&f, &flat(Values::F32(vec![3.0, f32::NEG_INFINITY]), 0)).unwrap();
        assert_eq!(p.min.unwrap(), f32::NEG_INFINITY.to_le_bytes().to_vec());
        assert_eq!(p.nan_count, Some(0));
    }

    #[test]
    fn float16_and_decimals() {
        let h = ty(
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            Some(L::Float16),
            ConvertedType::NONE,
        );
        let f16 = |bits: u16| FixedLenByteArray::from(bits.to_le_bytes().to_vec());
        // 1.0 = 0x3c00, -2.0 = 0xc000, NaN = 0x7e00, +0 = 0, -0 = 0x8000
        let ps = PageStats::compute(
            &h,
            &flat(
                Values::Fixed(vec![f16(0x3c00), f16(0xc000), f16(0x7e00), f16(0x8000)]),
                0,
            ),
        )
        .unwrap();
        assert_eq!(
            (ps.min.unwrap(), ps.max.unwrap(), ps.nan_count),
            (vec![0x00, 0xc0], vec![0x00, 0x3c], Some(1))
        );
        let dec = ty(
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            None,
            ConvertedType::DECIMAL,
        );
        let be = |v: i32| FixedLenByteArray::from(v.to_be_bytes().to_vec());
        let ps = PageStats::compute(
            &dec,
            &flat(Values::Fixed(vec![be(5), be(-300), be(70000), be(-1)]), 0),
        )
        .unwrap();
        assert_eq!(
            (ps.min.unwrap(), ps.max.unwrap()),
            (
                (-300i32).to_be_bytes().to_vec(),
                70000i32.to_be_bytes().to_vec()
            )
        );
        let bd = ty(PhysicalType::BYTE_ARRAY, None, ConvertedType::DECIMAL);
        // Variable-length decimals compare by value, not by length or bytes.
        assert!(bd.cmp(&[0xff], &[0x00, 0x01]).is_lt());
        assert!(bd.cmp(&[0x7f], &[0x00, 0x80]).is_lt());
        assert!(bd.cmp(&[0xff, 0x7f], &[0x80]).is_lt()); // -129 < -128
        assert!(bd.cmp(&[0x00, 0x05], &[0x05]).is_eq());
    }

    #[test]
    fn byte_arrays_and_truncation() {
        let s = ty(
            PhysicalType::BYTE_ARRAY,
            Some(L::String),
            ConvertedType::UTF8,
        );
        let ba = |x: &[u8]| ByteArray::from(x.to_vec());
        let ps = PageStats::compute(
            &s,
            &flat(
                Values::Bytes(vec![ba(b"b"), ba(b"ab"), ba(b"\xc3\xa9"), ba(b"")]),
                0,
            ),
        )
        .unwrap();
        assert_eq!(
            (ps.min.unwrap(), ps.max.unwrap()),
            (b"".to_vec(), "é".as_bytes().to_vec())
        );
        let long_a = "a".repeat(100);
        let long_z = format!("{}{}", "z".repeat(63), "é".repeat(10));
        let ps = PageStats::compute(
            &s,
            &flat(
                Values::Bytes(vec![ba(long_a.as_bytes()), ba(long_z.as_bytes())]),
                0,
            ),
        )
        .unwrap();
        assert_eq!(ps.min.as_deref(), Some("a".repeat(64).as_bytes()));
        // 63 'z' + 'é' does not fit 64 bytes: cut to 63 'z', last one incremented to '{'.
        assert_eq!(
            ps.max.as_deref(),
            Some(format!("{}{{", "z".repeat(62)).as_bytes())
        );
        assert!(!ps.min_exact && !ps.max_exact);
        assert!(std::str::from_utf8(ps.max.as_ref().unwrap()).is_ok());
        let b = ty(PhysicalType::BYTE_ARRAY, None, ConvertedType::NONE);
        let mut hi = vec![0x10u8; 70];
        hi[63] = 0xff;
        let ps = PageStats::compute(&b, &flat(Values::Bytes(vec![ba(&hi)]), 0)).unwrap();
        let mut want = vec![0x10u8; 63];
        want[62] = 0x11;
        assert_eq!(ps.max.unwrap(), want);
        // Untruncatable max (all 0xff) stays exact.
        let ps = PageStats::compute(&b, &flat(Values::Bytes(vec![ba(&[0xff; 80])]), 0)).unwrap();
        assert!(ps.max_exact && ps.max.unwrap().len() == 80 && !ps.min_exact);
        // Short values are never truncated.
        let ps = PageStats::compute(&b, &flat(Values::Bytes(vec![ba(&[1; 64])]), 0)).unwrap();
        assert!(ps.min_exact && ps.max_exact);
    }

    #[test]
    fn merging_and_boundary_order() {
        let t = ty(PhysicalType::INT32, None, ConvertedType::NONE);
        let p = |lo: i32, hi: i32, nulls: u64| {
            PageStats::compute(&t, &flat(Values::I32(vec![lo, hi]), nulls as usize)).unwrap()
        };
        let null = PageStats::compute(&t, &flat(Values::I32(vec![]), 3)).unwrap();
        let (a, b, c) = (p(1, 5, 1), p(5, 9, 0), p(-2, 3, 2));
        let m = merge_pages(&t, [&a, &null, &b, &c]);
        assert_eq!(
            (m.min.unwrap(), m.max.unwrap(), m.null_count),
            (
                (-2i32).to_le_bytes().to_vec(),
                9i32.to_le_bytes().to_vec(),
                Some(6)
            )
        );
        assert!(m.min_exact && m.max_exact && m.nan_count.is_none() && m.distinct_count.is_none());
        let all_null = merge_pages(&t, [&null]);
        assert!(all_null.min.is_none() && all_null.null_count == Some(3));
        assert_eq!(
            boundary_order(&t, &[&a, &null, &b]),
            BoundaryOrder::ASCENDING
        );
        assert_eq!(boundary_order(&t, &[&b, &a]), BoundaryOrder::DESCENDING);
        assert_eq!(boundary_order(&t, &[&a, &b, &c]), BoundaryOrder::UNORDERED);
        assert_eq!(boundary_order(&t, &[&null]), BoundaryOrder::ASCENDING);
        // Inexact bound wins only when strictly better.
        let mut inexact = p(-2, 3, 0);
        inexact.min_exact = false;
        let m = merge_pages(&t, [&inexact, &c]);
        assert!(m.min_exact);
        let f = ty(PhysicalType::DOUBLE, None, ConvertedType::NONE);
        let fp = PageStats::compute(&f, &flat(Values::F64(vec![1.0, f64::NAN]), 0)).unwrap();
        let m = merge_pages(&f, [&fp, &fp]);
        assert_eq!(m.nan_count, Some(2));
    }

    #[test]
    fn page_stats_roundtrip_json() {
        let t = ty(PhysicalType::INT32, None, ConvertedType::NONE);
        let ps = PageStats::compute(&t, &flat(Values::I32(vec![1, 2]), 1)).unwrap();
        let back: PageStats = serde_json::from_str(&serde_json::to_string(&ps).unwrap()).unwrap();
        assert_eq!(ps, back);
    }
}
