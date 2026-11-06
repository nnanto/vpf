//! Persisted form of column-chunk statistics.

use parquet::basic::Type as PhysicalType;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::statistics::{Statistics, ValueStatistics};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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
            b.try_into().map_err(|_| Error::Corrupt(format!("statistic has {} bytes, expected {N}", b.len())))
        }
        macro_rules! vs {
            ($variant:ident, $conv:expr) => {{
                let conv = $conv;
                let min = self.min.as_deref().map(&conv).transpose()?;
                let max = self.max.as_deref().map(&conv).transpose()?;
                Statistics::$variant(
                    ValueStatistics::new(min, max, self.distinct_count, self.null_count, self.deprecated)
                        .with_min_is_exact(self.min_exact)
                        .with_max_is_exact(self.max_exact)
                        .with_nan_count(self.nan_count),
                )
            }};
        }
        Ok(match physical {
            PhysicalType::BOOLEAN => vs!(Boolean, |b: &[u8]| -> Result<bool> { Ok(fixed::<1>(b)?[0] != 0) }),
            PhysicalType::INT32 => vs!(Int32, |b: &[u8]| -> Result<i32> { Ok(i32::from_le_bytes(fixed(b)?)) }),
            PhysicalType::INT64 => vs!(Int64, |b: &[u8]| -> Result<i64> { Ok(i64::from_le_bytes(fixed(b)?)) }),
            PhysicalType::FLOAT => vs!(Float, |b: &[u8]| -> Result<f32> { Ok(f32::from_le_bytes(fixed(b)?)) }),
            PhysicalType::DOUBLE => vs!(Double, |b: &[u8]| -> Result<f64> { Ok(f64::from_le_bytes(fixed(b)?)) }),
            PhysicalType::INT96 => vs!(Int96, |b: &[u8]| -> Result<Int96> {
                let a: [u8; 12] = fixed(b)?;
                let w = |i: usize| u32::from_le_bytes(a[i * 4..i * 4 + 4].try_into().unwrap());
                let mut v = Int96::new();
                v.set_data(w(0), w(1), w(2));
                Ok(v)
            }),
            PhysicalType::BYTE_ARRAY => vs!(ByteArray, |b: &[u8]| -> Result<ByteArray> { Ok(ByteArray::from(b.to_vec())) }),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => vs!(FixedLenByteArray, |b: &[u8]| -> Result<FixedLenByteArray> {
                Ok(FixedLenByteArray::from(ByteArray::from(b.to_vec())))
            }),
        })
    }
}
