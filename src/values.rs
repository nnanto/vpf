//! Physical (Parquet-level) column values for flat columns, plus a compact binary
//! serialization used for buffered batches and column defaults.

use parquet::basic::Type as PhysicalType;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};

use crate::error::{Error, Result};

/// Dense, non-null values of one physical type.
#[derive(Debug, Clone, PartialEq)]
pub enum Values {
    Bool(Vec<bool>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    I96(Vec<Int96>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bytes(Vec<ByteArray>),
    Fixed(Vec<FixedLenByteArray>),
}

macro_rules! each {
    ($v:expr, $x:ident => $body:expr) => {
        match $v {
            Values::Bool($x) => $body,
            Values::I32($x) => $body,
            Values::I64($x) => $body,
            Values::I96($x) => $body,
            Values::F32($x) => $body,
            Values::F64($x) => $body,
            Values::Bytes($x) => $body,
            Values::Fixed($x) => $body,
        }
    };
}

macro_rules! zip_same {
    ($a:expr, $b:expr, ($x:ident, $y:ident) => $body:expr) => {
        match ($a, $b) {
            (Values::Bool($x), Values::Bool($y)) => $body,
            (Values::I32($x), Values::I32($y)) => $body,
            (Values::I64($x), Values::I64($y)) => $body,
            (Values::I96($x), Values::I96($y)) => $body,
            (Values::F32($x), Values::F32($y)) => $body,
            (Values::F64($x), Values::F64($y)) => $body,
            (Values::Bytes($x), Values::Bytes($y)) => $body,
            (Values::Fixed($x), Values::Fixed($y)) => $body,
            _ => return Err(Error::Invalid("physical type mismatch".into())),
        }
    };
}

impl Values {
    pub fn empty(physical: PhysicalType) -> Self {
        match physical {
            PhysicalType::BOOLEAN => Values::Bool(vec![]),
            PhysicalType::INT32 => Values::I32(vec![]),
            PhysicalType::INT64 => Values::I64(vec![]),
            PhysicalType::INT96 => Values::I96(vec![]),
            PhysicalType::FLOAT => Values::F32(vec![]),
            PhysicalType::DOUBLE => Values::F64(vec![]),
            PhysicalType::BYTE_ARRAY => Values::Bytes(vec![]),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => Values::Fixed(vec![]),
        }
    }

    pub fn physical(&self) -> PhysicalType {
        match self {
            Values::Bool(_) => PhysicalType::BOOLEAN,
            Values::I32(_) => PhysicalType::INT32,
            Values::I64(_) => PhysicalType::INT64,
            Values::I96(_) => PhysicalType::INT96,
            Values::F32(_) => PhysicalType::FLOAT,
            Values::F64(_) => PhysicalType::DOUBLE,
            Values::Bytes(_) => PhysicalType::BYTE_ARRAY,
            Values::Fixed(_) => PhysicalType::FIXED_LEN_BYTE_ARRAY,
        }
    }

    pub fn len(&self) -> usize {
        each!(self, v => v.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn take_idx(&self, idx: &[usize]) -> Values {
        match self {
            Values::Bool(v) => Values::Bool(idx.iter().map(|&i| v[i]).collect()),
            Values::I32(v) => Values::I32(idx.iter().map(|&i| v[i]).collect()),
            Values::I64(v) => Values::I64(idx.iter().map(|&i| v[i]).collect()),
            Values::I96(v) => Values::I96(idx.iter().map(|&i| v[i].clone()).collect()),
            Values::F32(v) => Values::F32(idx.iter().map(|&i| v[i]).collect()),
            Values::F64(v) => Values::F64(idx.iter().map(|&i| v[i]).collect()),
            Values::Bytes(v) => Values::Bytes(idx.iter().map(|&i| v[i].clone()).collect()),
            Values::Fixed(v) => Values::Fixed(idx.iter().map(|&i| v[i].clone()).collect()),
        }
    }

    pub fn extend_from(&mut self, other: &Values) -> Result<()> {
        zip_same!(self, other, (a, b) => a.extend(b.iter().cloned()));
        Ok(())
    }

    /// Appends `other[i]` (other must be the same physical type).
    fn push_from(&mut self, other: &Values, i: usize) -> Result<()> {
        zip_same!(self, other, (a, b) => a.push(b[i].clone()));
        Ok(())
    }
}

/// Values of one leaf column for a run of rows, in Dremel form.
///
/// `defs` is `Some` iff `max_def > 0`; `reps` is `Some` iff the leaf is repeated. A level
/// carries a value iff its definition level equals `max_def`; `values` holds only those.
/// Without `reps` every level is one row (a flat column, or a nested leaf without
/// repetition); with `reps` a row starts at each repetition level 0. A required flat column
/// has neither levels nor `max_def` and stores one value per row.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnData {
    pub values: Values,
    pub defs: Option<Vec<i16>>,
    pub reps: Option<Vec<i16>>,
    pub max_def: i16,
}

/// Level range and value range of one row.
#[derive(Debug, Clone, Copy)]
struct RowSpan {
    levels: (usize, usize),
    values: (usize, usize),
}

impl ColumnData {
    /// A flat column: `defs` (0 = null, 1 = present) given iff the column is nullable.
    pub fn flat(values: Values, defs: Option<Vec<i16>>) -> Self {
        let max_def = defs.is_some() as i16;
        Self { values, defs, reps: None, max_def }
    }

    pub fn empty(physical: PhysicalType, nullable: bool) -> Self {
        Self::flat(Values::empty(physical), nullable.then(Vec::new))
    }

    /// An empty column with the given level structure.
    pub fn empty_leaf(physical: PhysicalType, max_def: i16, max_rep: i16) -> Self {
        Self {
            values: Values::empty(physical),
            defs: (max_def > 0).then(Vec::new),
            reps: (max_rep > 0).then(Vec::new),
            max_def,
        }
    }

    /// `n` rows whose top-level field is null/empty (all levels 0).
    pub fn nulls(physical: PhysicalType, max_def: i16, max_rep: i16, n: usize) -> Result<Self> {
        if max_def == 0 {
            return Err(Error::Invalid("required column needs a non-null value".into()));
        }
        Ok(Self {
            values: Values::empty(physical),
            defs: Some(vec![0; n]),
            reps: (max_rep > 0).then(|| vec![0; n]),
            max_def,
        })
    }

    fn num_levels(&self) -> usize {
        match &self.defs {
            Some(d) => d.len(),
            None => self.reps.as_ref().map_or(self.values.len(), Vec::len),
        }
    }

    pub fn num_rows(&self) -> usize {
        match &self.reps {
            Some(r) => r.iter().filter(|&&x| x == 0).count(),
            None => self.num_levels(),
        }
    }

    /// True for a flat nullable column (or a nested leaf whose top field is optional).
    pub fn nullable(&self) -> bool {
        self.defs.is_some()
    }

    /// True if the levels are those of a plain flat column.
    pub fn is_flat(&self) -> bool {
        self.reps.is_none() && self.max_def <= 1
    }

    pub fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(Error::Corrupt(format!("invalid levels: {m}")));
        if self.defs.is_some() != (self.max_def > 0) {
            return bad("definition levels presence does not match max level");
        }
        let n = self.num_levels();
        let present = match &self.defs {
            Some(d) => {
                if d.iter().any(|&x| x < 0 || x > self.max_def) {
                    return bad("definition level out of range");
                }
                d.iter().filter(|&&x| x == self.max_def).count()
            }
            None => n,
        };
        if present != self.values.len() {
            return bad("value count does not match definition levels");
        }
        if let Some(r) = &self.reps {
            if r.len() != n {
                return bad("repetition and definition level counts differ");
            }
            if r.first().is_some_and(|&x| x != 0) || r.iter().any(|&x| x < 0) {
                return bad("first repetition level must be 0");
            }
        }
        Ok(())
    }

    fn spans(&self) -> Vec<RowSpan> {
        let n = self.num_levels();
        let mut out = Vec::with_capacity(self.num_rows());
        let mut v = 0;
        let mut i = 0;
        while i < n {
            let (l0, v0) = (i, v);
            loop {
                if self.defs.as_ref().is_none_or(|d| d[i] == self.max_def) {
                    v += 1;
                }
                i += 1;
                if i >= n || self.reps.as_ref().is_none_or(|r| r[i] == 0) {
                    break;
                }
            }
            out.push(RowSpan { levels: (l0, i), values: (v0, v) });
        }
        out
    }

    fn with_levels(&self) -> ColumnData {
        ColumnData {
            values: Values::empty(self.values.physical()),
            defs: self.defs.as_ref().map(|_| Vec::new()),
            reps: self.reps.as_ref().map(|_| Vec::new()),
            max_def: self.max_def,
        }
    }

    fn push_span(&mut self, src: &ColumnData, s: RowSpan, idx: &mut Vec<usize>) {
        if let (Some(d), Some(sd)) = (&mut self.defs, &src.defs) {
            d.extend_from_slice(&sd[s.levels.0..s.levels.1]);
        }
        if let (Some(r), Some(sr)) = (&mut self.reps, &src.reps) {
            r.extend_from_slice(&sr[s.levels.0..s.levels.1]);
        }
        idx.extend(s.values.0..s.values.1);
    }

    /// Selects rows by index (in the given order).
    pub fn take(&self, rows: &[usize]) -> ColumnData {
        let spans = self.spans();
        let mut out = self.with_levels();
        let mut idx = Vec::new();
        for &r in rows {
            out.push_span(self, spans[r], &mut idx);
        }
        out.values = self.values.take_idx(&idx);
        out
    }

    /// Keeps rows where `keep[row]` is true.
    pub fn filter(&self, keep: &[bool]) -> ColumnData {
        let rows: Vec<usize> = keep.iter().enumerate().filter(|(_, k)| **k).map(|(i, _)| i).collect();
        self.take(&rows)
    }

    pub fn slice(&self, start: usize, len: usize) -> ColumnData {
        self.take(&(start..start + len).collect::<Vec<_>>())
    }

    pub fn append(&mut self, other: &ColumnData) -> Result<()> {
        if self.defs.is_some() != other.defs.is_some()
            || self.reps.is_some() != other.reps.is_some()
            || self.max_def != other.max_def
        {
            return Err(Error::Invalid("level structure mismatch".into()));
        }
        self.values.extend_from(&other.values)?;
        if let (Some(a), Some(b)) = (&mut self.defs, &other.defs) {
            a.extend_from_slice(b);
        }
        if let (Some(a), Some(b)) = (&mut self.reps, &other.reps) {
            a.extend_from_slice(b);
        }
        Ok(())
    }

    /// Replaces each row in `rows` with `value` (`None` = NULL, i.e. all levels 0 for nested
    /// leaves; `Some` must hold exactly one value and is only valid for flat columns).
    pub fn set_rows(&self, rows: &[usize], value: Option<&Values>) -> Result<ColumnData> {
        match value {
            Some(v) => {
                if !self.is_flat() {
                    return Err(Error::Unsupported("assigning a value to a nested column".into()));
                }
                if v.len() != 1 || v.physical() != self.values.physical() {
                    return Err(Error::Invalid("replacement must be a single value of the column type".into()));
                }
            }
            None if !self.nullable() => {
                return Err(Error::Invalid("cannot set NULL in a required column".into()));
            }
            None => {}
        }
        let spans = self.spans();
        let mut hit = vec![false; spans.len()];
        for &r in rows {
            *hit.get_mut(r).ok_or_else(|| Error::Invalid("row out of range".into()))? = true;
        }
        let mut out = self.with_levels();
        for (r, s) in spans.into_iter().enumerate() {
            if !hit[r] {
                out.push_span(self, s, &mut vec![]);
                for i in s.values.0..s.values.1 {
                    out.values.push_from(&self.values, i)?;
                }
                continue;
            }
            if let Some(r) = &mut out.reps {
                r.push(0);
            }
            match value {
                Some(v) => {
                    out.values.push_from(v, 0)?;
                    if let Some(d) = &mut out.defs {
                        d.push(self.max_def);
                    }
                }
                None => out.defs.as_mut().expect("nullable").push(0),
            }
        }
        Ok(out)
    }

    /// `n` copies of a value (`None` = NULL) for a flat column.
    pub fn repeat(physical: PhysicalType, nullable: bool, value: Option<&Values>, n: usize) -> Result<ColumnData> {
        let mut out = ColumnData::empty(physical, nullable);
        match value {
            None => {
                if !nullable {
                    return Err(Error::Invalid("required column needs a non-null value".into()));
                }
                out.defs = Some(vec![0; n]);
            }
            Some(v) => {
                if v.len() != 1 || v.physical() != physical {
                    return Err(Error::Invalid("default must be a single value of the column type".into()));
                }
                for _ in 0..n {
                    out.values.push_from(v, 0)?;
                }
                if nullable {
                    out.defs = Some(vec![1; n]);
                }
            }
        }
        Ok(out)
    }

    /// Lossless physical widening: INT32 -> INT64, FLOAT -> DOUBLE.
    pub fn widen(&self, to: PhysicalType) -> Result<ColumnData> {
        let values = match (&self.values, to) {
            (v, t) if v.physical() == t => v.clone(),
            (Values::I32(v), PhysicalType::INT64) => Values::I64(v.iter().map(|&x| x as i64).collect()),
            (Values::F32(v), PhysicalType::DOUBLE) => Values::F64(v.iter().map(|&x| x as f64).collect()),
            (v, t) => return Err(Error::Unsupported(format!("converting {} to {t}", v.physical()))),
        };
        Ok(ColumnData { values, ..self.clone() })
    }

    /// Adds one definition level above everything (the top-level field became optional).
    pub fn raise_defs(&self) -> ColumnData {
        let defs = match &self.defs {
            Some(d) => d.iter().map(|&x| x + 1).collect(),
            None => vec![1; self.num_levels()],
        };
        ColumnData { defs: Some(defs), max_def: self.max_def + 1, ..self.clone() }
    }

    /// Adapts nullability of a flat column: required -> optional adds levels;
    /// optional -> required succeeds only if there are no nulls.
    pub fn with_nullability(&self, nullable: bool) -> Result<ColumnData> {
        if !self.is_flat() {
            return Err(Error::Unsupported("changing nullability of a nested leaf".into()));
        }
        match (&self.defs, nullable) {
            (None, true) => Ok(ColumnData::flat(self.values.clone(), Some(vec![1; self.values.len()]))),
            (Some(d), false) => {
                if d.iter().any(|&l| l == 0) {
                    return Err(Error::Invalid("column contains NULLs but target is required".into()));
                }
                Ok(ColumnData::flat(self.values.clone(), None))
            }
            _ => Ok(self.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// Binary serialization: little-endian, length-prefixed.

const PAYLOAD_MAGIC: &[u8; 4] = b"VPB2";

fn phys_code(t: PhysicalType) -> u8 {
    match t {
        PhysicalType::BOOLEAN => 0,
        PhysicalType::INT32 => 1,
        PhysicalType::INT64 => 2,
        PhysicalType::INT96 => 3,
        PhysicalType::FLOAT => 4,
        PhysicalType::DOUBLE => 5,
        PhysicalType::BYTE_ARRAY => 6,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => 7,
    }
}

fn phys_from(c: u8) -> Result<PhysicalType> {
    Ok(match c {
        0 => PhysicalType::BOOLEAN,
        1 => PhysicalType::INT32,
        2 => PhysicalType::INT64,
        3 => PhysicalType::INT96,
        4 => PhysicalType::FLOAT,
        5 => PhysicalType::DOUBLE,
        6 => PhysicalType::BYTE_ARRAY,
        7 => PhysicalType::FIXED_LEN_BYTE_ARRAY,
        _ => return Err(Error::Corrupt(format!("bad physical type code {c}"))),
    })
}

struct W(Vec<u8>);
impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.0.extend_from_slice(b);
    }
}

struct R<'a>(&'a [u8], usize);
impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() - self.1 < n {
            return Err(Error::Corrupt("truncated payload".into()));
        }
        let s = &self.0[self.1..self.1 + n];
        self.1 += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn len(&mut self) -> Result<usize> {
        let n = self.u64()? as usize;
        if n > self.0.len() - self.1 {
            return Err(Error::Corrupt("length exceeds payload".into()));
        }
        Ok(n)
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.len()?;
        self.take(n)
    }
}

fn write_values(w: &mut W, v: &Values) {
    w.u8(phys_code(v.physical()));
    w.u64(v.len() as u64);
    match v {
        Values::Bool(x) => x.iter().for_each(|&b| w.u8(b as u8)),
        Values::I32(x) => x.iter().for_each(|i| w.0.extend_from_slice(&i.to_le_bytes())),
        Values::I64(x) => x.iter().for_each(|i| w.0.extend_from_slice(&i.to_le_bytes())),
        Values::I96(x) => x.iter().for_each(|i| i.data().iter().for_each(|p| w.0.extend_from_slice(&p.to_le_bytes()))),
        Values::F32(x) => x.iter().for_each(|i| w.0.extend_from_slice(&i.to_le_bytes())),
        Values::F64(x) => x.iter().for_each(|i| w.0.extend_from_slice(&i.to_le_bytes())),
        Values::Bytes(x) => x.iter().for_each(|b| w.bytes(b.data())),
        Values::Fixed(x) => x.iter().for_each(|b| w.bytes(b.data())),
    }
}

fn read_values(r: &mut R) -> Result<Values> {
    let phys = phys_from(r.u8()?)?;
    let n = r.u64()? as usize;
    // Every value needs at least one byte; guards against absurd allocations on corrupt input.
    if n > r.0.len() - r.1 {
        return Err(Error::Corrupt("value count exceeds payload".into()));
    }
    macro_rules! fixed {
        ($ty:ty, $w:expr, $variant:ident) => {{
            let raw = r.take(n * $w)?;
            Values::$variant(raw.chunks_exact($w).map(|c| <$ty>::from_le_bytes(c.try_into().unwrap())).collect())
        }};
    }
    Ok(match phys {
        PhysicalType::BOOLEAN => Values::Bool(r.take(n)?.iter().map(|&b| b != 0).collect()),
        PhysicalType::INT32 => fixed!(i32, 4, I32),
        PhysicalType::INT64 => fixed!(i64, 8, I64),
        PhysicalType::FLOAT => fixed!(f32, 4, F32),
        PhysicalType::DOUBLE => fixed!(f64, 8, F64),
        PhysicalType::INT96 => {
            let raw = r.take(n * 12)?;
            Values::I96(
                raw.chunks_exact(12)
                    .map(|c| {
                        let w = |i: usize| u32::from_le_bytes(c[i * 4..i * 4 + 4].try_into().unwrap());
                        let mut v = Int96::new();
                        v.set_data(w(0), w(1), w(2));
                        v
                    })
                    .collect(),
            )
        }
        PhysicalType::BYTE_ARRAY => {
            Values::Bytes((0..n).map(|_| r.bytes().map(|b| ByteArray::from(b.to_vec()))).collect::<Result<_>>()?)
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Values::Fixed(
            (0..n)
                .map(|_| r.bytes().map(|b| FixedLenByteArray::from(ByteArray::from(b.to_vec()))))
                .collect::<Result<_>>()?,
        ),
    })
}

fn write_levels(w: &mut W, l: &Option<Vec<i16>>) {
    match l {
        None => w.u8(0),
        Some(d) => {
            w.u8(1);
            w.u64(d.len() as u64);
            d.iter().for_each(|&x| w.0.extend_from_slice(&x.to_le_bytes()));
        }
    }
}

fn read_levels(r: &mut R) -> Result<Option<Vec<i16>>> {
    Ok(match r.u8()? {
        0 => None,
        1 => {
            let n = r.len()?;
            let raw = r.take(n.checked_mul(2).ok_or_else(|| Error::Corrupt("level count".into()))?)?;
            Some(raw.chunks_exact(2).map(|c| i16::from_le_bytes([c[0], c[1]])).collect())
        }
        _ => return Err(Error::Corrupt("bad level flag".into())),
    })
}

fn write_column(w: &mut W, c: &ColumnData) {
    w.0.extend_from_slice(&c.max_def.to_le_bytes());
    write_levels(w, &c.defs);
    write_levels(w, &c.reps);
    write_values(w, &c.values);
}

fn read_column(r: &mut R) -> Result<ColumnData> {
    let t = r.take(2)?;
    let max_def = i16::from_le_bytes([t[0], t[1]]);
    let defs = read_levels(r)?;
    let reps = read_levels(r)?;
    let c = ColumnData { values: read_values(r)?, defs, reps, max_def };
    c.validate()?;
    Ok(c)
}

/// Identifies a leaf column: (stable column id, leaf index within the field).
pub type LeafKey = (i64, i64);

/// Serializes a row batch keyed by leaf.
pub fn encode_batch(num_rows: usize, columns: &[(LeafKey, ColumnData)]) -> Result<Vec<u8>> {
    let mut w = W(Vec::new());
    w.0.extend_from_slice(PAYLOAD_MAGIC);
    w.u64(num_rows as u64);
    w.u64(columns.len() as u64);
    for ((id, leaf), c) in columns {
        if c.num_rows() != num_rows {
            return Err(Error::Invalid("column length differs from batch row count".into()));
        }
        w.u64(*id as u64);
        w.u64(*leaf as u64);
        write_column(&mut w, c);
    }
    Ok(w.0)
}

pub fn decode_batch(buf: &[u8]) -> Result<(usize, Vec<(LeafKey, ColumnData)>)> {
    let mut r = R(buf, 0);
    if r.take(4)? != PAYLOAD_MAGIC {
        return Err(Error::Corrupt("bad batch payload magic".into()));
    }
    let rows = r.u64()? as usize;
    let ncols = r.len()?;
    let mut cols = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let id = r.u64()? as i64;
        let leaf = r.u64()? as i64;
        let c = read_column(&mut r)?;
        if c.num_rows() != rows {
            return Err(Error::Corrupt("batch column length mismatch".into()));
        }
        cols.push(((id, leaf), c));
    }
    if r.1 != buf.len() {
        return Err(Error::Corrupt("trailing bytes in batch payload".into()));
    }
    Ok((rows, cols))
}

/// Serializes a single non-null value (column default).
pub fn encode_value(v: &Values) -> Result<Vec<u8>> {
    if v.len() != 1 {
        return Err(Error::Invalid("expected exactly one value".into()));
    }
    let mut w = W(Vec::new());
    write_values(&mut w, v);
    Ok(w.0)
}

pub fn decode_value(buf: &[u8]) -> Result<Values> {
    let mut r = R(buf, 0);
    let v = read_values(&mut r)?;
    if v.len() != 1 || r.1 != buf.len() {
        return Err(Error::Corrupt("bad encoded value".into()));
    }
    Ok(v)
}
