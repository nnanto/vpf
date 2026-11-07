//! Minimal Thrift compact-protocol reader, sufficient to parse Parquet `PageHeader`s.
//!
//! The parquet crate does not expose its thrift structures publicly, and we need raw page
//! boundaries (header length + compressed size) to reuse physical page bytes verbatim.

use crate::error::{Error, Result};

const T_STOP: u8 = 0;
const T_BOOL_TRUE: u8 = 1;
const T_BOOL_FALSE: u8 = 2;
const T_BYTE: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;
const T_UUID: u8 = 13;

const MAX_DEPTH: usize = 64;

pub(crate) struct CompactReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> CompactReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    fn eof() -> Error {
        Error::Corrupt("unexpected end of thrift data".into())
    }

    fn byte(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(Self::eof)?;
        self.pos += 1;
        Ok(b)
    }

    fn skip_bytes(&mut self, n: usize) -> Result<()> {
        if self.buf.len() - self.pos < n {
            return Err(Self::eof());
        }
        self.pos += n;
        Ok(())
    }

    fn varint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;
        loop {
            let b = self.byte()?;
            if shift >= 64 {
                return Err(Error::Corrupt("varint too long".into()));
            }
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    fn zigzag(&mut self) -> Result<i64> {
        let v = self.varint()?;
        Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        let v = self.zigzag()?;
        i32::try_from(v).map_err(|_| Error::Corrupt("i32 out of range".into()))
    }

    /// Reads a field header. Returns `None` on STOP, otherwise `(field_id, type)`.
    /// Boolean fields carry their value in the type nibble.
    pub fn field_header(&mut self, last_id: &mut i16) -> Result<Option<(i16, u8)>> {
        let b = self.byte()?;
        let ty = b & 0x0f;
        if ty == T_STOP {
            return Ok(None);
        }
        let delta = (b >> 4) as i16;
        let id = if delta == 0 {
            let v = self.zigzag()?;
            i16::try_from(v).map_err(|_| Error::Corrupt("field id out of range".into()))?
        } else {
            last_id
                .checked_add(delta)
                .ok_or_else(|| Error::Corrupt("field id overflow".into()))?
        };
        *last_id = id;
        Ok(Some((id, ty)))
    }

    fn list_header(&mut self) -> Result<(usize, u8)> {
        let b = self.byte()?;
        let elem = b & 0x0f;
        let size = match b >> 4 {
            15 => self.varint()? as usize,
            n => n as usize,
        };
        Ok((size, elem))
    }

    /// Skips a value of the given compact type (field context: bools have no payload).
    pub fn skip_field(&mut self, ty: u8) -> Result<()> {
        self.skip(ty, true, 0)
    }

    fn skip(&mut self, ty: u8, in_field: bool, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(Error::Corrupt("thrift nesting too deep".into()));
        }
        match ty {
            T_BOOL_TRUE | T_BOOL_FALSE => {
                if !in_field {
                    self.byte()?;
                }
            }
            T_BYTE => self.skip_bytes(1)?,
            T_I16 | T_I32 | T_I64 => {
                self.varint()?;
            }
            T_DOUBLE => self.skip_bytes(8)?,
            T_BINARY => {
                let n = self.varint()? as usize;
                self.skip_bytes(n)?;
            }
            T_UUID => self.skip_bytes(16)?,
            T_LIST | T_SET => {
                let (n, elem) = self.list_header()?;
                for _ in 0..n {
                    self.skip(elem, false, depth + 1)?;
                }
            }
            T_MAP => {
                let n = self.varint()? as usize;
                if n > 0 {
                    let kv = self.byte()?;
                    for _ in 0..n {
                        self.skip(kv >> 4, false, depth + 1)?;
                        self.skip(kv & 0x0f, false, depth + 1)?;
                    }
                }
            }
            T_STRUCT => {
                let mut last = 0i16;
                while let Some((_, t)) = self.field_header(&mut last)? {
                    self.skip(t, true, depth + 1)?;
                }
            }
            other => return Err(Error::Corrupt(format!("unknown thrift type {other}"))),
        }
        Ok(())
    }

    fn expect_i32(&mut self, ty: u8) -> Result<i32> {
        if ty != T_I32 {
            return Err(Error::Corrupt(format!("expected i32, got type {ty}")));
        }
        self.read_i32()
    }
}

/// Parquet page types (thrift `PageType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Data,
    Index,
    Dictionary,
    DataV2,
}

impl PageKind {
    pub fn code(self) -> i64 {
        match self {
            PageKind::Data => 0,
            PageKind::Index => 1,
            PageKind::Dictionary => 2,
            PageKind::DataV2 => 3,
        }
    }

    pub fn from_code(c: i64) -> Result<Self> {
        Ok(match c {
            0 => PageKind::Data,
            1 => PageKind::Index,
            2 => PageKind::Dictionary,
            3 => PageKind::DataV2,
            _ => return Err(Error::Corrupt(format!("unknown page type {c}"))),
        })
    }

    pub fn is_data(self) -> bool {
        matches!(self, PageKind::Data | PageKind::DataV2)
    }
}

/// The subset of a Parquet `PageHeader` needed to address and account for a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageHeaderInfo {
    pub kind: PageKind,
    pub header_len: usize,
    pub uncompressed_size: usize,
    pub compressed_size: usize,
    /// Number of values (including nulls) for data pages, or dictionary entries.
    pub num_values: i64,
    /// Only present in data page v2 headers.
    pub num_rows: Option<i64>,
}

impl PageHeaderInfo {
    /// Total on-disk length of the page (header + compressed body).
    pub fn total_len(&self) -> usize {
        self.header_len + self.compressed_size
    }
}

fn parse_sub_num_values(r: &mut CompactReader, want_rows: bool) -> Result<(i64, Option<i64>)> {
    let mut last = 0i16;
    let mut num_values = None;
    let mut num_rows = None;
    while let Some((id, ty)) = r.field_header(&mut last)? {
        match id {
            1 => num_values = Some(r.expect_i32(ty)? as i64),
            3 if want_rows => num_rows = Some(r.expect_i32(ty)? as i64),
            _ => r.skip_field(ty)?,
        }
    }
    let nv = num_values.ok_or_else(|| Error::Corrupt("page header missing num_values".into()))?;
    if want_rows && num_rows.is_none() {
        return Err(Error::Corrupt(
            "data page v2 header missing num_rows".into(),
        ));
    }
    Ok((nv, num_rows))
}

/// Parses a Parquet page header at the start of `buf`.
pub fn parse_page_header(buf: &[u8]) -> Result<PageHeaderInfo> {
    let mut r = CompactReader::new(buf);
    let mut last = 0i16;
    let mut kind = None;
    let mut uncompressed = None;
    let mut compressed = None;
    let mut num_values = None;
    let mut num_rows = None;
    while let Some((id, ty)) = r.field_header(&mut last)? {
        match id {
            1 => kind = Some(PageKind::from_code(r.expect_i32(ty)? as i64)?),
            2 => uncompressed = Some(r.expect_i32(ty)?),
            3 => compressed = Some(r.expect_i32(ty)?),
            5 | 7 if ty == T_STRUCT => num_values = Some(parse_sub_num_values(&mut r, false)?.0),
            8 if ty == T_STRUCT => {
                let (nv, nr) = parse_sub_num_values(&mut r, true)?;
                num_values = Some(nv);
                num_rows = nr;
            }
            _ => r.skip_field(ty)?,
        }
    }
    let kind = kind.ok_or_else(|| Error::Corrupt("page header missing type".into()))?;
    let uncompressed =
        uncompressed.ok_or_else(|| Error::Corrupt("missing uncompressed size".into()))?;
    let compressed = compressed.ok_or_else(|| Error::Corrupt("missing compressed size".into()))?;
    if uncompressed < 0 || compressed < 0 {
        return Err(Error::Corrupt("negative page size".into()));
    }
    let num_values = match (kind, num_values) {
        (PageKind::Index, None) => 0,
        (_, Some(v)) if v >= 0 => v,
        _ => return Err(Error::Corrupt("missing or invalid num_values".into())),
    };
    Ok(PageHeaderInfo {
        kind,
        header_len: r.position(),
        uncompressed_size: uncompressed as usize,
        compressed_size: compressed as usize,
        num_values,
        num_rows,
    })
}

/// A page located within a column chunk byte buffer.
#[derive(Debug, Clone)]
pub struct LocatedPage {
    pub offset: usize,
    pub header: PageHeaderInfo,
}

/// Splits the bytes of a full column chunk into its pages.
pub fn split_pages(chunk: &[u8]) -> Result<Vec<LocatedPage>> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < chunk.len() {
        let header = parse_page_header(&chunk[pos..])?;
        let len = header.total_len();
        if pos + len > chunk.len() {
            return Err(Error::Corrupt("page extends beyond column chunk".into()));
        }
        out.push(LocatedPage {
            offset: pos,
            header,
        });
        pos += len;
    }
    Ok(out)
}
