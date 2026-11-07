//! Page-level codec: decode physical pages to [`ColumnData`] and encode [`ColumnData`] to
//! physical pages, both delegating the actual Parquet encoding work to parquet-rs.

use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{Compression, Encoding, Type as PhysicalType};
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::*;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;

use crate::error::{Error, Result};
use crate::layout::{ByteSource, ChunkLayout, FileProps, PageSrc, RowGroupLayout, VirtualFile};
use crate::meta::PageRow;
use crate::schema::Leaf;
use crate::stats::{PageStats, StatType, StoredStats};
use crate::store::BlobRead;
use crate::thrift::{PageHeaderInfo, PageKind, split_pages};
use crate::values::{ColumnData, Values};

/// Raw bytes of a stored or freshly encoded page.
#[derive(Debug, Clone)]
pub struct RawPage {
    pub bytes: Bytes,
    pub kind: PageKind,
    pub num_values: i64,
    pub uncompressed_len: i64,
}

impl RawPage {
    pub fn load(blobs: &dyn BlobRead, p: &PageRow) -> Result<Self> {
        Ok(Self {
            bytes: Bytes::from(blobs.read(p.blob_offset, p.blob_len)?),
            kind: p.kind,
            num_values: p.num_values,
            uncompressed_len: p.uncompressed_len,
        })
    }

    pub fn from_header(bytes: Bytes, h: &PageHeaderInfo) -> Self {
        Self {
            bytes,
            kind: h.kind,
            num_values: h.num_values,
            uncompressed_len: (h.header_len + h.uncompressed_size) as i64,
        }
    }
}

/// A column chunk encoded by parquet-rs, split into raw pages.
#[derive(Debug, Clone)]
pub struct EncodedChunk {
    pub pages: Vec<(Bytes, PageHeaderInfo)>,
    pub encodings: Vec<Encoding>,
    pub stats: Option<StoredStats>,
    pub num_values: i64,
    pub codec: Compression,
    /// Rows held by each page (0 for dictionary pages); pages start on row boundaries.
    pub page_rows: Vec<i64>,
    /// Statistics of each page (`None` for dictionary pages and unordered types).
    pub page_stats: Vec<Option<PageStats>>,
}

impl EncodedChunk {
    pub fn layout(&self, physical: PhysicalType) -> Result<ChunkLayout> {
        Ok(ChunkLayout {
            pages: self
                .pages
                .iter()
                .zip(&self.page_rows)
                .zip(&self.page_stats)
                .map(|(((b, h), &rows), st)| PageSrc {
                    src: ByteSource::Mem(b.clone()),
                    len: b.len() as u64,
                    kind: h.kind,
                    num_values: h.num_values,
                    uncompressed_len: (h.header_len + h.uncompressed_size) as i64,
                    // 0 rows on a data page means unknown (records spanning pages).
                    num_rows: (h.kind.is_data() && rows > 0).then_some(rows),
                    stats: st.clone(),
                })
                .collect(),
            codec: self.codec,
            encodings: self.encodings.clone(),
            stats: self
                .stats
                .as_ref()
                .map(|s| s.to_parquet(physical))
                .transpose()?,
            num_values: self.num_values,
        })
    }
}

fn single_leaf_file(
    leaf: &Leaf,
    codec: Compression,
    pages: &[RawPage],
) -> Result<Option<VirtualFile>> {
    let num_values: i64 = pages
        .iter()
        .filter(|p| p.kind.is_data())
        .map(|p| p.num_values)
        .sum();
    if num_values == 0 {
        return Ok(None);
    }
    let chunk = ChunkLayout {
        pages: pages
            .iter()
            .map(|p| PageSrc {
                src: ByteSource::Mem(p.bytes.clone()),
                len: p.bytes.len() as u64,
                kind: p.kind,
                num_values: p.num_values,
                uncompressed_len: p.uncompressed_len,
                num_rows: None,
                stats: None,
            })
            .collect(),
        codec,
        encodings: vec![],
        stats: None,
        num_values,
    };
    // The row count only feeds the synthetic footer; reading stops when the pages run out.
    Ok(Some(VirtualFile::build(
        FileProps {
            schema: leaf.descriptor()?,
            created_by: None,
            key_value: None,
            version: 1,
            blobs: None,
        },
        vec![RowGroupLayout {
            num_rows: num_values,
            chunks: vec![chunk],
        }],
    )?))
}

/// Decodes a sequence of pages of one leaf column chunk (optionally starting with its
/// dictionary page), including definition and repetition levels.
pub fn decode_pages(leaf: &Leaf, codec: Compression, pages: &[RawPage]) -> Result<ColumnData> {
    let Some(vf) = single_leaf_file(leaf, codec, pages)? else {
        return Ok(ColumnData::empty_leaf(
            leaf.physical(),
            leaf.max_def,
            leaf.max_rep,
        ));
    };
    let reader = SerializedFileReader::new(vf)?;
    let col = reader.get_row_group(0)?.get_column_reader(0)?;
    let out = read_leaf(col, leaf.physical(), leaf.max_def, leaf.max_rep, None)?;
    let levels: i64 = pages
        .iter()
        .filter(|p| p.kind.is_data())
        .map(|p| p.num_values)
        .sum();
    let got = out.defs.as_ref().map_or(out.values.len(), Vec::len) as i64;
    if got != levels {
        return Err(Error::Corrupt(format!(
            "decoded {got} levels, pages declare {levels}"
        )));
    }
    Ok(out)
}

/// Rows held by each page of a chunk (0 for dictionary pages), or `None` if some data page
/// does not start at a row boundary (possible for repeated columns in V1 pages written by
/// other tools), in which case the pages cannot be addressed individually.
pub fn chunk_page_rows(
    leaf: &Leaf,
    codec: Compression,
    pages: &[RawPage],
) -> Result<Option<Vec<i64>>> {
    if leaf.max_rep == 0 {
        return Ok(Some(
            pages
                .iter()
                .map(|p| if p.kind.is_data() { p.num_values } else { 0 })
                .collect(),
        ));
    }
    let data = decode_pages(leaf, codec, pages)?;
    Ok(rows_per_page(
        data.reps.as_deref().unwrap_or(&[]),
        pages.iter().map(|p| (p.kind.is_data(), p.num_values)),
    ))
}

fn rows_per_page(reps: &[i16], pages: impl Iterator<Item = (bool, i64)>) -> Option<Vec<i64>> {
    let mut at = 0usize;
    let mut out = Vec::new();
    for (is_data, n) in pages {
        if !is_data {
            out.push(0);
            continue;
        }
        let slice = reps.get(at..at + n as usize)?;
        if slice.first().is_some_and(|&r| r != 0) {
            return None;
        }
        out.push(slice.iter().filter(|&&r| r == 0).count() as i64);
        at += n as usize;
    }
    (at == reps.len()).then_some(out)
}

/// Reads a leaf column: exactly `want` rows, or everything when `want` is `None`.
pub fn read_leaf(
    col: ColumnReader,
    physical: PhysicalType,
    max_def: i16,
    max_rep: i16,
    want: Option<usize>,
) -> Result<ColumnData> {
    fn drain<T: DataType>(
        mut r: ColumnReaderImpl<T>,
        want: Option<usize>,
        defs: Option<&mut Vec<i16>>,
        mut reps: Option<&mut Vec<i16>>,
        values: &mut Vec<T::T>,
    ) -> Result<()> {
        let mut defs = defs;
        let mut got = 0;
        let step = want.unwrap_or(usize::MAX);
        loop {
            let ask = match want {
                Some(w) if got >= w => break,
                Some(w) => w - got,
                None => step.min(1 << 20),
            };
            let (recs, _, _) =
                r.read_records(ask, defs.as_deref_mut(), reps.as_deref_mut(), values)?;
            if recs == 0 {
                break;
            }
            got += recs;
        }
        if let Some(w) = want
            && got != w
        {
            return Err(Error::Corrupt(format!("decoded {got} rows, expected {w}")));
        }
        Ok(())
    }

    let mut out = ColumnData::empty_leaf(physical, max_def, max_rep);
    let mut defs = Vec::new();
    let mut reps = Vec::new();
    let d = (max_def > 0).then_some(&mut defs);
    let r = (max_rep > 0).then_some(&mut reps);
    match (col, &mut out.values) {
        (ColumnReader::BoolColumnReader(c), Values::Bool(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::Int32ColumnReader(c), Values::I32(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::Int64ColumnReader(c), Values::I64(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::Int96ColumnReader(c), Values::I96(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::FloatColumnReader(c), Values::F32(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::DoubleColumnReader(c), Values::F64(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::ByteArrayColumnReader(c), Values::Bytes(v)) => drain(c, want, d, r, v)?,
        (ColumnReader::FixedLenByteArrayColumnReader(c), Values::Fixed(v)) => {
            drain(c, want, d, r, v)?
        }
        _ => return Err(Error::Corrupt("column reader type mismatch".into())),
    }
    if max_def > 0 {
        out.defs = Some(defs);
    }
    if max_rep > 0 {
        out.reps = Some(reps);
    }
    out.validate()?;
    Ok(out)
}

/// Reads exactly `want` rows of a flat column into physical values.
pub fn read_column(
    col: ColumnReader,
    physical: PhysicalType,
    nullable: bool,
    want: usize,
) -> Result<ColumnData> {
    read_leaf(col, physical, nullable as i16, 0, Some(want))
}

/// Target uncompressed data page size for pages written by vpf.
pub const DATA_PAGE_SIZE: usize = 1024 * 1024;
pub const DATA_PAGE_ROWS: usize = 20_000;

/// Encodes values as a single column chunk (V1 data pages, optional dictionary).
pub fn encode_column(
    leaf: &Leaf,
    codec: Compression,
    data: &ColumnData,
    dictionary: bool,
) -> Result<EncodedChunk> {
    let spec = &leaf.spec;
    if data.num_rows() == 0 {
        return Err(Error::Invalid("cannot encode an empty column chunk".into()));
    }
    if data.max_def != leaf.max_def
        || data.reps.is_some() != (leaf.max_rep > 0)
        || data.values.physical() != spec.physical
    {
        return Err(Error::Invalid(format!(
            "data does not match column '{}' type",
            spec.name
        )));
    }
    data.validate()?;
    if let Some(r) = &data.reps
        && r.iter().any(|&x| x > leaf.max_rep)
    {
        return Err(Error::Invalid(format!(
            "repetition level out of range for column '{}'",
            spec.name
        )));
    }
    if codec == Compression::LZO {
        return Err(Error::Unsupported("writing LZO-compressed pages".into()));
    }
    let descr = leaf.descriptor()?;
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(codec)
        .set_dictionary_enabled(dictionary)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_offset_index_disabled(true)
        .set_data_page_size_limit(DATA_PAGE_SIZE)
        .set_data_page_row_count_limit(DATA_PAGE_ROWS)
        .set_max_row_group_row_count(None)
        .build();
    let mut buf = Vec::new();
    {
        let mut w = SerializedFileWriter::new(&mut buf, descr.root_schema_ptr(), Arc::new(props))?;
        let mut rg = w.next_row_group()?;
        let mut col = rg
            .next_column()?
            .ok_or_else(|| Error::Corrupt("no column writer".into()))?;
        let defs = data.defs.as_deref();
        let reps = data.reps.as_deref();
        match &data.values {
            Values::Bool(v) => col.typed::<BoolType>().write_batch(v, defs, reps)?,
            Values::I32(v) => col.typed::<Int32Type>().write_batch(v, defs, reps)?,
            Values::I64(v) => col.typed::<Int64Type>().write_batch(v, defs, reps)?,
            Values::I96(v) => col.typed::<Int96Type>().write_batch(v, defs, reps)?,
            Values::F32(v) => col.typed::<FloatType>().write_batch(v, defs, reps)?,
            Values::F64(v) => col.typed::<DoubleType>().write_batch(v, defs, reps)?,
            Values::Bytes(v) => col.typed::<ByteArrayType>().write_batch(v, defs, reps)?,
            Values::Fixed(v) => {
                if let Some(bad) = v.iter().find(|b| b.len() as i32 != spec.type_length) {
                    return Err(Error::Invalid(format!(
                        "fixed-length value of {} bytes for column '{}' of length {}",
                        bad.len(),
                        spec.name,
                        spec.type_length
                    )));
                }
                col.typed::<FixedLenByteArrayType>()
                    .write_batch(v, defs, reps)?
            }
        };
        col.close()?;
        rg.close()?;
        w.close()?;
    }
    let bytes = Bytes::from(buf);
    let md = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
    if md.num_row_groups() != 1 {
        return Err(Error::Corrupt(
            "encoder produced multiple row groups".into(),
        ));
    }
    let cmd = md.row_group(0).column(0);
    let levels = data.defs.as_ref().map_or(data.values.len(), Vec::len) as i64;
    if cmd.num_values() != levels || md.row_group(0).num_rows() != data.num_rows() as i64 {
        return Err(Error::Corrupt("encoder value count mismatch".into()));
    }
    let (start, len) = cmd.byte_range();
    let chunk = bytes.slice(start as usize..(start + len) as usize);
    let pages = split_pages(&chunk)?
        .into_iter()
        .map(|p| {
            (
                chunk.slice(p.offset..p.offset + p.header.total_len()),
                p.header,
            )
        })
        .collect::<Vec<(Bytes, PageHeaderInfo)>>();
    let page_rows = match &data.reps {
        None => pages
            .iter()
            .map(|(_, h)| if h.kind.is_data() { h.num_values } else { 0 })
            .collect(),
        Some(r) => rows_per_page(
            r,
            pages.iter().map(|(_, h)| (h.kind.is_data(), h.num_values)),
        )
        .ok_or_else(|| Error::Corrupt("encoder split a record across pages".into()))?,
    };
    let page_stats = page_stats(&leaf.stat_type()?, data, &page_rows);
    Ok(EncodedChunk {
        page_stats,
        page_rows,
        pages,
        encodings: cmd.encodings().collect(),
        stats: cmd.statistics().map(StoredStats::from_parquet),
        num_values: cmd.num_values(),
        codec,
    })
}

/// Statistics of each page of a chunk holding `data`, split by `page_rows` (0 rows marks a
/// dictionary page, which gets `None`).
pub fn page_stats(t: &StatType, data: &ColumnData, page_rows: &[i64]) -> Vec<Option<PageStats>> {
    let mut start = 0usize;
    page_rows
        .iter()
        .map(|&rows| {
            if rows == 0 {
                return None;
            }
            let rows = rows as usize;
            let page = if start == 0 && rows == data.num_rows() {
                None
            } else {
                Some(data.slice(start, rows))
            };
            start += rows;
            PageStats::compute(t, page.as_ref().unwrap_or(data))
        })
        .collect()
}
