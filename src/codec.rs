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
use crate::meta::PageRow;
use crate::store::BlobStore;
use crate::layout::{ByteSource, ChunkLayout, FileProps, PageSrc, RowGroupLayout, VirtualFile};
use crate::schema::{ColumnSpec, build_descriptor};
use crate::stats::StoredStats;
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
    pub fn load(blobs: &BlobStore, p: &PageRow) -> Result<Self> {
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
}

impl EncodedChunk {
    pub fn layout(&self, physical: PhysicalType) -> Result<ChunkLayout> {
        Ok(ChunkLayout {
            pages: self
                .pages
                .iter()
                .map(|(b, h)| PageSrc {
                    src: ByteSource::Mem(b.clone()),
                    len: b.len() as u64,
                    kind: h.kind,
                    num_values: h.num_values,
                    uncompressed_len: (h.header_len + h.uncompressed_size) as i64,
                })
                .collect(),
            codec: self.codec,
            encodings: self.encodings.clone(),
            stats: self.stats.as_ref().map(|s| s.to_parquet(physical)).transpose()?,
            num_values: self.num_values,
        })
    }
}

/// Decodes a sequence of pages of one column chunk (optionally starting with its
/// dictionary page) into physical values. `spec` must describe the chunk's physical
/// type and nullability.
pub fn decode_pages(spec: &ColumnSpec, codec: Compression, pages: &[RawPage]) -> Result<ColumnData> {
    let num_rows: i64 = pages.iter().filter(|p| p.kind.is_data()).map(|p| p.num_values).sum();
    let mut out = ColumnData::empty(spec.physical, spec.nullable);
    if num_rows == 0 {
        return Ok(out);
    }
    let descr = build_descriptor("m", std::slice::from_ref(spec))?;
    let chunk = ChunkLayout {
        pages: pages
            .iter()
            .map(|p| PageSrc {
                src: ByteSource::Mem(p.bytes.clone()),
                len: p.bytes.len() as u64,
                kind: p.kind,
                num_values: p.num_values,
                uncompressed_len: p.uncompressed_len,
            })
            .collect(),
        codec,
        encodings: vec![],
        stats: None,
        num_values: num_rows,
    };
    let vf = VirtualFile::build(
        FileProps { schema: descr, created_by: None, key_value: None, version: 1, blobs: None },
        vec![RowGroupLayout { num_rows, chunks: vec![chunk] }],
    )?;
    let reader = SerializedFileReader::new(vf)?;
    let rg = reader.get_row_group(0)?;
    let col = rg.get_column_reader(0)?;
    let want = num_rows as usize;

    fn drain<T: DataType>(
        mut r: ColumnReaderImpl<T>,
        want: usize,
        nullable: bool,
        values: &mut Vec<T::T>,
        defs: &mut Vec<i16>,
    ) -> Result<()> {
        let mut got = 0;
        while got < want {
            let (recs, _, _) = r.read_records(want - got, nullable.then_some(&mut *defs), None, values)?;
            if recs == 0 {
                break;
            }
            got += recs;
        }
        if got != want {
            return Err(Error::Corrupt(format!("decoded {got} rows, expected {want}")));
        }
        Ok(())
    }

    let mut defs = Vec::new();
    let nullable = spec.nullable;
    match (col, &mut out.values) {
        (ColumnReader::BoolColumnReader(r), Values::Bool(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::Int32ColumnReader(r), Values::I32(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::Int64ColumnReader(r), Values::I64(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::Int96ColumnReader(r), Values::I96(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::FloatColumnReader(r), Values::F32(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::DoubleColumnReader(r), Values::F64(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::ByteArrayColumnReader(r), Values::Bytes(v)) => drain(r, want, nullable, v, &mut defs)?,
        (ColumnReader::FixedLenByteArrayColumnReader(r), Values::Fixed(v)) => {
            drain(r, want, nullable, v, &mut defs)?
        }
        _ => return Err(Error::Corrupt("column reader type mismatch".into())),
    }
    if nullable {
        out.defs = Some(defs);
    }
    out.validate()?;
    Ok(out)
}

/// Target uncompressed data page size for pages written by vpf.
pub const DATA_PAGE_SIZE: usize = 1024 * 1024;
pub const DATA_PAGE_ROWS: usize = 20_000;

/// Encodes values as a single column chunk (V1 data pages, optional dictionary).
pub fn encode_column(spec: &ColumnSpec, codec: Compression, data: &ColumnData, dictionary: bool) -> Result<EncodedChunk> {
    if data.num_rows() == 0 {
        return Err(Error::Invalid("cannot encode an empty column chunk".into()));
    }
    if data.nullable() != spec.nullable || data.values.physical() != spec.physical {
        return Err(Error::Invalid(format!("data does not match column '{}' type", spec.name)));
    }
    if codec == Compression::LZO {
        return Err(Error::Unsupported("writing LZO-compressed pages".into()));
    }
    let descr = build_descriptor("m", std::slice::from_ref(spec))?;
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
        let mut col = rg.next_column()?.ok_or_else(|| Error::Corrupt("no column writer".into()))?;
        let defs = data.defs.as_deref();
        match &data.values {
            Values::Bool(v) => col.typed::<BoolType>().write_batch(v, defs, None)?,
            Values::I32(v) => col.typed::<Int32Type>().write_batch(v, defs, None)?,
            Values::I64(v) => col.typed::<Int64Type>().write_batch(v, defs, None)?,
            Values::I96(v) => col.typed::<Int96Type>().write_batch(v, defs, None)?,
            Values::F32(v) => col.typed::<FloatType>().write_batch(v, defs, None)?,
            Values::F64(v) => col.typed::<DoubleType>().write_batch(v, defs, None)?,
            Values::Bytes(v) => col.typed::<ByteArrayType>().write_batch(v, defs, None)?,
            Values::Fixed(v) => {
                if let Some(bad) = v.iter().find(|b| b.len() as i32 != spec.type_length) {
                    return Err(Error::Invalid(format!(
                        "fixed-length value of {} bytes for column '{}' of length {}",
                        bad.len(),
                        spec.name,
                        spec.type_length
                    )));
                }
                col.typed::<FixedLenByteArrayType>().write_batch(v, defs, None)?
            }
        };
        col.close()?;
        rg.close()?;
        w.close()?;
    }
    let bytes = Bytes::from(buf);
    let md = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
    if md.num_row_groups() != 1 {
        return Err(Error::Corrupt("encoder produced multiple row groups".into()));
    }
    let cmd = md.row_group(0).column(0);
    if cmd.num_values() != data.num_rows() as i64 {
        return Err(Error::Corrupt("encoder value count mismatch".into()));
    }
    let (start, len) = cmd.byte_range();
    let chunk = bytes.slice(start as usize..(start + len) as usize);
    let pages = split_pages(&chunk)?
        .into_iter()
        .map(|p| (chunk.slice(p.offset..p.offset + p.header.total_len()), p.header))
        .collect();
    Ok(EncodedChunk {
        pages,
        encodings: cmd.encodings().collect(),
        stats: cmd.statistics().map(StoredStats::from_parquet),
        num_values: cmd.num_values(),
        codec,
    })
}
