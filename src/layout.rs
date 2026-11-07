//! Virtual Parquet file assembly: maps a list of row groups whose pages live in the blob
//! store (or in memory) to a contiguous, byte-addressable Parquet file with a generated footer.

use std::io::Read;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::{
    ColumnChunkMetaData, ColumnIndexBuilder, FileMetaData, KeyValue, OffsetIndexBuilder,
    ParquetMetaData, ParquetMetaDataWriter, RowGroupMetaData, page_index::PageIndexBuilder,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::Statistics;
use parquet::file::writer::TrackedWrite;
use parquet::schema::types::SchemaDescPtr;

use crate::error::{Error, Result};
use crate::stats::{PageStats, StatType, boundary_order};
use crate::store::{BlobRead, BlobStore};
use crate::thrift::PageKind;

const MAGIC: &[u8; 4] = b"PAR1";

#[derive(Debug, Clone)]
pub enum ByteSource {
    /// Bytes at an offset of the blob store.
    Blob { offset: u64 },
    /// Exact page bytes held in memory (synthesized pages).
    Mem(Bytes),
}

#[derive(Debug, Clone)]
pub struct PageSrc {
    pub src: ByteSource,
    pub len: u64,
    pub kind: PageKind,
    pub num_values: i64,
    pub uncompressed_len: i64,
    /// Whole rows held by a data page; `None` if unknown (no offset index is emitted).
    pub num_rows: Option<i64>,
    /// Data page statistics; a column index is emitted iff every data page has them.
    pub stats: Option<PageStats>,
}

#[derive(Debug, Clone)]
pub struct ChunkLayout {
    pub pages: Vec<PageSrc>,
    pub codec: Compression,
    pub encodings: Vec<Encoding>,
    pub stats: Option<Statistics>,
    pub num_values: i64,
}

#[derive(Debug, Clone)]
pub struct RowGroupLayout {
    pub num_rows: i64,
    /// One chunk per schema leaf, in schema order.
    pub chunks: Vec<ChunkLayout>,
}

#[derive(Debug, Clone)]
struct Extent {
    voff: u64,
    len: u64,
    src: ByteSource,
}

struct Inner {
    size: u64,
    extents: Vec<Extent>,
    metadata: Arc<ParquetMetaData>,
    footer_len: u64,
    blobs: Option<Arc<BlobStore>>,
}

/// An immutable, frozen virtual Parquet file. Cheap to clone.
#[derive(Clone)]
pub struct VirtualFile {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for VirtualFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualFile")
            .field("size", &self.inner.size)
            .field("extents", &self.inner.extents.len())
            .finish()
    }
}

pub struct FileProps<'a> {
    pub schema: SchemaDescPtr,
    pub created_by: Option<String>,
    pub key_value: Option<Vec<KeyValue>>,
    pub version: i32,
    pub blobs: Option<&'a Arc<BlobStore>>,
}

impl VirtualFile {
    pub fn build(props: FileProps<'_>, row_groups: Vec<RowGroupLayout>) -> Result<Self> {
        let ncols = props.schema.num_columns();
        let mut extents = vec![Extent {
            voff: 0,
            len: 4,
            src: ByteSource::Mem(Bytes::from_static(MAGIC)),
        }];
        let mut voff = 4u64;
        let mut rg_meta = Vec::with_capacity(row_groups.len());
        let mut total_rows = 0i64;
        let mut index = PageIndexBuilder::new(row_groups.len(), ncols);
        let mut has_index = false;
        for (rgi, rg) in row_groups.into_iter().enumerate() {
            if rg.chunks.len() != ncols {
                return Err(Error::Corrupt(format!(
                    "row group has {} chunks, schema has {ncols} columns",
                    rg.chunks.len()
                )));
            }
            let rg_start = voff;
            let mut cols = Vec::with_capacity(ncols);
            let mut total_uncompressed = 0i64;
            for (ci, chunk) in rg.chunks.into_iter().enumerate() {
                let mut dict_off = None;
                let mut data_off = None;
                let mut comp = 0i64;
                let mut uncomp = 0i64;
                let mut data_rows = 0i64;
                let stat_type = StatType::of_column(props.schema.column(ci).as_ref());
                let mut oi = Some(OffsetIndexBuilder::new());
                let mut first_row = 0i64;
                let mut page_stats: Option<Vec<PageStats>> = Some(Vec::new());
                for page in chunk.pages {
                    match page.kind {
                        PageKind::Dictionary => {
                            if dict_off.is_some() || data_off.is_some() {
                                return Err(Error::Corrupt(
                                    "dictionary page must be first in chunk".into(),
                                ));
                            }
                            dict_off = Some(voff as i64);
                        }
                        PageKind::Data | PageKind::DataV2 => {
                            data_off.get_or_insert(voff as i64);
                            data_rows += page.num_values;
                            match (page.num_rows, i32::try_from(page.len)) {
                                (Some(rows), Ok(len)) if oi.is_some() => {
                                    let b = oi.as_mut().unwrap();
                                    b.append_offset_and_size(voff as i64, len);
                                    b.append_row_count(rows);
                                    first_row += rows;
                                }
                                _ => oi = None,
                            }
                            match (page.stats, page_stats.as_mut()) {
                                (Some(st), Some(v)) => v.push(st),
                                _ => page_stats = None,
                            }
                        }
                        PageKind::Index => {}
                    }
                    if let ByteSource::Mem(b) = &page.src {
                        if b.len() as u64 != page.len {
                            return Err(Error::Corrupt("in-memory page length mismatch".into()));
                        }
                    }
                    comp += page.len as i64;
                    uncomp += page.uncompressed_len;
                    extents.push(Extent {
                        voff,
                        len: page.len,
                        src: page.src,
                    });
                    voff += page.len;
                }
                let data_off = data_off
                    .ok_or_else(|| Error::Corrupt("column chunk without data pages".into()))?;
                if data_rows != chunk.num_values {
                    return Err(Error::Corrupt(
                        "chunk num_values does not match pages".into(),
                    ));
                }
                if oi.is_some() && first_row != rg.num_rows {
                    return Err(Error::Corrupt(format!(
                        "chunk pages hold {first_row} rows, row group has {}",
                        rg.num_rows
                    )));
                }
                if let Some(oi) = oi {
                    index.put_offset_index(oi.build(), rgi, ci);
                    has_index = true;
                    if let Some(ps) = page_stats {
                        index.put_column_index(
                            column_index(&stat_type, props.schema.column(ci).physical_type(), &ps)?,
                            rgi,
                            ci,
                        );
                    }
                }
                let mut b = ColumnChunkMetaData::builder(props.schema.column(ci))
                    .set_compression(chunk.codec)
                    .set_encodings(chunk.encodings)
                    .set_num_values(chunk.num_values)
                    .set_total_compressed_size(comp)
                    .set_total_uncompressed_size(uncomp)
                    .set_data_page_offset(data_off)
                    .set_dictionary_page_offset(dict_off);
                if let Some(s) = chunk.stats {
                    b = b.set_statistics(s);
                }
                cols.push(b.build()?);
                total_uncompressed += uncomp;
            }
            rg_meta.push(
                RowGroupMetaData::builder(props.schema.clone())
                    .set_num_rows(rg.num_rows)
                    .set_total_byte_size(total_uncompressed)
                    .set_column_metadata(cols)
                    .set_file_offset(rg_start as i64)
                    .build()?,
            );
            total_rows += rg.num_rows;
        }
        let file_meta = FileMetaData::new(
            props.version,
            total_rows,
            props.created_by,
            props.key_value,
            props.schema.clone(),
            None,
        );
        let mut metadata = ParquetMetaData::new(file_meta, rg_meta);
        if has_index {
            metadata = metadata
                .into_builder()
                .set_page_index(Some(Arc::new(index.build())))
                .build();
        }
        let footer = write_footer(&metadata, voff)?;
        let footer_len = footer.len() as u64;
        extents.push(Extent {
            voff,
            len: footer_len,
            src: ByteSource::Mem(Bytes::from(footer)),
        });
        voff += footer_len;
        let needs_blobs = extents
            .iter()
            .any(|e| matches!(e.src, ByteSource::Blob { .. }));
        if needs_blobs && props.blobs.is_none() {
            return Err(Error::Invalid(
                "blob-backed pages require a blob store".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                size: voff,
                extents,
                metadata: Arc::new(metadata),
                footer_len,
                blobs: props.blobs.cloned(),
            }),
        })
    }

    pub fn size(&self) -> u64 {
        self.inner.size
    }

    /// Metadata describing this virtual file, including its page index. The page index
    /// locations (`column_index_offset` etc.) are only set in the serialized footer.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.inner.metadata
    }

    /// Length of the footer section (page indexes + metadata + 4-byte length + magic).
    pub fn footer_len(&self) -> u64 {
        self.inner.footer_len
    }

    /// Reads up to `buf.len()` bytes at `offset`; returns bytes read (0 at EOF).
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let inner = &*self.inner;
        if offset >= inner.size || buf.is_empty() {
            return Ok(0);
        }
        let want = buf.len().min((inner.size - offset) as usize);
        let mut idx = inner.extents.partition_point(|e| e.voff + e.len <= offset);
        let mut done = 0usize;
        while done < want {
            let e = &inner.extents[idx];
            let pos = offset + done as u64;
            let within = pos - e.voff;
            let n = ((e.len - within) as usize).min(want - done);
            let dst = &mut buf[done..done + n];
            match &e.src {
                ByteSource::Mem(b) => dst.copy_from_slice(&b[within as usize..within as usize + n]),
                ByteSource::Blob { offset: bo } => inner
                    .blobs
                    .as_ref()
                    .expect("checked at build")
                    .read_exact_at(dst, bo + within)?,
            }
            done += n;
            idx += 1;
        }
        Ok(want)
    }

    pub fn read_range(&self, offset: u64, len: usize) -> Result<Bytes> {
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > self.inner.size)
        {
            return Err(Error::Invalid(format!(
                "range {offset}+{len} beyond file size {}",
                self.inner.size
            )));
        }
        let mut buf = vec![0u8; len];
        self.read_at(offset, &mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Materializes the full virtual file.
    pub fn to_bytes(&self) -> Result<Bytes> {
        self.read_range(0, self.inner.size as usize)
    }

    /// Streams the full virtual file to a writer.
    pub fn write_to(&self, mut w: impl std::io::Write) -> Result<()> {
        let mut buf = vec![0u8; 1 << 20];
        let mut off = 0;
        while off < self.inner.size {
            let n = self.read_at(off, &mut buf)?;
            w.write_all(&buf[..n])?;
            off += n as u64;
        }
        Ok(())
    }
}

impl Length for VirtualFile {
    fn len(&self) -> u64 {
        self.inner.size
    }
}

/// Sequential reader over a virtual file.
pub struct VirtualReader {
    file: VirtualFile,
    pos: u64,
}

impl Read for VirtualReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self
            .file
            .read_at(self.pos, buf)
            .map_err(std::io::Error::other)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl ChunkReader for VirtualFile {
    type T = VirtualReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(VirtualReader {
            file: self.clone(),
            pos: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.read_range(start, length)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
    }
}

/// Builds the column index of one chunk from its data page statistics.
fn column_index(
    t: &StatType,
    physical: parquet::basic::Type,
    pages: &[PageStats],
) -> Result<ColumnIndexMetaData> {
    let has_nan = pages.iter().all(|p| p.nan_count.is_some());
    let mut b = ColumnIndexBuilder::new(physical);
    for p in pages {
        b.append(
            p.is_null_page(),
            p.min.clone().unwrap_or_default(),
            p.max.clone().unwrap_or_default(),
            p.null_count as i64,
            if has_nan {
                p.nan_count.map(|n| n as i64)
            } else {
                None
            },
        );
    }
    b.set_boundary_order(boundary_order(t, &pages.iter().collect::<Vec<_>>()));
    Ok(b.build()?)
}

/// A sink that drops the first `skip` bytes written to it and keeps the rest.
struct SkipSink {
    skip: u64,
    out: Vec<u8>,
}

impl std::io::Write for SkipSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let drop = (self.skip.min(buf.len() as u64)) as usize;
        self.skip -= drop as u64;
        self.out.extend_from_slice(&buf[drop..]);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serializes the footer (page indexes, file metadata, length, magic) as if it started at
/// file offset `start`, so that page index offsets in the metadata are absolute.
fn write_footer(md: &ParquetMetaData, start: u64) -> Result<Vec<u8>> {
    static ZEROS: [u8; 1 << 16] = [0; 1 << 16];
    let mut sink = SkipSink {
        skip: start,
        out: Vec::new(),
    };
    {
        let mut w = TrackedWrite::new(&mut sink);
        let mut left = start;
        while left > 0 {
            let n = left.min(ZEROS.len() as u64) as usize;
            std::io::Write::write_all(&mut w, &ZEROS[..n])?;
            left -= n as u64;
        }
        ParquetMetaDataWriter::new_with_tracked(w, md).finish()?;
    }
    Ok(sink.out)
}
