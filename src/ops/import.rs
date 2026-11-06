//! Importing an existing Parquet file by reusing its physical pages verbatim.

use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::ChunkReader;

use crate::codec::{RawPage, chunk_page_rows, decode_pages, encode_column};
use crate::error::{Error, Result};
use crate::ops::append::store_chunk;
use crate::meta::{self, SegmentKind};
use crate::schema::{ColumnSpec, Leaf, build_descriptor};
use crate::stats::{PageStats, StatOrder, StoredStats, merge_pages};
use crate::store::{NewChunk, Store};
use crate::thrift::{PageKind, split_pages};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSummary {
    pub snapshot_id: i64,
    pub num_rows: i64,
    pub row_groups: usize,
    pub pages: usize,
}

impl Store {
    /// Imports a Parquet file from a path as a new virtual file `name`.
    pub fn import_file(&self, name: &str, path: impl AsRef<std::path::Path>) -> Result<ImportSummary> {
        let f = std::fs::File::open(path)?;
        self.import(name, &f)
    }

    /// Imports Parquet data from any chunk reader as a new virtual file `name`.
    pub fn import<R: ChunkReader>(&self, name: &str, reader: &R) -> Result<ImportSummary> {
        let md = ParquetMetaDataReader::new().parse_and_finish(reader)?;
        let fmd = md.file_metadata();
        let descr = fmd.schema_descr();
        let root = descr.root_schema();
        let default_codec = md
            .row_groups()
            .first()
            .and_then(|rg| rg.columns().first())
            .map(|c| c.compression())
            .unwrap_or(Compression::SNAPPY);

        self.write(|t| {
            let file_id = t.create_file(name, default_codec, fmd.created_by(), fmd.version())?;
            let ncols = root.get_fields().len() as i64;
            let first_id = t.alloc_column_ids(file_id, ncols)?;
            let specs = root
                .get_fields()
                .iter()
                .enumerate()
                .map(|(i, f)| ColumnSpec::from_parquet_type(first_id + i as i64, f))
                .collect::<Result<Vec<_>>>()?;
            if build_descriptor(root.name(), &specs)?.root_schema() != root {
                return Err(Error::Unsupported("schema cannot be represented exactly".into()));
            }
            let svid = meta::insert_schema(t.conn(), file_id, None, root.name(), &specs, fmd.key_value_metadata().map(|v| v.as_slice()))?;
            let leaves = meta::load_schema(t.conn(), svid)?.leaves()?;
            if leaves.len() != descr.num_columns() {
                return Err(Error::Corrupt("leaf count mismatch".into()));
            }
            let sid = t.new_snapshot(file_id, svid, "import", None)?;
            let mut pages_total = 0;
            let mut rgs = 0;
            for rg in md.row_groups() {
                if rg.num_rows() == 0 {
                    continue;
                }
                let mut chunk_ids = Vec::with_capacity(leaves.len());
                for (leaf, col) in leaves.iter().zip(rg.columns()) {
                    if col.file_path().is_some() {
                        return Err(Error::Unsupported("column chunks in external files".into()));
                    }
                    let (start, len) = col.byte_range();
                    let bytes = reader.get_bytes(start, len as usize)?;
                    let pages: Vec<RawPage> = split_pages(&bytes)?
                        .into_iter()
                        .filter(|p| p.header.kind != PageKind::Index)
                        .map(|p| RawPage::from_header(bytes.slice(p.offset..p.offset + p.header.total_len()), &p.header))
                        .collect();
                    let headers: Vec<_> = split_pages(&bytes)?
                        .into_iter()
                        .filter(|p| p.header.kind != PageKind::Index)
                        .map(|p| p.header)
                        .collect();
                    let page_rows = if leaf.max_rep == 0 {
                        Some(headers.iter().map(|h| if h.kind.is_data() { h.num_rows.unwrap_or(h.num_values) } else { 0 }).collect())
                    } else {
                        chunk_page_rows(leaf, col.compression(), &pages)?
                    };
                    let chunk_id = match page_rows {
                        Some(page_rows) => {
                            let rows: i64 = page_rows.iter().sum();
                            if rows != rg.num_rows() {
                                return Err(Error::Corrupt(format!(
                                    "column '{}' pages cover {rows} rows, row group has {}",
                                    leaf.spec.name,
                                    rg.num_rows()
                                )));
                            }
                            let stat_type = leaf.stat_type()?;
                            let page_stats = import_page_stats(leaf, col.compression(), &pages, &page_rows)?;
                            let mut page_ids = Vec::with_capacity(pages.len());
                            for (((p, h), r), st) in pages.iter().zip(&headers).zip(&page_rows).zip(&page_stats) {
                                page_ids.push(t.put_page_rows(&p.bytes, h, *r, st.as_ref())?);
                            }
                            let merged = page_stats
                                .iter()
                                .zip(&page_rows)
                                .filter(|(_, r)| **r > 0)
                                .map(|(s, _)| s.as_ref())
                                .collect::<Option<Vec<_>>>()
                                .map(|ps| merge_pages(&stat_type, ps));
                            pages_total += page_ids.len();
                            let chunk = NewChunk {
                                physical: col.column_type(),
                                codec: col.compression(),
                                encodings: col.encodings().collect(),
                                stats: col.statistics().map(StoredStats::from_parquet).or(merged),
                            };
                            t.put_chunk(&chunk, &page_ids)?
                        }
                        None => {
                            // Records span pages: re-encode so every page starts a record.
                            let data = decode_pages(leaf, col.compression(), &pages)?;
                            if data.num_rows() as i64 != rg.num_rows() {
                                return Err(Error::Corrupt(format!(
                                    "column '{}' holds {} rows, row group has {}",
                                    leaf.spec.name,
                                    data.num_rows(),
                                    rg.num_rows()
                                )));
                            }
                            let codec = match col.compression() {
                                Compression::LZO => Compression::SNAPPY,
                                c => c,
                            };
                            let enc = encode_column(leaf, codec, &data, true)?;
                            pages_total += enc.pages.len();
                            store_chunk(t, leaf, &enc)?
                        }
                    };
                    chunk_ids.push(((leaf.column_id, leaf.leaf), chunk_id));
                }
                let rg_id = t.put_row_group(rg.num_rows(), &chunk_ids)?;
                t.append_segment(file_id, SegmentKind::RowGroup, rg_id, sid)?;
                rgs += 1;
            }
            let num_rows = t.finish_snapshot(file_id, sid)?;
            Ok(ImportSummary { snapshot_id: sid, num_rows, row_groups: rgs, pages: pages_total })
        })
    }
}

/// Page statistics of an imported chunk, computed from its decoded values (source page
/// statistics and column indexes are not trusted). Chunks that cannot be decoded here
/// (e.g. LZO) get none; this never fails the import.
fn import_page_stats(
    leaf: &Leaf,
    codec: Compression,
    pages: &[RawPage],
    page_rows: &[i64],
) -> Result<Vec<Option<PageStats>>> {
    let t = leaf.stat_type()?;
    if t.order == StatOrder::Undefined {
        return Ok(vec![None; pages.len()]);
    }
    Ok(match decode_pages(leaf, codec, pages) {
        Ok(data) if data.num_rows() as i64 == page_rows.iter().sum::<i64>() => {
            crate::codec::page_stats(&t, &data, page_rows)
        }
        _ => vec![None; pages.len()],
    })
}
