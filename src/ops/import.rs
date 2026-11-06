//! Importing an existing Parquet file by reusing its physical pages verbatim.

use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::ChunkReader;

use crate::error::{Error, Result};
use crate::meta::{self, SegmentKind};
use crate::schema::ColumnSpec;
use crate::stats::StoredStats;
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
        for c in descr.columns() {
            if c.max_rep_level() != 0 || c.path().parts().len() != 1 {
                return Err(Error::Unsupported(format!(
                    "nested column '{}' (only flat schemas are supported)",
                    c.path()
                )));
            }
        }
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
            let svid = meta::insert_schema(t.conn(), file_id, None, root.name(), &specs, fmd.key_value_metadata().map(|v| v.as_slice()))?;
            let sid = t.new_snapshot(file_id, svid, "import", None)?;
            let mut pages_total = 0;
            let mut rgs = 0;
            for rg in md.row_groups() {
                if rg.num_rows() == 0 {
                    continue;
                }
                let mut chunk_ids = Vec::with_capacity(specs.len());
                for (spec, col) in specs.iter().zip(rg.columns()) {
                    if col.file_path().is_some() {
                        return Err(Error::Unsupported("column chunks in external files".into()));
                    }
                    let (start, len) = col.byte_range();
                    let bytes = reader.get_bytes(start, len as usize)?;
                    let mut page_ids = Vec::new();
                    let mut rows = 0i64;
                    for p in split_pages(&bytes)? {
                        if p.header.kind == PageKind::Index {
                            continue;
                        }
                        if p.header.kind.is_data() {
                            rows += p.header.num_rows.unwrap_or(p.header.num_values);
                        }
                        let body = &bytes[p.offset..p.offset + p.header.total_len()];
                        page_ids.push(t.put_page(body, &p.header)?);
                    }
                    if rows != rg.num_rows() {
                        return Err(Error::Corrupt(format!(
                            "column '{}' pages cover {rows} rows, row group has {}",
                            spec.name,
                            rg.num_rows()
                        )));
                    }
                    pages_total += page_ids.len();
                    let chunk = NewChunk {
                        physical: col.column_type(),
                        codec: col.compression(),
                        encodings: col.encodings().collect(),
                        stats: col.statistics().map(StoredStats::from_parquet),
                    };
                    chunk_ids.push((spec.column_id, t.put_chunk(&chunk, &page_ids)?));
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
