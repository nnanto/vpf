//! Loading a snapshot's metadata and assembling its virtual Parquet file.

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::layout::{ByteSource, ChunkLayout, FileProps, PageSrc, RowGroupLayout, VirtualFile};
use crate::meta::{self, BatchRow, ChunkRow, FileRow, RowGroupRow, Segment, SegmentKind, SnapshotInfo};
use crate::schema::SchemaVersion;
use crate::ops::synth;
use crate::store::Store;

/// Fully-resolved metadata of one snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotView {
    pub file: FileRow,
    pub snapshot: SnapshotInfo,
    pub schema: SchemaVersion,
    pub segments: Vec<SegmentData>,
}

#[derive(Debug, Clone)]
pub enum SegmentData {
    RowGroup {
        seg: Segment,
        rg: RowGroupRow,
        /// One chunk per schema column, in schema order.
        chunks: Vec<ChunkRow>,
    },
    Batch {
        seg: Segment,
        batch: BatchRow,
    },
}

impl SegmentData {
    pub fn num_rows(&self) -> i64 {
        match self {
            SegmentData::RowGroup { rg, .. } => rg.num_rows,
            SegmentData::Batch { batch, .. } => batch.num_rows,
        }
    }

    pub fn segment(&self) -> &Segment {
        match self {
            SegmentData::RowGroup { seg, .. } | SegmentData::Batch { seg, .. } => seg,
        }
    }
}

pub fn load_view(conn: &Connection, name: &str, snapshot_id: Option<i64>) -> Result<SnapshotView> {
    let file = meta::load_file(conn, name)?;
    let sid = match snapshot_id {
        Some(s) => s,
        None => file.current_snapshot_id.ok_or_else(|| Error::NotFound(format!("file '{name}' has no snapshot")))?,
    };
    let snapshot = meta::load_snapshot(conn, file.file_id, sid)?;
    let schema = meta::load_schema(conn, snapshot.schema_version_id)?;
    let mut segments = Vec::new();
    for seg in meta::visible_segments(conn, file.file_id, sid)? {
        segments.push(match seg.kind {
            SegmentKind::RowGroup => {
                let rg = meta::load_row_group(conn, seg.obj_id)?;
                let chunks = schema
                    .columns
                    .iter()
                    .map(|c| {
                        let id = rg.chunks.get(&c.column_id).ok_or_else(|| {
                            Error::Corrupt(format!("row group {} has no chunk for column {}", rg.rg_id, c.name))
                        })?;
                        meta::load_chunk(conn, *id)
                    })
                    .collect::<Result<Vec<_>>>()?;
                SegmentData::RowGroup { seg, rg, chunks }
            }
            SegmentKind::Batch => SegmentData::Batch { batch: meta::load_batch(conn, seg.obj_id)?, seg },
        });
    }
    Ok(SnapshotView { file, snapshot, schema, segments })
}

pub fn chunk_layout(chunk: &ChunkRow) -> Result<ChunkLayout> {
    Ok(ChunkLayout {
        pages: chunk
            .pages
            .iter()
            .map(|p| PageSrc {
                src: ByteSource::Blob { offset: p.blob_offset },
                len: p.blob_len,
                kind: p.kind,
                num_values: p.num_values,
                uncompressed_len: p.uncompressed_len,
            })
            .collect(),
        codec: chunk.codec,
        encodings: chunk.encodings.clone(),
        stats: chunk.stats.as_ref().map(|s| s.to_parquet(chunk.physical)).transpose()?,
        num_values: chunk.num_values,
    })
}

impl Store {
    pub fn view(&self, name: &str, snapshot_id: Option<i64>) -> Result<SnapshotView> {
        self.read(|c| load_view(c, name, snapshot_id))
    }

    /// Opens a frozen, consistent virtual Parquet file for a snapshot (default: current).
    pub fn open_snapshot(&self, name: &str, snapshot_id: Option<i64>) -> Result<VirtualFile> {
        let view = self.view(name, snapshot_id)?;
        self.build_virtual_file(&view)
    }

    pub fn build_virtual_file(&self, view: &SnapshotView) -> Result<VirtualFile> {
        let codec = synth::write_codec(&view.file);
        let runs = synth::batch_runs(&view.segments, |s| match s {
            SegmentData::Batch { batch, .. } => Some(batch.num_rows),
            _ => None,
        });
        let mut rgs = Vec::new();
        let mut i = 0;
        let mut runs = runs.into_iter().peekable();
        while i < view.segments.len() {
            if let Some(run) = runs.next_if(|r| r.start == i) {
                let batches: Vec<&BatchRow> = view.segments[run.clone()]
                    .iter()
                    .map(|s| match s {
                        SegmentData::Batch { batch, .. } => batch,
                        _ => unreachable!(),
                    })
                    .collect();
                let key = (batches.iter().map(|b| b.batch_id).collect(), view.schema.schema_version_id);
                let rg = self.synth_cache.get_or_build(key, || synth::synthesize(&view.schema, codec, &batches))?;
                rgs.push(RowGroupLayout {
                    num_rows: rg.num_rows,
                    chunks: rg
                        .chunks
                        .iter()
                        .zip(&view.schema.columns)
                        .map(|(c, spec)| c.layout(spec.physical))
                        .collect::<Result<_>>()?,
                });
                i = run.end;
                continue;
            }
            match &view.segments[i] {
                SegmentData::RowGroup { rg, chunks, .. } => rgs.push(RowGroupLayout {
                    num_rows: rg.num_rows,
                    chunks: chunks.iter().map(chunk_layout).collect::<Result<_>>()?,
                }),
                SegmentData::Batch { .. } => unreachable!("batches are covered by runs"),
            }
            i += 1;
        }
        VirtualFile::build(
            FileProps {
                schema: view.schema.descriptor()?,
                created_by: view.file.created_by.clone(),
                key_value: view.schema.key_value.clone(),
                version: view.file.format_version,
                blobs: Some(self.blobs()),
            },
            rgs,
        )
    }

    /// Writes a snapshot as a standalone `.parquet` file.
    pub fn export(&self, name: &str, snapshot_id: Option<i64>, path: impl AsRef<std::path::Path>) -> Result<u64> {
        let vf = self.open_snapshot(name, snapshot_id)?;
        let f = std::fs::File::create(path)?;
        let mut w = std::io::BufWriter::new(f);
        vf.write_to(&mut w)?;
        use std::io::Write;
        w.flush()?;
        w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        Ok(vf.size())
    }
}
