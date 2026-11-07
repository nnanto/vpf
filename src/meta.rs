//! SQLite metadata schema and row-level accessors.
//!
//! Object model (all immutable once written, except segment visibility bounds):
//! pages (physical blobs) <- chunks (ordered page lists) <- row_groups (column_id -> chunk)
//! segments: ordered (row group | buffered batch) entries of a file with
//! `[added_in, removed_in)` snapshot visibility. Snapshot history per file is linear.

use std::collections::HashMap;

use parquet::basic::{Compression, ConvertedType, Encoding, Type as PhysicalType};
use parquet::file::metadata::KeyValue;
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{Error, Result};
use crate::schema::{ColumnSpec, SchemaVersion, logical_from_json, logical_to_json};
use crate::stats::{PageStats, StoredStats};
use crate::thrift::PageKind;

/// Upgrades stores created by older versions to the current schema.
pub fn migrate(conn: &Connection) -> Result<()> {
    let has_page_stats: bool = conn
        .prepare("SELECT 1 FROM pragma_table_info('pages') WHERE name = 'stats'")?
        .exists([])?;
    if !has_page_stats {
        conn.execute_batch("ALTER TABLE pages ADD COLUMN stats TEXT")?;
    }
    Ok(())
}

pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    current_snapshot_id INTEGER,
    next_column_id INTEGER NOT NULL,
    default_codec INTEGER NOT NULL,
    created_by TEXT,
    format_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_versions (
    schema_version_id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    parent_id INTEGER,
    root_name TEXT NOT NULL,
    kv_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS schema_columns (
    schema_version_id INTEGER NOT NULL,
    column_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    name TEXT NOT NULL,
    physical_type TEXT NOT NULL,
    type_length INTEGER NOT NULL,
    nullable INTEGER NOT NULL,
    logical_type TEXT,
    converted_type TEXT NOT NULL,
    precision INTEGER NOT NULL,
    scale INTEGER NOT NULL,
    field_id INTEGER,
    default_value BLOB,
    nested_type TEXT,
    PRIMARY KEY (schema_version_id, column_id)
);
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    parent_id INTEGER,
    schema_version_id INTEGER NOT NULL,
    num_rows INTEGER NOT NULL,
    operation TEXT NOT NULL,
    summary TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_snapshots_file ON snapshots(file_id, snapshot_id);
CREATE TABLE IF NOT EXISTS pages (
    page_id INTEGER PRIMARY KEY,
    blob_offset INTEGER NOT NULL,
    blob_len INTEGER NOT NULL,
    kind INTEGER NOT NULL,
    num_values INTEGER NOT NULL,
    num_rows INTEGER NOT NULL,
    uncompressed_len INTEGER NOT NULL,
    stats TEXT
);
CREATE TABLE IF NOT EXISTS blobs (
    start INTEGER PRIMARY KEY,
    data BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id INTEGER PRIMARY KEY,
    physical_type TEXT NOT NULL,
    codec INTEGER NOT NULL,
    encodings TEXT NOT NULL,
    stats TEXT,
    num_values INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chunk_pages (
    chunk_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    PRIMARY KEY (chunk_id, seq)
);
CREATE TABLE IF NOT EXISTS row_groups (
    rg_id INTEGER PRIMARY KEY,
    num_rows INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rg_chunks (
    rg_id INTEGER NOT NULL,
    column_id INTEGER NOT NULL,
    leaf INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL,
    PRIMARY KEY (rg_id, column_id, leaf)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id INTEGER PRIMARY KEY,
    schema_version_id INTEGER NOT NULL,
    num_rows INTEGER NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS segments (
    segment_id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    pos INTEGER NOT NULL,
    kind INTEGER NOT NULL,
    obj_id INTEGER NOT NULL,
    added_in INTEGER NOT NULL,
    removed_in INTEGER
);
CREATE INDEX IF NOT EXISTS idx_segments_vis ON segments(file_id, added_in, removed_in);
"#;

pub fn codec_to_code(c: Compression) -> Result<i64> {
    Ok(match c {
        Compression::UNCOMPRESSED => 0,
        Compression::SNAPPY => 1,
        Compression::GZIP(_) => 2,
        Compression::LZO => 3,
        Compression::BROTLI(_) => 4,
        Compression::LZ4 => 5,
        Compression::ZSTD(_) => 6,
        Compression::LZ4_RAW => 7,
    })
}

/// Parses a codec name: none, snappy, gzip, brotli, lz4, zstd.
pub fn parse_codec(s: &str) -> Result<Compression> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "brotli" => Compression::BROTLI(Default::default()),
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "zstd" => Compression::ZSTD(Default::default()),
        _ => return Err(Error::Invalid(format!("unknown codec '{s}'"))),
    })
}

pub fn codec_from_code(c: i64) -> Result<Compression> {
    Ok(match c {
        0 => Compression::UNCOMPRESSED,
        1 => Compression::SNAPPY,
        2 => Compression::GZIP(Default::default()),
        3 => Compression::LZO,
        4 => Compression::BROTLI(Default::default()),
        5 => Compression::LZ4,
        6 => Compression::ZSTD(Default::default()),
        7 => Compression::LZ4_RAW,
        _ => return Err(Error::Corrupt(format!("unknown codec {c}"))),
    })
}

#[derive(Debug, Clone)]
pub struct FileRow {
    pub file_id: i64,
    pub name: String,
    pub current_snapshot_id: Option<i64>,
    pub next_column_id: i64,
    pub default_codec: Compression,
    pub created_by: Option<String>,
    pub format_version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub file_id: i64,
    pub parent_id: Option<i64>,
    pub schema_version_id: i64,
    pub num_rows: i64,
    pub operation: String,
    pub summary: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRow {
    pub page_id: i64,
    pub blob_offset: u64,
    pub blob_len: u64,
    pub kind: PageKind,
    pub num_values: i64,
    /// Rows covered by the page (0 for dictionary pages). Flat columns: == num_values.
    pub num_rows: i64,
    /// header length + uncompressed body length
    pub uncompressed_len: i64,
    /// Data page statistics (absent for dictionary pages, unordered types, older stores).
    pub stats: Option<PageStats>,
}

#[derive(Debug, Clone)]
pub struct ChunkRow {
    pub chunk_id: i64,
    pub physical: PhysicalType,
    pub codec: Compression,
    pub encodings: Vec<Encoding>,
    pub stats: Option<StoredStats>,
    pub num_values: i64,
    pub pages: Vec<PageRow>,
}

#[derive(Debug, Clone)]
pub struct RowGroupRow {
    pub rg_id: i64,
    pub num_rows: i64,
    /// (column_id, leaf) -> chunk_id
    pub chunks: HashMap<(i64, i64), i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    RowGroup,
    Batch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub segment_id: i64,
    pub pos: i64,
    pub kind: SegmentKind,
    pub obj_id: i64,
}

#[derive(Debug, Clone)]
pub struct BatchRow {
    pub batch_id: i64,
    pub schema_version_id: i64,
    pub num_rows: i64,
    pub payload: Vec<u8>,
}

fn file_from_row(
    r: &rusqlite::Row,
) -> rusqlite::Result<(i64, String, Option<i64>, i64, i64, Option<String>, i32)> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
    ))
}

pub fn load_file(conn: &Connection, name: &str) -> Result<FileRow> {
    let row = conn
        .query_row(
            "SELECT file_id, name, current_snapshot_id, next_column_id, default_codec, created_by, format_version
             FROM files WHERE name = ?1",
            [name],
            file_from_row,
        )
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("file '{name}'")))?;
    Ok(FileRow {
        file_id: row.0,
        name: row.1,
        current_snapshot_id: row.2,
        next_column_id: row.3,
        default_codec: codec_from_code(row.4)?,
        created_by: row.5,
        format_version: row.6,
    })
}

pub fn list_files(conn: &Connection) -> Result<Vec<String>> {
    let mut st = conn.prepare("SELECT name FROM files ORDER BY name")?;
    let rows = st
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(rows)
}

fn snapshot_from_row(r: &rusqlite::Row) -> rusqlite::Result<SnapshotInfo> {
    Ok(SnapshotInfo {
        snapshot_id: r.get(0)?,
        file_id: r.get(1)?,
        parent_id: r.get(2)?,
        schema_version_id: r.get(3)?,
        num_rows: r.get(4)?,
        operation: r.get(5)?,
        summary: r.get(6)?,
        created_at: r.get(7)?,
    })
}

const SNAPSHOT_COLS: &str =
    "snapshot_id, file_id, parent_id, schema_version_id, num_rows, operation, summary, created_at";

pub fn load_snapshot(conn: &Connection, file_id: i64, snapshot_id: i64) -> Result<SnapshotInfo> {
    conn.query_row(
        &format!("SELECT {SNAPSHOT_COLS} FROM snapshots WHERE snapshot_id = ?1 AND file_id = ?2"),
        params![snapshot_id, file_id],
        snapshot_from_row,
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(format!("snapshot {snapshot_id}")))
}

pub fn list_snapshots(conn: &Connection, file_id: i64) -> Result<Vec<SnapshotInfo>> {
    let mut st = conn.prepare(&format!(
        "SELECT {SNAPSHOT_COLS} FROM snapshots WHERE file_id = ?1 ORDER BY snapshot_id"
    ))?;
    let rows = st
        .query_map([file_id], snapshot_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn load_schema(conn: &Connection, schema_version_id: i64) -> Result<SchemaVersion> {
    let (root_name, kv_json): (String, Option<String>) = conn
        .query_row(
            "SELECT root_name, kv_json FROM schema_versions WHERE schema_version_id = ?1",
            [schema_version_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("schema version {schema_version_id}")))?;
    let key_value = kv_json
        .map(|s| -> Result<Vec<KeyValue>> {
            let pairs: Vec<(String, Option<String>)> = serde_json::from_str(&s)?;
            Ok(pairs
                .into_iter()
                .map(|(k, v)| KeyValue { key: k, value: v })
                .collect())
        })
        .transpose()?;
    let mut st = conn.prepare(
        "SELECT column_id, name, physical_type, type_length, nullable, logical_type, converted_type,
                precision, scale, field_id, default_value, nested_type
         FROM schema_columns WHERE schema_version_id = ?1 ORDER BY ordinal",
    )?;
    let mut columns = Vec::new();
    let mut rows = st.query([schema_version_id])?;
    while let Some(r) = rows.next()? {
        let physical: String = r.get(2)?;
        let logical: Option<String> = r.get(5)?;
        let converted: String = r.get(6)?;
        let nested: Option<String> = r.get(11)?;
        columns.push(ColumnSpec {
            column_id: r.get(0)?,
            name: r.get(1)?,
            physical: physical.parse()?,
            type_length: r.get(3)?,
            nullable: r.get(4)?,
            logical: logical
                .map(|s| -> Result<_> { logical_from_json(&serde_json::from_str(&s)?) })
                .transpose()?,
            converted: converted.parse::<ConvertedType>()?,
            precision: r.get(7)?,
            scale: r.get(8)?,
            field_id: r.get(9)?,
            default_value: r.get(10)?,
            nested: nested
                .map(|s| -> Result<_> { crate::schema::type_from_json(&serde_json::from_str(&s)?) })
                .transpose()?,
        });
    }
    Ok(SchemaVersion {
        schema_version_id,
        root_name,
        columns,
        key_value,
    })
}

pub fn insert_schema(
    conn: &Connection,
    file_id: i64,
    parent_id: Option<i64>,
    root_name: &str,
    columns: &[ColumnSpec],
    kv: Option<&[KeyValue]>,
) -> Result<i64> {
    let mut names = std::collections::HashSet::new();
    for c in columns {
        if !names.insert(c.name.as_str()) {
            return Err(Error::Invalid(format!(
                "duplicate column name '{}'",
                c.name
            )));
        }
    }
    if columns.is_empty() {
        return Err(Error::Invalid(
            "schema must have at least one column".into(),
        ));
    }
    let kv_json = kv
        .map(|kv| serde_json::to_string(&kv.iter().map(|k| (&k.key, &k.value)).collect::<Vec<_>>()))
        .transpose()?;
    conn.execute(
        "INSERT INTO schema_versions (file_id, parent_id, root_name, kv_json) VALUES (?1, ?2, ?3, ?4)",
        params![file_id, parent_id, root_name, kv_json],
    )?;
    let id = conn.last_insert_rowid();
    let mut st = conn.prepare(
        "INSERT INTO schema_columns (schema_version_id, column_id, ordinal, name, physical_type, type_length,
            nullable, logical_type, converted_type, precision, scale, field_id, default_value, nested_type)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?;
    for (i, c) in columns.iter().enumerate() {
        let logical = c
            .logical
            .as_ref()
            .map(logical_to_json)
            .transpose()?
            .map(|v| v.to_string());
        let nested = c
            .nested
            .as_deref()
            .map(crate::schema::type_to_json)
            .transpose()?
            .map(|v| v.to_string());
        st.execute(params![
            id,
            c.column_id,
            i as i64,
            c.name,
            c.physical.to_string(),
            c.type_length,
            c.nullable,
            logical,
            c.converted.to_string(),
            c.precision,
            c.scale,
            c.field_id,
            c.default_value,
            nested,
        ])?;
    }
    Ok(id)
}

pub fn visible_segments(conn: &Connection, file_id: i64, snapshot_id: i64) -> Result<Vec<Segment>> {
    let mut st = conn.prepare_cached(
        "SELECT segment_id, pos, kind, obj_id FROM segments
         WHERE file_id = ?1 AND added_in <= ?2 AND (removed_in IS NULL OR removed_in > ?2)
         ORDER BY pos",
    )?;
    let rows = st
        .query_map(params![file_id, snapshot_id], |r| {
            let kind: i64 = r.get(2)?;
            Ok(Segment {
                segment_id: r.get(0)?,
                pos: r.get(1)?,
                kind: if kind == 0 {
                    SegmentKind::RowGroup
                } else {
                    SegmentKind::Batch
                },
                obj_id: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn load_row_group(conn: &Connection, rg_id: i64) -> Result<RowGroupRow> {
    let num_rows: i64 = conn
        .query_row(
            "SELECT num_rows FROM row_groups WHERE rg_id = ?1",
            [rg_id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| Error::Corrupt(format!("missing row group {rg_id}")))?;
    let mut st =
        conn.prepare_cached("SELECT column_id, leaf, chunk_id FROM rg_chunks WHERE rg_id = ?1")?;
    let chunks = st
        .query_map([rg_id], |r| Ok(((r.get(0)?, r.get(1)?), r.get(2)?)))?
        .collect::<rusqlite::Result<HashMap<(i64, i64), i64>>>()?;
    Ok(RowGroupRow {
        rg_id,
        num_rows,
        chunks,
    })
}

pub fn load_chunk(conn: &Connection, chunk_id: i64) -> Result<ChunkRow> {
    let (physical, codec, encodings, stats, num_values): (String, i64, String, Option<String>, i64) = conn
        .query_row(
            "SELECT physical_type, codec, encodings, stats, num_values FROM chunks WHERE chunk_id = ?1",
            [chunk_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?
        .ok_or_else(|| Error::Corrupt(format!("missing chunk {chunk_id}")))?;
    let enc_names: Vec<String> = serde_json::from_str(&encodings)?;
    let encodings = enc_names
        .iter()
        .map(|e| {
            e.parse::<Encoding>()
                .map_err(|e| Error::Corrupt(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut st = conn.prepare_cached(
        "SELECT p.page_id, p.blob_offset, p.blob_len, p.kind, p.num_values, p.num_rows, p.uncompressed_len, p.stats
         FROM chunk_pages cp JOIN pages p ON p.page_id = cp.page_id
         WHERE cp.chunk_id = ?1 ORDER BY cp.seq",
    )?;
    let mut pages = Vec::new();
    let mut rows = st.query([chunk_id])?;
    while let Some(r) = rows.next()? {
        let off: i64 = r.get(1)?;
        let len: i64 = r.get(2)?;
        pages.push(PageRow {
            page_id: r.get(0)?,
            blob_offset: off as u64,
            blob_len: len as u64,
            kind: PageKind::from_code(r.get(3)?)?,
            num_values: r.get(4)?,
            num_rows: r.get(5)?,
            uncompressed_len: r.get(6)?,
            stats: r
                .get::<_, Option<String>>(7)?
                .map(|s| serde_json::from_str(&s))
                .transpose()?,
        });
    }
    Ok(ChunkRow {
        chunk_id,
        physical: physical.parse()?,
        codec: codec_from_code(codec)?,
        encodings,
        stats: stats.map(|s| serde_json::from_str(&s)).transpose()?,
        num_values,
        pages,
    })
}

pub fn load_batch(conn: &Connection, batch_id: i64) -> Result<BatchRow> {
    conn.query_row(
        "SELECT schema_version_id, num_rows, payload FROM batches WHERE batch_id = ?1",
        [batch_id],
        |r| {
            Ok(BatchRow {
                batch_id,
                schema_version_id: r.get(0)?,
                num_rows: r.get(1)?,
                payload: r.get(2)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| Error::Corrupt(format!("missing batch {batch_id}")))
}
