//! On-disk store: one SQLite database file (`*.vpf`) holding a single table's metadata,
//! snapshot history and immutable page bytes.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use parquet::basic::Compression;
use rusqlite::{Connection, MAIN_DB, OpenFlags, OptionalExtension, TransactionBehavior, params};

use crate::error::{Error, Result};
use crate::meta::{self, SegmentKind, codec_to_code};
use crate::stats::{PageStats, StoredStats};
use crate::thrift::{PageHeaderInfo, PageKind};

/// Random access to page bytes by logical blob offset.
pub trait BlobRead {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

    fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        self.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }
}

impl<T: BlobRead + ?Sized> BlobRead for Arc<T> {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        (**self).read_exact_at(buf, offset)
    }
}

/// Page bytes live in the `blobs` table, keyed by a logical offset that grows with every
/// stored page. Rows are never updated or deleted, so any blob referenced by committed
/// metadata is stable for every snapshot that references it.
fn read_blob_at(conn: &Connection, buf: &mut [u8], offset: u64) -> Result<()> {
    let start: Option<i64> = conn
        .prepare_cached("SELECT start FROM blobs WHERE start <= ?1 ORDER BY start DESC LIMIT 1")?
        .query_row([offset as i64], |r| r.get(0))
        .optional()?;
    let start = start.ok_or_else(|| Error::Corrupt(format!("no blob at offset {offset}")))?;
    let blob = conn.blob_open(MAIN_DB, c"blobs", c"data", start, true)?;
    let within = (offset - start as u64) as usize;
    if within + buf.len() > blob.len() {
        return Err(Error::Corrupt(format!(
            "read {offset}+{} crosses a blob boundary",
            buf.len()
        )));
    }
    blob.read_at_exact(buf, within)?;
    Ok(())
}

/// Reads committed page bytes through a pool of read-only connections, so pinned
/// snapshots can be read concurrently without the store's write connection.
pub struct BlobStore {
    path: PathBuf,
    pool: Mutex<Vec<Connection>>,
}

impl BlobStore {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            pool: Mutex::new(Vec::new()),
        }
    }

    fn reader(&self) -> Result<Connection> {
        if let Some(c) = self.pool.lock().unwrap().pop() {
            return Ok(c);
        }
        let c = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        c.busy_timeout(std::time::Duration::from_secs(30))?;
        Ok(c)
    }
}

impl BlobRead for BlobStore {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let conn = self.reader()?;
        let out = read_blob_at(&conn, buf, offset);
        let mut pool = self.pool.lock().unwrap();
        if pool.len() < 16 {
            pool.push(conn);
        }
        out
    }
}

/// Reads page bytes inside a write transaction, including pages written by it.
pub struct TxnBlobs<'a>(&'a Connection);

impl BlobRead for TxnBlobs<'_> {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        read_blob_at(self.0, buf, offset)
    }
}

pub struct Store {
    path: PathBuf,
    conn: Mutex<Connection>,
    blobs: Arc<BlobStore>,
    pub(crate) synth_cache: crate::ops::synth::SynthCache,
}

impl Store {
    /// Opens (creating if needed) a `.vpf` file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.is_dir() {
            return Err(Error::Invalid(format!(
                "{} is a directory; a VPF store is a single file",
                path.display()
            )));
        }
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&path)?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        let fresh: i64 = conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))?;
        if fresh == 0 {
            conn.pragma_update(None, "page_size", 16384)?;
            conn.pragma_update(None, "application_id", APPLICATION_ID)?;
        } else {
            let id: i32 = conn.query_row("PRAGMA application_id", [], |r| r.get(0))?;
            if id != APPLICATION_ID {
                return Err(Error::Invalid(format!(
                    "{} is not a VPF file",
                    path.display()
                )));
            }
        }
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(meta::SCHEMA_SQL)?;
        meta::migrate(&conn)?;
        let blobs = Arc::new(BlobStore::new(&path));
        Ok(Self {
            path,
            conn: Mutex::new(conn),
            blobs,
            synth_cache: Default::default(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn blobs(&self) -> &Arc<BlobStore> {
        &self.blobs
    }

    /// Name of the file's table, if it has been created.
    pub fn table(&self) -> Result<Option<String>> {
        self.read(|c| Ok(meta::list_files(c)?.into_iter().next()))
    }

    /// Runs `f` against a consistent read view.
    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let out = f(&tx)?;
        tx.finish()?;
        Ok(out)
    }

    /// Runs `f` in an exclusive write transaction; page bytes and metadata commit atomically.
    pub fn write<T>(&self, f: impl FnOnce(&mut Txn) -> Result<T>) -> Result<T> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut txn = Txn { tx };
        let out = f(&mut txn)?;
        txn.tx.commit()?;
        Ok(out)
    }
}

/// `PRAGMA application_id` of VPF files ("VPF1").
pub const APPLICATION_ID: i32 = 0x5650_4631;

/// An open write transaction.
pub struct Txn<'a> {
    pub(crate) tx: rusqlite::Transaction<'a>,
}

/// Metadata for a new column chunk.
#[derive(Debug, Clone)]
pub struct NewChunk {
    pub physical: parquet::basic::Type,
    pub codec: Compression,
    pub encodings: Vec<parquet::basic::Encoding>,
    pub stats: Option<StoredStats>,
}

impl<'a> Txn<'a> {
    pub fn conn(&self) -> &Connection {
        &self.tx
    }

    pub fn blobs(&self) -> TxnBlobs<'_> {
        TxnBlobs(&self.tx)
    }

    /// Stores one physical page (header + body bytes) of a non-repeated column, where every
    /// level is a row.
    pub fn put_page(&mut self, bytes: &[u8], h: &PageHeaderInfo) -> Result<i64> {
        let num_rows = match h.kind {
            PageKind::Data => h.num_values,
            PageKind::DataV2 => h.num_rows.unwrap_or(h.num_values),
            _ => 0,
        };
        self.put_page_rows(bytes, h, num_rows, None)
    }

    /// Stores one physical page holding `num_rows` whole rows, with its statistics.
    pub fn put_page_rows(
        &mut self,
        bytes: &[u8],
        h: &PageHeaderInfo,
        num_rows: i64,
        stats: Option<&PageStats>,
    ) -> Result<i64> {
        if bytes.len() != h.total_len() {
            return Err(Error::Corrupt(
                "page byte length does not match header".into(),
            ));
        }
        if !h.kind.is_data() && num_rows != 0 {
            return Err(Error::Corrupt("non-data page with rows".into()));
        }
        let off: i64 = self.tx.query_row(
            "SELECT COALESCE((SELECT start + length(data) FROM blobs ORDER BY start DESC LIMIT 1), 0)",
            [],
            |r| r.get(0),
        )?;
        self.tx.execute(
            "INSERT INTO blobs (start, data) VALUES (?1, ?2)",
            params![off, bytes],
        )?;
        self.tx.execute(
            "INSERT INTO pages (blob_offset, blob_len, kind, num_values, num_rows, uncompressed_len, stats)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                off,
                bytes.len() as i64,
                h.kind.code(),
                h.num_values,
                num_rows,
                (h.header_len + h.uncompressed_size) as i64,
                stats.map(serde_json::to_string).transpose()?
            ],
        )?;
        Ok(self.tx.last_insert_rowid())
    }

    pub fn put_chunk(&mut self, c: &NewChunk, page_ids: &[i64]) -> Result<i64> {
        if page_ids.is_empty() {
            return Err(Error::Corrupt("column chunk without pages".into()));
        }
        let num_values: i64 = self.tx.query_row(
            &format!(
                "SELECT COALESCE(SUM(num_values), 0) FROM pages WHERE kind IN (0, 3) AND page_id IN ({})",
                page_ids.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",")
            ),
            [],
            |r| r.get(0),
        )?;
        let enc: Vec<String> = c.encodings.iter().map(|e| e.to_string()).collect();
        self.tx.execute(
            "INSERT INTO chunks (physical_type, codec, encodings, stats, num_values) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                c.physical.to_string(),
                codec_to_code(c.codec)?,
                serde_json::to_string(&enc)?,
                c.stats.as_ref().map(serde_json::to_string).transpose()?,
                num_values
            ],
        )?;
        let id = self.tx.last_insert_rowid();
        let mut st = self.tx.prepare_cached(
            "INSERT INTO chunk_pages (chunk_id, seq, page_id) VALUES (?1, ?2, ?3)",
        )?;
        for (i, p) in page_ids.iter().enumerate() {
            st.execute(params![id, i as i64, p])?;
        }
        Ok(id)
    }

    /// Records a row group; `chunks` maps (column_id, leaf) to chunk ids.
    pub fn put_row_group(&mut self, num_rows: i64, chunks: &[((i64, i64), i64)]) -> Result<i64> {
        self.tx
            .execute("INSERT INTO row_groups (num_rows) VALUES (?1)", [num_rows])?;
        let id = self.tx.last_insert_rowid();
        let mut st = self.tx.prepare_cached(
            "INSERT INTO rg_chunks (rg_id, column_id, leaf, chunk_id) VALUES (?1, ?2, ?3, ?4)",
        )?;
        for ((col, leaf), chunk) in chunks {
            st.execute(params![id, col, leaf, chunk])?;
        }
        Ok(id)
    }

    pub fn put_batch(
        &mut self,
        schema_version_id: i64,
        num_rows: i64,
        payload: &[u8],
    ) -> Result<i64> {
        self.tx.execute(
            "INSERT INTO batches (schema_version_id, num_rows, payload) VALUES (?1, ?2, ?3)",
            params![schema_version_id, num_rows, payload],
        )?;
        Ok(self.tx.last_insert_rowid())
    }

    pub fn create_file(
        &mut self,
        name: &str,
        codec: Compression,
        created_by: Option<&str>,
        version: i32,
    ) -> Result<i64> {
        let existing: Option<String> = self
            .tx
            .query_row("SELECT name FROM files LIMIT 1", [], |r| r.get(0))
            .optional()?;
        match existing {
            Some(n) if n == name => {
                return Err(Error::Invalid(format!("file '{name}' already exists")));
            }
            Some(n) => {
                return Err(Error::Invalid(format!(
                    "this VPF file already holds table '{n}'; a VPF file holds exactly one table"
                )));
            }
            None => {}
        }
        self.tx.execute(
            "INSERT INTO files (name, next_column_id, default_codec, created_by, format_version) VALUES (?1, 1, ?2, ?3, ?4)",
            params![name, codec_to_code(codec)?, created_by, version],
        )?;
        Ok(self.tx.last_insert_rowid())
    }

    /// Reserves `n` fresh, never-reused column ids for a file.
    pub fn alloc_column_ids(&mut self, file_id: i64, n: i64) -> Result<i64> {
        let start: i64 = self.tx.query_row(
            "SELECT next_column_id FROM files WHERE file_id = ?1",
            [file_id],
            |r| r.get(0),
        )?;
        self.tx.execute(
            "UPDATE files SET next_column_id = ?1 WHERE file_id = ?2",
            params![start + n, file_id],
        )?;
        Ok(start)
    }

    /// Creates a new snapshot as the child of the file's current snapshot and makes it current.
    pub fn new_snapshot(
        &mut self,
        file_id: i64,
        schema_version_id: i64,
        operation: &str,
        summary: Option<&str>,
    ) -> Result<i64> {
        let parent: Option<i64> = self.tx.query_row(
            "SELECT current_snapshot_id FROM files WHERE file_id = ?1",
            [file_id],
            |r| r.get(0),
        )?;
        // num_rows is filled by finish_snapshot once segments are final.
        self.tx.execute(
            "INSERT INTO snapshots (file_id, parent_id, schema_version_id, num_rows, operation, summary)
             VALUES (?1, ?2, ?3, 0, ?4, ?5)",
            params![file_id, parent, schema_version_id, operation, summary],
        )?;
        let id = self.tx.last_insert_rowid();
        self.tx.execute(
            "UPDATE files SET current_snapshot_id = ?1 WHERE file_id = ?2",
            params![id, file_id],
        )?;
        Ok(id)
    }

    /// Records the final row count of a snapshot from its visible segments.
    pub fn finish_snapshot(&mut self, file_id: i64, snapshot_id: i64) -> Result<i64> {
        let n: i64 = self.tx.query_row(
            "SELECT COALESCE(SUM(CASE s.kind WHEN 0 THEN rg.num_rows ELSE b.num_rows END), 0)
             FROM segments s
             LEFT JOIN row_groups rg ON s.kind = 0 AND rg.rg_id = s.obj_id
             LEFT JOIN batches b ON s.kind = 1 AND b.batch_id = s.obj_id
             WHERE s.file_id = ?1 AND s.added_in <= ?2 AND (s.removed_in IS NULL OR s.removed_in > ?2)",
            params![file_id, snapshot_id],
            |r| r.get(0),
        )?;
        self.tx.execute(
            "UPDATE snapshots SET num_rows = ?1 WHERE snapshot_id = ?2",
            params![n, snapshot_id],
        )?;
        Ok(n)
    }

    /// Appends a segment at the end of the file's row order.
    pub fn append_segment(
        &mut self,
        file_id: i64,
        kind: SegmentKind,
        obj_id: i64,
        snapshot_id: i64,
    ) -> Result<i64> {
        let pos: i64 = self.tx.query_row(
            "SELECT COALESCE(MAX(pos), -1) + 1 FROM segments WHERE file_id = ?1",
            [file_id],
            |r| r.get(0),
        )?;
        self.insert_segment(file_id, pos, kind, obj_id, snapshot_id)
    }

    /// Replaces a visible segment in place (same row position) as of `snapshot_id`.
    pub fn replace_segment(
        &mut self,
        file_id: i64,
        old: &meta::Segment,
        kind: SegmentKind,
        obj_id: i64,
        snapshot_id: i64,
    ) -> Result<i64> {
        self.remove_segment(old.segment_id, snapshot_id)?;
        self.insert_segment(file_id, old.pos, kind, obj_id, snapshot_id)
    }

    pub fn remove_segment(&mut self, segment_id: i64, snapshot_id: i64) -> Result<()> {
        let n = self.tx.execute(
            "UPDATE segments SET removed_in = ?1 WHERE segment_id = ?2 AND removed_in IS NULL",
            params![snapshot_id, segment_id],
        )?;
        if n != 1 {
            return Err(Error::Corrupt(format!("segment {segment_id} is not live")));
        }
        Ok(())
    }

    fn insert_segment(
        &mut self,
        file_id: i64,
        pos: i64,
        kind: SegmentKind,
        obj_id: i64,
        snapshot_id: i64,
    ) -> Result<i64> {
        let k = match kind {
            SegmentKind::RowGroup => 0,
            SegmentKind::Batch => 1,
        };
        self.tx.execute(
            "INSERT INTO segments (file_id, pos, kind, obj_id, added_in) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![file_id, pos, k, obj_id, snapshot_id],
        )?;
        Ok(self.tx.last_insert_rowid())
    }
}
