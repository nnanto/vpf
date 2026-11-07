use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error("not found: {0}")]
    NotFound(String),
    /// The table changed since the snapshot a write was based on; retry against the latest.
    #[error("write conflict: {0}")]
    Conflict(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
