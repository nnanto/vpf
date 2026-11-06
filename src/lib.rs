//! VPF: mutable virtual Parquet files.
//!
//! Physical Parquet pages are stored immutably in an append-only blob file and referenced
//! from SQLite metadata. Every committed snapshot is exposed as a byte-exact, valid Parquet
//! file assembled on the fly (virtual offsets + regenerated footer).

pub mod error;
pub mod meta;
pub mod schema;
pub mod stats;
pub mod store;
pub mod thrift;

pub use error::{Error, Result};
