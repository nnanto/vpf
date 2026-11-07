//! VPF: mutable virtual Parquet files.
//!
//! A `.vpf` file is a SQLite database holding one table: immutable Parquet pages plus the
//! metadata and snapshot history that reference them. Every committed snapshot is exposed as a byte-exact, valid Parquet
//! file assembled on the fly (virtual offsets + regenerated footer).

pub mod catalog;
pub mod codec;
pub mod convert;
pub mod error;
pub mod layout;
pub mod meta;
pub mod ops;
pub mod predicate;
pub mod schema;
pub mod stats;
pub mod store;
pub mod thrift;
pub mod values;

pub use error::{Error, Result};
