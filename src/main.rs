use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use clap::{Parser, Subcommand, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Type as PhysicalType};
use parquet::file::reader::Length;
use vpf::convert::arrow_schema;
use vpf::meta;
use vpf::ops::alter::AlterOp;
use vpf::ops::append::AppendMode;
use vpf::ops::snapshot::SegmentData;
use vpf::predicate::parse_literal;
use vpf::schema::spec_for_type_name;
use vpf::store::Store;

/// Mutable Parquet files with page-level copy-on-write and time travel.
#[derive(Parser)]
#[command(name = "vpf", version)]
struct Cli {
    /// Store directory (created if missing).
    #[arg(long, short = 's', env = "VPF_STORE", default_value = ".vpf")]
    store: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Creates the store directory.
    Init,
    /// Imports an existing Parquet file; its pages are stored as-is.
    Import { name: String, path: PathBuf },
    /// Creates an empty file from column definitions like `id:int64:required` or `name:string`.
    Create {
        name: String,
        #[arg(required = true)]
        columns: Vec<String>,
        #[arg(long, default_value = "snappy")]
        codec: String,
    },
    /// Lists files.
    Ls,
    /// Lists snapshots of a file.
    Snapshots { name: String },
    /// Prints the schema of a snapshot.
    Schema {
        name: String,
        #[arg(long)]
        at: Option<i64>,
    },
    /// Prints rows of a snapshot.
    Cat {
        name: String,
        #[arg(long)]
        at: Option<i64>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, value_enum, default_value = "table")]
        format: CatFormat,
    },
    /// Writes a snapshot as a standalone Parquet file.
    Export {
        name: String,
        out: PathBuf,
        #[arg(long)]
        at: Option<i64>,
    },
    /// Appends rows from a Parquet file or newline-delimited JSON (`-` for stdin).
    Append {
        name: String,
        #[arg(long, conflicts_with = "json", required_unless_present = "json")]
        parquet: Option<PathBuf>,
        #[arg(long)]
        json: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "auto")]
        mode: Mode,
    },
    /// Converts buffered appends into stored row groups.
    Flush { name: String },
    /// Deletes rows matching a predicate.
    Delete {
        name: String,
        #[arg(long = "where")]
        predicate: String,
    },
    /// Sets columns to literal values on rows matching a predicate.
    Update {
        name: String,
        #[arg(long)]
        set: String,
        #[arg(long = "where")]
        predicate: String,
    },
    /// Changes the schema.
    Alter {
        name: String,
        #[command(subcommand)]
        op: AlterCmd,
    },
    /// Shows storage statistics of a snapshot.
    Stats {
        name: String,
        #[arg(long)]
        at: Option<i64>,
    },
}

#[derive(Subcommand)]
enum AlterCmd {
    AddColumn {
        column: String,
        #[arg(value_name = "TYPE")]
        ty: String,
        #[arg(long)]
        required: bool,
        /// Literal for existing rows and omitted appends, e.g. 0, 'text', TRUE.
        #[arg(long)]
        default: Option<String>,
    },
    DropColumn { column: String },
    RenameColumn { from: String, to: String },
    Reorder {
        #[arg(required = true)]
        columns: Vec<String>,
    },
    /// INT32 -> int64, FLOAT -> double.
    Widen { column: String, to: WidenTo },
    MakeOptional { column: String },
}

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Auto,
    Buffered,
    Direct,
}

#[derive(Clone, Copy, ValueEnum)]
enum CatFormat {
    Table,
    Json,
    Csv,
}

#[derive(Clone, Copy, ValueEnum)]
enum WidenTo {
    Int64,
    Double,
}

fn main() {
    if let Err(e) = run(Cli::parse()) {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

fn parse_codec(s: &str) -> Result<Compression> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(Default::default()),
        "brotli" => Compression::BROTLI(Default::default()),
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "zstd" => Compression::ZSTD(Default::default()),
        _ => bail!("unknown codec '{s}'"),
    })
}

fn read_snapshot(store: &Store, name: &str, at: Option<i64>, limit: Option<usize>) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let vf = store.open_snapshot(name, at)?;
    let b = ParquetRecordBatchReaderBuilder::try_new(vf)?;
    let schema = b.schema().clone();
    let mut out = Vec::new();
    let mut left = limit.unwrap_or(usize::MAX);
    for batch in b.build()? {
        if left == 0 {
            break;
        }
        let batch = batch?;
        let n = batch.num_rows().min(left);
        left -= n;
        out.push(batch.slice(0, n));
    }
    Ok((schema, out))
}

/// Reads newline-delimited JSON using the file's current schema, restricted to the keys
/// that occur in the input so omitted columns get their defaults.
fn read_json(store: &Store, name: &str, path: &PathBuf) -> Result<RecordBatch> {
    let text = if path.as_os_str() == "-" {
        let mut s = String::new();
        for l in std::io::stdin().lock().lines() {
            s.push_str(&l?);
            s.push('\n');
        }
        s
    } else {
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?
    };
    let mut keys = BTreeSet::new();
    for (i, line) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).with_context(|| format!("line {}", i + 1))?;
        let obj = v.as_object().with_context(|| format!("line {} is not an object", i + 1))?;
        keys.extend(obj.keys().cloned());
    }
    let full = arrow_schema(&store.view(name, None)?.schema)?;
    if let Some(k) = keys.iter().find(|k| full.field_with_name(k).is_err()) {
        bail!("unknown column '{k}'");
    }
    let fields: Vec<_> = full.fields().iter().filter(|f| keys.contains(f.name())).cloned().collect();
    let schema = Arc::new(Schema::new(fields));
    let reader = arrow::json::ReaderBuilder::new(schema.clone()).with_batch_size(1 << 20).build(text.as_bytes())?;
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(arrow::compute::concat_batches(&schema, &batches)?)
}

fn run(cli: Cli) -> Result<()> {
    let store = Store::open(&cli.store).with_context(|| format!("opening store {}", cli.store.display()))?;
    let mut out = std::io::stdout().lock();
    match cli.cmd {
        Cmd::Init => writeln!(out, "initialized {}", cli.store.display())?,
        Cmd::Import { name, path } => {
            let s = store.import_file(&name, &path)?;
            writeln!(
                out,
                "imported {} rows, {} row groups, {} pages (snapshot {})",
                s.num_rows, s.row_groups, s.pages, s.snapshot_id
            )?;
        }
        Cmd::Create { name, columns, codec } => {
            let specs = columns
                .iter()
                .map(|c| {
                    let parts: Vec<&str> = c.split(':').collect();
                    let required = match parts.get(2) {
                        None => false,
                        Some(&"required") => true,
                        Some(&"optional") => false,
                        Some(x) => bail!("bad column modifier '{x}' in '{c}'"),
                    };
                    if parts.len() < 2 || parts.len() > 3 {
                        bail!("column must be name:type[:required]: '{c}'");
                    }
                    Ok(spec_for_type_name(0, parts[0], parts[1], !required)?)
                })
                .collect::<Result<Vec<_>>>()?;
            let sid = store.create(&name, &specs, parse_codec(&codec)?)?;
            writeln!(out, "created {name} (snapshot {sid})")?;
        }
        Cmd::Ls => {
            for name in store.read(meta::list_files)? {
                let v = store.view(&name, None)?;
                writeln!(out, "{name}\t{} rows\tsnapshot {}", v.snapshot.num_rows, v.snapshot.snapshot_id)?;
            }
        }
        Cmd::Snapshots { name } => {
            let file = store.read(|c| meta::load_file(c, &name))?;
            let snaps = store.read(|c| meta::list_snapshots(c, file.file_id))?;
            writeln!(out, "id\tparent\trows\tschema\top\tcreated\tsummary")?;
            for s in snaps {
                let cur = if Some(s.snapshot_id) == file.current_snapshot_id { "*" } else { "" };
                writeln!(
                    out,
                    "{}{cur}\t{}\t{}\t{}\t{}\t{}\t{}",
                    s.snapshot_id,
                    s.parent_id.map(|p| p.to_string()).unwrap_or("-".into()),
                    s.num_rows,
                    s.schema_version_id,
                    s.operation,
                    s.created_at,
                    s.summary.unwrap_or_default()
                )?;
            }
        }
        Cmd::Schema { name, at } => {
            let v = store.view(&name, at)?;
            let a = arrow_schema(&v.schema)?;
            writeln!(out, "schema version {}", v.schema.schema_version_id)?;
            for (c, f) in v.schema.columns.iter().zip(a.fields()) {
                let default = match &c.default_value {
                    Some(d) => format!(" default={:?}", vpf::values::decode_value(d)?),
                    None => String::new(),
                };
                writeln!(
                    out,
                    "{}\t{}\t{}\t{}{}",
                    c.name,
                    c.physical,
                    f.data_type(),
                    if c.nullable { "optional" } else { "required" },
                    default
                )?;
            }
        }
        Cmd::Cat { name, at, limit, format } => {
            let (schema, mut batches) = read_snapshot(&store, &name, at, limit)?;
            if batches.is_empty() {
                batches.push(RecordBatch::new_empty(schema));
            }
            match format {
                CatFormat::Table => writeln!(out, "{}", arrow::util::pretty::pretty_format_batches(&batches)?)?,
                CatFormat::Json => {
                    let mut w = arrow::json::LineDelimitedWriter::new(&mut out);
                    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
                    w.finish()?;
                }
                CatFormat::Csv => {
                    let mut w = arrow::csv::WriterBuilder::new().with_header(true).build(&mut out);
                    for b in &batches {
                        w.write(b)?;
                    }
                }
            }
        }
        Cmd::Export { name, out: path, at } => {
            let n = store.export(&name, at, &path)?;
            writeln!(out, "wrote {n} bytes to {}", path.display())?;
        }
        Cmd::Append { name, parquet, json, mode } => {
            let batch = match (parquet, json) {
                (Some(p), _) => {
                    let f = std::fs::File::open(&p).with_context(|| format!("opening {}", p.display()))?;
                    let b = ParquetRecordBatchReaderBuilder::try_new(f)?;
                    let schema = b.schema().clone();
                    let batches = b.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
                    arrow::compute::concat_batches(&schema, &batches)?
                }
                (None, Some(j)) => read_json(&store, &name, &j)?,
                (None, None) => unreachable!(),
            };
            let mode = match mode {
                Mode::Auto => AppendMode::Auto,
                Mode::Buffered => AppendMode::Buffered,
                Mode::Direct => AppendMode::Direct,
            };
            let s = store.append(&name, &batch, mode)?;
            let how = if s.buffered { "buffered" } else { "as a row group" };
            writeln!(out, "appended {} rows {how} (snapshot {})", s.num_rows, s.snapshot_id)?;
        }
        Cmd::Flush { name } => match store.flush(&name)? {
            Some(sid) => writeln!(out, "flushed (snapshot {sid})")?,
            None => writeln!(out, "nothing to flush")?,
        },
        Cmd::Delete { name, predicate } => {
            let s = store.delete_where(&name, &predicate)?;
            match s.snapshot_id {
                Some(sid) => writeln!(
                    out,
                    "deleted {} rows; segments removed {}, rewritten {}; pages reused {}, rewritten {}, dropped {} (snapshot {sid})",
                    s.rows_deleted, s.segments_removed, s.segments_rewritten, s.pages.reused, s.pages.rewritten, s.pages.dropped
                )?,
                None => writeln!(out, "no rows matched")?,
            }
        }
        Cmd::Update { name, set, predicate } => {
            let s = store.update_where(&name, &set, &predicate)?;
            match s.snapshot_id {
                Some(sid) => writeln!(
                    out,
                    "updated {} rows; chunks reused {}; pages reused {}, rewritten {} (snapshot {sid})",
                    s.rows_updated, s.chunks_reused, s.pages.reused, s.pages.rewritten
                )?,
                None => writeln!(out, "no rows matched")?,
            }
        }
        Cmd::Alter { name, op } => {
            let op = match op {
                AlterCmd::AddColumn { column, ty, required, default } => AlterOp::AddColumn {
                    spec: spec_for_type_name(0, &column, &ty, !required)?,
                    default: default.as_deref().map(parse_literal).transpose()?,
                },
                AlterCmd::DropColumn { column } => AlterOp::DropColumn(column),
                AlterCmd::RenameColumn { from, to } => AlterOp::RenameColumn { from, to },
                AlterCmd::Reorder { columns } => AlterOp::Reorder(columns),
                AlterCmd::Widen { column, to } => AlterOp::Widen {
                    column,
                    to: match to {
                        WidenTo::Int64 => PhysicalType::INT64,
                        WidenTo::Double => PhysicalType::DOUBLE,
                    },
                },
                AlterCmd::MakeOptional { column } => AlterOp::MakeOptional(column),
            };
            let s = store.alter(&name, op)?;
            writeln!(
                out,
                "schema version {}; {} chunks written (snapshot {})",
                s.schema_version_id, s.chunks_written, s.snapshot_id
            )?;
        }
        Cmd::Stats { name, at } => {
            let v = store.view(&name, at)?;
            let (mut rgs, mut batches, mut buffered_rows, mut pages, mut bytes) = (0, 0, 0, 0, 0u64);
            for s in &v.segments {
                match s {
                    SegmentData::RowGroup { chunks, .. } => {
                        rgs += 1;
                        for c in chunks {
                            pages += c.pages.len();
                            bytes += c.pages.iter().map(|p| p.blob_len).sum::<u64>();
                        }
                    }
                    SegmentData::Batch { batch, .. } => {
                        batches += 1;
                        buffered_rows += batch.num_rows;
                    }
                }
            }
            let vf = store.open_snapshot(&name, at)?;
            writeln!(out, "snapshot      {}", v.snapshot.snapshot_id)?;
            writeln!(out, "rows          {}", v.snapshot.num_rows)?;
            writeln!(out, "columns       {}", v.schema.columns.len())?;
            writeln!(out, "row groups    {rgs} stored + {batches} buffered batches ({buffered_rows} rows)")?;
            writeln!(out, "stored pages  {pages} ({bytes} bytes)")?;
            writeln!(out, "virtual size  {} bytes", vf.len())?;
        }
    }
    Ok(())
}
