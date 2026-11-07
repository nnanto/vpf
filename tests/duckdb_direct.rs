mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::store::Store;

#[test]
fn duckdb_reads_virtual_snapshots_directly() {
    let Some(python) = python() else {
        eprintln!("skipping: VPF_PYTHON not set");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let base = sample_batch(0, 1000);
    let imported = store
        .import(
            "f",
            &write_parquet(&[base], props(Compression::SNAPPY, false, true, 100, 400)),
        )
        .unwrap();
    let frozen = store
        .open_snapshot("f", Some(imported.snapshot_id))
        .unwrap();
    store
        .append("f", &sample_batch(1000, 200), AppendMode::Buffered)
        .unwrap();
    store
        .update_where("f", "name = 'changed'", "id >= 100 AND id < 150")
        .unwrap();
    store.delete_where("f", "id < 30").unwrap();
    let current = store.open_snapshot("f", None).unwrap();
    let snapshots = [frozen, current];

    let mut child = Command::new(python)
        .arg(py_script("check_duckdb_direct.py"))
        .arg(snapshots[0].size().to_string())
        .arg(snapshots[1].size().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut requests = [0usize; 2];
    let mut nonzero_offsets = [false; 2];
    loop {
        let mut line = String::new();
        if output.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let parts: Vec<_> = line.split_whitespace().collect();
        assert_eq!(parts.len(), 3, "invalid read request: {line:?}");
        let index: usize = parts[0].parse().unwrap();
        let offset: u64 = parts[1].parse().unwrap();
        let length: usize = parts[2].parse().unwrap();
        let snapshot = snapshots.get(index).expect("invalid snapshot index");
        let bytes = snapshot.read_range(offset, length).unwrap();
        input.write_all(&bytes).unwrap();
        input.flush().unwrap();
        requests[index] += 1;
        nonzero_offsets[index] |= offset > 0;
    }
    drop(input);
    assert!(
        child.wait().unwrap().success(),
        "DuckDB direct-read check failed"
    );
    assert!(
        requests.iter().all(|&n| n > 0),
        "both snapshots must be read"
    );
    assert!(
        nonzero_offsets.iter().all(|&seen| seen),
        "both snapshots must be read at nonzero offsets"
    );
}
