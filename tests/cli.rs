mod common;

use std::path::Path;
use std::process::{Command, Output};

use common::*;
use parquet::basic::Compression;

fn vpf(store: &Path, args: &[&str], stdin: Option<&str>) -> Output {
    use std::io::Write;
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_vpf"));
    cmd.arg("--store").arg(store).args(args);
    cmd.stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    if let Some(s) = stdin {
        input.write_all(s.as_bytes()).unwrap();
    }
    drop(input);
    child.wait_with_output().unwrap()
}

fn ok(store: &Path, args: &[&str]) -> String {
    let o = vpf(store, args, None);
    assert!(o.status.success(), "{args:?}: {}", String::from_utf8_lossy(&o.stderr));
    String::from_utf8(o.stdout).unwrap()
}

fn err(store: &Path, args: &[&str]) -> String {
    let o = vpf(store, args, None);
    assert!(!o.status.success(), "{args:?} should fail");
    String::from_utf8(o.stderr).unwrap()
}

#[test]
fn full_workflow() {
    let d = tempfile::tempdir().unwrap();
    let st = d.path().join("store");
    let src = d.path().join("src.parquet");
    let base = sample_batch(0, 1000);
    std::fs::write(&src, write_parquet(&[base.clone()], props(Compression::SNAPPY, false, true, 100, 400))).unwrap();
    ok(&st, &["init"]);
    assert!(ok(&st, &["import", "t", src.to_str().unwrap()]).contains("imported 1000 rows, 3 row groups"));
    let more = d.path().join("more.parquet");
    std::fs::write(&more, write_parquet(&[sample_batch(1000, 50)], props(Compression::SNAPPY, false, true, 100, 400))).unwrap();
    assert!(ok(&st, &["append", "t", "--parquet", more.to_str().unwrap()]).contains("appended 50 rows buffered"));
    assert!(ok(&st, &["append", "t", "--parquet", more.to_str().unwrap(), "--mode", "direct"]).contains("as a row group"));
    let o = vpf(&st, &["append", "t", "--json", "-"], Some("{\"id\": 5000, \"name\": \"json\"}\n{\"id\": 5001}\n"));
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(ok(&st, &["delete", "t", "--where", "id < 100"]).contains("deleted 100 rows"));
    assert!(ok(&st, &["update", "t", "--set", "name = 'upd'", "--where", "id = 500"]).contains("updated 1 rows"));
    assert!(ok(&st, &["delete", "t", "--where", "id = -1"]).contains("no rows matched"));
    ok(&st, &["alter", "t", "add-column", "level", "int64", "--required", "--default", "3"]);
    ok(&st, &["alter", "t", "rename-column", "score", "points"]);
    ok(&st, &["alter", "t", "widen", "i32", "int64"]);
    ok(&st, &["alter", "t", "make-optional", "id"]);
    ok(&st, &["alter", "t", "drop-column", "bin"]);
    assert!(ok(&st, &["flush", "t"]).contains("flushed"));
    assert!(ok(&st, &["flush", "t"]).contains("nothing to flush"));

    let schema = ok(&st, &["schema", "t"]);
    assert!(schema.contains("points\tDOUBLE\tFloat64\toptional"), "{schema}");
    assert!(schema.contains("i32\tINT64\tInt64"), "{schema}");
    assert!(schema.contains("level\tINT64\tInt64\trequired"), "{schema}");
    assert!(!schema.contains("bin"));
    let old_schema = ok(&st, &["schema", "t", "--at", "1"]);
    assert!(old_schema.contains("score") && old_schema.contains("bin"));

    let snaps = ok(&st, &["snapshots", "t"]);
    assert_eq!(snaps.lines().count(), 1 + 12, "{snaps}");
    assert!(snaps.lines().last().unwrap().contains("*\t"));
    assert!(ok(&st, &["ls"]).starts_with("t\t1002 rows"));

    let json = ok(&st, &["cat", "t", "--format", "json"]);
    assert_eq!(json.lines().count(), 1002);
    assert!(json.contains("\"id\":500,") && json.contains("\"name\":\"upd\""));
    assert!(json.lines().last().unwrap().contains("\"id\":5001"));
    assert!(json.lines().last().unwrap().contains("\"level\":3"));
    let csv = ok(&st, &["cat", "t", "--format", "csv", "--limit", "3"]);
    assert_eq!(csv.lines().count(), 4);
    assert!(csv.starts_with("id,i32,name,points"));
    let table = ok(&st, &["cat", "t", "--at", "1", "--limit", "2"]);
    assert!(table.contains("| id ") && table.lines().count() == 6, "{table}");
    let stats = ok(&st, &["stats", "t"]);
    assert!(stats.contains("rows          1002") && stats.contains("0 buffered batches"), "{stats}");

    let out = d.path().join("out.parquet");
    ok(&st, &["export", "t", out.to_str().unwrap()]);
    let got = concat(&read_parquet(std::fs::read(&out).unwrap().into()));
    assert_eq!(got.num_rows(), 1002);
    let old = d.path().join("old.parquet");
    ok(&st, &["export", "t", old.to_str().unwrap(), "--at", "1"]);
    assert_eq!(concat(&read_parquet(std::fs::read(&old).unwrap().into())), base);
    if python().is_some() {
        let exp = d.path().join("exp.parquet");
        std::fs::write(&exp, write_parquet(&[base], props(Compression::SNAPPY, false, true, 1000, 100_000))).unwrap();
        run_python(&[&py_script("check_equal.py"), exp.to_str().unwrap(), old.to_str().unwrap()]);
    }
}

#[test]
fn create_and_errors() {
    let d = tempfile::tempdir().unwrap();
    let st = d.path();
    ok(st, &["create", "e", "id:int64:required", "name:string", "amt:decimal(9,2)", "--codec", "zstd"]);
    assert!(ok(st, &["cat", "e"]).contains("id"));
    assert_eq!(ok(st, &["cat", "e", "--format", "csv"]).trim(), "id,name,amt");
    let o = vpf(st, &["append", "e", "--json", "-"], Some("{\"id\":1,\"amt\":\"12.34\"}\n{\"id\":2,\"name\":\"x\"}\n"));
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let rows = ok(st, &["cat", "e", "--format", "json"]);
    assert!(rows.contains("\"amt\":\"12.34\"") || rows.contains("\"amt\":12.34"), "{rows}");

    assert!(err(st, &["create", "e", "id:int64"]).contains("exists"));
    assert!(err(st, &["create", "x", "id:wat"]).contains("unknown column type"));
    assert!(err(st, &["create", "x", "id:int64:sometimes"]).contains("bad column modifier"));
    assert!(err(st, &["create", "x", "id:int64", "--codec", "lzo"]).contains("unknown codec"));
    assert!(err(st, &["cat", "missing"]).contains("not found"));
    assert!(err(st, &["delete", "e", "--where", "nope = 1"]).contains("nope"));
    assert!(err(st, &["delete", "e", "--where", "id = "]).contains("predicate"));
    assert!(err(st, &["update", "e", "--set", "id = NULL", "--where", "TRUE"]).len() > 0);
    assert!(err(st, &["alter", "e", "add-column", "z", "int64", "--required"]).contains("needs a default"));
    assert!(err(st, &["alter", "e", "widen", "name", "int64"]).contains("widening"));
    assert!(err(st, &["cat", "e", "--at", "999"]).contains("not found"));
    let o = vpf(st, &["append", "e", "--json", "-"], Some("{\"bogus\":1}\n"));
    assert!(!o.status.success() && String::from_utf8_lossy(&o.stderr).contains("unknown column 'bogus'"));
    // Failed commands left no trace.
    assert_eq!(ok(st, &["snapshots", "e"]).lines().count(), 1 + 2);
}
