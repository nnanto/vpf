mod common;

use std::path::Path;
use std::process::{Command, Output};

use common::*;
use parquet::basic::Compression;

fn vpf(store: &Path, args: &[&str], stdin: Option<&str>) -> Output {
    use std::io::Write;
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_vpf"));
    cmd.arg(store).args(args);
    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
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
    assert!(
        o.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&o.stderr)
    );
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
    let st = d.path().join("t.vpf");
    let src = d.path().join("src.parquet");
    let base = sample_batch(0, 1000);
    std::fs::write(
        &src,
        write_parquet(
            &[base.clone()],
            props(Compression::SNAPPY, false, true, 100, 400),
        ),
    )
    .unwrap();
    assert!(
        ok(&st, &["import", src.to_str().unwrap()]).contains("imported 1000 rows, 3 row groups")
    );
    let more = d.path().join("more.parquet");
    std::fs::write(
        &more,
        write_parquet(
            &[sample_batch(1000, 50)],
            props(Compression::SNAPPY, false, true, 100, 400),
        ),
    )
    .unwrap();
    assert!(
        ok(&st, &["append", "--parquet", more.to_str().unwrap()])
            .contains("appended 50 rows buffered")
    );
    assert!(
        ok(
            &st,
            &[
                "append",
                "--parquet",
                more.to_str().unwrap(),
                "--mode",
                "direct"
            ]
        )
        .contains("as a row group")
    );
    let o = vpf(
        &st,
        &["append", "--json", "-"],
        Some("{\"id\": 5000, \"name\": \"json\"}\n{\"id\": 5001}\n"),
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(ok(&st, &["delete", "--where", "id < 100"]).contains("deleted 100 rows"));
    assert!(
        ok(
            &st,
            &["update", "--set", "name = 'upd'", "--where", "id = 500"]
        )
        .contains("updated 1 rows")
    );
    assert!(ok(&st, &["delete", "--where", "id = -1"]).contains("no rows matched"));
    ok(
        &st,
        &[
            "alter",
            "add-column",
            "level",
            "int64",
            "--required",
            "--default",
            "3",
        ],
    );
    ok(&st, &["alter", "rename-column", "score", "points"]);
    ok(&st, &["alter", "widen", "i32", "int64"]);
    ok(&st, &["alter", "make-optional", "id"]);
    ok(&st, &["alter", "drop-column", "bin"]);
    assert!(ok(&st, &["flush"]).contains("flushed"));
    assert!(ok(&st, &["flush"]).contains("nothing to flush"));

    let schema = ok(&st, &["schema"]);
    assert!(
        schema.contains("points\tDOUBLE\tFloat64\toptional"),
        "{schema}"
    );
    assert!(schema.contains("i32\tINT64\tInt64"), "{schema}");
    assert!(schema.contains("level\tINT64\tInt64\trequired"), "{schema}");
    assert!(!schema.contains("bin"));
    let old_schema = ok(&st, &["schema", "--at", "1"]);
    assert!(old_schema.contains("score") && old_schema.contains("bin"));

    let snaps = ok(&st, &["snapshots"]);
    assert_eq!(snaps.lines().count(), 1 + 12, "{snaps}");
    assert!(snaps.lines().last().unwrap().contains("*\t"));
    assert!(ok(&st, &["info"]).starts_with("t\t1002 rows"));

    let json = ok(&st, &["cat", "--format", "json"]);
    assert_eq!(json.lines().count(), 1002);
    assert!(json.contains("\"id\":500,") && json.contains("\"name\":\"upd\""));
    assert!(json.lines().last().unwrap().contains("\"id\":5001"));
    assert!(json.lines().last().unwrap().contains("\"level\":3"));
    let csv = ok(&st, &["cat", "--format", "csv", "--limit", "3"]);
    assert_eq!(csv.lines().count(), 4);
    assert!(csv.starts_with("id,i32,name,points"));
    let table = ok(&st, &["cat", "--at", "1", "--limit", "2"]);
    assert!(
        table.contains("| id ") && table.lines().count() == 6,
        "{table}"
    );
    let stats = ok(&st, &["stats"]);
    assert!(
        stats.contains("rows          1002") && stats.contains("0 buffered batches"),
        "{stats}"
    );

    let out = d.path().join("out.parquet");
    ok(&st, &["export", out.to_str().unwrap()]);
    let got = concat(&read_parquet(std::fs::read(&out).unwrap().into()));
    assert_eq!(got.num_rows(), 1002);
    let old = d.path().join("old.parquet");
    ok(&st, &["export", old.to_str().unwrap(), "--at", "1"]);
    assert_eq!(
        concat(&read_parquet(std::fs::read(&old).unwrap().into())),
        base
    );
    if python().is_some() {
        let exp = d.path().join("exp.parquet");
        std::fs::write(
            &exp,
            write_parquet(
                &[base],
                props(Compression::SNAPPY, false, true, 1000, 100_000),
            ),
        )
        .unwrap();
        run_python(&[
            &py_script("check_equal.py"),
            exp.to_str().unwrap(),
            old.to_str().unwrap(),
        ]);
    }
}

#[test]
fn create_and_errors() {
    let d = tempfile::tempdir().unwrap();
    let st = &d.path().join("e.vpf");
    let x = &d.path().join("x.vpf");
    ok(
        st,
        &[
            "create",
            "id:int64:required",
            "name:string",
            "amt:decimal(9,2)",
            "--codec",
            "zstd",
        ],
    );
    assert!(ok(st, &["cat"]).contains("id"));
    assert_eq!(ok(st, &["cat", "--format", "csv"]).trim(), "id,name,amt");
    let o = vpf(
        st,
        &["append", "--json", "-"],
        Some("{\"id\":1,\"amt\":\"12.34\"}\n{\"id\":2,\"name\":\"x\"}\n"),
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let rows = ok(st, &["cat", "--format", "json"]);
    assert!(
        rows.contains("\"amt\":\"12.34\"") || rows.contains("\"amt\":12.34"),
        "{rows}"
    );

    assert!(err(st, &["create", "id:int64"]).contains("exists"));
    assert!(err(x, &["create", "id:wat"]).contains("unknown column type"));
    assert!(err(x, &["create", "id:int64:sometimes"]).contains("bad column modifier"));
    assert!(err(x, &["create", "id:int64", "--codec", "lzo"]).contains("unknown codec"));
    assert!(err(&d.path().join("missing.vpf"), &["cat"]).contains("does not exist"));
    assert!(!d.path().join("missing.vpf").exists());
    assert!(err(st, &["create", "--name", "other", "id:int64"]).contains("exactly one table"));
    assert!(err(st, &["delete", "--where", "nope = 1"]).contains("nope"));
    assert!(err(st, &["delete", "--where", "id = "]).contains("predicate"));
    assert!(err(st, &["update", "--set", "id = NULL", "--where", "TRUE"]).len() > 0);
    assert!(
        err(st, &["alter", "add-column", "z", "int64", "--required"]).contains("needs a default")
    );
    assert!(err(st, &["alter", "widen", "name", "int64"]).contains("widening"));
    assert!(err(st, &["cat", "--at", "999"]).contains("not found"));
    let o = vpf(st, &["append", "--json", "-"], Some("{\"bogus\":1}\n"));
    assert!(
        !o.status.success()
            && String::from_utf8_lossy(&o.stderr).contains("unknown column 'bogus'")
    );
    // Failed commands left no trace.
    assert_eq!(ok(st, &["snapshots"]).lines().count(), 1 + 2);
}

#[test]
fn nested_columns() {
    let d = tempfile::tempdir().unwrap();
    let st = &d.path().join("n.vpf");
    let x = &d.path().join("x.vpf");
    ok(
        st,
        &[
            "create",
            "id:int64:required",
            "tags:list<string>",
            "attrs:map<string,int64>",
            "pt:struct<x:double!,y:list<int32!>>:optional",
            "--codec",
            "zstd",
        ],
    );
    let schema = ok(st, &["schema"]);
    assert!(
        schema.contains("tags\tGROUP\tList(")
            && schema.contains("attrs\tGROUP\tMap(")
            && schema.contains("pt\tGROUP\tStruct("),
        "{schema}"
    );
    let rows = concat!(
        "{\"id\":1,\"tags\":[\"a\",null,\"b\"],\"attrs\":{\"k\":1,\"j\":null},\"pt\":{\"x\":1.5,\"y\":[1,2]}}\n",
        "{\"id\":2,\"tags\":[],\"pt\":{\"x\":2.5,\"y\":null}}\n",
        "{\"id\":3}\n",
    );
    let o = vpf(st, &["append", "--json", "-"], Some(rows));
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let o = vpf(
        st,
        &["append", "--json", "-", "--mode", "direct"],
        Some("{\"id\":4,\"tags\":[\"z\"],\"pt\":null}\n"),
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let json = ok(st, &["cat", "--format", "json"]);
    let lines: Vec<&str> = json.lines().collect();
    assert_eq!(lines.len(), 4);
    assert!(
        lines[0].contains("\"tags\":[\"a\",null,\"b\"]") && lines[0].contains("\"x\":1.5"),
        "{json}"
    );
    assert!(lines[0].contains("\"y\":[1,2]"), "{json}");
    assert!(ok(st, &["delete", "--where", "pt.x > 2"]).contains("deleted 1 rows"));
    assert!(
        ok(st, &["update", "--set", "tags = NULL", "--where", "id = 1"]).contains("updated 1 rows")
    );
    assert!(
        ok(
            st,
            &["delete", "--where", "pt IS NULL AND tags IS NOT NULL"]
        )
        .contains("deleted 1 rows")
    );
    ok(
        st,
        &[
            "alter",
            "add-column",
            "more",
            "list<struct<a:int32,b:string>>",
        ],
    );
    let json = ok(st, &["cat", "--format", "json"]);
    assert_eq!(json.lines().count(), 2, "{json}");
    let l: Vec<&str> = json.lines().collect();
    assert!(
        l[0].contains("\"id\":1,") && !l[0].contains("\"tags\":[") && l[1].contains("\"id\":3"),
        "{json}"
    );
    let o = vpf(
        st,
        &["append", "--json", "-"],
        Some("{\"id\":9,\"more\":[{\"a\":1,\"b\":\"q\"},{\"a\":null}]}\n"),
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(ok(st, &["cat", "--format", "json"]).contains("\"more\":[{\"a\":1,\"b\":\"q\"},{"));
    let out = d.path().join("out.parquet");
    ok(st, &["export", out.to_str().unwrap()]);
    assert_eq!(
        concat(&read_parquet(std::fs::read(&out).unwrap().into())).num_rows(),
        3
    );

    assert!(err(x, &["create", "a:list<int64"]).contains("expected '>'"));
    assert!(err(x, &["create", "a:struct<>"]).contains("expected a name"));
    assert!(err(x, &["create", "a:list<wat>"]).contains("unknown type"));
    assert!(err(x, &["create", "a:map<string,int64>:sometimes"]).contains("bad column modifier"));
    assert!(err(st, &["update", "--set", "tags = 'x'", "--where", "TRUE"]).len() > 0);
    assert!(err(st, &["alter", "widen", "pt", "int64"]).len() > 0);
}
