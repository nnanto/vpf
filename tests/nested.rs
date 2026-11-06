mod common;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use common::*;
use parquet::basic::Compression;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::{Value, json};
use vpf::codec::{RawPage, chunk_page_rows, decode_pages, encode_column};
use vpf::layout::{FileProps, RowGroupLayout, VirtualFile};
use vpf::ops::alter::AlterOp;
use vpf::ops::append::AppendMode;
use vpf::ops::snapshot::{SegmentData, chunk_layout};
use vpf::predicate::Literal;
use vpf::schema::ColumnSpec;
use vpf::store::Store;
use vpf::values::ColumnData;

fn item(dt: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new("item", dt, nullable))
}

fn inner_struct() -> DataType {
    DataType::Struct(Fields::from(vec![Field::new("d", DataType::Float64, true)]))
}

fn s_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("inner", inner_struct(), true),
    ]))
}

fn ls_item() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::List(item(DataType::Utf8, true)), true),
    ]))
}

fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int64, true),
            ])),
            false,
        )),
        false,
    )
}

fn nested_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("s", s_type(), true),
        Field::new("l", DataType::List(item(DataType::Int64, true)), true),
        Field::new("ls", DataType::List(item(ls_item(), false)), true),
        Field::new("m", map_type(), true),
        Field::new("ll", DataType::List(item(DataType::List(item(DataType::Int32, true)), true)), true),
        Field::new("req", DataType::Struct(Fields::from(vec![Field::new("k", DataType::Int32, false)])), false),
        Field::new("tag", DataType::Utf8, true),
    ]))
}

fn maybe<T: Into<Value>>(rng: &mut StdRng, p: f64, v: impl FnOnce(&mut StdRng) -> T) -> Value {
    if rng.random_bool(p) { Value::Null } else { v(rng).into() }
}

fn gen_row(rng: &mut StdRng, id: i64) -> Value {
    let long = rng.random_bool(0.05);
    let max_len = if long { 300 } else { 4 };
    json!({
        "id": id,
        "s": maybe(rng, 0.2, |r| json!({
            "a": maybe(r, 0.2, |r| r.random_range(-50..50)),
            "b": maybe(r, 0.2, |r| format!("b{}", r.random_range(0..20))),
            "inner": maybe(r, 0.3, |r| json!({"d": maybe(r, 0.3, |r| r.random_range(-1.0..1.0))})),
        })),
        "l": maybe(rng, 0.2, |r| (0..r.random_range(0..=max_len)).map(|_| maybe(r, 0.1, |r| r.random_range(0..1000i64))).collect::<Vec<_>>()),
        "ls": maybe(rng, 0.2, |r| (0..r.random_range(0..=3)).map(|_| json!({
            "x": r.random_range(0..9),
            "y": maybe(r, 0.3, |r| (0..r.random_range(0..=3)).map(|i| maybe(r, 0.2, |_| format!("y{i}"))).collect::<Vec<_>>()),
        })).collect::<Vec<_>>()),
        "m": maybe(rng, 0.2, |r| {
            let mut o = serde_json::Map::new();
            for k in 0..r.random_range(0..=3) {
                o.insert(format!("k{k}"), maybe(r, 0.2, |r| r.random_range(0..100i64)));
            }
            Value::Object(o)
        }),
        "ll": maybe(rng, 0.2, |r| (0..r.random_range(0..=3)).map(|_| maybe(r, 0.2, |r| (0..r.random_range(0..=3)).map(|_| maybe(r, 0.2, |r| r.random_range(0..10))).collect::<Vec<_>>())).collect::<Vec<_>>()),
        "req": {"k": rng.random_range(0..5)},
        "tag": maybe(rng, 0.3, |r| ["x", "y", "z"][r.random_range(0..3)]),
    })
}

fn gen_rows(rng: &mut StdRng, start: i64, n: usize) -> Vec<Value> {
    (0..n as i64).map(|i| gen_row(rng, start + i)).collect()
}

fn to_batch(schema: &SchemaRef, rows: &[Value]) -> RecordBatch {
    let text: String = rows.iter().map(|r| r.to_string() + "\n").collect();
    let mut reader = arrow::json::ReaderBuilder::new(schema.clone())
        .with_batch_size(rows.len().max(1))
        .build(std::io::Cursor::new(text))
        .unwrap();
    match reader.next() {
        Some(b) => b.unwrap(),
        None => RecordBatch::new_empty(schema.clone()),
    }
}

fn current(store: &Store, name: &str) -> RecordBatch {
    read_virtual(&store.open_snapshot(name, None).unwrap())
}

fn assert_model(store: &Store, name: &str, schema: &SchemaRef, rows: &[Value], ctx: &str) {
    let got = current(store, name);
    let want = to_batch(schema, rows);
    assert_eq!(got.num_rows(), want.num_rows(), "{ctx}: row count");
    assert_eq!(got.schema().fields(), want.schema().fields(), "{ctx}: schema");
    for (i, f) in want.schema().fields().iter().enumerate() {
        assert_eq!(got.column(i).as_ref(), want.column(i).as_ref(), "{ctx}: column {}", f.name());
    }
}

#[test]
fn arrow_nested_files_import_and_pages_roundtrip() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let schema = nested_schema();
    let mut rng = StdRng::seed_from_u64(1);
    let rows = gen_rows(&mut rng, 0, 1500);
    let batch = to_batch(&schema, &rows);
    let mut i = 0;
    for codec in [Compression::SNAPPY, Compression::UNCOMPRESSED, Compression::ZSTD(Default::default())] {
        for v2 in [false, true] {
            for dict in [false, true] {
                let name = format!("f{i}");
                i += 1;
                let bytes = write_parquet(&[batch.clone()], props(codec, v2, dict, 97, 700));
                store.import(&name, &bytes).unwrap();
                assert_eq!(current(&store, &name), batch, "{name}");
                let view = store.view(&name, None).unwrap();
                let leaves = view.schema.leaves().unwrap();
                assert_eq!(leaves.len(), 12);
                for seg in &view.segments {
                    let SegmentData::RowGroup { rg, chunks, .. } = seg else { panic!() };
                    let mut layouts: Vec<_> = chunks.iter().map(|c| chunk_layout(c).unwrap()).collect();
                    for (li, (leaf, chunk)) in leaves.iter().zip(chunks).enumerate() {
                        let raw: Vec<RawPage> =
                            chunk.pages.iter().map(|p| RawPage::load(store.blobs(), p).unwrap()).collect();
                        let whole = decode_pages(leaf, chunk.codec, &raw).unwrap();
                        assert_eq!(whole.num_rows() as i64, rg.num_rows, "{}", leaf.spec.name);
                        // Stored page row counts match the levels.
                        let rows = chunk_page_rows(leaf, chunk.codec, &raw).unwrap().unwrap();
                        assert_eq!(rows, chunk.pages.iter().map(|p| p.num_rows).collect::<Vec<_>>());
                        // Page-by-page decoding equals whole-chunk decoding.
                        let dict_pages: Vec<RawPage> = raw.iter().filter(|p| !p.kind.is_data()).cloned().collect();
                        let mut by_page = ColumnData::empty_leaf(leaf.physical(), leaf.max_def, leaf.max_rep);
                        for (p, pr) in raw.iter().zip(&chunk.pages) {
                            if !p.kind.is_data() {
                                continue;
                            }
                            let mut pages = dict_pages.clone();
                            pages.push(p.clone());
                            let part = decode_pages(leaf, chunk.codec, &pages).unwrap();
                            assert_eq!(part.num_rows() as i64, pr.num_rows);
                            by_page.append(&part).unwrap();
                        }
                        assert_eq!(by_page, whole, "{}", leaf.spec.name);
                        // Re-encoding is lossless and keeps record-aligned pages.
                        let enc = encode_column(leaf, chunk.codec, &whole, li % 2 == 0).unwrap();
                        assert_eq!(enc.page_rows.iter().sum::<i64>(), rg.num_rows);
                        let raw2: Vec<RawPage> =
                            enc.pages.iter().map(|(b, h)| RawPage::from_header(b.clone(), h)).collect();
                        assert_eq!(decode_pages(leaf, chunk.codec, &raw2).unwrap(), whole);
                        layouts[li] = enc.layout(leaf.physical()).unwrap();
                    }
                    // A virtual file made of re-encoded chunks reads the same.
                    let vf = VirtualFile::build(
                        FileProps {
                            schema: view.schema.descriptor().unwrap(),
                            created_by: None,
                            key_value: view.schema.key_value.clone(),
                            version: 1,
                            blobs: Some(store.blobs()),
                        },
                        vec![RowGroupLayout { num_rows: rg.num_rows, chunks: layouts }],
                    )
                    .unwrap();
                    let start = view.segments.iter().take_while(|s| !std::ptr::eq(*s, seg)).map(|s| s.num_rows()).sum::<i64>();
                    assert_eq!(read_virtual(&vf), batch.slice(start as usize, rg.num_rows as usize));
                }
            }
        }
    }
}

fn eval(pred: &str, r: &Value) -> bool {
    let id = r["id"].as_i64().unwrap();
    match pred {
        "s IS NULL" => r["s"].is_null(),
        "s.a > 20" => r["s"]["a"].as_i64().is_some_and(|a| a > 20),
        "s.inner.d IS NOT NULL" => r["s"]["inner"]["d"].is_number(),
        "m IS NULL AND id < 300" => r["m"].is_null() && id < 300,
        "tag = 'x'" => r["tag"] == "x",
        "l IS NOT NULL AND tag = 'y'" => !r["l"].is_null() && r["tag"] == "y",
        "req.k = 3" => r["req"]["k"] == 3,
        p if p.starts_with("id < ") => id < p[5..].parse::<i64>().unwrap(),
        p if p.starts_with("id >= ") => id >= p[6..].parse::<i64>().unwrap(),
        p => panic!("unknown predicate {p}"),
    }
}

#[test]
fn nested_mutations_match_model() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("s")).unwrap();
    let schema = nested_schema();
    let mut rng = StdRng::seed_from_u64(7);
    let mut model = gen_rows(&mut rng, 0, 900);
    store.import("f", &write_parquet(&[to_batch(&schema, &model)], props(Compression::SNAPPY, false, true, 50, 400))).unwrap();
    assert_model(&store, "f", &schema, &model, "import");

    let mut next = 900i64;
    let preds = [
        "s IS NULL", "s.a > 20", "s.inner.d IS NOT NULL", "m IS NULL AND id < 300", "tag = 'x'",
        "l IS NOT NULL AND tag = 'y'", "req.k = 3",
    ];
    for step in 0..60 {
        let ctx = format!("step {step}");
        match rng.random_range(0..7) {
            0 | 1 => {
                let n = rng.random_range(1..80);
                let rows = gen_rows(&mut rng, next, n);
                next += n as i64;
                let mode = [AppendMode::Buffered, AppendMode::Direct, AppendMode::Auto][rng.random_range(0..3)];
                store.append("f", &to_batch(&schema, &rows), mode).unwrap();
                model.extend(rows);
            }
            2 => {
                let p = if rng.random_bool(0.5) {
                    preds[rng.random_range(0..preds.len())].to_string()
                } else {
                    let a = rng.random_range(0..next);
                    format!("id < {a}")
                };
                // Restrict to a window to keep data around.
                let lo = rng.random_range(0..next.max(1));
                let p = format!("({p}) AND id >= {lo} AND id < {}", lo + 40);
                let s = store.delete_where("f", &p).unwrap();
                let before = model.len();
                model.retain(|r| {
                    let id = r["id"].as_i64().unwrap();
                    let inner = p[1..p.find(") AND").unwrap()].to_string();
                    !(eval(&inner, r) && id >= lo && id < lo + 40)
                });
                assert_eq!(s.rows_deleted as usize, before - model.len(), "{ctx} {p}");
            }
            3 => {
                let p = preds[rng.random_range(0..preds.len())];
                let (set, col) = match rng.random_range(0..4) {
                    0 => ("s = NULL", "s"),
                    1 => ("m = NULL, l = NULL", "m"),
                    2 => ("ll = NULL", "ll"),
                    _ => ("tag = 'w'", "tag"),
                };
                let s = store.update_where("f", set, p).unwrap();
                let mut hits = 0;
                for r in model.iter_mut().filter(|r| eval(p, r)) {
                    hits += 1;
                    match col {
                        "m" => {
                            r["m"] = Value::Null;
                            r["l"] = Value::Null;
                        }
                        "tag" => r["tag"] = json!("w"),
                        c => r[c] = Value::Null,
                    }
                }
                assert_eq!(s.rows_updated, hits, "{ctx} {set} WHERE {p}");
            }
            4 => {
                store.flush("f").unwrap();
            }
            _ => {
                // Snapshot isolation: an opened snapshot keeps reading the same rows.
                let vf = store.open_snapshot("f", None).unwrap();
                let before = read_virtual(&vf);
                let rows = gen_rows(&mut rng, next, 3);
                next += 3;
                store.append("f", &to_batch(&schema, &rows), AppendMode::Buffered).unwrap();
                model.extend(rows);
                assert_eq!(read_virtual(&vf), before);
            }
        }
        assert_model(&store, "f", &schema, &model, &ctx);
    }
    // Unsupported assignments on nested columns fail cleanly and leave no snapshot.
    let sid = store.view("f", None).unwrap().snapshot.snapshot_id;
    assert!(store.update_where("f", "s = 'x'", "id < 10").is_err());
    assert!(store.update_where("f", "req = NULL", "id < 10").is_err());
    assert!(store.delete_where("f", "s.nope = 1").is_err());
    assert!(store.delete_where("f", "l > 1").is_err());
    assert_eq!(store.view("f", None).unwrap().snapshot.snapshot_id, sid);
    assert_model(&store, "f", &schema, &model, "after errors");

    // Other readers see the same nested data.
    if python().is_some() {
        let exp = d.path().join("exp.parquet");
        std::fs::write(&exp, write_parquet(&[to_batch(&schema, &model)], props(Compression::SNAPPY, false, true, 1000, 100_000))).unwrap();
        let out = d.path().join("out.parquet");
        store.export("f", None, &out).unwrap();
        run_python(&[&py_script("check_equal.py"), exp.to_str().unwrap(), out.to_str().unwrap()]);
    }
}

#[test]
fn nested_appends_into_created_file() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let schema = nested_schema();
    store.create_from_arrow("c", &schema, Compression::ZSTD(Default::default())).unwrap();
    let mut rng = StdRng::seed_from_u64(3);
    let mut model = Vec::new();
    for (i, mode) in [AppendMode::Buffered, AppendMode::Direct, AppendMode::Buffered, AppendMode::Buffered].iter().enumerate() {
        let rows = gen_rows(&mut rng, i as i64 * 1000, 250);
        store.append("c", &to_batch(&schema, &rows), *mode).unwrap();
        model.extend(rows);
        assert_model(&store, "c", &schema, &model, &format!("append {i}"));
    }
    store.flush("c").unwrap();
    assert_model(&store, "c", &schema, &model, "flush");

    // Missing nested columns are filled with NULL; a missing required nested column fails.
    let partial = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("req", DataType::Struct(Fields::from(vec![Field::new("k", DataType::Int32, false)])), false),
    ]));
    let rows = vec![json!({"id": 5000, "req": {"k": 1}})];
    store.append("c", &to_batch(&partial, &rows), AppendMode::Buffered).unwrap();
    model.push(json!({"id": 5000, "req": {"k": 1}}));
    assert_model(&store, "c", &schema, &model, "partial");
    let only_id = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    assert!(store.append("c", &to_batch(&only_id, &[json!({"id": 1})]), AppendMode::Buffered).is_err());

    // Compatible but different Arrow types are cast (Int32 list items -> Int64, LargeUtf8 -> Utf8).
    let alt = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("l", DataType::List(item(DataType::Int32, true)), true),
        Field::new("req", DataType::Struct(Fields::from(vec![Field::new("k", DataType::Int32, false)])), false),
        Field::new("tag", DataType::LargeUtf8, true),
    ]));
    let rows = vec![json!({"id": 6000, "l": [1, null, 3], "req": {"k": 2}, "tag": "t"})];
    store.append("c", &to_batch(&alt, &rows), AppendMode::Direct).unwrap();
    model.extend(rows);
    assert_model(&store, "c", &schema, &model, "cast");
    // Incompatible nested types are rejected.
    let wrong = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("s", DataType::Int32, true),
        Field::new("req", DataType::Struct(Fields::from(vec![Field::new("k", DataType::Int32, false)])), false),
    ]));
    assert!(store.append("c", &to_batch(&wrong, &[json!({"id": 1, "s": 1, "req": {"k": 1}})]), AppendMode::Direct).is_err());
    // Nulls inside a required nested field are rejected.
    let req_fields = Fields::from(vec![Field::new("k", DataType::Int32, false)]);
    let req_col = arrow::array::StructArray::new(
        req_fields.clone(),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 0]))],
        Some(vec![true, false].into()),
    );
    let lax = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("req", DataType::Struct(req_fields), true),
    ]));
    let nulls = RecordBatch::try_new(lax, vec![Arc::new(arrow::array::Int64Array::from(vec![7000, 7001])), Arc::new(req_col)]).unwrap();
    for mode in [AppendMode::Direct, AppendMode::Buffered] {
        assert!(store.append("c", &nulls, mode).is_err());
    }
    assert_model(&store, "c", &schema, &model, "after errors");
}

#[test]
fn nested_schema_evolution() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let schema = nested_schema();
    let mut rng = StdRng::seed_from_u64(11);
    let mut model = gen_rows(&mut rng, 0, 600);
    store.import("f", &write_parquet(&[to_batch(&schema, &model)], props(Compression::SNAPPY, true, true, 60, 250))).unwrap();
    let extra = gen_rows(&mut rng, 600, 20);
    store.append("f", &to_batch(&schema, &extra), AppendMode::Buffered).unwrap();
    model.extend(extra);

    // Add a nested column: existing rows read NULL; it is appendable afterwards.
    let tree = parquet::arrow::ArrowSchemaConverter::new()
        .convert(&Schema::new(vec![Field::new("extra", DataType::List(item(DataType::Utf8, true)), true)]))
        .unwrap();
    let spec = ColumnSpec::from_parquet_type(0, &tree.root_schema().get_fields()[0]).unwrap();
    assert!(store.alter("f", AlterOp::AddColumn { spec: spec.clone(), default: Some(Literal::Str("x".into())) }).is_err());
    let sum = store.alter("f", AlterOp::AddColumn { spec, default: None }).unwrap();
    assert_eq!(sum.row_groups_rewritten, 3);
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("extra", DataType::List(item(DataType::Utf8, true)), true));
    let mut schema = Arc::new(Schema::new(fields));
    assert_model(&store, "f", &schema, &model, "add");
    let rows = vec![json!({"id": 1000, "req": {"k": 1}, "extra": ["a", null]})];
    store.append("f", &to_batch(&schema, &rows), AppendMode::Buffered).unwrap();
    model.extend(rows);
    assert_model(&store, "f", &schema, &model, "append after add");

    // Rename / reorder / drop are metadata only.
    store.alter("f", AlterOp::RenameColumn { from: "s".into(), to: "s2".into() }).unwrap();
    let rename = |schema: &SchemaRef, from: &str, to: &str| {
        Arc::new(Schema::new(
            schema.fields().iter().map(|f| if f.name() == from { f.as_ref().clone().with_name(to) } else { f.as_ref().clone() }).collect::<Vec<_>>(),
        ))
    };
    schema = rename(&schema, "s", "s2");
    for r in &mut model {
        let v = r.as_object_mut().unwrap().remove("s").unwrap_or(Value::Null);
        r["s2"] = v;
    }
    assert_model(&store, "f", &schema, &model, "rename");
    assert!(store.delete_where("f", "s2.a > 40").unwrap().rows_deleted > 0);
    model.retain(|r| !r["s2"]["a"].as_i64().is_some_and(|a| a > 40));
    assert_model(&store, "f", &schema, &model, "delete by renamed path");

    let order = ["m", "id", "ll", "s2", "ls", "l", "extra", "req", "tag"];
    store.alter("f", AlterOp::Reorder(order.iter().map(|s| s.to_string()).collect())).unwrap();
    schema = Arc::new(Schema::new(order.iter().map(|n| schema.field_with_name(n).unwrap().clone()).collect::<Vec<_>>()));
    assert_model(&store, "f", &schema, &model, "reorder");

    store.alter("f", AlterOp::DropColumn("ls".into())).unwrap();
    schema = Arc::new(Schema::new(schema.fields().iter().filter(|f| f.name() != "ls").map(|f| f.as_ref().clone()).collect::<Vec<_>>()));
    assert_model(&store, "f", &schema, &model, "drop");

    // Required nested -> optional: every leaf gains a definition level.
    assert!(store.alter("f", AlterOp::MakeOptional("m".into())).is_err());
    store.alter("f", AlterOp::MakeOptional("req".into())).unwrap();
    schema = Arc::new(Schema::new(
        schema.fields().iter().map(|f| if f.name() == "req" { f.as_ref().clone().with_nullable(true) } else { f.as_ref().clone() }).collect::<Vec<_>>(),
    ));
    assert_model(&store, "f", &schema, &model, "make optional");
    let s = store.update_where("f", "req = NULL", "id < 100").unwrap();
    for r in model.iter_mut().filter(|r| r["id"].as_i64().unwrap() < 100) {
        r["req"] = Value::Null;
    }
    assert!(s.rows_updated > 0);
    assert_model(&store, "f", &schema, &model, "null after make optional");
    let rows = vec![json!({"id": 2000, "req": null}), json!({"id": 2001, "req": {"k": 4}})];
    store.append("f", &to_batch(&schema, &rows), AppendMode::Direct).unwrap();
    model.extend(rows);
    store.flush("f").unwrap();
    assert_model(&store, "f", &schema, &model, "append after make optional");

    // Widening nested columns is unsupported.
    assert!(store.alter("f", AlterOp::Widen { column: "l".into(), to: parquet::basic::Type::INT64 }).is_err());
    assert_model(&store, "f", &schema, &model, "final");
}

#[test]
fn pyarrow_nested_fixtures() {
    if python().is_none() {
        eprintln!("skipping: VPF_PYTHON not set");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let fx = d.path().join("fx");
    std::fs::create_dir_all(&fx).unwrap();
    run_python(&[&py_script("gen_nested.py"), fx.to_str().unwrap()]);
    let store = Store::open(d.path().join("s")).unwrap();
    let mut names: Vec<String> = std::fs::read_dir(&fx)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().trim_end_matches(".parquet").to_string())
        .collect();
    names.sort();
    assert!(names.len() >= 6, "{names:?}");
    for name in &names {
        let src = fx.join(format!("{name}.parquet"));
        store.import_file(name, &src).unwrap();
        let out = d.path().join(format!("{name}.out.parquet"));
        store.export(name, None, &out).unwrap();
        run_python(&[&py_script("check_equal.py"), src.to_str().unwrap(), out.to_str().unwrap()]);
        // Mutate: delete a window of rows and null a nested column, then compare against
        // the same operations done by pyarrow.
        let view = store.view(name, None).unwrap();
        let s = store.delete_where(name, "id >= 100 AND id < 160").unwrap();
        assert!(s.rows_deleted > 0, "{name}");
        let col = view.schema.columns.iter().find(|c| c.is_nested() && c.nullable).map(|c| c.name.clone());
        if let Some(c) = &col {
            store.update_where(name, &format!("{c} = NULL"), "id < 50").unwrap();
        }
        store.export(name, None, &out).unwrap();
        run_python(&[
            &py_script("check_nested_mutation.py"),
            src.to_str().unwrap(),
            out.to_str().unwrap(),
            col.as_deref().unwrap_or(""),
        ]);
    }
}

#[test]
fn legacy_two_level_lists() {
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    let schema = Arc::new(
        parquet::schema::parser::parse_message_type(
            "message m { required int64 id; optional group tags (LIST) { repeated binary str (UTF8); } repeated int32 nums; }",
        )
        .unwrap(),
    );
    let mut buf = Vec::new();
    {
        let props = Arc::new(WriterProperties::builder().set_data_page_row_count_limit(7).set_write_batch_size(5).build());
        let mut w = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let ids: Vec<i64> = (0..50).collect();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>().write_batch(&ids, None, None).unwrap();
        c.close().unwrap();
        // tags: row i has i % 4 elements; i % 5 == 0 is NULL.
        let (mut vals, mut defs, mut reps) = (Vec::new(), Vec::new(), Vec::new());
        for i in 0..50 {
            if i % 5 == 0 {
                defs.push(0);
                reps.push(0);
                continue;
            }
            let n = i % 4;
            if n == 0 {
                defs.push(1);
                reps.push(0);
            }
            for j in 0..n {
                vals.push(ByteArray::from(format!("t{i}_{j}").as_str()));
                defs.push(2);
                reps.push(if j == 0 { 0 } else { 1 });
            }
        }
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), Some(&reps)).unwrap();
        c.close().unwrap();
        let (mut vals, mut defs, mut reps) = (Vec::new(), Vec::new(), Vec::new());
        for i in 0..50i32 {
            let n = i % 3;
            if n == 0 {
                defs.push(0);
                reps.push(0);
            }
            for j in 0..n {
                vals.push(i * 10 + j);
                defs.push(1);
                reps.push((j > 0) as i16);
            }
        }
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<parquet::data_type::Int32Type>().write_batch(&vals, Some(&defs), Some(&reps)).unwrap();
        c.close().unwrap();
        rg.close().unwrap();
        w.close().unwrap();
    }
    let bytes = bytes::Bytes::from(buf);
    let original = concat(&read_parquet(bytes.clone()));
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    store.import("leg", &bytes).unwrap();
    let view = store.view("leg", None).unwrap();
    let SegmentData::RowGroup { chunks, .. } = &view.segments[0] else { panic!() };
    assert!(chunks[1].pages.len() > 2, "multiple pages");
    assert_eq!(current(&store, "leg"), original);

    // Page-level delete and update work on legacy layouts (no Arrow writer involved).
    let s = store.delete_where("leg", "id >= 10 AND id < 20").unwrap();
    assert_eq!(s.rows_deleted, 10);
    assert!(s.pages.reused > 0, "{:?}", s.pages);
    store.update_where("leg", "tags = NULL", "id > 40").unwrap();
    let got = current(&store, "leg");
    let keep: arrow::array::BooleanArray = (0..50).map(|i| Some(!(10..20).contains(&i))).collect();
    let exp = arrow::compute::filter_record_batch(&original, &keep).unwrap();
    assert_eq!(got.num_rows(), 40);
    assert_eq!(got.column(0), exp.column(0));
    assert_eq!(got.column(2), exp.column(2));
    let tags = got.column(1);
    for i in 0..40 {
        let id = exp.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(i);
        if id > 40 {
            assert!(tags.is_null(i));
        } else {
            assert_eq!(tags.slice(i, 1).to_data(), exp.column(1).slice(i, 1).to_data());
        }
    }

    // Appending Arrow data reproduces the legacy layout exactly.
    let app = got.slice(0, 5);
    store.append("leg", &app, AppendMode::Direct).unwrap();
    store.append("leg", &app, AppendMode::Buffered).unwrap();
    let after = current(&store, "leg");
    assert_eq!(after.slice(40, 5), app);
    assert_eq!(after.slice(45, 5), app);
}

/// Minimal Thrift compact encoding of a V1 `PageHeader` (PLAIN values, RLE levels).
fn v1_header(num_values: i32, body_len: i32) -> Vec<u8> {
    fn varint(out: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    fn i32f(out: &mut Vec<u8>, delta: u8, v: i32) {
        out.push((delta << 4) | 5);
        varint(out, ((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    let mut h = Vec::new();
    i32f(&mut h, 1, 0); // type = DATA_PAGE
    i32f(&mut h, 1, body_len); // uncompressed_page_size
    i32f(&mut h, 1, body_len); // compressed_page_size
    h.push((2 << 4) | 12); // field 5: data_page_header
    i32f(&mut h, 1, num_values);
    i32f(&mut h, 1, 0); // encoding = PLAIN
    i32f(&mut h, 1, 3); // definition_level_encoding = RLE
    i32f(&mut h, 1, 3); // repetition_level_encoding = RLE
    h.push(0);
    h.push(0);
    h
}

fn rle_levels(levels: &[i16]) -> Vec<u8> {
    let mut runs = Vec::new();
    for &l in levels {
        runs.push(2u8); // run of length 1 (header = 1 << 1)
        runs.push(l as u8);
    }
    let mut out = (runs.len() as u32).to_le_bytes().to_vec();
    out.extend(runs);
    out
}

#[test]
fn records_split_across_v1_pages_are_reencoded() {
    use vpf::thrift::parse_page_header;
    let descr = Arc::new(parquet::schema::types::SchemaDescriptor::new(Arc::new(
        parquet::schema::parser::parse_message_type(
            "message m { optional group l (LIST) { repeated group list { optional int32 element; } } }",
        )
        .unwrap(),
    )));
    // Records: NULL, [], [NULL], [i, i+1, ..] of varying length.
    let (mut defs, mut reps, mut vals) = (Vec::new(), Vec::new(), Vec::new());
    let num_rows = 60;
    for i in 0..num_rows {
        match i % 7 {
            0 => {
                defs.push(0);
                reps.push(0);
            }
            1 => {
                defs.push(1);
                reps.push(0);
            }
            2 => {
                defs.push(2);
                reps.push(0);
            }
            _ => {
                for j in 0..(i % 13 + 1) {
                    defs.push(3);
                    reps.push((j > 0) as i16);
                    vals.push(i * 100 + j);
                }
            }
        }
    }
    // Cut pages every 9 levels: most cuts fall inside a record.
    let mut pages = Vec::new();
    let mut vi = 0;
    for start in (0..defs.len()).step_by(9) {
        let end = (start + 9).min(defs.len());
        let mut body = rle_levels(&reps[start..end]);
        body.extend(rle_levels(&defs[start..end]));
        for &d in &defs[start..end] {
            if d == 3 {
                body.extend((vals[vi] as i32).to_le_bytes());
                vi += 1;
            }
        }
        let mut bytes = v1_header((end - start) as i32, body.len() as i32);
        bytes.extend(body);
        let h = parse_page_header(&bytes).unwrap();
        pages.push((bytes::Bytes::from(bytes), h));
    }
    let n_pages = pages.len();
    let enc = vpf::codec::EncodedChunk {
        pages,
        encodings: vec![parquet::basic::Encoding::PLAIN, parquet::basic::Encoding::RLE],
        stats: None,
        num_values: defs.len() as i64,
        codec: Compression::UNCOMPRESSED,
        page_rows: vec![0; n_pages],
        page_stats: vec![None; n_pages],
    };
    let vf = VirtualFile::build(
        FileProps { schema: descr, created_by: None, key_value: None, version: 1, blobs: None },
        vec![RowGroupLayout { num_rows, chunks: vec![enc.layout(parquet::basic::Type::INT32).unwrap()] }],
    )
    .unwrap();
    let bytes = bytes::Bytes::from(vf.to_bytes().unwrap());
    let original = concat(&read_parquet(bytes.clone()));
    assert_eq!(original.num_rows(), num_rows as usize);

    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    store.import("split", &bytes).unwrap();
    assert_eq!(current(&store, "split"), original);
    // Stored pages are record aligned and their row counts are exact.
    let view = store.view("split", None).unwrap();
    let leaf = &view.schema.leaves().unwrap()[0];
    let SegmentData::RowGroup { chunks, .. } = &view.segments[0] else { panic!() };
    let raw: Vec<RawPage> = chunks[0].pages.iter().map(|p| RawPage::load(store.blobs(), p).unwrap()).collect();
    let rows = chunk_page_rows(leaf, chunks[0].codec, &raw).unwrap().expect("aligned after import");
    assert_eq!(rows, chunks[0].pages.iter().map(|p| p.num_rows).collect::<Vec<_>>());
    assert_eq!(rows.iter().sum::<i64>(), num_rows);
    // And page-level mutations work afterwards.
    store.delete_where("split", "l IS NULL").unwrap();
    let keep: arrow::array::BooleanArray = (0..num_rows).map(|i| Some(i % 7 != 0)).collect();
    assert_eq!(current(&store, "split"), arrow::compute::filter_record_batch(&original, &keep).unwrap());
}
