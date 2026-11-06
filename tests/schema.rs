mod common;

use std::sync::Arc;

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::*;
use parquet::basic::{Compression, Type as PhysicalType};
use vpf::ops::alter::AlterOp;
use vpf::ops::append::AppendMode;
use vpf::ops::snapshot::SegmentData;
use vpf::predicate::{Literal, parse_literal};
use vpf::schema::spec_for_type_name;
use vpf::store::Store;

fn rows(store: &Store, name: &str, snap: Option<i64>) -> RecordBatch {
    read_virtual(&store.open_snapshot(name, snap).unwrap())
}

/// Applies an alter op to an Arrow batch: the reference model.
fn model(b: &RecordBatch, op: &AlterOp) -> RecordBatch {
    let s = b.schema();
    let mut fields: Vec<Field> = s.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut cols = b.columns().to_vec();
    match op {
        AlterOp::AddColumn { spec, default } => {
            let ty = match (spec.physical, spec.logical.as_ref()) {
                (PhysicalType::INT64, None) => DataType::Int64,
                (PhysicalType::INT32, None) => DataType::Int32,
                (PhysicalType::DOUBLE, None) => DataType::Float64,
                (PhysicalType::BOOLEAN, None) => DataType::Boolean,
                (PhysicalType::BYTE_ARRAY, Some(_)) => DataType::Utf8,
                (PhysicalType::INT64, Some(_)) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                (PhysicalType::FIXED_LEN_BYTE_ARRAY, Some(_)) => {
                    DataType::Decimal128(spec.precision as u8, spec.scale as i8)
                }
                other => panic!("model lacks {other:?}"),
            };
            let one = match default {
                Some(l) => vpf::predicate::literal_array(l, &ty).unwrap(),
                None => new_null_array(&ty, 1),
            };
            let idx = UInt32Array::from(vec![0u32; b.num_rows()]);
            cols.push(arrow::compute::take(&one, &idx, None).unwrap());
            fields.push(Field::new(&spec.name, ty, spec.nullable));
        }
        AlterOp::DropColumn(n) => {
            let i = s.index_of(n).unwrap();
            fields.remove(i);
            cols.remove(i);
        }
        AlterOp::RenameColumn { from, to } => {
            let i = s.index_of(from).unwrap();
            fields[i] = fields[i].clone().with_name(to);
        }
        AlterOp::Reorder(order) => {
            let p: Vec<usize> = order.iter().map(|n| s.index_of(n).unwrap()).collect();
            fields = p.iter().map(|&i| fields[i].clone()).collect();
            cols = p.iter().map(|&i| cols[i].clone()).collect();
        }
        AlterOp::Widen { column, to } => {
            let i = s.index_of(column).unwrap();
            let ty = if *to == PhysicalType::INT64 { DataType::Int64 } else { DataType::Float64 };
            cols[i] = cast(&cols[i], &ty).unwrap();
            fields[i] = fields[i].clone().with_data_type(ty);
        }
        AlterOp::MakeOptional(n) => {
            let i = s.index_of(n).unwrap();
            fields[i] = fields[i].clone().with_nullable(true);
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn add(name: &str, ty: &str, nullable: bool, default: Option<&str>) -> AlterOp {
    AlterOp::AddColumn {
        spec: spec_for_type_name(0, name, ty, nullable).unwrap(),
        default: default.map(|d| parse_literal(d).unwrap()),
    }
}

/// Builds a file with imported row groups, a direct row group and buffered batches.
fn mixed_store(dir: &std::path::Path, codec: Compression) -> (Store, RecordBatch) {
    let store = Store::open(dir).unwrap();
    let a = sample_batch(0, 2500);
    store.import("f", &write_parquet(&[a.clone()], props(codec, false, true, 300, 1000))).unwrap();
    let b = sample_batch(2500, 700);
    store.append("f", &b, AppendMode::Direct).unwrap();
    let c = sample_batch(3200, 40);
    store.append("f", &c, AppendMode::Buffered).unwrap();
    let d = sample_batch(3240, 9);
    store.append("f", &d, AppendMode::Buffered).unwrap();
    (store, concat(&[a, b, c, d]))
}

fn rg_ids(store: &Store) -> Vec<Vec<i64>> {
    store
        .view("f", None)
        .unwrap()
        .segments
        .iter()
        .filter_map(|s| match s {
            SegmentData::RowGroup { chunks, .. } => Some(chunks.iter().map(|c| c.chunk_id).collect()),
            _ => None,
        })
        .collect()
}

fn check_python(store: &Store, expected: &RecordBatch, tag: &str) {
    if python().is_none() {
        eprintln!("skipping python check");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let act = d.path().join(format!("{tag}-act.parquet"));
    let exp = d.path().join(format!("{tag}-exp.parquet"));
    store.export("f", None, &act).unwrap();
    std::fs::write(&exp, write_parquet(&[expected.clone()], props(Compression::SNAPPY, false, true, 1000, 100_000)))
        .unwrap();
    run_python(&[&py_script("check_equal.py"), exp.to_str().unwrap(), act.to_str().unwrap()]);
}

#[test]
fn every_op_matches_model_and_keeps_history() {
    let ops = vec![
        add("note", "string", true, None),
        add("level", "int64", false, Some("7")),
        add("price", "decimal(10,3)", true, Some("12.5")),
        AlterOp::RenameColumn { from: "name".into(), to: "label".into() },
        AlterOp::Widen { column: "i32".into(), to: PhysicalType::INT64 },
        AlterOp::Widen { column: "f32".into(), to: PhysicalType::DOUBLE },
        AlterOp::MakeOptional("id".into()),
        AlterOp::DropColumn("bin".into()),
        AlterOp::Reorder(vec![
            "level", "id", "label", "i32", "score", "f32", "flag", "day", "ts", "amount", "u16", "note", "price",
        ]
        .into_iter()
        .map(String::from)
        .collect()),
    ];
    for codec in [Compression::SNAPPY, Compression::UNCOMPRESSED, Compression::ZSTD(Default::default())] {
        let d = tempfile::tempdir().unwrap();
        let (store, mut expected) = mixed_store(d.path(), codec);
        let mut history = vec![(store.view("f", None).unwrap().snapshot.snapshot_id, expected.clone())];
        for op in &ops {
            let before = rg_ids(&store);
            let s = store.alter("f", op.clone()).unwrap();
            expected = model(&expected, op);
            assert_eq!(rows(&store, "f", None), expected, "{op:?} {codec:?}");
            let after = rg_ids(&store);
            assert_eq!(before.len(), after.len());
            let reused: usize =
                after.iter().map(|c| c.iter().filter(|id| before.iter().flatten().any(|b| b == *id)).count()).sum();
            let total: usize = after.iter().map(Vec::len).sum();
            match op {
                AlterOp::DropColumn(_) | AlterOp::RenameColumn { .. } | AlterOp::Reorder(_) => {
                    assert_eq!(s.chunks_written, 0);
                    assert_eq!(reused, total, "metadata-only op reuses every chunk");
                }
                AlterOp::AddColumn { .. } => {
                    // 3 imported row groups of 1000/1000/500 rows plus one of 700: 3 distinct sizes.
                    assert_eq!(s.chunks_written, 3);
                    assert_eq!(reused, total - after.len());
                }
                _ => {
                    assert_eq!(s.chunks_written, after.len());
                    assert_eq!(reused, total - after.len(), "only the altered column is rewritten");
                }
            }
            history.push((s.snapshot_id, expected.clone()));
        }
        for (sid, exp) in &history {
            assert_eq!(&rows(&store, "f", Some(*sid)), exp, "time travel to {sid}");
        }
        check_python(&store, &expected, "every");
        // Appending with the old (pre-alter) column set: missing columns get defaults,
        // renamed/widened columns are matched by name and cast.
        let more = sample_batch(5000, 30);
        let more_m = ops.iter().fold(more.clone(), |b, op| model(&b, op));
        let more_schema = more.schema();
        let renamed = more_schema.fields().iter().map(|f| {
            let n = if f.name() == "name" { "label" } else { f.name().as_str() };
            f.as_ref().clone().with_name(n)
        });
        let keep: Vec<usize> =
            more.schema().fields().iter().enumerate().filter(|(_, f)| f.name() != "bin").map(|(i, _)| i).collect();
        let renamed: Vec<Field> = renamed.enumerate().filter(|(i, _)| keep.contains(i)).map(|(_, f)| f).collect();
        let cols: Vec<ArrayRef> = keep.iter().map(|&i| more.column(i).clone()).collect();
        let input = RecordBatch::try_new(Arc::new(Schema::new(renamed)), cols).unwrap();
        for mode in [AppendMode::Buffered, AppendMode::Direct] {
            store.append("f", &input, mode).unwrap();
            expected = concat(&[expected, more_m.clone()]);
        }
        assert_eq!(rows(&store, "f", None), expected);
        store.flush("f").unwrap();
        assert_eq!(rows(&store, "f", None), expected);
        check_python(&store, &expected, "after-append");
    }
}

#[test]
fn delete_and_update_after_alter() {
    let d = tempfile::tempdir().unwrap();
    let (store, base) = mixed_store(d.path(), Compression::SNAPPY);
    let ops = [
        add("tag", "string", true, Some("'t'")),
        AlterOp::Widen { column: "i32".into(), to: PhysicalType::INT64 },
        AlterOp::RenameColumn { from: "score".into(), to: "pts".into() },
    ];
    let mut expected = base;
    for op in &ops {
        store.alter("f", op.clone()).unwrap();
        expected = model(&expected, op);
    }
    store.update_where("f", "tag = 'hit', i32 = 9999999999", "id IN (0, 100, 1700, 2600, 3210) OR id >= 3245").unwrap();
    store.delete_where("f", "pts IS NULL OR id BETWEEN 10 AND 20").unwrap();
    let got = rows(&store, "f", None);
    let m = |p: &str| vpf::predicate::Expr::parse(p).unwrap().matches(&expected).unwrap();
    let hit = BooleanArray::from(m("id IN (0, 100, 1700, 2600, 3210) OR id >= 3245"));
    let mut cols = expected.columns().to_vec();
    let s = expected.schema();
    for (c, v) in [("tag", Literal::Str("hit".into())), ("i32", parse_literal("9999999999").unwrap())] {
        let i = s.index_of(c).unwrap();
        let v = vpf::predicate::literal_array(&v, cols[i].data_type()).unwrap();
        cols[i] = arrow::compute::kernels::zip::zip(&hit, &Scalar::new(v), &cols[i]).unwrap();
    }
    let updated = RecordBatch::try_new(s, cols).unwrap();
    let keep = arrow::compute::not(&BooleanArray::from(
        vpf::predicate::Expr::parse("pts IS NULL OR id BETWEEN 10 AND 20").unwrap().matches(&updated).unwrap(),
    ))
    .unwrap();
    let want = arrow::compute::filter_record_batch(&updated, &keep).unwrap();
    assert_eq!(got, want);
    check_python(&store, &want, "dml");
}

#[test]
fn arrow_schema_hint_survives_alter() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("Europe/Paris".into())), true),
        Field::new("cat", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        Field::new("v", DataType::Int32, true),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..100)),
            Arc::new(TimestampMillisecondArray::from_iter_values((0..100).map(|i| 1_700_000_000_000 + i)).with_timezone("Europe/Paris")),
            Arc::new((0..100).map(|i| Some(["a", "b", "c"][i % 3])).collect::<DictionaryArray<arrow::datatypes::Int32Type>>()),
            Arc::new(Int32Array::from_iter((0..100).map(|i| (i % 4 != 0).then_some(i)))),
        ],
    )
    .unwrap();
    store.import("f", &write_parquet(&[b.clone()], props(Compression::SNAPPY, false, true, 30, 60))).unwrap();
    let mut expected = b;
    for op in [
        AlterOp::RenameColumn { from: "ts".into(), to: "when".into() },
        AlterOp::RenameColumn { from: "cat".into(), to: "kind".into() },
        AlterOp::Widen { column: "v".into(), to: PhysicalType::INT64 },
        add("extra", "int32", true, Some("3")),
        AlterOp::Reorder(["kind", "when", "id", "v", "extra"].map(String::from).to_vec()),
    ] {
        store.alter("f", op.clone()).unwrap();
        expected = model(&expected, &op);
        assert_eq!(rows(&store, "f", None), expected, "{op:?}");
    }
    let s = rows(&store, "f", None).schema();
    assert!(matches!(s.field(0).data_type(), DataType::Dictionary(..)));
    assert_eq!(s.field(1).data_type(), &DataType::Timestamp(TimeUnit::Millisecond, Some("Europe/Paris".into())));
}

#[test]
fn widen_existing_default_and_batches() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path()).unwrap();
    let base = sample_batch(0, 50);
    store.import("f", &write_parquet(&[base.clone()], props(Compression::SNAPPY, false, false, 10, 20))).unwrap();
    store.alter("f", add("small", "int32", false, Some("-5"))).unwrap();
    store.alter("f", add("fl", "float", true, Some("1.25"))).unwrap();
    let more = sample_batch(50, 5);
    store.append("f", &more, AppendMode::Buffered).unwrap();
    store.alter("f", AlterOp::Widen { column: "small".into(), to: PhysicalType::INT64 }).unwrap();
    store.alter("f", AlterOp::Widen { column: "fl".into(), to: PhysicalType::DOUBLE }).unwrap();
    let got = rows(&store, "f", None);
    assert_eq!(got.num_rows(), 55);
    let small = got.column_by_name("small").unwrap().as_primitive::<arrow::datatypes::Int64Type>();
    assert!(small.iter().all(|v| v == Some(-5)));
    let fl = got.column_by_name("fl").unwrap().as_primitive::<arrow::datatypes::Float64Type>();
    assert!(fl.iter().all(|v| v == Some(1.25)));
    assert!(!got.schema().field_with_name("small").unwrap().is_nullable());
    // A later append omitting the columns uses the widened defaults.
    store.append("f", &sample_batch(55, 3), AppendMode::Direct).unwrap();
    let got = rows(&store, "f", None);
    let small = got.column_by_name("small").unwrap().as_primitive::<arrow::datatypes::Int64Type>();
    assert!(small.iter().all(|v| v == Some(-5)));
}

#[test]
fn invalid_alters_are_rejected_without_new_snapshot() {
    let d = tempfile::tempdir().unwrap();
    let (store, _) = mixed_store(d.path(), Compression::SNAPPY);
    let sid = store.view("f", None).unwrap().snapshot.snapshot_id;
    let bad = vec![
        add("name", "string", true, None),
        add("req", "int64", false, None),
        add("x", "int32", true, Some("'abc'")),
        add("x", "int32", true, Some("99999999999")),
        AlterOp::DropColumn("nope".into()),
        AlterOp::RenameColumn { from: "id".into(), to: "name".into() },
        AlterOp::RenameColumn { from: "id".into(), to: "".into() },
        AlterOp::Reorder(vec!["id".into()]),
        AlterOp::Reorder(sample_batch(0, 1).schema().fields().iter().map(|f| f.name().clone()).chain(["id".into()]).collect()),
        AlterOp::Widen { column: "u16".into(), to: PhysicalType::INT64 },
        AlterOp::Widen { column: "name".into(), to: PhysicalType::INT64 },
        AlterOp::Widen { column: "day".into(), to: PhysicalType::INT64 },
        AlterOp::Widen { column: "id".into(), to: PhysicalType::INT32 },
        AlterOp::Widen { column: "score".into(), to: PhysicalType::FLOAT },
        AlterOp::MakeOptional("name".into()),
    ];
    for op in bad {
        assert!(store.alter("f", op.clone()).is_err(), "{op:?} should fail");
        assert_eq!(store.view("f", None).unwrap().snapshot.snapshot_id, sid, "{op:?}");
    }
    let s = sample_batch(0, 1).schema();
    for f in s.fields().iter().skip(1) {
        store.alter("f", AlterOp::DropColumn(f.name().clone())).unwrap();
    }
    assert!(store.alter("f", AlterOp::DropColumn("id".into())).is_err(), "cannot drop the last column");
    assert_eq!(rows(&store, "f", None).num_columns(), 1);
}

#[test]
fn drop_then_add_same_name_is_a_new_column() {
    let d = tempfile::tempdir().unwrap();
    let (store, base) = mixed_store(d.path(), Compression::SNAPPY);
    store.alter("f", AlterOp::DropColumn("score".into())).unwrap();
    store.alter("f", add("score", "double", true, None)).unwrap();
    let got = rows(&store, "f", None);
    assert_eq!(got.num_rows(), base.num_rows());
    assert_eq!(got.column_by_name("score").unwrap().null_count(), base.num_rows());
}
