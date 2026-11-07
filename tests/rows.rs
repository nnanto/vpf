mod common;

use std::sync::Arc;

use arrow::array::*;
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema};
use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::store::Store;

/// Rows in file order from row groups, a buffered batch and a direct append.
fn mixed(d: &tempfile::TempDir) -> (Store, RecordBatch) {
    let store = Store::open(d.path().join("f.vpf")).unwrap();
    let a = sample_batch(0, 1000);
    store
        .import(
            "f",
            &write_parquet(
                &[a.clone()],
                props(Compression::SNAPPY, false, true, 100, 400),
            ),
        )
        .unwrap();
    let b = sample_batch(1000, 37);
    store.append("f", &b, AppendMode::Buffered).unwrap();
    let c = sample_batch(1037, 200);
    store.append("f", &c, AppendMode::Direct).unwrap();
    let all = concat_batches(&a.schema(), &[a, b, c]).unwrap();
    assert_eq!(read_virtual(&store.open_snapshot("f", None).unwrap()), all);
    (store, all)
}

fn current(store: &Store) -> i64 {
    store.view("f", None).unwrap().snapshot.snapshot_id
}

#[test]
fn delete_rows_matches_model() {
    let d = tempfile::tempdir().unwrap();
    let (store, all) = mixed(&d);
    let base = current(&store);
    // Every 7th row, all of one row group (400..800) and all buffered rows, unsorted.
    let mut rows: Vec<u64> = (0..all.num_rows() as u64)
        .filter(|r| r % 7 == 0 || (400..800).contains(r) || (1000..1037).contains(r))
        .collect();
    rows.reverse();
    let s = store.delete_rows("f", base, &rows).unwrap();
    assert_eq!(s.rows_deleted as usize, rows.len());
    assert!(s.segments_removed >= 2, "{s:?}");
    let keep = BooleanArray::from(
        (0..all.num_rows() as u64)
            .map(|r| !rows.contains(&r))
            .collect::<Vec<_>>(),
    );
    let expected = filter_record_batch(&all, &keep).unwrap();
    assert_eq!(
        read_virtual(&store.open_snapshot("f", None).unwrap()),
        expected
    );
    assert_eq!(
        read_virtual(&store.open_snapshot("f", Some(base)).unwrap()),
        all
    );

    // Row ids of a stale snapshot are rejected; nothing changes.
    let e = store.delete_rows("f", base, &[1]).unwrap_err();
    assert!(matches!(e, vpf::Error::Conflict(_)), "{e}");
    let now = current(&store);
    assert!(
        store
            .delete_rows("f", now, &[expected.num_rows() as u64])
            .is_err()
    );
    assert_eq!(current(&store), now);
    assert_eq!(store.delete_rows("f", now, &[]).unwrap().snapshot_id, None);
}

#[test]
fn update_rows_matches_model() {
    let d = tempfile::tempdir().unwrap();
    let (store, all) = mixed(&d);
    let base = current(&store);
    let rows: Vec<u64> = vec![1236, 5, 0, 399, 400, 1000, 1036, 1037, 777];
    let names = StringArray::from_iter(
        rows.iter()
            .map(|r| (r % 2 == 0).then(|| format!("new-{r}"))),
    );
    let scores = Float64Array::from_iter(rows.iter().map(|&r| Some(r as f64 + 0.25)));
    let values = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ])),
        vec![Arc::new(names.clone()), Arc::new(scores.clone())],
    )
    .unwrap();
    let s = store.update_rows("f", base, &rows, &values).unwrap();
    assert_eq!(s.rows_updated as usize, rows.len());

    let n = all.num_rows();
    let mut name: Vec<Option<String>> = all
        .column(2)
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(str::to_string))
        .collect();
    let mut score: Vec<Option<f64>> = all
        .column(3)
        .as_primitive::<arrow::datatypes::Float64Type>()
        .iter()
        .collect();
    for (i, &r) in rows.iter().enumerate() {
        name[r as usize] = names.is_valid(i).then(|| names.value(i).to_string());
        score[r as usize] = Some(scores.value(i));
    }
    let mut cols = all.columns().to_vec();
    cols[2] = Arc::new(StringArray::from(name));
    cols[3] = Arc::new(Float64Array::from(score));
    let expected = RecordBatch::try_new(all.schema(), cols).unwrap();
    assert_eq!(
        read_virtual(&store.open_snapshot("f", None).unwrap()),
        expected
    );
    assert_eq!(
        read_virtual(&store.open_snapshot("f", Some(base)).unwrap()),
        all
    );
    assert_eq!(expected.num_rows(), n);

    let now = current(&store);
    let one = |f: Field, a: ArrayRef| {
        RecordBatch::try_new(Arc::new(Schema::new(vec![f])), vec![a]).unwrap()
    };
    let nulls = one(
        Field::new("id", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![None::<i64>])),
    );
    assert!(
        store
            .update_rows("f", now, &[1], &nulls)
            .unwrap_err()
            .to_string()
            .contains("NULL")
    );
    let bogus = one(
        Field::new("nope", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![1])),
    );
    assert!(store.update_rows("f", now, &[1], &bogus).is_err());
    let ids = one(
        Field::new("id", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1, 2])),
    );
    assert!(
        store
            .update_rows("f", now, &[3, 3], &ids)
            .unwrap_err()
            .to_string()
            .contains("unique")
    );
    assert!(matches!(
        store
            .update_rows(
                "f",
                base,
                &[3],
                &one(
                    Field::new("id", DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![1]))
                )
            )
            .unwrap_err(),
        vpf::Error::Conflict(_)
    ));
    assert_eq!(current(&store), now);
}

#[test]
fn update_rows_sets_nested_values() {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(d.path().join("n.vpf")).unwrap();
    let item = Arc::new(Field::new("item", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", DataType::List(item.clone()), true),
    ]));
    let list = |v: Vec<Option<Vec<Option<&str>>>>| {
        let mut b = ListBuilder::new(StringBuilder::new()).with_field(item.clone());
        for row in v {
            match row {
                Some(xs) => {
                    for x in xs {
                        b.values().append_option(x);
                    }
                    b.append(true);
                }
                None => b.append(false),
            }
        }
        Arc::new(b.finish()) as ArrayRef
    };
    store
        .create_from_arrow("n", &schema, Compression::ZSTD(Default::default()))
        .unwrap();
    let rows: Vec<Option<Vec<Option<&str>>>> = (0..300)
        .map(|i| match i % 3 {
            0 => None,
            1 => Some(vec![]),
            _ => Some(vec![Some("a"), None, Some("b")]),
        })
        .collect();
    let b1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..300)),
            list(rows.clone()),
        ],
    )
    .unwrap();
    store.append("n", &b1, AppendMode::Direct).unwrap();
    let b2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(300..310)),
            list(rows[..10].to_vec()),
        ],
    )
    .unwrap();
    store.append("n", &b2, AppendMode::Buffered).unwrap();
    let base = store.view("n", None).unwrap().snapshot.snapshot_id;

    let targets = [305u64, 2, 0, 151];
    let new: Vec<Option<Vec<Option<&str>>>> = vec![
        Some(vec![Some("x")]),
        None,
        Some(vec![Some("y"), Some("z"), None, Some("w")]),
        Some(vec![]),
    ];
    let tag_field = Field::new("tags", DataType::List(item.clone()), true);
    let values = RecordBatch::try_new(
        Arc::new(Schema::new(vec![tag_field])),
        vec![list(new.clone())],
    )
    .unwrap();
    store.update_rows("n", base, &targets, &values).unwrap();

    let mut model = rows.clone();
    model.extend(rows[..10].iter().cloned());
    for (t, v) in targets.iter().zip(new) {
        model[*t as usize] = v;
    }
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..310)), list(model)],
    )
    .unwrap();
    assert_eq!(
        read_virtual(&store.open_snapshot("n", None).unwrap()),
        expected
    );
}
