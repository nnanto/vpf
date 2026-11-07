mod common;

use arrow::compute::concat_batches;
use common::*;
use parquet::basic::Compression;
use vpf::ops::append::AppendMode;
use vpf::store::Store;

fn rows(s: &Store) -> usize {
    read_virtual(&s.open_snapshot("t", None).unwrap()).num_rows()
}

#[test]
fn explicit_transactions_commit_and_roll_back() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("t.vpf");
    let a = Store::open(&path).unwrap();
    let b0 = sample_batch(0, 500);
    a.import(
        "t",
        &write_parquet(
            &[b0.clone()],
            props(Compression::SNAPPY, false, true, 100, 200),
        ),
    )
    .unwrap();
    let other = Store::open(&path).unwrap();

    // Own writes are visible inside the transaction (including uncommitted page bytes),
    // invisible to other connections until commit.
    a.begin().unwrap();
    assert!(a.in_transaction());
    let b1 = sample_batch(500, 300);
    a.append("t", &b1, AppendMode::Direct).unwrap();
    let b2 = sample_batch(800, 5);
    a.append("t", &b2, AppendMode::Buffered).unwrap();
    let base = a.view("t", None).unwrap().snapshot.snapshot_id;
    a.delete_rows("t", base, &[0, 600, 801]).unwrap();
    let all = concat_batches(&b0.schema(), &[b0.clone(), b1.clone(), b2.clone()]).unwrap();
    let keep: Vec<usize> = (0..805).filter(|r| ![0, 600, 801].contains(r)).collect();
    let expected = arrow::compute::take_record_batch(
        &all,
        &arrow::array::UInt64Array::from_iter_values(keep.iter().map(|&r| r as u64)),
    )
    .unwrap();
    assert_eq!(read_virtual(&a.open_snapshot("t", None).unwrap()), expected);
    assert_eq!(rows(&other), 500);
    assert!(a.begin().is_err());
    // A failing statement leaves the transaction's earlier work intact.
    assert!(a.delete_rows("t", base, &[1]).is_err());
    a.commit().unwrap();
    assert!(!a.in_transaction());
    assert_eq!(
        read_virtual(&other.open_snapshot("t", None).unwrap()),
        expected
    );

    // Rollback discards everything, and reused batch ids do not serve stale cached data.
    let before = a.view("t", None).unwrap().snapshot.snapshot_id;
    a.begin().unwrap();
    a.append("t", &sample_batch(9000, 7), AppendMode::Buffered)
        .unwrap();
    assert_eq!(rows(&a), 802 + 7);
    a.rollback().unwrap();
    assert_eq!(a.view("t", None).unwrap().snapshot.snapshot_id, before);
    assert_eq!(rows(&a), 802);
    let b3 = sample_batch(7000, 7);
    a.append("t", &b3, AppendMode::Buffered).unwrap();
    let expected = concat_batches(&b0.schema(), &[expected, b3]).unwrap();
    assert_eq!(read_virtual(&a.open_snapshot("t", None).unwrap()), expected);
    assert_eq!(
        read_virtual(&other.open_snapshot("t", None).unwrap()),
        expected
    );
    assert!(a.commit().is_err() && a.rollback().is_err());
}
