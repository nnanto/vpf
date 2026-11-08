mod common;

use common::*;
use parquet::basic::Compression;
use vpf::ops::alter::AlterOp;
use vpf::ops::append::AppendMode;
use vpf::store::{SnapshotMetadataMode, Store};

fn layout_count(store: &Store) -> i64 {
    store
        .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM snapshot_layouts", [], |r| r.get(0))?))
        .unwrap()
}

fn check_snapshot(store: &Store, id: i64) {
    let view = store.view("t", Some(id)).unwrap();
    let file = store.open_snapshot("t", Some(id)).unwrap();
    let assembled = store.build_virtual_file(&view).unwrap();
    assert_eq!(file.to_bytes().unwrap(), assembled.to_bytes().unwrap());
    assert_eq!(read_virtual(&file), read_virtual(&assembled));
    assert_eq!(store.snapshot_info("t", Some(id)).unwrap(), view.snapshot);
    let schema = store.snapshot_schema("t", Some(id)).unwrap();
    assert_eq!(schema.columns, view.schema.columns);
    assert_eq!(
        vpf::convert::arrow_schema(&schema).unwrap(),
        read_virtual(&file).schema().as_ref().clone(),
    );
    for group in 0..file.metadata().num_row_groups() {
        let actual = file.metadata().page_index_for_row_group(group);
        let expected = assembled.metadata().page_index_for_row_group(group);
        for column in 0..file.metadata().row_group(group).num_columns() {
            assert_eq!(actual.offset_index(column), expected.offset_index(column));
            assert_eq!(actual.column_index(column), expected.column_index(column));
        }
    }
}

#[test]
fn both_modes_preserve_bytes_schema_indexes_and_history() {
    for mode in [SnapshotMetadataMode::Lazy, SnapshotMetadataMode::Eager] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vpf");
        let store = Store::open(&path).unwrap();
        store.set_metadata_mode(mode);
        let data = sample_batch(0, 400);
        store
            .import(
                "t",
                &write_parquet(&[data], props(Compression::SNAPPY, false, true, 50, 150)),
            )
            .unwrap();
        let mut ids = vec![store.snapshot_info("t", None).unwrap().snapshot_id];
        for append_mode in [
            AppendMode::Direct,
            AppendMode::Buffered,
            AppendMode::Buffered,
        ] {
            store
                .append("t", &sample_batch(1000, 7), append_mode)
                .unwrap();
            ids.push(store.snapshot_info("t", None).unwrap().snapshot_id);
        }
        store
            .update_where("t", "name = 'changed'", "id = 25")
            .unwrap();
        ids.push(store.snapshot_info("t", None).unwrap().snapshot_id);
        store.delete_where("t", "id = 1000").unwrap();
        ids.push(store.snapshot_info("t", None).unwrap().snapshot_id);
        store.flush("t").unwrap();
        ids.push(store.snapshot_info("t", None).unwrap().snapshot_id);
        store
            .alter(
                "t",
                AlterOp::RenameColumn {
                    from: "name".into(),
                    to: "label".into(),
                },
            )
            .unwrap();
        ids.push(store.snapshot_info("t", None).unwrap().snapshot_id);
        for &id in &ids {
            check_snapshot(&store, id);
        }
        assert_eq!(
            layout_count(&store),
            if mode == SnapshotMetadataMode::Eager {
                ids.len() as i64
            } else {
                0
            }
        );
        let reopened = Store::open(&path).unwrap();
        for id in ids {
            check_snapshot(&reopened, id);
        }
    }
}

#[test]
fn eager_layouts_are_atomic_and_rollback_does_not_reuse_stale_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.vpf");
    let store = Store::open(&path).unwrap();
    store.set_metadata_mode(SnapshotMetadataMode::Eager);
    let data = sample_batch(0, 5);
    store
        .create_from_arrow("t", &data.schema(), Compression::SNAPPY)
        .unwrap();
    store.append("t", &data, AppendMode::Buffered).unwrap();
    let other = Store::open(&path).unwrap();
    let before = store.snapshot_info("t", None).unwrap();
    let count = layout_count(&store);

    store.begin().unwrap();
    store
        .append("t", &sample_batch(100, 3), AppendMode::Buffered)
        .unwrap();
    let rolled_back = store.snapshot_info("t", None).unwrap().snapshot_id;
    check_snapshot(&store, rolled_back);
    assert_eq!(layout_count(&store), count + 1);
    assert_eq!(layout_count(&other), count);
    assert_eq!(other.snapshot_info("t", None).unwrap(), before);
    store.rollback().unwrap();
    assert_eq!(layout_count(&store), count);
    assert!(store.open_snapshot("t", Some(rolled_back)).is_err());

    store
        .append("t", &sample_batch(200, 3), AppendMode::Buffered)
        .unwrap();
    assert_eq!(
        store.snapshot_info("t", None).unwrap().snapshot_id,
        rolled_back
    );
    check_snapshot(&store, rolled_back);
    let expected = concat(&[data, sample_batch(200, 3)]);
    assert_eq!(
        read_virtual(&other.open_snapshot("t", None).unwrap()),
        expected
    );

    store
        .write(|t| {
            t.conn().execute_batch(
                "CREATE TRIGGER reject_layout BEFORE INSERT ON snapshot_layouts
             BEGIN SELECT RAISE(ABORT, 'layout rejected'); END;",
            )?;
            Ok(())
        })
        .unwrap();
    let count = layout_count(&store);
    assert!(
        store
            .append("t", &sample_batch(300, 2), AppendMode::Direct)
            .is_err()
    );
    assert_eq!(layout_count(&store), count);
    assert_eq!(
        read_virtual(&store.open_snapshot("t", None).unwrap()),
        expected
    );
}

#[test]
fn mode_changes_apply_to_subsequent_writes_and_corrupt_layouts_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("t.vpf")).unwrap();
    let data = sample_batch(0, 5);
    store
        .create_from_arrow("t", &data.schema(), Compression::SNAPPY)
        .unwrap();
    assert_eq!(layout_count(&store), 0);
    store.set_metadata_mode(SnapshotMetadataMode::Eager);
    store.append("t", &data, AppendMode::Buffered).unwrap();
    let eager = store.snapshot_info("t", None).unwrap().snapshot_id;
    assert_eq!(layout_count(&store), 1);
    store.set_metadata_mode(SnapshotMetadataMode::Lazy);
    store
        .append("t", &sample_batch(100, 5), AppendMode::Direct)
        .unwrap();
    let lazy = store.snapshot_info("t", None).unwrap().snapshot_id;
    assert_eq!(layout_count(&store), 1);
    check_snapshot(&store, eager);
    check_snapshot(&store, lazy);
    store
        .write(|t| {
            t.conn().execute(
                "UPDATE snapshot_layouts SET layout = x'00' WHERE snapshot_id = ?1",
                [eager],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(
        store
            .open_snapshot("t", Some(eager))
            .unwrap_err()
            .to_string()
            .contains("cached snapshot layout")
    );
    check_snapshot(&store, lazy);
}

#[test]
fn metadata_only_lookups_do_not_read_page_tables() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("t.vpf")).unwrap();
    let data = sample_batch(0, 5);
    store
        .create_from_arrow("t", &data.schema(), Compression::SNAPPY)
        .unwrap();
    store.append("t", &data, AppendMode::Direct).unwrap();
    let info = store.snapshot_info("t", None).unwrap();
    store
        .write(|t| {
            t.conn().execute_batch("DROP TABLE chunk_pages")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(store.snapshot_info("t", None).unwrap(), info);
    assert_eq!(
        vpf::convert::arrow_schema(&store.snapshot_schema("t", None).unwrap()).unwrap(),
        *data.schema()
    );
    assert!(store.open_snapshot("t", None).is_err());
}
