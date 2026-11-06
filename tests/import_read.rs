mod common;

use common::*;
use parquet::basic::Compression;
use parquet::file::metadata::ParquetMetaDataReader;
use vpf::store::Store;

fn store() -> (tempfile::TempDir, Store) {
    let d = tempfile::tempdir().unwrap();
    let s = Store::open(d.path().join("s")).unwrap();
    (d, s)
}

#[test]
fn import_roundtrip_all_codecs_and_page_formats() {
    let batches = vec![sample_batch(0, 3000), sample_batch(3000, 2500)];
    let expected = concat(&batches);
    let (_d, store) = store();
    let mut i = 0;
    for codec in codecs() {
        for v2 in [false, true] {
            for dict in [false, true] {
                let bytes = write_parquet(&batches, props(codec, v2, dict, 400, 1500));
                let name = format!("f{i}");
                i += 1;
                let s = store.import(&name, &bytes).unwrap();
                assert_eq!(s.num_rows, 5500);
                assert_eq!(s.row_groups, 4);
                let vf = store.open_snapshot(&name, None).unwrap();
                assert_eq!(read_virtual(&vf), expected, "codec={codec} v2={v2} dict={dict}");

                // Every original page is byte-identical inside the virtual file.
                let orig = ParquetMetaDataReader::new().parse_and_finish(&bytes).unwrap();
                let virt = vf.metadata();
                assert_eq!(orig.num_row_groups(), virt.num_row_groups());
                for (ro, rv) in orig.row_groups().iter().zip(virt.row_groups()) {
                    assert_eq!(ro.num_rows(), rv.num_rows());
                    for (co, cv) in ro.columns().iter().zip(rv.columns()) {
                        let (so, lo) = co.byte_range();
                        let (sv, lv) = cv.byte_range();
                        assert_eq!(lo, lv);
                        assert_eq!(&bytes[so as usize..(so + lo) as usize], &vf.read_range(sv, lv as usize).unwrap()[..]);
                        assert_eq!(co.statistics(), cv.statistics());
                        assert_eq!(co.num_values(), cv.num_values());
                        assert_eq!(co.compression(), cv.compression());
                        assert_eq!(co.dictionary_page_offset().is_some(), cv.dictionary_page_offset().is_some());
                    }
                }
                // The original ARROW:schema metadata is carried over.
                assert_eq!(
                    orig.file_metadata().key_value_metadata(),
                    virt.file_metadata().key_value_metadata()
                );
            }
        }
    }
}

#[test]
fn read_at_matches_materialized_bytes_at_arbitrary_ranges() {
    let (_d, store) = store();
    let bytes = write_parquet(&[sample_batch(0, 4000)], props(Compression::SNAPPY, false, true, 300, 1000));
    store.import("f", &bytes).unwrap();
    let vf = store.open_snapshot("f", None).unwrap();
    let all = vf.to_bytes().unwrap();
    assert_eq!(all.len() as u64, vf.size());
    assert_eq!(&all[..4], b"PAR1");
    assert_eq!(&all[all.len() - 4..], b"PAR1");
    let mut seed = 99u64;
    let mut rnd = |m: u64| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (seed >> 33) % m
    };
    for _ in 0..3000 {
        let off = rnd(vf.size() + 10);
        let len = rnd(5000) as usize;
        let mut buf = vec![0u8; len];
        let n = vf.read_at(off, &mut buf).unwrap();
        let exp_n = if off >= vf.size() { 0 } else { len.min((vf.size() - off) as usize) };
        assert_eq!(n, exp_n);
        if n > 0 {
            assert_eq!(&buf[..n], &all[off as usize..off as usize + n]);
        }
    }
    // A virtual file parsed from its own materialized bytes matches.
    assert_eq!(read_parquet(all.clone()).len(), 4);
    assert!(vf.read_range(vf.size() - 1, 2).is_err());
}

#[test]
fn reopen_and_export() {
    let d = tempfile::tempdir().unwrap();
    let bytes = write_parquet(&[sample_batch(10, 777)], props(Compression::ZSTD(Default::default()), true, true, 100, 300));
    {
        let store = Store::open(d.path().join("s")).unwrap();
        store.import("f", &bytes).unwrap();
        assert!(store.import("f", &bytes).is_err(), "duplicate names rejected");
    }
    let store = Store::open(d.path().join("s")).unwrap();
    let out = d.path().join("out.parquet");
    let size = store.export("f", None, &out).unwrap();
    assert_eq!(std::fs::metadata(&out).unwrap().len(), size);
    let got = concat(&read_parquet(bytes::Bytes::from(std::fs::read(&out).unwrap())));
    assert_eq!(got, sample_batch(10, 777));
}

#[test]
fn pyarrow_fixtures_import_and_cross_reader_equality() {
    if python().is_none() {
        eprintln!("skipping: VPF_PYTHON not set");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let fx = d.path().join("fx");
    std::fs::create_dir_all(&fx).unwrap();
    run_python(&[&py_script("gen_fixtures.py"), fx.to_str().unwrap()]);
    let store = Store::open(d.path().join("s")).unwrap();
    for name in ["v1_snappy", "v2_zstd", "gzip_nodict", "pageindex_lz4", "int96_brotli", "uncompressed_v2_crc", "empty"] {
        let src = fx.join(format!("{name}.parquet"));
        store.import_file(name, &src).unwrap();
        let out = d.path().join(format!("{name}.out.parquet"));
        store.export(name, None, &out).unwrap();
        run_python(&[&py_script("check_equal.py"), src.to_str().unwrap(), out.to_str().unwrap()]);
    }
    for name in ["nested", "struct"] {
        let src = fx.join(format!("{name}.parquet"));
        store.import_file(name, &src).unwrap();
        let out = d.path().join(format!("{name}.out.parquet"));
        store.export(name, None, &out).unwrap();
        let (a, b) = (std::fs::read(&src).unwrap(), std::fs::read(&out).unwrap());
        let data_end = |f: &[u8]| f.len() - 8 - u32::from_le_bytes(f[f.len() - 8..f.len() - 4].try_into().unwrap()) as usize;
        assert_eq!(a[..data_end(&a)], b[..data_end(&b)], "{name}: pages are reused verbatim");
        run_python(&[&py_script("check_equal.py"), src.to_str().unwrap(), out.to_str().unwrap()]);
    }
}
