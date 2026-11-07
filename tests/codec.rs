mod common;

use common::*;
use parquet::basic::{Compression, Type as PhysicalType};
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::schema::parser::parse_message_type;
use vpf::codec::{RawPage, decode_pages, encode_column};
use vpf::layout::{FileProps, RowGroupLayout, VirtualFile};
use vpf::ops::snapshot::{SegmentData, chunk_layout};
use vpf::schema::{ColumnSpec, Leaf};
use vpf::store::Store;
use vpf::values::*;

#[test]
fn per_page_decode_reencode_is_lossless_for_all_formats() {
    let d = tempfile::tempdir().unwrap();
    let batch = sample_batch(0, 2500);
    let mut i = 0;
    for codec in codecs() {
        for v2 in [false, true] {
            for dict in [false, true] {
                let name = format!("f{i}");
                i += 1;
                let store = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
                store
                    .import(
                        &name,
                        &write_parquet(&[batch.clone()], props(codec, v2, dict, 300, 2500)),
                    )
                    .unwrap();
                let view = store.view(&name, None).unwrap();
                let SegmentData::RowGroup { rg, chunks, .. } = &view.segments[0] else {
                    panic!()
                };
                let mut layouts: Vec<_> = chunks.iter().map(|c| chunk_layout(c).unwrap()).collect();
                for (ci, (leaf, chunk)) in
                    view.schema.leaves().unwrap().iter().zip(chunks).enumerate()
                {
                    let raw: Vec<RawPage> = chunk
                        .pages
                        .iter()
                        .map(|p| RawPage::load(store.blobs(), p).unwrap())
                        .collect();
                    let whole = decode_pages(leaf, chunk.codec, &raw).unwrap();
                    assert_eq!(whole.num_rows(), 2500);
                    // Decoding page by page (with the dictionary page prepended) gives the same values.
                    let dict_page: Vec<RawPage> =
                        raw.iter().filter(|p| !p.kind.is_data()).cloned().collect();
                    let mut by_page =
                        ColumnData::empty_leaf(leaf.physical(), leaf.max_def, leaf.max_rep);
                    for p in raw.iter().filter(|p| p.kind.is_data()) {
                        let mut pages = dict_page.clone();
                        pages.push(p.clone());
                        let part = decode_pages(leaf, chunk.codec, &pages).unwrap();
                        assert_eq!(part.num_rows() as i64, p.num_values);
                        by_page.append(&part).unwrap();
                    }
                    assert_eq!(
                        by_page, whole,
                        "{} codec={codec} v2={v2} dict={dict}",
                        leaf.spec.name
                    );
                    // Re-encoding gives identical values (with and without dictionary).
                    for re_dict in [false, true] {
                        let enc = encode_column(leaf, chunk.codec, &whole, re_dict).unwrap();
                        let raw2: Vec<RawPage> = enc
                            .pages
                            .iter()
                            .map(|(b, h)| RawPage::from_header(b.clone(), h))
                            .collect();
                        assert_eq!(decode_pages(leaf, chunk.codec, &raw2).unwrap(), whole);
                        assert_eq!(enc.num_values, 2500);
                    }
                    // Swap the re-encoded chunk into the virtual file: readers see the same data.
                    layouts[ci] = encode_column(leaf, chunk.codec, &whole, false)
                        .unwrap()
                        .layout(leaf.physical())
                        .unwrap();
                }
                let vf = VirtualFile::build(
                    FileProps {
                        schema: view.schema.descriptor().unwrap(),
                        created_by: None,
                        key_value: view.schema.key_value.clone(),
                        version: 1,
                        blobs: Some(store.blobs()),
                    },
                    vec![RowGroupLayout {
                        num_rows: rg.num_rows,
                        chunks: layouts,
                    }],
                )
                .unwrap();
                assert_eq!(read_virtual(&vf), batch);
            }
        }
    }
}

fn spec_of(msg: &str) -> Leaf {
    let t = parse_message_type(msg).unwrap();
    Leaf::flat(&ColumnSpec::from_parquet_type(1, &t.get_fields()[0]).unwrap())
}

fn i96(a: u32, b: u32, c: u32) -> Int96 {
    let mut v = Int96::new();
    v.set_data(a, b, c);
    v
}

#[test]
fn exotic_physical_types_roundtrip() {
    let cases: Vec<(&str, Values)> = vec![
        (
            "message m { optional int96 x; }",
            Values::I96(vec![i96(1, 2, 3), i96(u32::MAX, 0, 7)]),
        ),
        (
            "message m { optional fixed_len_byte_array(3) x (DECIMAL(6,2)); }",
            Values::Fixed(vec![
                FixedLenByteArray::from(vec![0u8, 1, 2]),
                FixedLenByteArray::from(vec![255u8, 255, 0]),
            ]),
        ),
        (
            "message m { optional boolean x; }",
            Values::Bool(vec![true, false]),
        ),
        (
            "message m { optional float x; }",
            Values::F32(vec![f32::NAN, -0.0]),
        ),
        (
            "message m { optional binary x (STRING); }",
            Values::Bytes(vec![ByteArray::from("é"), ByteArray::from("")]),
        ),
        (
            "message m { optional int32 x (INTEGER(32,false)); }",
            Values::I32(vec![-1, 5]),
        ),
    ];
    for (msg, vals) in cases {
        let spec = spec_of(msg);
        let data = ColumnData::flat(vals, Some(vec![1, 0, 0, 1]));
        for codec in codecs() {
            let enc = encode_column(&spec, codec, &data, true).unwrap();
            let raw: Vec<RawPage> = enc
                .pages
                .iter()
                .map(|(b, h)| RawPage::from_header(b.clone(), h))
                .collect();
            let back = decode_pages(&spec, codec, &raw).unwrap();
            // NaN != NaN, so compare via payload bytes.
            assert_eq!(
                encode_batch(4, &[((1, 0), back)]).unwrap(),
                encode_batch(4, &[((1, 0), data.clone())]).unwrap(),
                "{msg} {codec}"
            );
        }
    }
    // Wrong FLBA width is rejected rather than silently written.
    let spec = spec_of("message m { required fixed_len_byte_array(3) x; }");
    let bad = ColumnData::flat(
        Values::Fixed(vec![FixedLenByteArray::from(vec![1u8])]),
        None,
    );
    assert!(encode_column(&spec, Compression::SNAPPY, &bad, false).is_err());
}

#[test]
fn large_columns_split_into_multiple_pages() {
    let spec = spec_of("message m { required int64 x; }");
    let data = ColumnData::flat(Values::I64((0..100_000).collect()), None);
    let enc = encode_column(&spec, Compression::SNAPPY, &data, false).unwrap();
    assert!(enc.pages.len() >= 5, "{}", enc.pages.len());
    let raw: Vec<RawPage> = enc
        .pages
        .iter()
        .map(|(b, h)| RawPage::from_header(b.clone(), h))
        .collect();
    assert_eq!(
        decode_pages(&spec, Compression::SNAPPY, &raw).unwrap(),
        data
    );
    let st = enc.stats.unwrap();
    assert_eq!(st.min.unwrap(), 0i64.to_le_bytes());
    assert_eq!(st.max.unwrap(), 99_999i64.to_le_bytes());
    assert_eq!(st.null_count, Some(0));
}

fn opt_i32(v: &[Option<i32>]) -> ColumnData {
    ColumnData::flat(
        Values::I32(v.iter().flatten().copied().collect()),
        Some(v.iter().map(|x| x.is_some() as i16).collect()),
    )
}

#[test]
fn column_data_operations() {
    let c = opt_i32(&[Some(1), None, Some(3), Some(4), None]);
    assert_eq!(c.num_rows(), 5);
    assert_eq!(
        c.filter(&[true, true, false, true, false]),
        opt_i32(&[Some(1), None, Some(4)])
    );
    assert_eq!(c.take(&[4, 0]), opt_i32(&[None, Some(1)]));
    assert_eq!(c.slice(1, 3), opt_i32(&[None, Some(3), Some(4)]));
    assert_eq!(
        c.set_rows(&[1, 2], Some(&Values::I32(vec![9]))).unwrap(),
        opt_i32(&[Some(1), Some(9), Some(9), Some(4), None])
    );
    assert_eq!(
        c.set_rows(&[0], None).unwrap(),
        opt_i32(&[None, None, Some(3), Some(4), None])
    );
    assert!(c.set_rows(&[0], Some(&Values::I64(vec![1]))).is_err());
    assert!(c.set_rows(&[7], None).is_err());
    let mut a = c.clone();
    a.append(&opt_i32(&[Some(5)])).unwrap();
    assert_eq!(a.num_rows(), 6);
    let w = c.widen(PhysicalType::INT64).unwrap();
    assert_eq!(w.values, Values::I64(vec![1, 3, 4]));
    assert!(c.widen(PhysicalType::FLOAT).is_err());
    assert!(c.with_nullability(false).is_err());
    let req = ColumnData::flat(Values::I32(vec![1, 2]), None);
    assert_eq!(
        req.with_nullability(true).unwrap(),
        opt_i32(&[Some(1), Some(2)])
    );
    assert!(req.set_rows(&[0], None).is_err());
    assert_eq!(
        ColumnData::repeat(PhysicalType::INT32, true, Some(&Values::I32(vec![7])), 3).unwrap(),
        opt_i32(&[Some(7), Some(7), Some(7)])
    );
    assert_eq!(
        ColumnData::repeat(PhysicalType::INT32, true, None, 2).unwrap(),
        opt_i32(&[None, None])
    );
    assert!(ColumnData::repeat(PhysicalType::INT32, false, None, 2).is_err());
    let f = ColumnData::flat(Values::F32(vec![1.5]), None);
    assert_eq!(
        f.widen(PhysicalType::DOUBLE).unwrap().values,
        Values::F64(vec![1.5])
    );
}

#[test]
fn batch_payload_roundtrip_and_corruption() {
    let cols = vec![
        ((3, 0), opt_i32(&[Some(1), None, Some(2)])),
        (
            (5, 0),
            ColumnData::flat(
                Values::Bytes(vec!["a".into(), "".into(), "ccc".into()]),
                None,
            ),
        ),
        (
            (6, 0),
            ColumnData::flat(Values::I96(vec![i96(1, 2, 3)]), Some(vec![0, 1, 0])),
        ),
        (
            (7, 0),
            ColumnData::flat(
                Values::Fixed(vec![FixedLenByteArray::from(vec![1u8, 2])]),
                Some(vec![0, 0, 1]),
            ),
        ),
        (
            (8, 0),
            ColumnData::flat(Values::Bool(vec![true, false, true]), None),
        ),
        (
            (9, 0),
            ColumnData::flat(Values::F64(vec![1.0, 2.0, 3.0]), None),
        ),
    ];
    let buf = encode_batch(3, &cols).unwrap();
    let (n, back) = decode_batch(&buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(back, cols);
    assert!(encode_batch(4, &cols).is_err());
    for cut in 0..buf.len() {
        assert!(decode_batch(&buf[..cut]).is_err());
    }
    let mut extra = buf.clone();
    extra.push(0);
    assert!(decode_batch(&extra).is_err());
    let v = Values::Bytes(vec!["x".into()]);
    assert_eq!(decode_value(&encode_value(&v).unwrap()).unwrap(), v);
    assert!(encode_value(&Values::I32(vec![1, 2])).is_err());
}

#[test]
fn plain_pages_spliced_into_dictionary_chunk() {
    let d = tempfile::tempdir().unwrap();
    let batch = sample_batch(0, 2000);
    for codec in codecs() {
        let name = format!("{codec}").replace(['(', ')', ' ', ','], "_");
        let store = Store::open(d.path().join(format!("{name}.vpf"))).unwrap();
        store
            .import(
                &name,
                &write_parquet(&[batch.clone()], props(codec, false, true, 250, 2000)),
            )
            .unwrap();
        let view = store.view(&name, None).unwrap();
        let SegmentData::RowGroup { rg, chunks, .. } = &view.segments[0] else {
            panic!()
        };
        let mut layouts = Vec::new();
        for (leaf, chunk) in view.schema.leaves().unwrap().iter().zip(chunks) {
            let mut layout = chunk_layout(chunk).unwrap();
            let raw: Vec<RawPage> = chunk
                .pages
                .iter()
                .map(|p| RawPage::load(store.blobs(), p).unwrap())
                .collect();
            let has_dict = raw.iter().any(|p| !p.kind.is_data());
            let dict: Vec<RawPage> = raw.iter().filter(|p| !p.kind.is_data()).cloned().collect();
            // Replace the 2nd data page with a freshly PLAIN-encoded copy of its values.
            let idx = raw
                .iter()
                .enumerate()
                .filter(|(_, p)| p.kind.is_data())
                .nth(1)
                .unwrap()
                .0;
            let mut pages = dict.clone();
            pages.push(raw[idx].clone());
            let vals = decode_pages(leaf, chunk.codec, &pages).unwrap();
            let enc = encode_column(leaf, chunk.codec, &vals, false).unwrap();
            let new_pages = enc.layout(leaf.physical()).unwrap().pages;
            assert!(new_pages.iter().all(|p| p.kind.is_data()));
            layout.pages.splice(idx..idx + 1, new_pages);
            if has_dict {
                for e in enc.encodings {
                    if !layout.encodings.contains(&e) {
                        layout.encodings.push(e);
                    }
                }
            }
            layouts.push(layout);
        }
        let vf = VirtualFile::build(
            FileProps {
                schema: view.schema.descriptor().unwrap(),
                created_by: None,
                key_value: view.schema.key_value.clone(),
                version: 1,
                blobs: Some(store.blobs()),
            },
            vec![RowGroupLayout {
                num_rows: rg.num_rows,
                chunks: layouts,
            }],
        )
        .unwrap();
        assert_eq!(read_virtual(&vf), batch, "{codec}");
        if python().is_some() {
            let exp = d.path().join(format!("{name}.expected.parquet"));
            let act = d.path().join(format!("{name}.parquet"));
            std::fs::write(
                &exp,
                write_parquet(
                    &[batch.clone()],
                    props(Compression::SNAPPY, false, false, 1000, 2000),
                )
                .to_vec(),
            )
            .unwrap();
            std::fs::write(&act, vf.to_bytes().unwrap()).unwrap();
            run_python(&[
                &py_script("check_equal.py"),
                exp.to_str().unwrap(),
                act.to_str().unwrap(),
            ]);
        }
    }
}
