mod common;

use common::*;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use vpf::thrift::{PageKind, split_pages};

#[test]
fn split_pages_matches_offset_index_across_writers() {
    let batch = sample_batch(0, 5000);
    for codec in codecs() {
        for v2 in [false, true] {
            for dict in [false, true] {
                let bytes = write_parquet(&[batch.clone()], props(codec, v2, dict, 300, 2000));
                let md = ParquetMetaDataReader::new()
                    .with_page_index_policy(PageIndexPolicy::Required)
                    .parse_and_finish(&bytes)
                    .unwrap();
                assert_eq!(md.num_row_groups(), 3);
                for (rgi, rg) in md.row_groups().iter().enumerate() {
                    let pidx = md.page_index_for_row_group(rgi);
                    for (ci, col) in rg.columns().iter().enumerate() {
                        let (start, len) = col.byte_range();
                        let chunk = &bytes[start as usize..(start + len) as usize];
                        let pages = split_pages(chunk).unwrap();
                        let locs = pidx.page_locations(ci).unwrap();
                        let data: Vec<_> =
                            pages.iter().filter(|p| p.header.kind.is_data()).collect();
                        assert_eq!(data.len(), locs.len(), "codec={codec} v2={v2} dict={dict}");
                        let mut first_row = 0i64;
                        for (p, loc) in data.iter().zip(locs) {
                            assert_eq!(start as i64 + p.offset as i64, loc.offset);
                            assert_eq!(p.header.total_len() as i32, loc.compressed_page_size);
                            assert_eq!(first_row, loc.first_row_index);
                            let rows = p.header.num_rows.unwrap_or(p.header.num_values);
                            if v2 {
                                assert_eq!(p.header.kind, PageKind::DataV2);
                                assert_eq!(p.header.num_rows, Some(p.header.num_values));
                            } else {
                                assert_eq!(p.header.kind, PageKind::Data);
                            }
                            first_row += rows;
                        }
                        assert_eq!(first_row, rg.num_rows());
                        let dict_pages = pages
                            .iter()
                            .filter(|p| p.header.kind == PageKind::Dictionary)
                            .count();
                        assert_eq!(dict_pages, col.dictionary_page_offset().is_some() as usize);
                        let total: usize = pages.iter().map(|p| p.header.total_len()).sum();
                        assert_eq!(total as i64, col.compressed_size());
                        let values: i64 = data.iter().map(|p| p.header.num_values).sum();
                        assert_eq!(values, col.num_values());
                    }
                }
            }
        }
    }
}

#[test]
fn corrupt_headers_are_rejected_not_panicking() {
    let bytes = write_parquet(
        &[sample_batch(0, 100)],
        props(parquet::basic::Compression::SNAPPY, false, true, 50, 100),
    );
    let md = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();
    let (start, len) = md.row_group(0).column(2).byte_range();
    let chunk = bytes[start as usize..(start + len) as usize].to_vec();
    for cut in 0..chunk.len().min(64) {
        assert!(split_pages(&chunk[..cut]).is_err() || cut == 0);
    }
    // Random garbage must error, not panic.
    let mut seed = 12345u64;
    for _ in 0..2000 {
        let n = (seed % 40) as usize;
        let buf: Vec<u8> = (0..n)
            .map(|_| {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (seed >> 33) as u8
            })
            .collect();
        let _ = split_pages(&buf);
    }
}
