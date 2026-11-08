# Analytical SQL benchmark

Recorded 2026-11-08T02:21:19.280962+00:00 on `962146fa26a6596f27fa2ff969fa8be9426b4f2b`.

Median warm-cache query latency in milliseconds (lower is better).
VPF metadata mode: `lazy`.

| Rows | Query | SQLite | Parquet / DuckDB | VPF / DuckDB |
|---:|---|---:|---:|---:|
| 10,000 | count | 0.023 | 0.462 | 1.391 |
| 10,000 | aggregate | 1.552 | 1.087 | 1.980 |
| 10,000 | range | 0.304 | 1.040 | 2.068 |
| 10,000 | filtered_aggregate | 0.352 | 0.994 | 2.133 |
| 10,000 | group_by | 2.407 | 1.174 | 2.339 |
| 10,000 | filtered_group_by | 0.990 | 1.386 | 2.394 |
| 10,000 | top_n | 2.287 | 1.224 | 2.183 |
| 10,000 | distinct | 1.928 | 0.908 | 1.821 |
| 100,000 | count | 0.060 | 0.519 | 1.645 |
| 100,000 | aggregate | 15.373 | 2.246 | 3.682 |
| 100,000 | range | 2.950 | 1.989 | 3.296 |
| 100,000 | filtered_aggregate | 3.460 | 1.838 | 4.385 |
| 100,000 | group_by | 25.209 | 2.508 | 6.192 |
| 100,000 | filtered_group_by | 9.000 | 2.679 | 4.011 |
| 100,000 | top_n | 18.260 | 3.051 | 3.956 |
| 100,000 | distinct | 14.620 | 1.624 | 2.692 |
| 1,000,000 | count | 0.869 | 0.833 | 4.772 |
| 1,000,000 | aggregate | 156.284 | 14.787 | 21.455 |
| 1,000,000 | range | 30.975 | 2.516 | 5.682 |
| 1,000,000 | filtered_aggregate | 37.803 | 10.646 | 28.464 |
| 1,000,000 | group_by | 310.061 | 14.250 | 43.808 |
| 1,000,000 | filtered_group_by | 105.196 | 16.007 | 22.038 |
| 1,000,000 | top_n | 212.453 | 16.552 | 22.153 |
| 1,000,000 | distinct | 142.534 | 8.713 | 12.168 |
| 10,000,000 | count | 105.929 | 2.857 | 33.597 |
| 10,000,000 | aggregate | 1654.253 | 135.313 | 184.901 |
| 10,000,000 | range | 381.480 | 5.877 | 25.405 |
| 10,000,000 | filtered_aggregate | 467.911 | 87.275 | 254.471 |
| 10,000,000 | group_by | 4012.860 | 124.234 | 404.399 |
| 10,000,000 | filtered_group_by | 2242.267 | 125.331 | 191.856 |
| 10,000,000 | top_n | 2686.492 | 158.774 | 186.342 |
| 10,000,000 | distinct | 1541.016 | 75.230 | 102.110 |

Total of per-query medians, excluding metadata-friendly `COUNT(*)`. This is an equally weighted workload score, not one timed transaction.

| Rows | SQLite (ms) | Parquet (ms) | VPF (ms) | SQLite / VPF | VPF / Parquet |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 9.820 | 7.813 | 14.918 | 0.66x | 1.91x |
| 100,000 | 88.873 | 15.934 | 28.215 | 3.15x | 1.77x |
| 1,000,000 | 995.306 | 83.470 | 155.766 | 6.39x | 1.87x |
| 10,000,000 | 12986.279 | 712.033 | 1349.484 | 9.62x | 1.90x |

| Rows | SQLite (MiB) | Parquet (MiB) | VPF (MiB) |
|---:|---:|---:|---:|
| 10,000 | 0.27 | 0.14 | 0.47 |
| 100,000 | 2.71 | 1.07 | 1.42 |
| 1,000,000 | 27.38 | 10.57 | 11.34 |
| 10,000,000 | 283.32 | 105.37 | 110.08 |

VPF fixture import elapsed time includes CLI process startup; one setup observation per size, not repeated write measurements.

| Rows | Import (ms) |
|---:|---:|
| 10,000 | 17.583 |
| 100,000 | 34.150 |
| 1,000,000 | 160.952 |
| 10,000,000 | 1300.530 |
