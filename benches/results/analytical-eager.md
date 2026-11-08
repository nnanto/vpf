# Analytical SQL benchmark

Recorded 2026-11-08T02:24:56.253161+00:00 on `962146fa26a6596f27fa2ff969fa8be9426b4f2b`.

Median warm-cache query latency in milliseconds (lower is better).
VPF metadata mode: `eager`.

| Rows | Query | SQLite | Parquet / DuckDB | VPF / DuckDB |
|---:|---|---:|---:|---:|
| 10,000 | count | 0.024 | 0.459 | 1.109 |
| 10,000 | aggregate | 1.572 | 1.024 | 1.700 |
| 10,000 | range | 0.303 | 1.031 | 1.725 |
| 10,000 | filtered_aggregate | 0.355 | 0.968 | 1.861 |
| 10,000 | group_by | 2.397 | 1.145 | 2.102 |
| 10,000 | filtered_group_by | 0.967 | 1.324 | 2.054 |
| 10,000 | top_n | 2.200 | 1.227 | 1.866 |
| 10,000 | distinct | 1.922 | 0.934 | 1.537 |
| 100,000 | count | 0.068 | 0.529 | 1.239 |
| 100,000 | aggregate | 15.406 | 2.475 | 3.233 |
| 100,000 | range | 2.977 | 2.090 | 2.793 |
| 100,000 | filtered_aggregate | 3.530 | 1.973 | 3.993 |
| 100,000 | group_by | 25.409 | 2.682 | 5.953 |
| 100,000 | filtered_group_by | 9.071 | 2.872 | 3.589 |
| 100,000 | top_n | 18.206 | 2.859 | 3.704 |
| 100,000 | distinct | 15.182 | 1.599 | 2.321 |
| 1,000,000 | count | 0.919 | 0.832 | 2.418 |
| 1,000,000 | aggregate | 158.296 | 14.877 | 18.267 |
| 1,000,000 | range | 32.344 | 2.969 | 3.545 |
| 1,000,000 | filtered_aggregate | 36.692 | 10.506 | 25.869 |
| 1,000,000 | group_by | 312.136 | 15.106 | 40.481 |
| 1,000,000 | filtered_group_by | 107.231 | 14.960 | 20.164 |
| 1,000,000 | top_n | 212.293 | 16.823 | 20.115 |
| 1,000,000 | distinct | 143.686 | 9.642 | 9.635 |
| 10,000,000 | count | 108.407 | 2.896 | 12.472 |
| 10,000,000 | aggregate | 1720.384 | 133.177 | 166.284 |
| 10,000,000 | range | 389.981 | 5.875 | 6.462 |
| 10,000,000 | filtered_aggregate | 442.634 | 88.590 | 236.821 |
| 10,000,000 | group_by | 4150.603 | 126.320 | 399.256 |
| 10,000,000 | filtered_group_by | 2216.998 | 125.687 | 164.855 |
| 10,000,000 | top_n | 2810.862 | 148.514 | 178.328 |
| 10,000,000 | distinct | 1530.467 | 72.304 | 85.953 |

Total of per-query medians, excluding metadata-friendly `COUNT(*)`. This is an equally weighted workload score, not one timed transaction.

| Rows | SQLite (ms) | Parquet (ms) | VPF (ms) | SQLite / VPF | VPF / Parquet |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 9.716 | 7.652 | 12.845 | 0.76x | 1.68x |
| 100,000 | 89.781 | 16.549 | 25.585 | 3.51x | 1.55x |
| 1,000,000 | 1002.677 | 84.883 | 138.075 | 7.26x | 1.63x |
| 10,000,000 | 13261.929 | 700.467 | 1237.958 | 10.71x | 1.77x |

| Rows | SQLite (MiB) | Parquet (MiB) | VPF (MiB) |
|---:|---:|---:|---:|
| 10,000 | 0.27 | 0.14 | 0.47 |
| 100,000 | 2.71 | 1.07 | 1.42 |
| 1,000,000 | 27.38 | 10.57 | 11.36 |
| 10,000,000 | 283.32 | 105.37 | 110.27 |

VPF fixture import elapsed time includes CLI process startup; one setup observation per size, not repeated write measurements.

| Rows | Import (ms) |
|---:|---:|
| 10,000 | 27.322 |
| 100,000 | 39.359 |
| 1,000,000 | 219.846 |
| 10,000,000 | 1676.004 |
