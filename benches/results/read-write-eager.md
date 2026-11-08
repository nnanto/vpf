# SQL read/write benchmark

Recorded 2026-11-08T04:12:53.087684+00:00 on `7f6cd4a4d748f818971a6cfc33ecd0b6f3998cb8`.

Median warm-cache query latency in milliseconds (lower is better).
VPF metadata mode: `eager`.
Read workload version: 2 (14 queries).

| Rows | Query | SQLite | Parquet / DuckDB | VPF / DuckDB |
|---:|---|---:|---:|---:|
| 10,000 | count | 0.022 | 0.458 | 1.147 |
| 10,000 | point_lookup | 0.241 | 1.051 | 2.005 |
| 10,000 | range_lookup | 0.392 | 1.441 | 2.201 |
| 10,000 | equality_filter | 0.276 | 0.896 | 1.616 |
| 10,000 | null_filter | 0.369 | 0.830 | 1.608 |
| 10,000 | like_filter | 0.540 | 0.941 | 1.688 |
| 10,000 | or_filter | 0.338 | 1.047 | 1.666 |
| 10,000 | aggregate | 1.587 | 1.044 | 1.786 |
| 10,000 | range | 0.313 | 1.063 | 1.771 |
| 10,000 | filtered_aggregate | 0.354 | 1.017 | 1.879 |
| 10,000 | group_by | 2.428 | 1.253 | 2.117 |
| 10,000 | filtered_group_by | 0.964 | 1.427 | 2.158 |
| 10,000 | top_n | 3.515 | 1.287 | 1.962 |
| 10,000 | distinct | 1.906 | 0.977 | 1.654 |
| 100,000 | count | 0.061 | 0.491 | 1.193 |
| 100,000 | point_lookup | 2.352 | 2.003 | 4.012 |
| 100,000 | range_lookup | 2.971 | 2.352 | 4.178 |
| 100,000 | equality_filter | 2.624 | 1.641 | 2.658 |
| 100,000 | null_filter | 3.592 | 1.909 | 2.666 |
| 100,000 | like_filter | 5.383 | 1.678 | 3.813 |
| 100,000 | or_filter | 3.371 | 1.973 | 3.194 |
| 100,000 | aggregate | 15.446 | 2.238 | 3.231 |
| 100,000 | range | 2.963 | 2.004 | 2.916 |
| 100,000 | filtered_aggregate | 3.438 | 1.840 | 3.920 |
| 100,000 | group_by | 25.411 | 2.464 | 5.624 |
| 100,000 | filtered_group_by | 9.060 | 2.554 | 3.619 |
| 100,000 | top_n | 18.119 | 2.892 | 3.512 |
| 100,000 | distinct | 15.385 | 1.570 | 2.252 |
| 1,000,000 | count | 0.787 | 0.735 | 2.275 |
| 1,000,000 | point_lookup | 25.224 | 2.719 | 4.681 |
| 1,000,000 | range_lookup | 31.318 | 3.257 | 5.241 |
| 1,000,000 | equality_filter | 27.931 | 9.048 | 12.632 |
| 1,000,000 | null_filter | 38.967 | 10.779 | 13.559 |
| 1,000,000 | like_filter | 56.455 | 8.985 | 23.923 |
| 1,000,000 | or_filter | 36.196 | 2.466 | 18.777 |
| 1,000,000 | aggregate | 157.235 | 14.568 | 18.706 |
| 1,000,000 | range | 31.249 | 2.443 | 3.036 |
| 1,000,000 | filtered_aggregate | 36.421 | 10.340 | 25.015 |
| 1,000,000 | group_by | 308.642 | 13.900 | 40.116 |
| 1,000,000 | filtered_group_by | 104.688 | 13.680 | 19.321 |
| 1,000,000 | top_n | 209.923 | 16.405 | 18.183 |
| 1,000,000 | distinct | 141.521 | 8.343 | 9.731 |
| 10,000,000 | count | 100.587 | 3.020 | 11.907 |
| 10,000,000 | point_lookup | 338.844 | 5.391 | 7.694 |
| 10,000,000 | range_lookup | 388.989 | 7.517 | 8.688 |
| 10,000,000 | equality_filter | 358.055 | 84.860 | 107.179 |
| 10,000,000 | null_filter | 467.478 | 103.209 | 119.762 |
| 10,000,000 | like_filter | 630.678 | 87.130 | 227.454 |
| 10,000,000 | or_filter | 430.419 | 4.788 | 180.460 |
| 10,000,000 | aggregate | 1651.423 | 132.603 | 167.394 |
| 10,000,000 | range | 383.218 | 5.402 | 5.946 |
| 10,000,000 | filtered_aggregate | 452.414 | 86.830 | 230.307 |
| 10,000,000 | group_by | 3944.863 | 124.055 | 387.514 |
| 10,000,000 | filtered_group_by | 2014.857 | 127.860 | 166.644 |
| 10,000,000 | top_n | 2790.024 | 144.823 | 187.303 |
| 10,000,000 | distinct | 1529.198 | 77.835 | 80.630 |

Total of per-query medians, excluding metadata-friendly `COUNT(*)`. This is an equally weighted workload score, not one timed transaction.

| Rows | SQLite (ms) | Parquet (ms) | VPF (ms) | SQLite / VPF | VPF / Parquet |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 13.223 | 14.275 | 24.110 | 0.55x | 1.69x |
| 100,000 | 110.116 | 27.118 | 45.595 | 2.42x | 1.68x |
| 1,000,000 | 1205.770 | 116.934 | 212.921 | 5.66x | 1.82x |
| 10,000,000 | 15380.458 | 992.305 | 1876.973 | 8.19x | 1.89x |

| Rows | SQLite (MiB) | Parquet (MiB) | VPF (MiB) |
|---:|---:|---:|---:|
| 10,000 | 0.27 | 0.14 | 0.48 |
| 100,000 | 2.71 | 1.07 | 1.44 |
| 1,000,000 | 27.38 | 10.57 | 11.38 |
| 10,000,000 | 283.32 | 105.37 | 110.28 |

VPF fixture import elapsed time includes CLI process startup; one setup observation per size, not repeated write measurements.

| Rows | Import (ms) |
|---:|---:|
| 10,000 | 24.068 |
| 100,000 | 33.488 |
| 1,000,000 | 142.162 |
| 10,000,000 | 1151.923 |

## Durable writes

Median milliseconds including commit, WAL checkpoint where applicable, file fsync and directory fsync. Fixture copying/syncing and verification are excluded. SQLite uses native INSERT SELECT, not Python row ingestion; Parquet appends/updates/deletes rewrite the complete file.

| Rows | Write | Logical rows changed | SQLite | Parquet / DuckDB | VPF / DuckDB |
|---:|---|---:|---:|---:|---:|
| 10,000 | fresh | 10,000 | 5.340 | 4.373 | 17.433 |
| 10,000 | append | 100 | 2.390 | 5.656 | 6.363 |
| 10,000 | update | 100 | 2.831 | 5.328 | 8.599 |
| 10,000 | delete | 100 | 2.894 | 5.400 | 11.863 |
| 100,000 | fresh | 100,000 | 23.153 | 21.729 | 43.568 |
| 100,000 | append | 1,000 | 3.453 | 23.348 | 8.303 |
| 100,000 | update | 100 | 6.484 | 22.994 | 17.010 |
| 100,000 | delete | 100 | 6.571 | 23.823 | 35.906 |
| 1,000,000 | fresh | 1,000,000 | 211.787 | 145.397 | 288.759 |
| 1,000,000 | append | 10,000 | 8.405 | 154.508 | 23.303 |
| 1,000,000 | update | 100 | 56.030 | 149.573 | 25.751 |
| 1,000,000 | delete | 100 | 46.851 | 155.379 | 47.360 |
| 10,000,000 | fresh | 10,000,000 | 2020.046 | 1339.083 | 2064.484 |
| 10,000,000 | append | 100,000 | 28.319 | 1360.402 | 90.070 |
| 10,000,000 | update | 100 | 398.745 | 1354.599 | 63.731 |
| 10,000,000 | delete | 100 | 385.282 | 1437.278 | 86.121 |
