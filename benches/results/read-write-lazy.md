# SQL read/write benchmark

Recorded 2026-11-08T04:24:11.737827+00:00 on `7f6cd4a4d748f818971a6cfc33ecd0b6f3998cb8`.

Median warm-cache query latency in milliseconds (lower is better).
VPF metadata mode: `lazy`.
Read workload version: 2 (14 queries).

| Rows | Query | SQLite | Parquet / DuckDB | VPF / DuckDB |
|---:|---|---:|---:|---:|
| 10,000 | count | 0.017 | 0.479 | 1.420 |
| 10,000 | point_lookup | 0.250 | 1.157 | 2.271 |
| 10,000 | range_lookup | 0.401 | 1.479 | 2.516 |
| 10,000 | equality_filter | 0.277 | 0.923 | 1.905 |
| 10,000 | null_filter | 0.364 | 0.894 | 1.862 |
| 10,000 | like_filter | 0.587 | 0.890 | 1.972 |
| 10,000 | or_filter | 0.346 | 0.972 | 1.972 |
| 10,000 | aggregate | 1.573 | 1.066 | 2.074 |
| 10,000 | range | 0.325 | 1.087 | 2.046 |
| 10,000 | filtered_aggregate | 0.371 | 1.061 | 2.167 |
| 10,000 | group_by | 2.455 | 1.219 | 2.481 |
| 10,000 | filtered_group_by | 0.985 | 1.381 | 2.398 |
| 10,000 | top_n | 2.207 | 1.322 | 2.201 |
| 10,000 | distinct | 1.964 | 0.964 | 1.881 |
| 100,000 | count | 0.058 | 0.524 | 1.678 |
| 100,000 | point_lookup | 2.392 | 2.306 | 4.476 |
| 100,000 | range_lookup | 3.029 | 2.605 | 4.652 |
| 100,000 | equality_filter | 2.683 | 1.727 | 3.132 |
| 100,000 | null_filter | 3.621 | 1.799 | 3.087 |
| 100,000 | like_filter | 5.373 | 1.679 | 4.295 |
| 100,000 | or_filter | 3.421 | 1.966 | 3.697 |
| 100,000 | aggregate | 15.637 | 2.457 | 3.754 |
| 100,000 | range | 3.074 | 1.976 | 3.393 |
| 100,000 | filtered_aggregate | 3.448 | 1.949 | 4.406 |
| 100,000 | group_by | 25.520 | 2.329 | 6.082 |
| 100,000 | filtered_group_by | 9.193 | 2.610 | 4.103 |
| 100,000 | top_n | 18.227 | 2.998 | 4.515 |
| 100,000 | distinct | 14.577 | 1.622 | 2.683 |
| 1,000,000 | count | 0.778 | 0.819 | 5.014 |
| 1,000,000 | point_lookup | 25.029 | 2.840 | 7.015 |
| 1,000,000 | range_lookup | 31.327 | 3.504 | 7.928 |
| 1,000,000 | equality_filter | 28.103 | 9.820 | 15.288 |
| 1,000,000 | null_filter | 37.865 | 11.360 | 16.542 |
| 1,000,000 | like_filter | 55.013 | 9.296 | 26.447 |
| 1,000,000 | or_filter | 36.768 | 2.792 | 21.350 |
| 1,000,000 | aggregate | 156.903 | 15.015 | 21.604 |
| 1,000,000 | range | 30.902 | 3.327 | 5.438 |
| 1,000,000 | filtered_aggregate | 36.960 | 11.512 | 28.199 |
| 1,000,000 | group_by | 306.011 | 14.620 | 43.345 |
| 1,000,000 | filtered_group_by | 105.883 | 14.542 | 22.760 |
| 1,000,000 | top_n | 212.317 | 17.413 | 21.048 |
| 1,000,000 | distinct | 142.419 | 8.655 | 12.504 |
| 10,000,000 | count | 158.146 | 4.174 | 34.054 |
| 10,000,000 | point_lookup | 347.016 | 6.133 | 29.879 |
| 10,000,000 | range_lookup | 405.283 | 11.992 | 43.155 |
| 10,000,000 | equality_filter | 372.612 | 90.846 | 143.889 |
| 10,000,000 | null_filter | 487.042 | 123.361 | 153.608 |
| 10,000,000 | like_filter | 642.182 | 79.805 | 247.460 |
| 10,000,000 | or_filter | 439.403 | 5.151 | 213.784 |
| 10,000,000 | aggregate | 1691.935 | 137.850 | 186.003 |
| 10,000,000 | range | 390.113 | 7.225 | 25.912 |
| 10,000,000 | filtered_aggregate | 450.435 | 94.085 | 259.558 |
| 10,000,000 | group_by | 4341.719 | 129.608 | 409.787 |
| 10,000,000 | filtered_group_by | 2311.557 | 126.718 | 187.637 |
| 10,000,000 | top_n | 2979.310 | 149.343 | 190.727 |
| 10,000,000 | distinct | 1516.912 | 79.143 | 113.825 |

Total of per-query medians, excluding metadata-friendly `COUNT(*)`. This is an equally weighted workload score, not one timed transaction.

| Rows | SQLite (ms) | Parquet (ms) | VPF (ms) | SQLite / VPF | VPF / Parquet |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 12.105 | 14.414 | 27.744 | 0.44x | 1.92x |
| 100,000 | 110.195 | 28.023 | 52.275 | 2.11x | 1.87x |
| 1,000,000 | 1205.500 | 124.696 | 249.466 | 4.83x | 2.00x |
| 10,000,000 | 16375.518 | 1041.259 | 2205.224 | 7.43x | 2.12x |

| Rows | SQLite (MiB) | Parquet (MiB) | VPF (MiB) |
|---:|---:|---:|---:|
| 10,000 | 0.27 | 0.14 | 0.48 |
| 100,000 | 2.71 | 1.07 | 1.44 |
| 1,000,000 | 27.38 | 10.57 | 11.36 |
| 10,000,000 | 283.32 | 105.37 | 110.09 |

VPF fixture import elapsed time includes CLI process startup; one setup observation per size, not repeated write measurements.

| Rows | Import (ms) |
|---:|---:|
| 10,000 | 23.404 |
| 100,000 | 31.070 |
| 1,000,000 | 137.666 |
| 10,000,000 | 1147.117 |

## Durable writes

Median milliseconds including commit, WAL checkpoint where applicable, file fsync and directory fsync. Fixture copying/syncing and verification are excluded. SQLite uses native INSERT SELECT, not Python row ingestion; Parquet appends/updates/deletes rewrite the complete file.

| Rows | Write | Logical rows changed | SQLite | Parquet / DuckDB | VPF / DuckDB |
|---:|---|---:|---:|---:|---:|
| 10,000 | fresh | 10,000 | 5.666 | 4.668 | 14.077 |
| 10,000 | append | 100 | 2.476 | 6.000 | 5.771 |
| 10,000 | update | 100 | 2.850 | 5.481 | 9.646 |
| 10,000 | delete | 100 | 2.749 | 9.096 | 11.366 |
| 100,000 | fresh | 100,000 | 23.178 | 21.678 | 43.314 |
| 100,000 | append | 1,000 | 3.281 | 22.703 | 6.472 |
| 100,000 | update | 100 | 6.503 | 23.138 | 16.707 |
| 100,000 | delete | 100 | 6.693 | 23.658 | 36.344 |
| 1,000,000 | fresh | 1,000,000 | 215.418 | 142.512 | 256.636 |
| 1,000,000 | append | 10,000 | 6.530 | 146.515 | 13.936 |
| 1,000,000 | update | 100 | 54.575 | 147.940 | 21.261 |
| 1,000,000 | delete | 100 | 43.341 | 161.698 | 43.386 |
| 10,000,000 | fresh | 10,000,000 | 1998.681 | 1326.249 | 1892.237 |
| 10,000,000 | append | 100,000 | 27.911 | 1373.412 | 67.766 |
| 10,000,000 | update | 100 | 410.248 | 1367.122 | 60.494 |
| 10,000,000 | delete | 100 | 413.846 | 1413.939 | 83.573 |
