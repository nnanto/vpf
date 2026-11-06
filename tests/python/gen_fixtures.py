"""Generates Parquet fixtures with pyarrow covering writer features vpf must import."""
import datetime as dt
import decimal
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
n = 5000
rows = range(n)
tbl = pa.table({
    "id": pa.array(list(rows), pa.int64()),
    "name": pa.array([None if i % 7 == 0 else f"n{i % 50}" for i in rows], pa.string()),
    "big": pa.array([f"x{i}" * (i % 3) for i in rows], pa.large_string()),
    "price": pa.array([None if i % 5 == 0 else decimal.Decimal(i) / 100 for i in rows], pa.decimal128(10, 2)),
    "wide": pa.array([decimal.Decimal(i) for i in rows], pa.decimal128(30, 0)),
    "ts": pa.array([dt.datetime(2020, 1, 1) + dt.timedelta(seconds=i) for i in rows], pa.timestamp("ns")),
    "tstz": pa.array([dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(minutes=i) for i in rows], pa.timestamp("ms", tz="UTC")),
    "d": pa.array([dt.date(2000, 1, 1) + dt.timedelta(days=i % 900) for i in rows], pa.date32()),
    "t": pa.array([dt.time(i % 24, i % 60) for i in rows], pa.time64("us")),
    "f": pa.array([None if i % 3 == 0 else i * 0.5 for i in rows], pa.float32()),
    "b": pa.array([i % 2 == 0 for i in rows], pa.bool_()),
    "u": pa.array([i % 250 for i in rows], pa.uint8()),
    "cat": pa.array([["red", "green", "blue"][i % 3] for i in rows]).dictionary_encode(),
    "fixed": pa.array([bytes([i % 256]) * 4 for i in rows], pa.binary(4)),
})
common = dict(row_group_size=1700, data_page_size=4096)
pq.write_table(tbl, f"{out}/v1_snappy.parquet", data_page_version="1.0", compression="snappy", **common)
pq.write_table(tbl, f"{out}/v2_zstd.parquet", data_page_version="2.0", compression="zstd", **common)
pq.write_table(tbl, f"{out}/gzip_nodict.parquet", compression="gzip", use_dictionary=False, **common)
pq.write_table(tbl, f"{out}/pageindex_lz4.parquet", compression="lz4", write_page_index=True, write_statistics=True, **common)
pq.write_table(tbl, f"{out}/int96_brotli.parquet", compression="brotli", use_deprecated_int96_timestamps=True, **common)
pq.write_table(tbl, f"{out}/uncompressed_v2_crc.parquet", compression="none", data_page_version="2.0", write_page_checksum=True, **common)
pq.write_table(tbl.slice(0, 0), f"{out}/empty.parquet")
nested = pa.table({"id": [1, 2], "tags": pa.array([["a"], ["b", "c"]])})
pq.write_table(nested, f"{out}/nested.parquet")
st = pa.table({"id": [1, 2], "s": pa.array([{"x": 1}, {"x": 2}])})
pq.write_table(st, f"{out}/struct.parquet")
