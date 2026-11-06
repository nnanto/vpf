"""Writes nested pyarrow fixtures (structs, lists, maps, list<list>) into a directory."""
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
rng = random.Random(5)
N = 2000


def maybe(p, f):
    return None if rng.random() < p else f()


def row(i):
    big = rng.random() < 0.03
    return {
        "id": i,
        "s": maybe(0.2, lambda: {
            "a": maybe(0.2, lambda: rng.randint(-50, 50)),
            "b": maybe(0.2, lambda: f"b{rng.randint(0, 20)}"),
            "inner": maybe(0.3, lambda: {"d": maybe(0.3, lambda: rng.random())}),
        }),
        "l": maybe(0.2, lambda: [maybe(0.1, lambda: rng.randint(0, 999)) for _ in range(rng.randint(0, 400 if big else 4))]),
        "ls": maybe(0.2, lambda: [{"x": rng.randint(0, 9), "y": maybe(0.3, lambda: [maybe(0.2, lambda: f"y{j}") for j in range(rng.randint(0, 3))])} for _ in range(rng.randint(0, 3))]),
        "m": maybe(0.2, lambda: [(f"k{k}", maybe(0.2, lambda: rng.randint(0, 99))) for k in range(rng.randint(0, 3))]),
        "ll": maybe(0.2, lambda: [maybe(0.2, lambda: [maybe(0.2, lambda: rng.randint(0, 9)) for _ in range(rng.randint(0, 3))]) for _ in range(rng.randint(0, 3))]),
        "tag": maybe(0.3, lambda: rng.choice(["x", "y", "z"])),
    }


schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("s", pa.struct([("a", pa.int32()), ("b", pa.string()), ("inner", pa.struct([("d", pa.float64())]))])),
    pa.field("l", pa.list_(pa.int64())),
    pa.field("ls", pa.list_(pa.field("item", pa.struct([pa.field("x", pa.int32(), nullable=False), ("y", pa.list_(pa.string()))]), nullable=False))),
    pa.field("m", pa.map_(pa.string(), pa.int64())),
    pa.field("ll", pa.list_(pa.list_(pa.int32()))),
    pa.field("tag", pa.string()),
])
table = pa.Table.from_pylist([row(i) for i in range(N)], schema=schema)

variants = {
    "v1_snappy": dict(compression="snappy", data_page_version="1.0"),
    "v2_zstd": dict(compression="zstd", data_page_version="2.0"),
    "v1_tiny_pages": dict(compression="none", data_page_version="1.0", data_page_size=256, use_dictionary=False),
    "v2_tiny_pages": dict(compression="gzip", data_page_version="2.0", data_page_size=256),
    "noncompliant": dict(compression="snappy", use_compliant_nested_type=False),
    "multi_rg": dict(compression="lz4", row_group_size=333),
}
for name, kw in variants.items():
    pq.write_table(table, f"{out}/{name}.parquet", **kw)

# Separate single-type fixtures, including a required struct.
req = pa.Table.from_pylist(
    [{"id": i, "r": {"k": i % 7, "v": maybe(0.5, lambda: "v")}} for i in range(500)],
    schema=pa.schema([pa.field("id", pa.int64(), nullable=False),
                      pa.field("r", pa.struct([pa.field("k", pa.int32(), nullable=False), ("v", pa.string())]), nullable=False)]),
)
pq.write_table(req, f"{out}/required_struct.parquet", data_page_size=128)
