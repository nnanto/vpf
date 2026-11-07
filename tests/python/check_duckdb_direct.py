"""Read VPF snapshots through DuckDB's registered Python filesystem."""

import io
import os
import sys
import threading
from datetime import datetime, timezone

import duckdb
import fsspec


sizes = [int(size) for size in sys.argv[1:]]
lock = threading.Lock()


class VirtualFile(io.RawIOBase):
    def __init__(self, index):
        super().__init__()
        self.index = index
        # DuckDB can seek and read the same handle concurrently from different threads.
        self.positions = threading.local()

    @property
    def pos(self):
        return getattr(self.positions, "offset", 0)

    @pos.setter
    def pos(self, value):
        self.positions.offset = value

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_CUR:
            offset += self.pos
        elif whence == os.SEEK_END:
            offset += sizes[self.index]
        elif whence != os.SEEK_SET:
            raise ValueError(f"invalid whence: {whence}")
        if offset < 0:
            raise ValueError("negative seek position")
        self.pos = offset
        return self.pos

    def read(self, size=-1):
        remaining = max(0, sizes[self.index] - self.pos)
        length = remaining if size is None or size < 0 else min(size, remaining)
        if not length:
            return b""
        with lock:
            sys.stdout.write(f"{self.index} {self.pos} {length}\n")
            sys.stdout.flush()
            data = bytearray()
            while len(data) < length:
                part = sys.stdin.buffer.read(length - len(data))
                if not part:
                    raise EOFError("VPF range server closed unexpectedly")
                data.extend(part)
        self.pos += length
        return bytes(data)

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


class VirtualFileSystem(fsspec.AbstractFileSystem):
    protocol = "vpf"

    def _index(self, path):
        path = self._strip_protocol(path).lstrip("/")
        if path == "old.parquet":
            return 0
        if path == "current.parquet":
            return 1
        raise FileNotFoundError(path)

    def info(self, path, **kwargs):
        index = self._index(path)
        return {"name": path, "size": sizes[index], "type": "file"}

    def modified(self, path):
        self._index(path)
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _open(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError(f"unsupported mode: {mode}")
        return VirtualFile(self._index(path))


def expected_rows(end, current):
    rows = []
    for i in range(30 if current else 0, end):
        name = None if i % 5 == 0 else f"name-{i % 97}"
        if current and 100 <= i < 150:
            name = "changed"
        rows.append((i, None if i % 7 == 0 else i * 3, name,
                     None if i % 11 == 0 else i * 1.5,
                     None if i % 3 == 0 else i % 2 == 0))
    return rows


con = duckdb.connect()
con.execute("SET threads = 4")
con.register_filesystem(VirtualFileSystem())
for name, end, current in [("old", 1000, False), ("current", 1200, True)]:
    path = f"vpf://{name}.parquet"
    expected = expected_rows(end, current)
    got = con.execute(
        "SELECT id, i32, name, score, flag FROM read_parquet(?) ORDER BY id", [path]
    ).fetchall()
    assert got == expected, f"{name}: data mismatch"
    filtered = con.execute(
        "SELECT id, name FROM read_parquet(?) WHERE id >= 95 AND id < 155 ORDER BY id",
        [path],
    ).fetchall()
    assert filtered == [(row[0], row[2]) for row in expected if 95 <= row[0] < 155]

con.unregister_filesystem("vpf")
con.close()
del con
