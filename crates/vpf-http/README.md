# vpf-http

Read VPF snapshots directly over HTTP, without exporting Parquet or mounting a filesystem.
The server exposes a directory of `.vpf` files; `sales.vpf` is served as `/files/sales/...`.
Bodies stream in bounded 64 KiB chunks from `VirtualFile::read_range`.

```sh
cargo run -p vpf-http -- --dir ./tables --listen 127.0.0.1:8080
curl http://127.0.0.1:8080/files/sales/latest
# {"snapshot_id":42}
curl -I http://127.0.0.1:8080/files/sales/snapshots/42.parquet
curl -H 'Range: bytes=-8' http://127.0.0.1:8080/files/sales/snapshots/42.parquet
```

Resolve `latest` once, then use that ID for every request in a query. Snapshot URLs
never change: HEAD, footer reads, projected reads and later opens all see the same
version, even while another process updates the store. `latest` is a JSON resolver,
not a mutable Parquet URL or a redirect.

DuckDB uses its official `httpfs` extension:

```sql
INSTALL httpfs;
LOAD httpfs;
SELECT * FROM read_parquet('http://127.0.0.1:8080/files/sales/snapshots/42.parquet');
```

Names are file stems made of ASCII letters, digits, `_`, `-` and `.`. The endpoint supports
GET, HEAD, single byte ranges (including suffix ranges), ETag, If-Match and
ETag-based If-Range. Invalid, unsatisfiable and multipart ranges return 416.
HEAD ignores Range. Unknown files or snapshots return 404.

```sh
cargo test -p vpf-http
VPF_PYTHON=/path/to/python cargo test -p vpf-http --test http -- --nocapture
```

The Python interpreter needs `duckdb` and its official `httpfs` extension installed
for its exact DuckDB version/platform. The optional DuckDB test is skipped when
`VPF_PYTHON` is unset; if configured, missing extension support is a test failure.
It does not install extensions automatically.
For an already-downloaded signed extension, set `VPF_DUCKDB_HTTPFS=/absolute/path/to/httpfs.duckdb_extension`.
DuckDB's normal signature verification remains enabled.

The server is read-only but has no authentication or TLS. It defaults to loopback;
do not expose sensitive files to an untrusted network. Opening a file uses VPF's
normal SQLite WAL setup/migrations and may upgrade it.
