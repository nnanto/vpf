# vpf-fuse

`vpf-fuse` mounts a directory of `.vpf` files as a read-only filesystem. Each
`<name>.vpf` file is exposed at the mount root as `<name>.parquet`.

## Prerequisites

- Linux: FUSE 3 userspace/kernel support and permission to mount FUSE filesystems.
  On Debian or Ubuntu, install `fuse3`; depending on system policy, add your user
  to the `fuse` group or use the system's permitted mount configuration.
- macOS: install and enable macFUSE, then allow its system extension when prompted.
  macFUSE may require an administrator-approved security setting. Install `pkg-config`
  and ensure the macFUSE `fuse.pc` (or older `osxfuse.pc`) is discoverable in
  `PKG_CONFIG_PATH` if it is not found automatically.
- Rust toolchain compatible with the repository.

On Linux the crate disables `fuser`'s default `libfuse` feature and does not
need the libfuse development package. On macOS, `fuser` requires the `libfuse`
feature and links against macFUSE; the crate enables that feature automatically.
The macOS build and mount have not been tested here.

## Laptop usage

Create a mount point, then run:

```sh
mkdir -p ./vpf-mount
cargo run -p vpf-fuse -- ./path/to/tables ./vpf-mount
```

The process stays in the foreground. In another terminal, read a file as usual:

```sh
ls ./vpf-mount
parquet-tools schema ./vpf-mount/my-table.parquet
```

Stop the mount with Ctrl-C. If the mount remains after an interrupted process,
unmount it with `fusermount3 -u ./vpf-mount` on Linux or `umount ./vpf-mount`
on macOS.

## Semantics and limitations

The filesystem is read-only. Only file stems made of ASCII letters, digits,
`_`, `-` and `.` (not starting with `.`) are exposed. Only the current snapshot is
exposed; immutable snapshot-specific paths are not currently provided.

Opening a file pins its current immutable `VirtualFile`: reads and handle-based
attributes remain on that snapshot until release. A later open resolves the
latest snapshot. This provides per-open-handle consistency, not a single
snapshot view across an entire mount, shell command, or multi-file query.
Filesystem metadata caching is disabled so lookups and attributes of mutable
latest paths do not retain stale size information. File opens request direct I/O
without keep-cache so the kernel page cache cannot mix bytes from different
snapshots; this may reduce read performance and limit memory-mapped access.
Handle-specific attributes are returned when the FUSE kernel supplies a handle;
path-based attributes describe the latest snapshot.
