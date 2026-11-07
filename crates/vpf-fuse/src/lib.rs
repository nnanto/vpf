use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use vpf::layout::VirtualFile;
use vpf::meta;
use vpf::store::Store;

const ROOT_INO: u64 = 1;
const FILE_TTL: Duration = Duration::ZERO;
const ROOT_TTL: Duration = Duration::ZERO;
const OPEN_FLAGS: u32 = fuser::consts::FOPEN_DIRECT_IO;

pub struct VpfFs {
    store: Store,
    next_ino: u64,
    file_inos: HashMap<String, u64>,
    ino_files: HashMap<u64, String>,
    next_handle: u64,
    handles: HashMap<u64, VirtualFile>,
}

impl VpfFs {
    pub fn open(path: impl AsRef<Path>) -> vpf::Result<Self> {
        Ok(Self {
            store: Store::open(path)?,
            next_ino: ROOT_INO + 1,
            file_inos: HashMap::new(),
            ino_files: HashMap::new(),
            next_handle: 1,
            handles: HashMap::new(),
        })
    }

    fn inode_for(&mut self, name: &str) -> u64 {
        if let Some(&ino) = self.file_inos.get(name) {
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
        self.file_inos.insert(name.to_owned(), ino);
        self.ino_files.insert(ino, name.to_owned());
        ino
    }

    fn list_logical_files(&self) -> vpf::Result<Vec<String>> {
        self.store.read(meta::list_files)
    }

    fn logical_name_from_path(&self, path_name: &str) -> vpf::Result<Option<String>> {
        let Some(name) = path_name.strip_suffix(".parquet") else {
            return Ok(None);
        };
        if !is_safe_component(name) {
            return Ok(None);
        }
        Ok(self
            .list_logical_files()?
            .into_iter()
            .find(|stored| stored == name))
    }

    fn open_handle(&mut self, ino: u64, flags: i32) -> Result<u64, i32> {
        if ino == ROOT_INO {
            return Err(libc::EISDIR);
        }
        if flags & libc::O_ACCMODE != libc::O_RDONLY
            || flags & (libc::O_TRUNC | libc::O_APPEND) != 0
        {
            return Err(libc::EROFS);
        }
        let name = self.ino_files.get(&ino).ok_or(libc::ENOENT)?.clone();
        let file = self.store.open_snapshot(&name, None).map_err(|error| {
            log_error("opening file snapshot", &error);
            errno_for(&error)
        })?;
        let handle = self.next_handle;
        self.next_handle += 1;
        self.handles.insert(handle, file);
        Ok(handle)
    }

    fn read_handle(&self, handle: u64, offset: i64, size: u32) -> Result<Vec<u8>, i32> {
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let file = self.handles.get(&handle).ok_or(libc::EBADF)?;
        let offset = offset as u64;
        let size = size.min(file.size().saturating_sub(offset).min(u32::MAX as u64) as u32);
        let mut bytes = vec![0; size as usize];
        let count = file.read_at(offset, &mut bytes).map_err(|error| {
            log_error("reading open snapshot", &error);
            errno_for(&error)
        })?;
        bytes.truncate(count);
        Ok(bytes)
    }

    fn release_handle(&mut self, handle: u64) -> Result<(), i32> {
        self.handles.remove(&handle).map(|_| ()).ok_or(libc::EBADF)
    }

    fn file_attr(ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o444,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn dir_attr() -> FileAttr {
        FileAttr {
            ino: ROOT_INO,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o555,
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn attr_for(&self, ino: u64, fh: Option<u64>) -> Result<FileAttr, i32> {
        if ino == ROOT_INO {
            return Ok(Self::dir_attr());
        }
        let name = self.ino_files.get(&ino).ok_or(libc::ENOENT)?;
        let size = if let Some(handle) = fh {
            self.handles.get(&handle).ok_or(libc::EBADF)?.size()
        } else {
            self.store
                .open_snapshot(name, None)
                .map_err(|error| {
                    log_error("getting latest file attributes", &error);
                    errno_for(&error)
                })?
                .size()
        };
        Ok(Self::file_attr(ino, size))
    }
}

impl Filesystem for VpfFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != ROOT_INO {
            reply.error(libc::ENOENT);
            return;
        }
        let Some(path_name) = name.to_str() else {
            reply.error(libc::ENOENT);
            return;
        };
        match self.logical_name_from_path(path_name) {
            Ok(Some(logical_name)) => {
                let ino = self.inode_for(&logical_name);
                match self.attr_for(ino, None) {
                    Ok(attr) => reply.entry(&FILE_TTL, &attr, 0),
                    Err(errno) => reply.error(errno),
                }
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(error) => {
                log_error("listing store files", &error);
                reply.error(errno_for(&error));
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        match self.attr_for(ino, fh) {
            Ok(attr) => reply.attr(
                if ino == ROOT_INO {
                    &ROOT_TTL
                } else {
                    &FILE_TTL
                },
                &attr,
            ),
            Err(errno) => reply.error(errno),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.open_handle(ino, flags) {
            Ok(handle) => reply.opened(handle, OPEN_FLAGS),
            Err(errno) => reply.error(errno),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.read_handle(fh, offset, size) {
            Ok(data) => reply.data(&data),
            Err(errno) => reply.error(errno),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.release_handle(fh) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if ino == ROOT_INO {
            reply.opened(0, 0);
        } else {
            reply.error(libc::ENOTDIR);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != ROOT_INO {
            reply.error(libc::ENOTDIR);
            return;
        }
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }

        let mut entries = vec![
            (ROOT_INO, FileType::Directory, ".".to_owned()),
            (ROOT_INO, FileType::Directory, "..".to_owned()),
        ];
        match self.list_logical_files() {
            Ok(files) => {
                for name in files.into_iter().filter(|name| is_safe_component(name)) {
                    let ino = self.inode_for(&name);
                    entries.push((ino, FileType::RegularFile, format!("{name}.parquet")));
                }
            }
            Err(error) => {
                log_error("reading store directory", &error);
                reply.error(errno_for(&error));
                return;
            }
        }

        for (index, (entry_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*entry_ino, index as i64 + 1, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(libc::EROFS);
    }
}

fn is_safe_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

fn errno_for(error: &vpf::Error) -> i32 {
    match error {
        vpf::Error::NotFound(_) => libc::ENOENT,
        vpf::Error::Invalid(_) => libc::EINVAL,
        vpf::Error::Unsupported(_) => libc::EOPNOTSUPP,
        vpf::Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => libc::ENOENT,
        _ => libc::EIO,
    }
}

fn log_error(context: &str, error: &vpf::Error) {
    eprintln!("vpf-fuse: {context}: {error}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::basic::Compression;
    use std::sync::Arc;
    use vpf::ops::append::AppendMode;

    fn add_row(store: &Store, value: i64) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap();
        store
            .append("sample", &batch, AppendMode::Buffered)
            .unwrap();
    }

    #[test]
    fn handles_pin_snapshot_and_release_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .create_from_arrow(
                "sample",
                &Schema::new(vec![Field::new("value", DataType::Int64, false)]),
                Compression::UNCOMPRESSED,
            )
            .unwrap();
        add_row(&store, 11);

        let mut fs = VpfFs::open(dir.path()).unwrap();
        let ino = fs.inode_for("sample");
        let old_handle = fs.open_handle(ino, libc::O_RDONLY).unwrap();
        let old_size = fs.handles.get(&old_handle).unwrap().size();
        let old_bytes = fs.read_handle(old_handle, 0, u32::MAX).unwrap();

        add_row(&store, 22);
        let pinned = fs.read_handle(old_handle, 0, u32::MAX).unwrap();
        assert_eq!(pinned, old_bytes);
        assert_eq!(fs.attr_for(ino, Some(old_handle)).unwrap().size, old_size);

        let latest_handle = fs.open_handle(ino, libc::O_RDONLY).unwrap();
        let latest = fs.read_handle(latest_handle, 0, u32::MAX).unwrap();
        assert!(latest.len() > old_bytes.len());
        assert_ne!(latest, old_bytes);
        assert_eq!(fs.attr_for(ino, Some(old_handle)).unwrap().size, old_size);
        assert_eq!(
            fs.attr_for(ino, Some(latest_handle)).unwrap().size,
            latest.len() as u64
        );
        assert_eq!(fs.attr_for(ino, None).unwrap().size, latest.len() as u64);

        fs.release_handle(old_handle).unwrap();
        assert_eq!(fs.read_handle(old_handle, 0, 1), Err(libc::EBADF));
        fs.release_handle(latest_handle).unwrap();
    }

    #[test]
    fn reads_eof_invalid_handles_and_write_attempts_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .create_from_arrow(
                "sample",
                &Schema::new(vec![Field::new("value", DataType::Int64, false)]),
                Compression::UNCOMPRESSED,
            )
            .unwrap();
        add_row(&store, 7);
        let mut fs = VpfFs::open(dir.path()).unwrap();
        let ino = fs.inode_for("sample");
        let handle = fs.open_handle(ino, libc::O_RDONLY).unwrap();
        let size = fs.handles.get(&handle).unwrap().size();

        assert!(fs.read_handle(handle, size as i64, 32).unwrap().is_empty());
        assert_eq!(fs.read_handle(handle, -1, 1), Err(libc::EINVAL));
        assert_eq!(fs.read_handle(u64::MAX, 0, 1), Err(libc::EBADF));
        assert_eq!(fs.open_handle(ino, libc::O_WRONLY), Err(libc::EROFS));
        assert_eq!(fs.open_handle(ino, libc::O_RDWR), Err(libc::EROFS));
        assert_eq!(
            fs.open_handle(ino, libc::O_RDONLY | libc::O_TRUNC),
            Err(libc::EROFS)
        );
        fs.release_handle(handle).unwrap();
    }

    #[test]
    fn unsafe_logical_names_are_not_exposed() {
        assert!(!is_safe_component(""));
        assert!(!is_safe_component("../outside"));
        assert!(!is_safe_component("nested/file"));
        assert!(!is_safe_component(r"nested\file"));
        assert!(is_safe_component("ordinary-name"));
    }

    #[test]
    fn file_open_bypasses_the_page_cache() {
        assert_ne!(OPEN_FLAGS & fuser::consts::FOPEN_DIRECT_IO, 0);
        assert_eq!(OPEN_FLAGS & fuser::consts::FOPEN_KEEP_CACHE, 0);
    }
}
