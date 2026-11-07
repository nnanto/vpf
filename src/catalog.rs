//! A directory of `.vpf` files, one table per file, addressed by file stem.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::store::Store;

pub const EXTENSION: &str = "vpf";

pub struct Catalog {
    dir: PathBuf,
    open: Mutex<HashMap<String, Arc<Store>>>,
}

/// Names map directly to file names, so only plain, non-hidden names are accepted.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 200
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

impl Catalog {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            open: Mutex::new(HashMap::new()),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn path_of(&self, name: &str) -> Result<PathBuf> {
        if !valid_name(name) {
            return Err(Error::Invalid(format!("invalid table name '{name}'")));
        }
        Ok(self.dir.join(format!("{name}.{EXTENSION}")))
    }

    /// Stems of the `.vpf` files in the directory, sorted.
    pub fn list(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some(EXTENSION) || !path.is_file() {
                continue;
            }
            if let Some(stem) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .filter(|s| valid_name(s))
            {
                out.push(stem.to_string());
            }
        }
        out.sort();
        Ok(out)
    }

    /// Opens an existing file and returns it with the name of the table stored inside.
    pub fn table(&self, name: &str) -> Result<(Arc<Store>, String)> {
        let path = self.path_of(name)?;
        let store = {
            let mut open = self.open.lock().unwrap();
            match open.get(name) {
                Some(s) if path.is_file() => s.clone(),
                _ => {
                    open.remove(name);
                    if !path.is_file() {
                        return Err(Error::NotFound(format!("table '{name}'")));
                    }
                    let s = Arc::new(Store::open(&path)?);
                    open.insert(name.to_string(), s.clone());
                    s
                }
            }
        };
        let table = store
            .table()?
            .ok_or_else(|| Error::NotFound(format!("table '{name}'")))?;
        Ok((store, table))
    }
}
