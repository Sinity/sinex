//! Only readers own conversions; weak entries share immutable folders without retaining scratch.
use super::*;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, Weak};

pub(super) struct Conversion {
    pub(super) facts: ObservationFacts,
    pub(super) version: String,
    pub(super) files: Vec<PathBuf>,
    pub(super) empty: Option<PathBuf>,
    stage: tempfile::TempDir,
}
impl Conversion {
    pub(super) fn root(&self) -> &Path {
        self.stage.path()
    }
}

#[derive(PartialEq, Eq, Hash)]
struct Key {
    input: PathBuf,
    converter: PathBuf,
    version: String,
    size: Option<u64>,
    mtime: Option<i128>,
    ctime: Option<i128>,
    inode: Option<u64>,
    device: Option<u64>,
}

pub(super) fn get(input: &Path, converter: &Path) -> Result<Arc<Conversion>, String> {
    let facts = sinex_source_core::drivers::file_facts(input).map_err(|error| error.to_string())?;
    let version = Command::new(converter)
        .arg("-V")
        .output()
        .map_err(|error| format!("readpst version: {error}"))?;
    if !version.status.success() {
        return Err(format!("readpst version failed: {}", version.status));
    }
    let mut bytes = version.stdout;
    bytes.extend(version.stderr);
    let version = String::from_utf8(bytes).map_err(|error| format!("readpst version: {error}"))?;
    let version = version.trim().to_owned();
    if version.is_empty() {
        return Err("readpst returned an empty version".into());
    }
    let key = Key {
        input: input.to_owned(),
        converter: converter.to_owned(),
        version: version.clone(),
        size: facts.size,
        mtime: facts.mtime_ns,
        ctime: facts.ctime_ns,
        inode: facts.inode,
        device: facts.device,
    };
    static CONVERSIONS: OnceLock<Mutex<HashMap<Key, Weak<Conversion>>>> = OnceLock::new();
    let mut conversions = CONVERSIONS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|error| format!("Outlook conversion lock: {error}"))?;
    conversions.retain(|_, conversion| conversion.strong_count() > 0);
    if let Some(conversion) = conversions.get(&key).and_then(Weak::upgrade) {
        return Ok(conversion);
    }
    let stage = scratch()?;
    let files = convert(input, stage.path(), converter)?;
    let empty = if files.is_empty() {
        let path = stage.path().join("empty-conversion.mbox");
        std::fs::File::create(&path).map_err(|error| error.to_string())?;
        Some(path)
    } else {
        None
    };
    let after = sinex_source_core::drivers::file_facts(input).map_err(|error| error.to_string())?;
    if !same_revision(&facts, &after) {
        return Err("PST changed during conversion; retry an unchanged revision".into());
    }
    let conversion = Arc::new(Conversion {
        facts,
        version,
        files,
        empty,
        stage,
    });
    conversions.insert(key, Arc::downgrade(&conversion));
    Ok(conversion)
}
