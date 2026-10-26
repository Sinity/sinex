//! Bindings: one instance of a source on one input, with the settings it was
//! read with. A source module declares a [`InputFormat`]: how to find its inputs,
//! how to build its parser and its driver for one binding, and, for live
//! sources, how to capture into a spool. Backfill, the daemon's binding runner,
//! `sinex-session`, replay and the NixOS catalog all work from the formats, so
//! adding a source touches only its own module (and one line in the registry
//! list).
use crate::drivers::append_file::{AppendFile, AppendState};
use crate::drivers::dir_snapshot::{DirSnapshot, DirState};
use crate::drivers::sqlite_rows::{RowState, SqliteRows};
use crate::drivers::{Acquired, DriverError};
use crate::registry::{Source, SourceSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The zone named `name` (an IANA name such as `Europe/Warsaw`), or this
/// system's zone.
pub fn zone(name: Option<&str>) -> Result<jiff::tz::TimeZone, String> {
    match name {
        Some(name) => jiff::tz::TimeZone::get(name).map_err(|e| format!("zone {name}: {e}")),
        None => Ok(jiff::tz::TimeZone::system()),
    }
}

/// What a binding was read with. Stored with the binding, so a replay reads
/// its materials exactly as the capture did (no guessed zones).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    /// The host the input is on.
    pub host: String,
    /// IANA zone for sources whose records carry local times without one,
    /// resolved when the binding is planned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    /// The source's own options (a selection, a capture interval).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub options: Value,
}

impl Settings {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            ..Self::default()
        }
    }

    /// The zone to read local times in: the configured one, or the system's,
    /// resolved to its IANA name so it can be stored.
    pub fn with_zone(mut self) -> Result<Self, String> {
        let zone = zone(self.zone.as_deref())?;
        self.zone = Some(
            zone.iana_name()
                .ok_or("the system time zone has no IANA name; pass --zone")?
                .to_owned(),
        );
        Ok(self)
    }

    /// The resolved zone; UTC only when none was ever recorded.
    pub fn zone(&self) -> Result<jiff::tz::TimeZone, String> {
        match &self.zone {
            Some(name) => zone(Some(name)),
            None => Ok(jiff::tz::TimeZone::UTC),
        }
    }

    /// A string option.
    pub fn option(&self, key: &str) -> Option<&str> {
        self.options.get(key).and_then(Value::as_str)
    }

    /// An unsigned option.
    pub fn number(&self, key: &str) -> Option<u64> {
        self.options.get(key).and_then(Value::as_u64)
    }
}

/// One source on one input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    /// The position key: what the cursor and the stored materials are filed
    /// under.
    pub name: String,
    /// The [`SourceSpec::name`] it reads.
    pub source: String,
    /// The [`SourceSpec::parser`] it reads with. A name is shared by one
    /// source read different ways (a live capture and its lake lane, a
    /// database's tables); name and parser together pick the declaration.
    pub parser: String,
    /// The file, directory, database, spool or configuration it reads.
    pub input: PathBuf,
    pub settings: Settings,
}

impl Binding {
    /// A binding of `spec` on `input`, named `spec:input@host`.
    pub fn of(spec: &SourceSpec, input: &Path, settings: Settings) -> Self {
        Self {
            name: format!("{}:{}@{}", spec.name, input.display(), settings.host),
            source: spec.name.to_owned(),
            parser: spec.parser.to_owned(),
            input: input.to_path_buf(),
            settings,
        }
    }

    /// The same binding with a distinguishing suffix in its name (a table, a
    /// selection), for sources reading several things from one input.
    pub fn named(mut self, part: &str) -> Self {
        let at = self.name.rfind('@').unwrap_or(self.name.len());
        self.name.insert_str(at, &format!("#{part}"));
        self
    }
}

/// A material the binding stored, offered to a reader rebuilding what it
/// remembers after a restart.
pub struct StoredMaterial {
    pub reference: sinex_model::MaterialRef,
    /// Its bytes, read from the content store when asked for.
    pub bytes: std::sync::Arc<dyn crate::drain::RecordRead>,
    /// Whether it belongs to the same revision of the input as the newest
    /// material offered.
    pub same_revision: bool,
}

/// A binding polled alone. A reader that remembers what it read rebuilds that
/// memory before its first poll, from the memory it saved with its cursor and
/// the stored materials after it, so a restart reads only what changed. A
/// material only partly admitted when the process stopped is not resumed: the
/// reader reads on from the committed cursor.
pub trait Reader: Send {
    /// The newest material of the stored history this reader rebuilds its
    /// memory from, given the committed cursor; `None` when it remembers
    /// nothing.
    fn history_from(&self, _cursor: &Value) -> Option<sinex_model::MaterialId> {
        None
    }

    /// One stored material, newest first along its sequence of parts. Returns
    /// whether to go on with the one before it.
    fn restore(&mut self, _cursor: &Value, _material: StoredMaterial) -> Result<bool, DriverError> {
        Ok(false)
    }

    /// The memory this reader saved at the end of an older part, offered
    /// after every part newer than it; what a newer part decided stays.
    fn restore_memory(&mut self, _memory: &[u8]) -> Result<(), DriverError> {
        Ok(())
    }

    /// Called once the history has been offered.
    fn restored(&mut self) -> Result<(), DriverError> {
        Ok(())
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError>;

    /// Spool readers drop the spool files a committed cursor has passed.
    fn remove_passed(&self, _committed: &Value) -> Result<usize, DriverError> {
        Ok(0)
    }
}

/// How a format finds its inputs: the bindings for `input`.
pub type Plan = fn(&Path, &Settings) -> Result<Vec<Binding>, String>;
/// The parser of one binding.
type SourceFor = fn(&Binding) -> Result<Box<dyn Source>, String>;
/// How a binding is read: polled alone, or in a pass over a stored file
/// shared with every compatible binding of that file.
pub enum Reading {
    Polled(Box<dyn Reader>),
    Shared(Box<dyn crate::shared_pass::SharedPass>),
}

impl Reading {
    /// The reader of a binding that is polled alone.
    pub fn into_polled(self) -> Result<Box<dyn Reader>, String> {
        match self {
            Reading::Polled(reader) => Ok(reader),
            Reading::Shared(shared) => Err(format!(
                "{} is read in a pass shared with its other bindings, not polled",
                shared.path().display()
            )),
        }
    }
}

/// The driver of one binding.
type ReaderFor = fn(&Binding) -> Result<Reading, String>;
/// Live capture of one binding into its spool, until stopped or until the
/// source ends; the number of captured lines or segments.
pub type Capture = fn(&Binding, &crate::capture::CaptureStop) -> Result<u64, String>;

/// An input format and the sources that read it: what `--read FORMAT=PATH`,
/// `sources.json` and `sinex-session` name.
pub struct InputFormat {
    pub name: &'static str,
    pub specs: fn() -> Vec<&'static SourceSpec>,
    /// `None` for sources written only through another path (the API's
    /// library requests, the crawl command): they are replayable but not
    /// planned from a path.
    pub plan: Option<Plan>,
    pub source: SourceFor,
    pub reader: Option<ReaderFor>,
    pub capture: Option<Capture>,
}

impl InputFormat {
    /// Whether this format reads `spec_name`.
    pub fn reads(&self, spec_name: &str) -> bool {
        (self.specs)().iter().any(|s| s.name == spec_name)
    }
}

/// Bindings of `spec` for each file `keep` accepts under `input` (or `input`
/// itself when it is a file), in name order.
pub fn per_file(
    spec: &SourceSpec,
    input: &Path,
    settings: &Settings,
    recursive: bool,
    keep: fn(&Path) -> bool,
) -> Vec<Binding> {
    files(input, recursive, keep)
        .into_iter()
        .map(|f| Binding::of(spec, &f, settings.clone()))
        .collect()
}

/// One binding of `spec` on `input` as given.
pub fn single(spec: &SourceSpec, input: &Path, settings: &Settings) -> Vec<Binding> {
    vec![Binding::of(spec, input, settings.clone())]
}

/// Accept every file.
pub fn any(_: &Path) -> bool {
    true
}

/// `path` itself, or a directory's files in name order (recursively when
/// `recursive`), keeping those `keep` accepts.
pub fn files(path: &Path, recursive: bool, keep: fn(&Path) -> bool) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path.to_path_buf()];
    }
    let mut out = Vec::new();
    let mut dirs = vec![path.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let p = entry.path();
            if p.is_dir() {
                if recursive {
                    dirs.push(p);
                }
            } else if keep(&p) {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// A driver with no state of its own: exports, spools, API cursors.
pub struct Stateless<D>(pub D);

/// The polls a memoryless driver offers.
pub trait StatelessDriver: Send {
    fn poll_once(&self, cursor: &Value) -> Result<Acquired, DriverError>;
    fn remove_passed_files(&self, _committed: &Value) -> Result<usize, DriverError> {
        Ok(0)
    }
}

impl<D: StatelessDriver> Reader for Stateless<D> {
    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.0.poll_once(cursor)
    }

    fn remove_passed(&self, committed: &Value) -> Result<usize, DriverError> {
        self.0.remove_passed_files(committed)
    }
}

/// `driver`, polled alone with no memory of its own.
pub fn stateless<D: StatelessDriver + 'static>(driver: D) -> Reading {
    Reading::Polled(Box::new(Stateless(driver)))
}

/// An export archive, read in a pass shared with the other bindings of it.
pub fn shared(driver: crate::drivers::export_archive::ExportArchive) -> Reading {
    Reading::Shared(Box::new(driver))
}

impl StatelessDriver for crate::drivers::live_stream::LiveStream {
    fn poll_once(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.poll(cursor)
    }

    fn remove_passed_files(&self, committed: &Value) -> Result<usize, DriverError> {
        self.remove_passed(committed)
            .map_err(|e| DriverError::Io(e.to_string()))
    }
}

impl StatelessDriver for crate::drivers::media::Media {
    fn poll_once(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.poll(cursor)
    }

    fn remove_passed_files(&self, committed: &Value) -> Result<usize, DriverError> {
        self.remove_passed(committed)
            .map_err(|e| DriverError::Io(e.to_string()))
    }
}

impl<A: crate::drivers::api_cursor::Api + Send + Sync + 'static> StatelessDriver
    for crate::drivers::api_cursor::ApiCursor<A>
{
    fn poll_once(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.poll(cursor)
    }
}

/// An append-only file read with state about the records it stored, so a
/// rewrite is aligned rather than read again. The newest material is held
/// until all of it is admitted, so a failed admission retries the same bytes.
pub struct Appended {
    held: Option<crate::drivers::held::HeldMaterial>,
    driver: AppendFile,
    state: AppendState,
    /// The tail rebuilt during a restart, newest record first.
    tail: Option<crate::drivers::append_file::AppendTail>,
}

pub fn appended(driver: AppendFile) -> Reading {
    Reading::Polled(Box::new(Appended {
        held: None,
        driver,
        state: AppendState::default(),
        tail: None,
    }))
}

impl Reader for Appended {
    fn history_from(&self, cursor: &Value) -> Option<sinex_model::MaterialId> {
        crate::drivers::cursor_material(cursor).map(|(material, _)| material)
    }

    fn restore(&mut self, cursor: &Value, material: StoredMaterial) -> Result<bool, DriverError> {
        if !material.same_revision {
            return Ok(false);
        }
        let reference = &material.reference;
        // Only the admitted part of the newest material counts as read.
        let admitted = match crate::drivers::cursor_material(cursor) {
            Some((id, Some(through))) if id == reference.id => through.min(reference.length),
            _ => reference.length,
        };
        let records = crate::drivers::record_ranges(material.bytes.as_ref(), self.driver.framer)
            .map_err(|e| DriverError::Io(e.to_string()))?;
        let tail = self.tail.get_or_insert_with(Default::default);
        for (start, end) in records
            .into_iter()
            .rev()
            .filter(|&(_, end)| end as u64 <= admitted)
        {
            let mut raw = vec![0; end - start];
            material
                .bytes
                .read_exact_at(start as u64, &mut raw)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            let location = sinex_model::RecordLocation {
                material: reference.id,
                anchor: start as u64,
            };
            if !tail.push_raw(location, &raw)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn restored(&mut self) -> Result<(), DriverError> {
        if let Some(tail) = self.tail.take() {
            self.state = tail.into_state()?;
        }
        Ok(())
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        if let Some(held) = &self.held {
            if !held.admitted(cursor) {
                return held.acquired(cursor);
            }
            self.held = None;
        }
        let acquired = self.driver.poll(cursor, &mut self.state)?;
        if acquired.material.is_none() {
            return Ok(acquired);
        }
        let held = crate::drivers::held::HeldMaterial::hold(acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        self.held = Some(held);
        Ok(acquired)
    }
}

/// A SQLite table, read with state about rows that can still change when
/// its rows are updated in place.
pub struct Rows {
    driver: SqliteRows,
    state: RowState,
}

pub fn rows(driver: SqliteRows) -> Reading {
    Reading::Polled(Box::new(Rows {
        driver,
        state: RowState::default(),
    }))
}

impl Reader for Rows {
    /// An appended table needs only its cursor, so it reads no history.
    fn history_from(&self, cursor: &Value) -> Option<sinex_model::MaterialId> {
        match self.driver.changes {
            crate::drivers::sqlite_rows::Changes::InPlace { .. } => {
                crate::drivers::cursor_material(cursor).map(|(material, _)| material)
            }
            crate::drivers::sqlite_rows::Changes::Appended => None,
        }
    }

    fn restore(&mut self, cursor: &Value, material: StoredMaterial) -> Result<bool, DriverError> {
        let reference = &material.reference;
        let admitted = match crate::drivers::cursor_material(cursor) {
            Some((id, Some(through))) if id == reference.id => through.min(reference.length),
            _ => reference.length,
        };
        let mut bytes = vec![
            0;
            usize::try_from(admitted).map_err(|_| {
                DriverError::Io("a stored row part exceeds the address space".into())
            })?
        ];
        material
            .bytes
            .read_exact_at(0, &mut bytes)
            .map_err(|e| DriverError::Io(e.to_string()))?;
        self.state.remember_rows(&bytes);
        Ok(true)
    }

    fn restore_memory(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        self.state.remember_saved(memory)
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.driver.poll(cursor, &mut self.state)
    }
}

/// A directory walked snapshot by snapshot. The committed cursor confirms
/// what the reader remembers of each walk; an unfinished bundle is held for
/// retry. A restart remembers which files the admitted bundles hold, and the
/// next walk reads them again; admission suppresses the repeats.
struct Walked {
    driver: DirSnapshot,
    state: DirState,
}

pub fn walked(driver: DirSnapshot) -> Reading {
    Reading::Polled(Box::new(Walked {
        driver,
        state: DirState::default(),
    }))
}

impl Reader for Walked {
    fn history_from(&self, cursor: &Value) -> Option<sinex_model::MaterialId> {
        crate::drivers::cursor_material(cursor).map(|(material, _)| material)
    }

    fn restore(&mut self, cursor: &Value, material: StoredMaterial) -> Result<bool, DriverError> {
        let reference = &material.reference;
        let admitted = match crate::drivers::cursor_material(cursor) {
            Some((id, through)) if id == reference.id => through
                .ok_or_else(|| DriverError::Cursor("directory cursor has no processed end".into()))?
                .min(reference.length),
            _ => reference.length,
        };
        self.state.remember_members(&reference.manifest, admitted)?;
        Ok(true)
    }

    fn restore_memory(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        self.state.remember_saved(memory)
    }

    fn restored(&mut self) -> Result<(), DriverError> {
        self.state.finish_remembering();
        Ok(())
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.driver.poll(cursor, &mut self.state)
    }
}

#[cfg(test)]
mod tests;
