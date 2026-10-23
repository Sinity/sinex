//! Bindings (architecture §5.4): one instance of a source on one input, with
//! the settings it was read with. A source module declares a [`InputKind`]: how to
//! find its inputs, how to build its parser and its driver for one binding,
//! and, for live sources, how to capture into a spool. Backfill, the daemon's
//! binding runner, `sinex-session`, replay and the NixOS catalog all work from
//! the kinds, so adding a source touches only its own module (and one line in
//! the registry list).
use crate::drivers::append_file::{AppendFile, AppendState};
use crate::drivers::dir_snapshot::{DirSnapshot, DirState};
use crate::drivers::sqlite_rows::{RowState, SqliteRows};
use crate::drivers::{Acquired, DriverError};
use crate::registry::{Framer, Source, SourceSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::MaterialId;
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

/// How a binding is read. Drivers that remember what they stored have that
/// state rebuilt from the stored materials before the first poll, so a
/// restart reads only what changed.
pub trait Reader: Send {
    fn configure(&mut self, settings: &Settings) -> Result<(), String> {
        if settings.options.get("sqlite_images").is_some() {
            return Err("SQLite image configuration requires a SQLite rows reader".into());
        }
        Ok(())
    }
    fn image_policy(&self) -> Option<&crate::drivers::sqlite_image::Policy> {
        None
    }
    fn restore_image(&mut self, _previous: crate::drivers::sqlite_image::Previous) {}
    fn cancel_pending(&mut self) {}

    /// The export declaration, when this reader can share a physical archive traversal.
    fn export_archive(&self) -> Option<&crate::drivers::export_archive::ExportArchive> {
        None
    }

    fn restore_rows(
        &mut self,
        _reference: sinex_model::MaterialRef,
        _stored: crate::drivers::RetainedRecords,
        _cursor: &Value,
    ) -> Result<(), DriverError> {
        Ok(())
    }

    /// Directory membership uses exact accounted anchors; only the current
    /// incomplete material supplies a retained body for resumption.
    fn restore_directory(
        &mut self,
        _reference: sinex_model::MaterialRef,
        _admitted: &[usize],
        _pending: Option<crate::drivers::RetainedRecords>,
        _cursor: Option<&Value>,
    ) -> Result<(), DriverError> {
        Ok(())
    }

    fn remember(&mut self, _stored: &[(MaterialId, Vec<u8>)]) {}

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError>;

    /// Spool readers drop the spool files a committed cursor has passed.
    fn remove_passed(&self, _committed: &Value) -> Result<usize, DriverError> {
        Ok(0)
    }
}

/// How a kind finds its inputs: the bindings for `input`.
pub type Plan = fn(&Path, &Settings) -> Result<Vec<Binding>, String>;
/// The parser of one binding.
pub type SourceFor = fn(&Binding) -> Result<Box<dyn Source>, String>;
/// The driver of one binding.
pub type ReaderFor = fn(&Binding) -> Result<Box<dyn Reader>, String>;
/// Live capture of one binding into its spool, until stopped or until the
/// source ends; the number of captured lines or segments.
pub type Capture = fn(&Binding, &crate::capture::Control) -> Result<u64, String>;

/// A kind of input and the sources that read it: what `--source KIND=PATH`,
/// `sources.json` and `sinex-session` name.
pub struct InputKind {
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

impl InputKind {
    /// Whether this kind reads `spec_name`.
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
    fn export_archive(&self) -> Option<&crate::drivers::export_archive::ExportArchive> {
        None
    }

    fn poll_once(&self, cursor: &Value) -> Result<Acquired, DriverError>;
    fn remove_passed_files(&self, _committed: &Value) -> Result<usize, DriverError> {
        Ok(0)
    }
}

impl<D: StatelessDriver> Reader for Stateless<D> {
    fn export_archive(&self) -> Option<&crate::drivers::export_archive::ExportArchive> {
        self.0.export_archive()
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.0.poll_once(cursor)
    }

    fn remove_passed(&self, committed: &Value) -> Result<usize, DriverError> {
        self.0.remove_passed_files(committed)
    }
}

/// A box of `driver` as a [`Reader`].
pub fn stateless<D: StatelessDriver + 'static>(driver: D) -> Box<dyn Reader> {
    Box::new(Stateless(driver))
}

impl StatelessDriver for crate::drivers::export_archive::ExportArchive {
    fn export_archive(&self) -> Option<&crate::drivers::export_archive::ExportArchive> {
        Some(self)
    }

    fn poll_once(&self, _cursor: &Value) -> Result<Acquired, DriverError> {
        Err(DriverError::InvalidMaterial(
            "export archives require retained traversal, not a standalone poll".into(),
        ))
    }
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
/// rewrite is aligned rather than read again.
pub struct Appended {
    driver: AppendFile,
    framer: Framer,
    state: AppendState,
}

pub fn appended(driver: AppendFile, framer: Framer) -> Box<dyn Reader> {
    Box::new(Appended {
        driver,
        framer,
        state: AppendState::default(),
    })
}

impl Reader for Appended {
    fn remember(&mut self, stored: &[(MaterialId, Vec<u8>)]) {
        self.state = AppendState::rebuild(
            stored.iter().map(|(id, b)| (*id, b.as_slice())),
            self.framer,
        );
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.driver.poll(cursor, &mut self.state)
    }
}

/// A SQLite table read with state about rows that can still change.
pub struct Rows {
    driver: SqliteRows,
    state: RowState,
}

pub fn rows(driver: SqliteRows) -> Box<dyn Reader> {
    Box::new(Rows {
        driver,
        state: RowState::default(),
    })
}

impl Reader for Rows {
    fn restore_rows(
        &mut self,
        reference: sinex_model::MaterialRef,
        stored: crate::drivers::RetainedRecords,
        cursor: &Value,
    ) -> Result<(), DriverError> {
        self.state.restore_pending(reference, stored, cursor)
    }

    fn configure(&mut self, settings: &Settings) -> Result<(), String> {
        if let Some(value) = settings.options.get("sqlite_images") {
            let policy: crate::drivers::sqlite_image::Policy =
                serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
            policy.validate()?;
            self.state.image_policy = Some(policy);
        }
        Ok(())
    }
    fn image_policy(&self) -> Option<&crate::drivers::sqlite_image::Policy> {
        self.state.image_policy.as_ref()
    }
    fn restore_image(&mut self, previous: crate::drivers::sqlite_image::Previous) {
        self.state.previous_image = Some(previous);
    }
    fn cancel_pending(&mut self) {
        self.state.cancel_scan();
    }

    fn remember(&mut self, stored: &[(MaterialId, Vec<u8>)]) {
        let policy = self.state.image_policy.take();
        self.state = RowState::rebuild(stored.iter().map(|(_, b)| b.as_slice()));
        self.state.image_policy = policy;
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.driver.poll(cursor, &mut self.state)
    }
}

/// A directory walked snapshot by snapshot. The supplied committed cursor
/// confirms file-cache updates; an unfinished bundle is retained for retry.
/// Restart restores accounted member paths and completes the retained pending
/// bundle first. Present files are then reread; admission suppresses repeats.
pub struct Walked {
    driver: DirSnapshot,
    state: DirState,
}

pub fn walked(driver: DirSnapshot) -> Box<dyn Reader> {
    Box::new(Walked {
        driver,
        state: DirState::default(),
    })
}

impl Reader for Walked {
    fn restore_directory(
        &mut self,
        reference: sinex_model::MaterialRef,
        admitted: &[usize],
        pending: Option<crate::drivers::RetainedRecords>,
        cursor: Option<&Value>,
    ) -> Result<(), DriverError> {
        if reference.previous_part.is_none() {
            self.state = DirState::default();
        }
        self.state.remember_members(&reference.manifest, admitted)?;
        match (pending, cursor) {
            (Some(pending), Some(cursor)) => self.state.restore_pending(reference, pending, cursor),
            (None, None) => Ok(()),
            _ => Err(DriverError::Cursor(
                "directory pending body and cursor must agree".into(),
            )),
        }
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.driver.poll(cursor, &mut self.state)
    }
}

#[cfg(test)]
mod tests;
