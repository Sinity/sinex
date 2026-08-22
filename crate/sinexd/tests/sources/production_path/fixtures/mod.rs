//! Per-adapter fixture helpers.
//!
//! Each submodule provides a `build(data)` function that prepares a fixture
//! appropriate for its adapter kind. Fixtures return a `FixtureHandle` that
//! carries the adapter-specific binding parameters and cleans up on drop.

pub mod append_only_file;
pub mod clipboard;
pub mod dbus;
pub mod file_drop;
pub mod journal;
pub mod sqlite_row;
pub mod static_file;
pub mod unix_socket;

use futures::StreamExt;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeAdapter, MaterialAnchor};
use sinexd::runtime::parser::{
    AppendOnlyFileAdapter, AppendOnlyFileConfig, ClipboardPollingAdapter, ClipboardPollingConfig,
    DbusBus, DbusStreamAdapter, DbusStreamConfig, FileDropAdapter, FileDropConfig,
    MockClipboardBackend, MockDbusBackend, SqliteRowAdapter, SqliteRowConfig, StaticFileAdapter,
    StaticFileConfig, UnixSocketStreamAdapter, UnixSocketStreamConfig,
};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::{fmt, io::Write};

use tempfile::NamedTempFile;

/// The binding parameters produced by a fixture — passed to the obligation
/// layer so it can configure the source invocation.
#[derive(Debug, Clone)]
pub enum FixtureBinding {
    /// A filesystem path (file or watched directory).
    FilePath(PathBuf),
    /// Pre-built source records (journal, dbus, clipboard).
    InMemoryRecords(Vec<Vec<u8>>),
    /// Unix socket path.
    UnixSocketPath(PathBuf),
}

/// An active fixture that holds any tempdir/tempfile handles alive for the
/// duration of the test. Drop to clean up.
pub struct FixtureHandle {
    pub binding: FixtureBinding,
    /// Opaque cleanup resources kept alive by ownership.
    #[allow(dead_code)]
    _resources: Vec<Box<dyn std::any::Any + Send>>,
}

impl FixtureHandle {
    /// Construct a fixture handle from a binding and a set of owned resources
    /// whose Drop impls perform cleanup.
    #[must_use]
    pub fn new(binding: FixtureBinding, resources: Vec<Box<dyn std::any::Any + Send>>) -> Self {
        Self {
            binding,
            _resources: resources,
        }
    }

    /// Convenience: construct with a single owned resource.
    pub fn with_resource(
        binding: FixtureBinding,
        resource: impl std::any::Any + Send + 'static,
    ) -> Self {
        Self::new(binding, vec![Box::new(resource)])
    }

    /// Convenience: no external resources needed (in-memory only).
    #[must_use]
    pub fn in_memory(binding: FixtureBinding) -> Self {
        Self::new(binding, vec![])
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FileFixtureKind {
    AppendOnly,
    Static,
}

impl fmt::Display for FileFixtureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppendOnly => f.write_str("append-only"),
            Self::Static => f.write_str("static"),
        }
    }
}

pub fn build_file_fixture(kind: FileFixtureKind, data: &[u8]) -> Result<FixtureHandle, String> {
    let mut file =
        NamedTempFile::new().map_err(|e| format!("failed to create {kind} fixture file: {e}"))?;
    file.write_all(data)
        .map_err(|e| format!("failed to write {kind} fixture data: {e}"))?;
    file.flush()
        .map_err(|e| format!("failed to flush {kind} fixture data: {e}"))?;
    let path = file.path().to_owned();
    Ok(FixtureHandle::with_resource(
        FixtureBinding::FilePath(path),
        file,
    ))
}

/// Build the fixture selected by [`AdapterKind`](crate::AdapterKind) and
/// exercise its production adapter where the adapter has a synthetic seam.
///
/// Parser obligations remain responsible for source-specific event semantics.
/// This gate owns the complementary proof that the selected adapter accepts
/// the fixture and yields an anchored record, so `AdapterKind` cannot become
/// informational metadata again.
pub async fn exercise_adapter_binding(
    adapter_kind: crate::AdapterKind,
    data: &[u8],
) -> Result<(), String> {
    let material_id = Id::<SourceMaterial>::new();
    match adapter_kind {
        crate::AdapterKind::AppendOnlyFile => {
            let fixture = append_only_file::build(data)?;
            let path = file_path(&fixture)?;
            let mut stream = AppendOnlyFileAdapter::default()
                .open(
                    material_id,
                    &AppendOnlyFileConfig {
                        path: path.display().to_string(),
                        skip_empty: false,
                    },
                    None,
                )
                .await
                .map_err(|error| format!("append-only adapter open failed: {error}"))?;
            expect_record(&mut stream, "append-only").await?;
        }
        crate::AdapterKind::StaticFile => {
            let fixture = static_file::build(data)?;
            let path = file_path(&fixture)?;
            let mut stream = StaticFileAdapter
                .open(
                    material_id,
                    &StaticFileConfig {
                        path: path.display().to_string(),
                    },
                    None,
                )
                .await
                .map_err(|error| format!("static-file adapter open failed: {error}"))?;
            let record = expect_record(&mut stream, "static-file").await?;
            if record.bytes != data {
                return Err("static-file adapter changed fixture bytes".to_string());
            }
        }
        crate::AdapterKind::SqliteRow => {
            let payload = String::from_utf8_lossy(data).into_owned();
            let row = [("payload", payload.as_str())];
            let fixture = sqlite_row::build(
                "CREATE TABLE fixture_records (payload TEXT NOT NULL)",
                &[&row],
            )?;
            let path = file_path(&fixture)?;
            let mut stream = SqliteRowAdapter::default()
                .open(
                    material_id,
                    &SqliteRowConfig {
                        path: path.display().to_string(),
                        query: "fixture_records".to_string(),
                        table: "fixture_records".to_string(),
                        ..Default::default()
                    },
                    None,
                )
                .await
                .map_err(|error| format!("SQLite-row adapter open failed: {error}"))?;
            let record = expect_record(&mut stream, "SQLite-row").await?;
            if !matches!(record.anchor, MaterialAnchor::SqliteRow { .. }) {
                return Err(format!(
                    "SQLite-row adapter emitted wrong anchor: {:?}",
                    record.anchor
                ));
            }
        }
        crate::AdapterKind::Clipboard => {
            let snapshots = clipboard::snapshots_from_bytes(data)?;
            let expected = snapshots
                .iter()
                .flatten()
                .next()
                .cloned()
                .ok_or_else(|| "clipboard fixture contains no text snapshot".to_string())?;
            let adapter =
                ClipboardPollingAdapter::from_backend(MockClipboardBackend::new(snapshots));
            let mut stream = adapter
                .open(
                    material_id,
                    &ClipboardPollingConfig {
                        poll_interval_ms: 1,
                        ..Default::default()
                    },
                    None,
                )
                .await
                .map_err(|error| format!("clipboard adapter open failed: {error}"))?;
            let record = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                expect_record(&mut stream, "clipboard"),
            )
            .await
            .map_err(|_| "clipboard adapter did not emit a synthetic snapshot".to_string())??;
            if record.bytes != expected.as_bytes() {
                return Err("clipboard adapter did not preserve the synthetic snapshot".to_string());
            }
        }
        crate::AdapterKind::Dbus => {
            let messages = dbus::messages_from_bytes(data)?;
            let first = messages
                .first()
                .ok_or_else(|| "D-Bus fixture contains no messages".to_string())?;
            let interface = first.interface.clone();
            let expected_body =
                serde_json::to_vec(&first.body_json).map_err(|error| error.to_string())?;
            let adapter = DbusStreamAdapter::with_backend(MockDbusBackend::new(messages));
            let mut stream = adapter
                .open(
                    material_id,
                    &DbusStreamConfig {
                        bus: DbusBus::Session,
                        match_rules: vec![format!("type='signal',interface='{interface}'")],
                    },
                    None,
                )
                .await
                .map_err(|error| format!("D-Bus adapter open failed: {error}"))?;
            let record = expect_record(&mut stream, "D-Bus").await?;
            if record.bytes != expected_body {
                return Err("D-Bus adapter did not preserve the synthetic message body".to_string());
            }
        }
        crate::AdapterKind::UnixSocket => {
            let fixture = unix_socket::build(data).await?;
            let path = unix_socket_path(&fixture)?;
            let mut stream = UnixSocketStreamAdapter
                .open(
                    material_id,
                    &UnixSocketStreamConfig {
                        socket_path: camino::Utf8PathBuf::from_path_buf(path).map_err(|path| {
                            format!("unix socket fixture path is not UTF-8: {path:?}")
                        })?,
                        mode: Default::default(),
                        reconnect_on_eof: false,
                    },
                    None,
                )
                .await
                .map_err(|error| format!("unix-socket adapter open failed: {error}"))?;
            expect_record(&mut stream, "unix-socket").await?;
        }
        crate::AdapterKind::FileDrop => {
            let fixture = file_drop::build(data)?;
            let watched_dir = file_path(&fixture)?;
            let mut stream = FileDropAdapter
                .open(
                    material_id,
                    &FileDropConfig {
                        watch_paths: vec![
                            camino::Utf8PathBuf::from_path_buf(watched_dir.clone()).map_err(
                                |path| format!("file-drop fixture path is not UTF-8: {path:?}"),
                            )?,
                        ],
                        recursive: false,
                        max_depth: None,
                        ignored_directory_names: Vec::new(),
                        ignored_file_suffixes: Vec::new(),
                        max_watches: NonZeroUsize::MIN,
                        events: Vec::new(),
                    },
                    None,
                )
                .await
                .map_err(|error| format!("file-drop adapter open failed: {error}"))?;
            std::fs::write(watched_dir.join("after-watch.dat"), data)
                .map_err(|error| format!("file-drop fixture write failed: {error}"))?;
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                expect_record(&mut stream, "file-drop"),
            )
            .await
            .map_err(|_| "file-drop adapter did not observe a synthetic write".to_string())??;
        }
        crate::AdapterKind::Journal => {
            let fixture = journal::build(data)?;
            let FixtureBinding::InMemoryRecords(records) = &fixture.binding else {
                return Err("journal fixture did not produce in-memory records".to_string());
            };
            if records.is_empty() {
                return Err("journal fixture produced no synthetic records".to_string());
            }
        }
    }
    Ok(())
}

fn file_path(fixture: &FixtureHandle) -> Result<PathBuf, String> {
    match &fixture.binding {
        FixtureBinding::FilePath(path) => Ok(path.clone()),
        other => Err(format!("fixture returned non-file binding: {other:?}")),
    }
}

fn unix_socket_path(fixture: &FixtureHandle) -> Result<PathBuf, String> {
    match &fixture.binding {
        FixtureBinding::UnixSocketPath(path) => Ok(path.clone()),
        other => Err(format!("fixture returned non-socket binding: {other:?}")),
    }
}

async fn expect_record(
    stream: &mut futures::stream::BoxStream<
        'static,
        sinex_primitives::parser::ParserResult<sinex_primitives::parser::SourceRecord>,
    >,
    adapter: &str,
) -> Result<sinex_primitives::parser::SourceRecord, String> {
    match stream.next().await {
        Some(Ok(record)) => Ok(record),
        Some(Err(error)) => Err(format!("{adapter} adapter yielded an error: {error}")),
        None => Err(format!(
            "{adapter} adapter yielded no records for its fixture"
        )),
    }
}
