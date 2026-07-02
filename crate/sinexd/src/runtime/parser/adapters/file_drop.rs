//! Adapter for hot-folder (`FileDrop`) watching.
//!
//! Uses `notify` to observe a set of paths for filesystem events. Each event
//! yields one [`SourceRecord`] with a [`MaterialAnchor::DirectoryEntry`] anchor.
//!
//! This adapter produces a live stream — there is no cursor (the anchor is the
//! stable identifier). Callers drive the stream until the backing watcher is
//! dropped.

use async_trait::async_trait;
use camino::Utf8PathBuf;
#[cfg(feature = "messaging")]
use futures::StreamExt;
use futures::stream::BoxStream;
use notify::event::ModifyKind;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
#[cfg(feature = "messaging")]
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{InputShapeKind, MaterialAnchor, SourceRecord};

#[cfg(feature = "messaging")]
use crate::runtime::parser::InputShapeAdapterExt;
use crate::runtime::parser::{InputShapeAdapter, ParserError, ParserResult};
#[cfg(feature = "messaging")]
use crate::runtime::{
    acquisition_manager::AcquisitionManager, source_material::stage_material_from_file_bounded,
};

#[cfg(target_os = "linux")]
const INOTIFY_MAX_USER_WATCHES_PATH: &str = "/proc/sys/fs/inotify/max_user_watches";
pub const DEFAULT_FILE_DROP_MAX_WATCHES: usize = 524_288;

// =============================================================================
// FileDropEventKind
// =============================================================================

/// Which filesystem events the adapter should yield.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileDropEventKind {
    Created,
    Modified,
    Deleted,
    Moved,
}

impl FileDropEventKind {
    fn metadata_label(self) -> &'static str {
        match self {
            Self::Created => "Created",
            Self::Modified => "Modified",
            Self::Deleted => "Deleted",
            Self::Moved => "Moved",
        }
    }

    fn from_metadata_label(label: &str) -> Option<Self> {
        match label {
            "Created" => Some(Self::Created),
            "Modified" => Some(Self::Modified),
            "Deleted" => Some(Self::Deleted),
            "Moved" => Some(Self::Moved),
            _ => None,
        }
    }
}

/// Role of a path inside a paired file move notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileDropMoveRole {
    From,
    To,
}

impl FileDropMoveRole {
    fn metadata_label(self) -> &'static str {
        match self {
            Self::From => "from",
            Self::To => "to",
        }
    }

    fn from_metadata_label(label: &str) -> Option<Self> {
        match label {
            "from" => Some(Self::From),
            "to" => Some(Self::To),
            _ => None,
        }
    }
}

/// Metadata emitted with every [`FileDropAdapter`] record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileDropRecordMetadata {
    pub event_kind: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub move_from_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub move_to_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub move_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_materialized: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_skipped_reason: Option<String>,
}

impl FileDropRecordMetadata {
    pub fn from_value(value: &serde_json::Value) -> ParserResult<Self> {
        serde_json::from_value(value.clone()).map_err(|error| {
            ParserError::Parse(format!("invalid file-drop record metadata: {error}"))
        })
    }

    #[must_use]
    pub fn event_kind(&self) -> Option<FileDropEventKind> {
        FileDropEventKind::from_metadata_label(&self.event_kind)
    }

    #[must_use]
    pub fn move_role(&self) -> Option<FileDropMoveRole> {
        self.move_role
            .as_deref()
            .and_then(FileDropMoveRole::from_metadata_label)
    }

    fn new(kind: FileDropEventKind, path: &Utf8PathBuf) -> Self {
        Self {
            event_kind: kind.metadata_label().to_string(),
            path: path.as_str().to_string(),
            move_from_path: None,
            move_to_path: None,
            move_role: None,
            content_materialized: None,
            content_size_bytes: None,
            content_skipped_reason: None,
        }
    }

    fn with_move_pair(
        mut self,
        from_path: &Utf8PathBuf,
        to_path: &Utf8PathBuf,
        role: FileDropMoveRole,
    ) -> Self {
        self.move_from_path = Some(from_path.as_str().to_string());
        self.move_to_path = Some(to_path.as_str().to_string());
        self.move_role = Some(role.metadata_label().to_string());
        self
    }

    #[cfg(any(feature = "messaging", test))]
    fn with_materialized_content(mut self, content_size_bytes: u64) -> Self {
        self.content_materialized = Some(true);
        self.content_size_bytes = Some(content_size_bytes);
        self.content_skipped_reason = None;
        self
    }

    #[cfg(any(feature = "messaging", test))]
    fn with_skipped_content(mut self, content_size_bytes: u64, reason: &str) -> Self {
        self.content_materialized = Some(false);
        self.content_size_bytes = Some(content_size_bytes);
        self.content_skipped_reason = Some(reason.to_string());
        self
    }

    fn into_json(self) -> serde_json::Value {
        serde_json::json!(self)
    }
}

// =============================================================================
// FileDropAdapter
// =============================================================================

/// Adapter for a hot folder — watches paths and emits one record per event.
///
/// Suitable for `file.created` / `file.modified` / `file.deleted` / `file.moved`
/// event streams (e.g., the fs-source and system.udev source contracts).
///
/// Cursor is `()` — this is a live stream with no replay capability.
#[derive(Debug, Clone, Default)]
pub struct FileDropAdapter;

/// File-drop adapter variant that can stage regular file contents as source material.
#[derive(Debug, Clone, Default)]
pub struct FileContentDropAdapter;

/// Configuration for [`FileDropAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileDropConfig {
    /// Paths to watch (files or directories).
    #[schemars(with = "Vec<String>")]
    pub watch_paths: Vec<Utf8PathBuf>,

    /// Whether to watch directories recursively.
    #[serde(default)]
    pub recursive: bool,

    /// Maximum relative path depth to emit under a watched directory.
    ///
    /// `0` means direct children of a watched directory only, `1` includes one
    /// nested directory level, and `None` leaves depth unbounded.
    #[serde(default)]
    pub max_depth: Option<usize>,

    /// Directory names to suppress before records leave the adapter.
    #[serde(default)]
    pub ignored_directory_names: Vec<String>,

    /// File-name suffix patterns to suppress before records leave the adapter.
    ///
    /// Matched as a case-sensitive suffix on the file's basename. Use this to
    /// drop transient/volatile files (`SQLite` `-wal` / `-shm`, pytest's
    /// `.testmondata-wal`, editor swap files) that would otherwise stage
    /// hundreds of materials per minute and bloat the CAS without producing
    /// meaningful user signal. Surfaced by issue #1543 — the live
    /// `sinex_prod` deployment accumulated 449 GB of duckdb.wal and
    /// testmondata-wal captures before the fs source was stopped.
    #[serde(default)]
    pub ignored_file_suffixes: Vec<String>,

    /// Maximum native watches the adapter should plan for before applying the
    /// host kernel limit.
    #[serde(default = "default_file_drop_max_watches")]
    pub max_watches: NonZeroUsize,

    /// Which event kinds to report. If empty, all kinds are reported.
    #[serde(default)]
    pub events: Vec<FileDropEventKind>,
}

/// Configuration for [`FileContentDropAdapter`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileContentDropConfig {
    /// Base file-drop watcher configuration.
    #[serde(flatten)]
    pub file_drop: FileDropConfig,

    /// Maximum regular-file payload to stage for created/modified records.
    #[serde(default = "default_file_content_max_capture_bytes")]
    pub max_capture_bytes: u64,
}

fn default_file_drop_max_watches() -> NonZeroUsize {
    match NonZeroUsize::new(DEFAULT_FILE_DROP_MAX_WATCHES) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    }
}

fn default_file_content_max_capture_bytes() -> u64 {
    10 * 1024 * 1024
}

/// Directory survey used to choose a native filesystem watch strategy.
///
/// `accessible_watch_count` is the number of directory watches a recursive
/// native watcher may need. `filtered_watch_count` is the smaller target count
/// after applying adapter policy such as ignored directory names and depth
/// limits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileDropWatchSurvey {
    pub accessible_watch_count: usize,
    pub filtered_watch_count: usize,
    /// Whether a max-depth policy was applied while surveying.
    ///
    /// A native recursive watch would continue observing future descendants
    /// below the configured depth, so any depth-limited survey requires
    /// filtered native watch targets even when the current tree is shallow.
    #[serde(default)]
    pub depth_limited: bool,
    #[serde(default)]
    pub unreadable_directories: usize,
    #[serde(default)]
    pub ignored_directories: usize,
    /// Concrete non-recursive targets for filtered native watch mode.
    #[serde(default)]
    pub filtered_targets: Vec<PathBuf>,
}

/// Effective watch budget after host/kernel limits are accounted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileDropWatchBudget {
    pub configured_max_watches: NonZeroUsize,
    pub effective_max_watches: NonZeroUsize,
    #[serde(default)]
    pub kernel_max_watches: Option<NonZeroUsize>,
}

impl FileDropWatchBudget {
    /// Builds a budget from a configured limit and an optional observed kernel
    /// limit.
    #[must_use]
    pub fn from_limits(
        configured_max_watches: NonZeroUsize,
        kernel_max_watches: Option<NonZeroUsize>,
    ) -> Self {
        let effective_max_watches = kernel_max_watches.map_or(configured_max_watches, |limit| {
            configured_max_watches.min(limit)
        });

        Self {
            configured_max_watches,
            effective_max_watches,
            kernel_max_watches,
        }
    }

    /// Detects the host inotify limit from `/proc/sys/fs/inotify/max_user_watches`.
    ///
    /// Non-Linux hosts, unreadable procfs files, and malformed values simply
    /// leave `kernel_max_watches` empty; the configured limit remains effective.
    #[must_use]
    pub fn detect(configured_max_watches: NonZeroUsize) -> Self {
        Self::from_limits(configured_max_watches, read_kernel_inotify_watch_limit())
    }
}

/// Native watcher mode selected for a surveyed file-drop tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileDropWatchMode {
    NativeRecursive,
    NativeFiltered,
}

impl FileDropWatchMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NativeRecursive => "native-recursive",
            Self::NativeFiltered => "native-filtered",
        }
    }
}

/// Decision result for native file-drop watching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileDropWatchPlan {
    pub mode: FileDropWatchMode,
    pub survey: FileDropWatchSurvey,
    pub budget: FileDropWatchBudget,
    pub effective_watch_count: usize,
}

/// Selects recursive native watching or filtered native watching for a surveyed
/// directory tree.
pub fn choose_file_drop_watch_plan(
    survey: FileDropWatchSurvey,
    budget: FileDropWatchBudget,
) -> ParserResult<FileDropWatchPlan> {
    let needs_filtered_plan = survey.accessible_watch_count > budget.effective_max_watches.get()
        || survey.depth_limited
        || survey.unreadable_directories > 0
        || survey.ignored_directories > 0;

    if !needs_filtered_plan {
        return Ok(FileDropWatchPlan {
            mode: FileDropWatchMode::NativeRecursive,
            effective_watch_count: survey.accessible_watch_count,
            survey,
            budget,
        });
    }

    if survey.filtered_watch_count <= budget.effective_max_watches.get() {
        return Ok(FileDropWatchPlan {
            mode: FileDropWatchMode::NativeFiltered,
            effective_watch_count: survey.filtered_watch_count,
            survey,
            budget,
        });
    }

    let mut message = format!(
        "file-drop watch budget exceeded after filtered planning: configured_max_watches={}, effective_max_watches={}, accessible_watch_count={}, filtered_watch_count={}, depth_limited={}, unreadable_directories={}, ignored_directories={}",
        budget.configured_max_watches,
        budget.effective_max_watches,
        survey.accessible_watch_count,
        survey.filtered_watch_count,
        survey.depth_limited,
        survey.unreadable_directories,
        survey.ignored_directories
    );
    if let Some(kernel_max_watches) = budget.kernel_max_watches {
        message.push_str(&format!(", kernel_max_user_watches={kernel_max_watches}"));
    }
    Err(ParserError::Adapter(message))
}

fn file_drop_permission_denied(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::PermissionDenied
}

fn file_drop_path_component_is_ignored(
    path: &Path,
    ignored_directory_names: &HashSet<String>,
) -> bool {
    path.file_name()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|name| ignored_directory_names.contains(name))
}

fn file_drop_metadata_is_directory(
    metadata: &std::fs::Metadata,
    follow_symlinks: bool,
    path: &Path,
) -> ParserResult<bool> {
    if metadata.is_dir() {
        return Ok(true);
    }

    if follow_symlinks && metadata.file_type().is_symlink() {
        return std::fs::metadata(path)
            .map(|resolved| resolved.is_dir())
            .map_err(|error| {
                ParserError::Adapter(format!(
                    "failed to follow file-drop watch symlink {}: {error}",
                    path.display()
                ))
            });
    }

    Ok(false)
}

/// Survey a native file-drop watch target so callers can choose a bounded
/// recursive or filtered watch plan.
pub fn survey_file_drop_watch_tree(
    path: &Path,
    start_depth: usize,
    max_depth: Option<usize>,
    follow_symlinks: bool,
    ignored_directory_names: &HashSet<String>,
) -> ParserResult<FileDropWatchSurvey> {
    fn inspect_path(
        path: &Path,
        depth: usize,
        max_depth: Option<usize>,
        follow_symlinks: bool,
        ignored_directory_names: &HashSet<String>,
        visited: &mut HashSet<(u64, u64)>,
    ) -> ParserResult<FileDropWatchSurvey> {
        use std::os::unix::fs::MetadataExt;

        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if depth > 0 && file_drop_permission_denied(&error) => {
                warn!(
                    path = %path.display(),
                    "Skipping unreadable directory while surveying file-drop watch strategy"
                );
                return Ok(FileDropWatchSurvey {
                    accessible_watch_count: 1,
                    unreadable_directories: 1,
                    ..FileDropWatchSurvey::default()
                });
            }
            Err(error) => {
                return Err(ParserError::Adapter(format!(
                    "failed to inspect file-drop watch target {}: {error}",
                    path.display()
                )));
            }
        };

        if !file_drop_metadata_is_directory(&metadata, follow_symlinks, path)? {
            return Ok(FileDropWatchSurvey {
                accessible_watch_count: 1,
                filtered_watch_count: 1,
                filtered_targets: vec![path.to_path_buf()],
                ..FileDropWatchSurvey::default()
            });
        }

        let resolved_meta = if metadata.file_type().is_symlink() {
            std::fs::metadata(path).unwrap_or(metadata)
        } else {
            metadata
        };
        let inode_key = (resolved_meta.dev(), resolved_meta.ino());
        if !visited.insert(inode_key) {
            warn!(
                path = %path.display(),
                "Symlink cycle detected while surveying file-drop watch strategy; skipping"
            );
            return Ok(FileDropWatchSurvey::default());
        }

        let mut survey = FileDropWatchSurvey {
            accessible_watch_count: 1,
            filtered_watch_count: 1,
            filtered_targets: vec![path.to_path_buf()],
            ..FileDropWatchSurvey::default()
        };

        if max_depth.is_some_and(|limit| depth >= limit) {
            return Ok(survey);
        }

        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if depth > 0 && file_drop_permission_denied(&error) => {
                warn!(
                    path = %path.display(),
                    "Skipping unreadable directory while surveying file-drop watch strategy"
                );
                return Ok(FileDropWatchSurvey {
                    accessible_watch_count: 1,
                    unreadable_directories: 1,
                    ..FileDropWatchSurvey::default()
                });
            }
            Err(error) => {
                return Err(ParserError::Adapter(format!(
                    "failed to enumerate file-drop watch directory {}: {error}",
                    path.display()
                )));
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if depth > 0 && file_drop_permission_denied(&error) => {
                    warn!(
                        path = %path.display(),
                        "Skipping unreadable directory entry while surveying file-drop watch strategy"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(ParserError::Adapter(format!(
                        "failed to read file-drop watch directory entry under {}: {error}",
                        path.display()
                    )));
                }
            };
            let entry_path = entry.path();
            let metadata = match std::fs::symlink_metadata(&entry_path) {
                Ok(metadata) => metadata,
                Err(error) if depth > 0 && file_drop_permission_denied(&error) => {
                    warn!(
                        path = %entry_path.display(),
                        "Skipping unreadable watch directory entry while surveying file-drop watch strategy"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(ParserError::Adapter(format!(
                        "failed to inspect file-drop watch directory entry {}: {error}",
                        entry_path.display()
                    )));
                }
            };

            if file_drop_metadata_is_directory(&metadata, follow_symlinks, &entry_path)? {
                if file_drop_path_component_is_ignored(&entry_path, ignored_directory_names) {
                    survey.accessible_watch_count += 1;
                    survey.ignored_directories += 1;
                    continue;
                }
                let child_survey = inspect_path(
                    &entry_path,
                    depth + 1,
                    max_depth,
                    follow_symlinks,
                    ignored_directory_names,
                    visited,
                )?;
                survey.accessible_watch_count += child_survey.accessible_watch_count;
                survey.filtered_watch_count += child_survey.filtered_watch_count;
                survey.unreadable_directories += child_survey.unreadable_directories;
                survey.ignored_directories += child_survey.ignored_directories;
                survey
                    .filtered_targets
                    .extend(child_survey.filtered_targets);
            }
        }

        Ok(survey)
    }

    let mut visited: HashSet<(u64, u64)> = HashSet::new();
    let mut survey = inspect_path(
        path,
        start_depth,
        max_depth,
        follow_symlinks,
        ignored_directory_names,
        &mut visited,
    )?;
    survey.depth_limited = max_depth.is_some();
    Ok(survey)
}

fn planned_file_drop_watch_targets(
    config: &FileDropConfig,
) -> ParserResult<Vec<(PathBuf, RecursiveMode)>> {
    let watch_paths = normalized_file_drop_watch_paths(config);

    if !config.recursive {
        return Ok(watch_paths
            .into_iter()
            .map(|path| (path, RecursiveMode::NonRecursive))
            .collect());
    }

    let ignored_directory_names = config
        .ignored_directory_names
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let budget = FileDropWatchBudget::detect(config.max_watches);
    let mut aggregate_survey = FileDropWatchSurvey::default();

    for path in &watch_paths {
        let survey = survey_file_drop_watch_tree(
            path,
            0,
            config.max_depth,
            false,
            &ignored_directory_names,
        )?;

        aggregate_survey.accessible_watch_count += survey.accessible_watch_count;
        aggregate_survey.filtered_watch_count += survey.filtered_watch_count;
        aggregate_survey.depth_limited |= survey.depth_limited;
        aggregate_survey.unreadable_directories += survey.unreadable_directories;
        aggregate_survey.ignored_directories += survey.ignored_directories;
        aggregate_survey
            .filtered_targets
            .extend(survey.filtered_targets);
    }

    let plan = choose_file_drop_watch_plan(aggregate_survey, budget)?;
    let targets = match plan.mode {
        FileDropWatchMode::NativeRecursive => watch_paths
            .into_iter()
            .map(|path| (path, RecursiveMode::Recursive))
            .collect(),
        FileDropWatchMode::NativeFiltered => plan
            .survey
            .filtered_targets
            .into_iter()
            .map(|target| (target, RecursiveMode::NonRecursive))
            .collect(),
    };

    Ok(targets)
}

fn normalized_file_drop_watch_paths(config: &FileDropConfig) -> Vec<PathBuf> {
    normalized_file_drop_watch_roots(config)
        .into_iter()
        .map(|path| path.as_std_path().to_path_buf())
        .collect()
}

#[must_use]
pub fn normalized_file_drop_watch_roots(config: &FileDropConfig) -> Vec<Utf8PathBuf> {
    let mut seen = HashSet::new();
    let ignored_directory_names = config
        .ignored_directory_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let can_subsume_nested_roots = config.recursive && config.max_depth.is_none();
    let mut roots = Vec::new();

    config
        .watch_paths
        .iter()
        .filter(|path| seen.insert((*path).clone()))
        .for_each(|path| {
            if can_subsume_nested_roots
                && roots
                    .iter()
                    .any(|root| file_drop_watch_root_subsumes(root, path, &ignored_directory_names))
            {
                return;
            }

            if can_subsume_nested_roots {
                roots.retain(|root| {
                    !file_drop_watch_root_subsumes(path, root, &ignored_directory_names)
                });
            }

            roots.push(path.clone());
        });
    roots
}

fn file_drop_watch_root_subsumes(
    parent: &Utf8PathBuf,
    child: &Utf8PathBuf,
    ignored_directory_names: &HashSet<&str>,
) -> bool {
    let Ok(relative) = child.strip_prefix(parent) else {
        return false;
    };
    if relative.components().next().is_none() {
        return true;
    }
    !relative
        .components()
        .any(|component| ignored_directory_names.contains(component.as_str()))
}

/// No cursor for [`FileDropAdapter`] — live streams are anchor-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDropCursor;

#[async_trait]
impl InputShapeAdapter for FileDropAdapter {
    type Config = FileDropConfig;
    type Cursor = FileDropCursor;
    const KIND: InputShapeKind = InputShapeKind::FileDrop;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        _cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let (tx, rx) = mpsc::channel::<notify::Result<Event>>(256);
        let event_filter = config.events.clone();
        let path_filter = FileDropPathFilter::from_config(config);

        // Build watcher on the current thread; it sends events to the channel.
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            // Ignore send errors — channel closed means the stream was dropped.
            let _ = tx.blocking_send(res);
        })
        .map_err(|e| ParserError::Adapter(format!("failed to create file watcher: {e}")))?;

        for (path, mode) in planned_file_drop_watch_targets(config)? {
            watcher.watch(&path, mode).map_err(|e| {
                ParserError::Adapter(format!("failed to watch {}: {e}", path.display()))
            })?;
        }

        let stream = build_file_drop_stream(material_id, rx, event_filter, path_filter, watcher);
        Ok(stream)
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        // FileDrop is anchor-only; there is no cursor to advance.
        Ok(FileDropCursor)
    }
}

#[async_trait]
impl InputShapeAdapter for FileContentDropAdapter {
    type Config = FileContentDropConfig;
    type Cursor = FileDropCursor;
    const KIND: InputShapeKind = InputShapeKind::FileDrop;

    async fn open(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        FileDropAdapter
            .open(material_id, &config.file_drop, cursor)
            .await
    }

    fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
        Ok(FileDropCursor)
    }
}

#[cfg(feature = "messaging")]
#[async_trait]
impl InputShapeAdapterExt for FileContentDropAdapter {
    async fn open_with_acquisition(
        &self,
        material_id: Id<SourceMaterial>,
        config: &Self::Config,
        cursor: Option<Self::Cursor>,
        acquisition: Option<Arc<AcquisitionManager>>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let Some(acquisition) = acquisition else {
            return InputShapeAdapter::open(self, material_id, config, cursor).await;
        };
        let mut stream = FileDropAdapter
            .open(material_id, &config.file_drop, cursor)
            .await?;
        let max_capture_bytes = config.max_capture_bytes;
        let stream = async_stream::stream! {
            while let Some(record_result) = stream.next().await {
                match record_result {
                    Ok(record) => {
                        yield materialize_file_content_record(
                            record,
                            Arc::clone(&acquisition),
                            max_capture_bytes,
                        ).await;
                    }
                    Err(error) => yield Err(error),
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

fn build_file_drop_stream(
    material_id: Id<SourceMaterial>,
    mut rx: mpsc::Receiver<notify::Result<Event>>,
    event_filter: Vec<FileDropEventKind>,
    path_filter: FileDropPathFilter,
    _watcher: impl Watcher + 'static, // keep alive until stream ends
) -> BoxStream<'static, ParserResult<SourceRecord>> {
    let stream = async_stream::stream! {
        // `_watcher` is moved into this async block and lives until the
        // stream is dropped.
        while let Some(notify_result) = rx.recv().await {
            match notify_result {
                Err(e) => {
                    yield Err(ParserError::Adapter(format!("notify error: {e}")));
                }
                Ok(event) => {
                    for record in records_from_file_drop_event(
                        material_id,
                        &event,
                        &event_filter,
                        &path_filter,
                    ) {
                        yield Ok(record);
                    }
                }
            }
        }
    };

    Box::pin(stream)
}

// =============================================================================
// Helpers
// =============================================================================

fn map_notify_kind(kind: &EventKind) -> Option<FileDropEventKind> {
    match kind {
        EventKind::Create(_) => Some(FileDropEventKind::Created),
        EventKind::Modify(ModifyKind::Name(_)) => Some(FileDropEventKind::Moved),
        EventKind::Modify(_) => Some(FileDropEventKind::Modified),
        EventKind::Remove(_) => Some(FileDropEventKind::Deleted),
        EventKind::Access(_) => None,
        EventKind::Other => None,
        EventKind::Any => None,
    }
}

fn read_kernel_inotify_watch_limit() -> Option<NonZeroUsize> {
    std::fs::read_to_string(INOTIFY_MAX_USER_WATCHES_PATH)
        .ok()?
        .trim()
        .parse::<NonZeroUsize>()
        .ok()
}

fn records_from_file_drop_event(
    material_id: Id<SourceMaterial>,
    event: &Event,
    event_filter: &[FileDropEventKind],
    path_filter: &FileDropPathFilter,
) -> Vec<SourceRecord> {
    let Some(kind) = map_notify_kind(&event.kind) else {
        return Vec::new();
    };
    if !event_filter.is_empty() && !event_filter.contains(&kind) {
        return Vec::new();
    }

    let paths = event
        .paths
        .iter()
        .cloned()
        .filter_map(|path| {
            let utf8_path = Utf8PathBuf::from_path_buf(path)
                .unwrap_or_else(|path| Utf8PathBuf::from(path.to_string_lossy().to_string()));
            if !path_filter.includes(&utf8_path) {
                return None;
            }
            Some(utf8_path)
        })
        .collect::<Vec<_>>();
    let rename_pair = if kind == FileDropEventKind::Moved && paths.len() == 2 {
        Some((paths[0].clone(), paths[1].clone()))
    } else {
        None
    };

    paths
        .into_iter()
        .enumerate()
        .map(|(index, utf8_path)| {
            let mut metadata = FileDropRecordMetadata::new(kind, &utf8_path);
            if let Some((from_path, to_path)) = &rename_pair {
                metadata = metadata.with_move_pair(
                    from_path,
                    to_path,
                    if index == 0 {
                        FileDropMoveRole::From
                    } else {
                        FileDropMoveRole::To
                    },
                );
            }
            SourceRecord {
                material_id,
                anchor: MaterialAnchor::DirectoryEntry {
                    path: utf8_path.clone(),
                    content_hash: None,
                },
                bytes: utf8_path.as_str().as_bytes().to_vec(),
                logical_path: Some(utf8_path),
                source_ts_hint: None,
                metadata: metadata.into_json(),
            }
        })
        .collect()
}

#[cfg(feature = "messaging")]
async fn materialize_file_content_record(
    record: SourceRecord,
    acquisition: Arc<AcquisitionManager>,
    max_capture_bytes: u64,
) -> ParserResult<SourceRecord> {
    let Ok(metadata) = FileDropRecordMetadata::from_value(&record.metadata) else {
        return Ok(record);
    };
    if !matches!(
        metadata.event_kind(),
        Some(FileDropEventKind::Created | FileDropEventKind::Modified)
    ) {
        return Ok(record);
    }

    let Some(path) = record.logical_path.clone() else {
        return Ok(record);
    };
    let Ok(file_metadata) = tokio::fs::metadata(path.as_std_path()).await else {
        return Ok(record);
    };
    if !file_metadata.is_file() {
        return Ok(record);
    }
    let len = file_metadata.len();
    if len == 0 {
        return Ok(record);
    }

    if len > max_capture_bytes {
        return Ok(SourceRecord {
            metadata: metadata.with_skipped_content(len, "oversized").into_json(),
            ..record
        });
    }

    let material_metadata = metadata.with_materialized_content(len).into_json();
    let (material_id, total_bytes) = stage_material_from_file_bounded(
        &acquisition,
        &path,
        "file-drop-content-material",
        Some(material_metadata.clone()),
        Some(max_capture_bytes),
    )
    .await
    .map_err(ParserError::Sinex)?;

    Ok(SourceRecord {
        material_id: Id::from_uuid(material_id),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: total_bytes.max(0) as u64,
        },
        metadata: material_metadata,
        ..record
    })
}

#[derive(Debug, Clone)]
struct FileDropPathFilter {
    watch_roots: Vec<Utf8PathBuf>,
    max_depth: Option<usize>,
    ignored_directory_names: HashSet<String>,
    ignored_file_suffixes: Vec<String>,
}

impl FileDropPathFilter {
    fn from_config(config: &FileDropConfig) -> Self {
        Self {
            watch_roots: normalized_file_drop_watch_roots(config),
            max_depth: config.max_depth,
            ignored_directory_names: config.ignored_directory_names.iter().cloned().collect(),
            ignored_file_suffixes: config.ignored_file_suffixes.clone(),
        }
    }

    #[cfg(test)]
    fn unrestricted() -> Self {
        Self {
            watch_roots: Vec::new(),
            max_depth: None,
            ignored_directory_names: HashSet::new(),
            ignored_file_suffixes: Vec::new(),
        }
    }

    fn includes(&self, path: &Utf8PathBuf) -> bool {
        if self.has_ignored_component(path) {
            return false;
        }
        if self.has_ignored_file_suffix(path) {
            return false;
        }

        let Some(max_depth) = self.max_depth else {
            return true;
        };

        self.relative_depth(path)
            .is_none_or(|depth| depth <= max_depth)
    }

    fn has_ignored_file_suffix(&self, path: &Utf8PathBuf) -> bool {
        if self.ignored_file_suffixes.is_empty() {
            return false;
        }
        let Some(name) = path.file_name() else {
            return false;
        };
        self.ignored_file_suffixes
            .iter()
            .any(|suffix| name.ends_with(suffix))
    }

    fn has_ignored_component(&self, path: &Utf8PathBuf) -> bool {
        if self.ignored_directory_names.is_empty() {
            return false;
        }

        if self.watch_roots.is_empty() {
            return path
                .components()
                .any(|component| self.ignored_directory_names.contains(component.as_str()));
        }

        let mut matched_root = false;
        for relative in self
            .watch_roots
            .iter()
            .filter_map(|root| path.strip_prefix(root).ok())
        {
            matched_root = true;
            if !relative
                .components()
                .any(|component| self.ignored_directory_names.contains(component.as_str()))
            {
                return false;
            }
        }

        matched_root
    }

    fn relative_depth(&self, path: &Utf8PathBuf) -> Option<usize> {
        self.watch_roots
            .iter()
            .filter_map(|root| path.strip_prefix(root).ok())
            .map(|relative| relative.components().count().saturating_sub(1))
            .min()
    }
}

#[cfg(test)]
#[path = "file_drop_test.rs"]
mod tests;
