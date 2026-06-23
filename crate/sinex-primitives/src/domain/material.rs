//! Source-material storage, lifecycle, format, and timing vocabulary.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use super::temporal::TemporalSourceType;

// ─────────────────────────────────────────────────────────────
// Source-material lifecycle status
// ─────────────────────────────────────────────────────────────

/// Lifecycle status of a row in `raw.source_material_registry`.
///
/// The values are stored as TEXT in the database. All variant strings are
/// intentionally lowercase with underscores to match the existing DB data.
///
/// `Sensing` is the only non-terminal status; all others are terminal (the
/// assembly state machine emits no further transitions once a terminal status
/// is written).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MaterialStatus {
    /// Material is still being assembled (in-flight capture).
    Sensing,
    /// Assembly completed successfully.
    Completed,
    /// Assembly completed with partial recovery (some data may be missing).
    RecoveredPartial,
    /// Assembly failed unrecoverably.
    Failed,
    /// Assembly was cancelled before completion.
    Cancelled,
}

impl MaterialStatus {
    /// Returns the canonical string representation stored in the database.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sensing => "sensing",
            Self::Completed => "completed",
            Self::RecoveredPartial => "recovered_partial",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns `true` when this is a terminal status (assembly has ended).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Sensing)
    }
}

impl fmt::Display for MaterialStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MaterialStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sensing" => Ok(Self::Sensing),
            "completed" => Ok(Self::Completed),
            "recovered_partial" => Ok(Self::RecoveredPartial),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unknown material status: {s:?}")),
        }
    }
}

/// Storage/backend kind stored on `raw.source_material_registry.material_kind`.
///
/// This is deliberately narrower than source-package material classes. It
/// describes how raw material is stored or addressed by the registry, not
/// whether the material is an email message, OCR segment, transcript, API page,
/// or stream batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MaterialStorageKind {
    Annex,
    Git,
    LocalCas,
}

impl MaterialStorageKind {
    pub const ALL: &'static [&'static str] = &["annex", "git", "local_cas"];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Annex => "annex",
            Self::Git => "git",
            Self::LocalCas => "local_cas",
        }
    }
}

impl fmt::Display for MaterialStorageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MaterialStorageKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "annex" => Ok(Self::Annex),
            "git" => Ok(Self::Git),
            "local_cas" => Ok(Self::LocalCas),
            _ => Err(format!("unknown material storage kind: {s:?}")),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Temporal Vocabulary
// ─────────────────────────────────────────────────────────────

/// Coarse material-level timing category stored on `raw.source_material_registry`.
///
/// Fine-grained evidence for byte ranges lives in `raw.temporal_ledger` via
/// [`TemporalSourceType`]. This enum is the registry summary used by source
/// staging, replay previews, and parser scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceMaterialTimingInfoType {
    /// Timestamp was observed during live capture.
    Realtime,
    /// Timestamp is intrinsic to the material content.
    Intrinsic,
    /// Timestamp is inferred from external evidence such as mtime/ctime.
    Inferred,
    /// Timestamp or range was explicitly declared by the operator/user.
    Declared,
    /// Material is intentionally not time-addressed.
    Atemporal,
    /// Only staging time is known.
    StagedAt,
    /// Timing information is unknown or not yet determined.
    Unknown,
}

impl SourceMaterialTimingInfoType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Realtime => "realtime",
            Self::Intrinsic => "intrinsic",
            Self::Inferred => "inferred",
            Self::Declared => "declared",
            Self::Atemporal => "atemporal",
            Self::StagedAt => "staged_at",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn from_temporal_source(source: TemporalSourceType) -> Self {
        match source {
            TemporalSourceType::RealtimeCapture => Self::Realtime,
            TemporalSourceType::IntrinsicContent => Self::Intrinsic,
            TemporalSourceType::InferredMtime | TemporalSourceType::InferredCtime => Self::Inferred,
            TemporalSourceType::InferredUser => Self::Declared,
            TemporalSourceType::StagedAt => Self::StagedAt,
        }
    }

    /// Map the coarse material-tier timing category to a quality rung on the
    /// temporal ladder (#1570 Prong B). Categories with no real-world timing
    /// (`Atemporal`, `Unknown`) collapse to the `StagedAt` floor. The registry
    /// cannot distinguish mtime from ctime, so `Inferred` maps to the
    /// `InferredMtime` rung.
    #[must_use]
    pub const fn to_temporal_source(self) -> TemporalSourceType {
        match self {
            Self::Realtime => TemporalSourceType::RealtimeCapture,
            Self::Intrinsic => TemporalSourceType::IntrinsicContent,
            Self::Inferred => TemporalSourceType::InferredMtime,
            Self::Declared => TemporalSourceType::InferredUser,
            Self::StagedAt | Self::Atemporal | Self::Unknown => TemporalSourceType::StagedAt,
        }
    }
}

impl fmt::Display for SourceMaterialTimingInfoType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceMaterialTimingInfoType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "realtime" => Ok(Self::Realtime),
            "intrinsic" => Ok(Self::Intrinsic),
            "inferred" => Ok(Self::Inferred),
            "declared" | "user_declared" | "conceptual" => Ok(Self::Declared),
            "atemporal" => Ok(Self::Atemporal),
            "staged_at" | "staged-at" => Ok(Self::StagedAt),
            "unknown" | "" => Ok(Self::Unknown),
            _ => Err(format!("unknown source-material timing info type: {s}")),
        }
    }
}

/// Typed source-material format vocabulary used by staged-source metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceMaterialFormat {
    Json,
    Jsonl,
    Sqlite,
    Markdown,
    Text,
    Csv,
    Tsv,
    Html,
    Pdf,
    Directory,
    Repository,
    Image,
    Audio,
    Video,
    Archive,
    Binary,
    Unknown,
}

impl SourceMaterialFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Sqlite => "sqlite",
            Self::Markdown => "markdown",
            Self::Text => "text",
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Html => "html",
            Self::Pdf => "pdf",
            Self::Directory => "directory",
            Self::Repository => "repository",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Archive => "archive",
            Self::Binary => "binary",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub fn infer_from_path(path: &str) -> Self {
        let lower = path.to_lowercase();
        if lower.ends_with(".tar.gz")
            || lower.ends_with(".tar.zst")
            || lower.ends_with(".tar.xz")
            || lower.ends_with(".tgz")
        {
            return Self::Archive;
        }

        match lower.rsplit('.').next() {
            Some("json") => Self::Json,
            Some("jsonl" | "ndjson") => Self::Jsonl,
            Some("sqlite" | "sqlite3" | "db") => Self::Sqlite,
            Some("md" | "markdown" | "mdown") => Self::Markdown,
            Some("txt" | "log") => Self::Text,
            Some("csv") => Self::Csv,
            Some("tsv") => Self::Tsv,
            Some("html" | "htm") => Self::Html,
            Some("pdf") => Self::Pdf,
            Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "avif" | "bmp") => Self::Image,
            Some("mp3" | "flac" | "wav" | "ogg" | "m4a" | "opus") => Self::Audio,
            Some("mp4" | "mkv" | "webm" | "mov" | "avi") => Self::Video,
            Some("zip" | "tar" | "gz" | "xz" | "zst" | "7z" | "rar") => Self::Archive,
            Some("bin" | "dat") => Self::Binary,
            Some(_) | None => Self::Unknown,
        }
    }
}

impl fmt::Display for SourceMaterialFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceMaterialFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "jsonl" | "ndjson" => Ok(Self::Jsonl),
            "sqlite" | "sqlite3" => Ok(Self::Sqlite),
            "markdown" | "md" => Ok(Self::Markdown),
            "text" | "txt" => Ok(Self::Text),
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            "html" | "htm" => Ok(Self::Html),
            "pdf" => Ok(Self::Pdf),
            "directory" | "dir" => Ok(Self::Directory),
            "repository" | "repo" | "git" => Ok(Self::Repository),
            "image" => Ok(Self::Image),
            "audio" => Ok(Self::Audio),
            "video" => Ok(Self::Video),
            "archive" => Ok(Self::Archive),
            "binary" | "bin" => Ok(Self::Binary),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown source-material format: {s}")),
        }
    }
}
