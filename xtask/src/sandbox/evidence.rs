//! Structured evidence artifacts for sandbox and scenario tests.
//!
//! The harness owns evidence collection because it owns isolated runtime state,
//! but the schema is deliberately plain Rust data so scenario code can reuse it
//! without depending on xtask command semantics.

use color_eyre::eyre::{Context, Result as EyreResult};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use walkdir::WalkDir;

pub const EVIDENCE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCaptureLevel {
    Summary,
    Debug,
}

impl Default for EvidenceCaptureLevel {
    fn default() -> Self {
        Self::Summary
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCollectorKind {
    Logs,
    Database,
    Nats,
    MaterialSpool,
    Process,
    Proof,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCollectorStatus {
    Registered,
    Captured,
    Unavailable,
    Failed,
}

impl Default for EvidenceCollectorStatus {
    fn default() -> Self {
        Self::Registered
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceTimelineEvent {
    pub elapsed_ms: u64,
    pub label: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub fields: JsonValue,
}

impl EvidenceTimelineEvent {
    pub fn new(
        elapsed_ms: u64,
        label: impl Into<String>,
        message: impl Into<String>,
        fields: JsonValue,
    ) -> Self {
        Self {
            elapsed_ms,
            label: label.into(),
            message: message.into(),
            fields,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceArtifactRef {
    pub name: String,
    pub kind: String,
    pub format: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl EvidenceArtifactRef {
    pub fn new(
        name: impl Into<String>,
        kind: impl Into<String>,
        format: impl Into<String>,
        path: impl Into<PathBuf>,
        summary: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            format: format.into(),
            path: path.into().display().to_string(),
            summary,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceCollectorRegistration {
    pub name: String,
    pub kind: EvidenceCollectorKind,
    pub level: EvidenceCaptureLevel,
    pub status: EvidenceCollectorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl EvidenceCollectorRegistration {
    pub fn new(
        name: impl Into<String>,
        kind: EvidenceCollectorKind,
        level: EvidenceCaptureLevel,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            level,
            status: EvidenceCollectorStatus::Registered,
            summary: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceCapture {
    pub name: String,
    pub kind: EvidenceCollectorKind,
    pub status: EvidenceCollectorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub data: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<EvidenceArtifactRef>,
}

impl EvidenceCapture {
    pub fn captured(
        name: impl Into<String>,
        kind: EvidenceCollectorKind,
        summary: Option<String>,
        data: JsonValue,
        artifact: Option<EvidenceArtifactRef>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            status: EvidenceCollectorStatus::Captured,
            summary,
            data,
            artifact,
        }
    }

    pub fn unavailable(
        name: impl Into<String>,
        kind: EvidenceCollectorKind,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            status: EvidenceCollectorStatus::Unavailable,
            summary: Some(summary.into()),
            data: JsonValue::Null,
            artifact: None,
        }
    }

    pub fn failed(
        name: impl Into<String>,
        kind: EvidenceCollectorKind,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            status: EvidenceCollectorStatus::Failed,
            summary: Some(summary.into()),
            data: JsonValue::Null,
            artifact: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ProofMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subject_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claim_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reproducer: Option<String>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub environment: JsonValue,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TestEvidence {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timeline: Vec<EvidenceTimelineEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collectors: Vec<EvidenceCollectorRegistration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub captures: Vec<EvidenceCapture>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<EvidenceArtifactRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<ProofMetadata>,
}

impl TestEvidence {
    pub fn record_event(
        &mut self,
        elapsed_ms: u64,
        label: impl Into<String>,
        message: impl Into<String>,
        fields: JsonValue,
    ) {
        self.timeline.push(EvidenceTimelineEvent::new(
            elapsed_ms, label, message, fields,
        ));
    }

    pub fn register_collector(
        &mut self,
        name: impl Into<String>,
        kind: EvidenceCollectorKind,
        level: EvidenceCaptureLevel,
    ) {
        self.collectors
            .push(EvidenceCollectorRegistration::new(name, kind, level));
    }

    pub fn attach_artifact(&mut self, artifact: EvidenceArtifactRef) {
        self.artifacts.push(artifact);
    }

    pub fn attach_capture(&mut self, capture: EvidenceCapture) {
        self.mark_collector_status(
            &capture.name,
            capture.status.clone(),
            capture.summary.clone(),
        );
        if let Some(artifact) = capture.artifact.clone() {
            self.attach_artifact(artifact);
        }
        self.captures.push(capture);
    }

    pub fn set_proof(&mut self, proof: ProofMetadata) {
        self.proof = Some(proof);
    }

    pub fn add_note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }

    fn mark_collector_status(
        &mut self,
        name: &str,
        status: EvidenceCollectorStatus,
        summary: Option<String>,
    ) {
        if let Some(registration) = self.collectors.iter_mut().find(|c| c.name == name) {
            registration.status = status;
            registration.summary = summary;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceRuntimeSnapshot {
    pub process_id: u32,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub process_tree: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceBundle {
    pub schema_version: u32,
    pub kind: String,
    pub test: String,
    pub status: String,
    pub error: String,
    pub timestamp: String,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub context: JsonValue,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub pool: JsonValue,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub pool_detail: JsonValue,
    pub runtime: EvidenceRuntimeSnapshot,
    #[serde(flatten)]
    pub evidence: TestEvidence,
}

impl EvidenceBundle {
    pub fn failed(
        test: impl Into<String>,
        error: impl Into<String>,
        timestamp: impl Into<String>,
        context: JsonValue,
        pool: JsonValue,
        pool_detail: JsonValue,
        runtime: EvidenceRuntimeSnapshot,
        evidence: TestEvidence,
    ) -> Self {
        Self {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            kind: "sinex.test.evidence".to_string(),
            test: test.into(),
            status: "failed".to_string(),
            error: error.into(),
            timestamp: timestamp.into(),
            context,
            pool,
            pool_detail,
            runtime,
            evidence,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEvidenceSummary {
    pub count: usize,
    pub preview: Vec<String>,
}

impl LogEvidenceSummary {
    pub fn new(logs: &[String], preview_limit: usize) -> Self {
        Self {
            count: logs.len(),
            preview: logs.iter().take(preview_limit).cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DbEvidenceSummary {
    pub database_name: String,
    pub event_count: i64,
    pub material_event_count: i64,
    pub synthesis_event_count: i64,
    pub source_material_count: i64,
    pub blob_count: i64,
    pub created_event_count: usize,
    pub created_material_count: usize,
    pub recent_source_materials: Vec<SourceMaterialEvidenceRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceMaterialEvidenceRow {
    pub id: String,
    pub material_kind: String,
    pub source_identifier: String,
    pub status: String,
    pub total_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NatsEvidenceSummary {
    pub enabled: bool,
    pub namespace: String,
    pub streams: Vec<NatsStreamEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NatsStreamEvidence {
    pub name: String,
    pub subjects: Vec<String>,
    pub messages: u64,
    pub bytes: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub consumer_count: usize,
    pub consumers: Vec<NatsConsumerEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NatsConsumerEvidence {
    pub name: String,
    pub durable_name: Option<String>,
    pub filter_subject: String,
    pub num_pending: u64,
    pub num_ack_pending: usize,
    pub num_redelivered: usize,
    pub num_waiting: usize,
    pub delivered_stream_sequence: u64,
    pub ack_floor_stream_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectoryEvidenceSummary {
    pub root: String,
    pub exists: bool,
    pub file_count: usize,
    pub directory_count: usize,
    pub total_bytes: u64,
    pub wal_files: Vec<FileEvidenceSummary>,
    pub largest_files: Vec<FileEvidenceSummary>,
}

impl DirectoryEvidenceSummary {
    pub fn missing(path: &Path) -> Self {
        Self {
            root: path.display().to_string(),
            exists: false,
            file_count: 0,
            directory_count: 0,
            total_bytes: 0,
            wal_files: Vec::new(),
            largest_files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEvidenceSummary {
    pub path: String,
    pub bytes: u64,
}

pub fn summarize_directory(path: &Path) -> DirectoryEvidenceSummary {
    if !path.exists() {
        return DirectoryEvidenceSummary::missing(path);
    }

    let mut file_count = 0usize;
    let mut directory_count = 0usize;
    let mut total_bytes = 0u64;
    let mut wal_files = Vec::new();
    let mut files = Vec::new();

    for entry in WalkDir::new(path)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        let entry_path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            directory_count = directory_count.saturating_add(1);
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        file_count = file_count.saturating_add(1);
        let bytes = metadata.len();
        total_bytes = total_bytes.saturating_add(bytes);
        let summary = FileEvidenceSummary {
            path: entry_path.display().to_string(),
            bytes,
        };
        if is_wal_path(entry_path) {
            wal_files.push(summary.clone());
        }
        files.push(summary);
    }

    files.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.path.cmp(&b.path)));
    files.truncate(10);
    wal_files.sort_by(|a, b| a.path.cmp(&b.path));

    DirectoryEvidenceSummary {
        root: path.display().to_string(),
        exists: true,
        file_count,
        directory_count,
        total_bytes,
        wal_files,
        largest_files: files,
    }
}

fn is_wal_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "state.wal" || name.ends_with(".wal"))
}

pub fn evidence_root() -> PathBuf {
    std::env::var("SINEX_TEST_FAIL_DIR").map_or_else(
        |_| crate::config::workspace_state_root().join("test-artifacts"),
        PathBuf::from,
    )
}

pub fn test_artifact_dir(test_name: &str) -> PathBuf {
    evidence_root().join(sanitize_component(test_name))
}

pub fn sanitize_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unnamed".to_string()
    } else {
        out
    }
}

pub fn write_json_artifact<T: Serialize>(
    test_name: &str,
    name: &str,
    kind: &str,
    value: &T,
    summary: Option<String>,
) -> EyreResult<EvidenceArtifactRef> {
    let artifact_dir = test_artifact_dir(test_name);
    fs::create_dir_all(&artifact_dir).with_context(|| {
        format!(
            "failed to create evidence artifact dir {}",
            artifact_dir.display()
        )
    })?;
    let path = artifact_dir.join(format!("{}.json", sanitize_component(name)));
    let data = serde_json::to_vec_pretty(value)
        .with_context(|| format!("failed to serialize evidence artifact {name}"))?;
    fs::write(&path, data)
        .with_context(|| format!("failed to write evidence artifact {}", path.display()))?;
    Ok(EvidenceArtifactRef::new(name, kind, "json", path, summary))
}

pub fn write_text_artifact(
    test_name: &str,
    name: &str,
    kind: &str,
    text: &str,
    summary: Option<String>,
) -> EyreResult<EvidenceArtifactRef> {
    let artifact_dir = test_artifact_dir(test_name);
    fs::create_dir_all(&artifact_dir).with_context(|| {
        format!(
            "failed to create evidence artifact dir {}",
            artifact_dir.display()
        )
    })?;
    let path = artifact_dir.join(format!("{}.txt", sanitize_component(name)));
    fs::write(&path, text)
        .with_context(|| format!("failed to write evidence artifact {}", path.display()))?;
    Ok(EvidenceArtifactRef::new(name, kind, "text", path, summary))
}

pub fn render_human_summary(bundle: &EvidenceBundle) -> String {
    let mut lines = Vec::new();
    lines.push(format!("test: {}", bundle.test));
    lines.push(format!("status: {}", bundle.status));
    lines.push(format!("error: {}", bundle.error));

    if !bundle.evidence.timeline.is_empty() {
        lines.push("timeline:".to_string());
        for event in bundle.evidence.timeline.iter().rev().take(8).rev() {
            lines.push(format!(
                "  - {:>6}ms {}: {}",
                event.elapsed_ms, event.label, event.message
            ));
        }
    }

    if !bundle.evidence.captures.is_empty() {
        lines.push("captures:".to_string());
        for capture in &bundle.evidence.captures {
            let summary = capture.summary.as_deref().unwrap_or("no summary");
            lines.push(format!(
                "  - {} [{:?}/{:?}]: {}",
                capture.name, capture.kind, capture.status, summary
            ));
        }
    }

    if !bundle.evidence.artifacts.is_empty() {
        lines.push("artifacts:".to_string());
        for artifact in &bundle.evidence.artifacts {
            lines.push(format!(
                "  - {} [{}]: {}",
                artifact.name, artifact.format, artifact.path
            ));
        }
    }

    if let Some(proof) = &bundle.evidence.proof {
        lines.push("proof:".to_string());
        if let Some(runner_id) = &proof.runner_id {
            lines.push(format!("  - runner: {runner_id}"));
        }
        if !proof.subject_refs.is_empty() {
            lines.push(format!("  - subjects: {}", proof.subject_refs.join(", ")));
        }
        if !proof.claim_ids.is_empty() {
            lines.push(format!("  - claims: {}", proof.claim_ids.join(", ")));
        }
    }

    lines.join("\n")
}

pub fn current_process_tree_json(sample_window: Duration) -> JsonValue {
    #[cfg(target_os = "linux")]
    {
        crate::process::probe_process_tree_metrics(std::process::id(), sample_window)
            .and_then(|metrics| serde_json::to_value(metrics).ok())
            .unwrap_or(JsonValue::Null)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = sample_window;
        JsonValue::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sanitize_component_removes_path_separators() {
        assert_eq!(sanitize_component("a/b::c d"), "a_b__c_d");
    }

    #[test]
    fn evidence_bundle_keeps_proof_and_timeline_shape() {
        let mut evidence = TestEvidence::default();
        evidence.record_event(12, "fixture", "created stack", json!({"stack": "core"}));
        evidence.register_collector(
            "db",
            EvidenceCollectorKind::Database,
            EvidenceCaptureLevel::Summary,
        );
        evidence.attach_capture(EvidenceCapture::captured(
            "db",
            EvidenceCollectorKind::Database,
            Some("1 event, 1 material".to_string()),
            json!({"event_count": 1, "source_material_count": 1}),
            None,
        ));
        evidence.set_proof(ProofMetadata {
            runner_id: Some("runner:test".to_string()),
            subject_refs: vec!["subject:node/terminal".to_string()],
            claim_ids: vec!["claim:material-provenance".to_string()],
            status: Some("failed".to_string()),
            reproducer: Some("xtask test -p xtask".to_string()),
            environment: json!({"profile": "fast"}),
        });

        let bundle = EvidenceBundle::failed(
            "sample_test",
            "assertion failed",
            "2026-04-22T00:00:00Z",
            JsonValue::Null,
            JsonValue::Null,
            JsonValue::Null,
            EvidenceRuntimeSnapshot {
                process_id: 123,
                process_tree: JsonValue::Null,
            },
            evidence,
        );
        let rendered = serde_json::to_value(&bundle).expect("bundle serializes");

        assert_eq!(rendered["schema_version"], EVIDENCE_SCHEMA_VERSION);
        assert_eq!(rendered["timeline"][0]["label"], "fixture");
        assert_eq!(rendered["captures"][0]["status"], "captured");
        assert_eq!(
            rendered["proof"]["claim_ids"][0],
            "claim:material-provenance"
        );
    }

    #[test]
    fn human_summary_mentions_key_artifacts() {
        let mut evidence = TestEvidence::default();
        evidence.record_event(1, "start", "fixture initialized", JsonValue::Null);
        evidence.attach_artifact(EvidenceArtifactRef::new(
            "db",
            "database",
            "json",
            "/tmp/db.json",
            Some("database summary".to_string()),
        ));
        let bundle = EvidenceBundle::failed(
            "sample_test",
            "boom",
            "2026-04-22T00:00:00Z",
            JsonValue::Null,
            JsonValue::Null,
            JsonValue::Null,
            EvidenceRuntimeSnapshot {
                process_id: 123,
                process_tree: JsonValue::Null,
            },
            evidence,
        );

        let summary = render_human_summary(&bundle);

        assert!(summary.contains("timeline:"));
        assert!(summary.contains("/tmp/db.json"));
    }
}
