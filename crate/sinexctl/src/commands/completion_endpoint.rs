use std::collections::{BTreeMap, BTreeSet};

use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_primitives::events::schema_registry::{PayloadInfo, get_all_payloads};
use sinex_primitives::views::SourceCoverageListView;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::format_yaml;
use crate::model::OutputFormat;

/// Structured, read-only completion endpoint for shell and picker frontends.
#[derive(Debug, Args)]
pub struct CompletionEndpointCommand {
    /// Full command line buffer.
    #[arg(long)]
    line: String,

    /// Cursor byte offset in the command line buffer.
    #[arg(long)]
    cursor: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionCandidate {
    pub value: String,
    pub insert: String,
    pub replace_start: usize,
    pub replace_end: usize,
    pub display: String,
    pub kind: String,
    pub group: String,
    pub description: String,
    pub source: Option<String>,
    pub stale: bool,
    pub danger: String,
    pub privacy: String,
    pub preview: Option<String>,
    pub score: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub schema_version: u8,
    pub line: String,
    pub cursor: usize,
    pub active_token: String,
    pub candidates: Vec<CompletionCandidate>,
}

impl CompletionEndpointCommand {
    pub async fn execute(
        &self,
        client: Option<&GatewayClient>,
        format: OutputFormat,
    ) -> Result<()> {
        let response = self.complete(client).await;

        match format {
            OutputFormat::Table => print_completion_table(&response),
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&response)?),
            OutputFormat::Yaml => println!("{}", format_yaml(&response)?),
            OutputFormat::Ndjson => {
                for candidate in &response.candidates {
                    println!("{}", serde_json::to_string(candidate)?);
                }
            }
            OutputFormat::Dot => {
                return Err(color_eyre::eyre::eyre!(
                    "_complete is a finite completion view; use json, yaml, ndjson, or table"
                ));
            }
        }

        Ok(())
    }

    async fn complete(&self, client: Option<&GatewayClient>) -> CompletionResponse {
        let active_token = active_token(&self.line, self.cursor).to_string();
        let mut vocabulary = CompletionVocabulary::from_payload_inventory();
        if let Some(client) = client
            && let Ok(runtime) = RuntimeCompletionVocabulary::load(client).await
        {
            vocabulary.merge_runtime(runtime);
        }

        let candidates = build_candidates(&self.line, self.cursor, &active_token, &vocabulary);
        CompletionResponse {
            schema_version: 1,
            line: self.line.clone(),
            cursor: self.cursor,
            active_token,
            candidates,
        }
    }
}

#[derive(Debug, Default)]
struct CompletionVocabulary {
    sources: BTreeSet<String>,
    event_types: BTreeSet<String>,
    event_types_by_source: BTreeMap<String, BTreeSet<String>>,
    payload_keys_by_pair: BTreeMap<(String, String), BTreeSet<String>>,
    source_descriptions: BTreeMap<String, String>,
    source_privacy: BTreeMap<String, String>,
    runtime_sources: BTreeSet<String>,
}

impl CompletionVocabulary {
    fn from_payload_inventory() -> Self {
        let mut vocabulary = Self::default();
        for payload in get_all_payloads() {
            vocabulary.add_payload(payload);
        }
        vocabulary
    }

    fn add_payload(&mut self, payload: &'static PayloadInfo) {
        self.sources.insert(payload.source.to_string());
        self.event_types.insert(payload.event_type.to_string());
        self.event_types_by_source
            .entry(payload.source.to_string())
            .or_default()
            .insert(payload.event_type.to_string());

        let keys = payload_schema_keys(payload);
        if !keys.is_empty() {
            self.payload_keys_by_pair.insert(
                (payload.source.to_string(), payload.event_type.to_string()),
                keys,
            );
        }
    }

    fn merge_runtime(&mut self, runtime: RuntimeCompletionVocabulary) {
        for source in runtime.sources {
            self.sources.insert(source.source_id.clone());
            self.source_descriptions
                .insert(source.source_id.clone(), source.description);
            self.source_privacy
                .insert(source.source_id.clone(), source.privacy);
            self.runtime_sources.insert(source.source_id);
        }
        for (source, event_types) in runtime.event_types_by_source {
            let entry = self.event_types_by_source.entry(source).or_default();
            for event_type in event_types {
                self.event_types.insert(event_type.clone());
                entry.insert(event_type);
            }
        }
    }
}

#[derive(Debug)]
struct RuntimeCompletionVocabulary {
    sources: Vec<RuntimeSourceCompletion>,
    event_types_by_source: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug)]
struct RuntimeSourceCompletion {
    source_id: String,
    description: String,
    privacy: String,
}

impl RuntimeCompletionVocabulary {
    async fn load(client: &GatewayClient) -> Result<Self> {
        let envelope = client.sources_status_view().await?;
        Ok(Self::from_source_status(envelope.payload))
    }

    fn from_source_status(view: SourceCoverageListView) -> Self {
        let mut sources = Vec::new();
        let mut event_types_by_source = BTreeMap::new();
        for source in view.sources {
            event_types_by_source.insert(
                source.source_id.clone(),
                source.event_types.iter().cloned().collect::<BTreeSet<_>>(),
            );
            sources.push(RuntimeSourceCompletion {
                source_id: source.source_id.clone(),
                description: format!(
                    "{} event(s), {} material(s), {:?}",
                    source.event_count, source.material_count, source.continuity
                ),
                privacy: format!("{}/{}", source.privacy.tier, source.privacy.context),
            });
        }
        Self {
            sources,
            event_types_by_source,
        }
    }
}

fn active_token(line: &str, cursor: usize) -> &str {
    let cursor = cursor.min(line.len());
    let prefix = &line[..cursor];
    prefix.rsplit_once(char::is_whitespace).map_or(prefix, |(_, token)| token)
}

fn active_token_start(line: &str, cursor: usize, active: &str) -> usize {
    let cursor = cursor.min(line.len());
    cursor.saturating_sub(active.len())
}

fn selected_value(line: &str, cursor: usize, key: &str) -> Option<String> {
    let cursor = cursor.min(line.len());
    line[..cursor].split_whitespace().find_map(|token| {
        token.strip_prefix(key)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn build_candidates(
    line: &str,
    cursor: usize,
    active: &str,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidate> {
    let source = selected_value(line, cursor, "source:");
    let event_type = selected_value(line, cursor, "type:")
        .or_else(|| selected_value(line, cursor, "event_type:"));
    let replace_start = active_token_start(line, cursor, active);
    let replace_end = cursor.min(line.len());

    let mut candidates = if let Some(prefix) = active.strip_prefix("source:") {
        source_candidates(prefix, replace_start, replace_end, vocabulary)
    } else if let Some(prefix) = active
        .strip_prefix("type:")
        .or_else(|| active.strip_prefix("event_type:"))
    {
        event_type_candidates(prefix, replace_start, replace_end, source.as_deref(), vocabulary)
    } else if let Some(prefix) = active.strip_prefix("payload.") {
        payload_key_candidates(
            prefix,
            replace_start,
            replace_end,
            source.as_deref(),
            event_type.as_deref(),
            vocabulary,
        )
    } else if command_context(line, cursor).as_deref() == Some("ops dlq") {
        ops_dlq_candidates(active, replace_start, replace_end)
    } else {
        grammar_candidates(active, replace_start, replace_end)
    };

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.value.cmp(&right.value))
    });
    candidates.truncate(50);
    candidates
}

fn source_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidate> {
    vocabulary
        .sources
        .iter()
        .filter(|source| source.starts_with(prefix))
        .map(|source| {
            let value = format!("source:{source}");
            CompletionCandidate::new(
                value.clone(),
                "query-field-value",
                "Sources",
                vocabulary
                    .source_descriptions
                    .get(source)
                    .cloned()
                    .unwrap_or_else(|| "payload inventory source".to_string()),
                replace_start,
                replace_end,
                if prefix.is_empty() { 80 } else { 100 },
            )
            .source(source)
            .privacy(
                vocabulary
                    .source_privacy
                    .get(source)
                    .cloned()
                    .unwrap_or_else(|| "metadata-only".to_string()),
            )
            .stale(!vocabulary.runtime_sources.contains(source))
            .preview(format!("sinexctl events query {value}"))
        })
        .collect()
}

fn event_type_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
    source: Option<&str>,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidate> {
    let event_types: Box<dyn Iterator<Item = &String> + '_> = if let Some(source) = source {
        Box::new(
            vocabulary
                .event_types_by_source
                .get(source)
                .into_iter()
                .flat_map(|event_types| event_types.iter()),
        )
    } else {
        Box::new(vocabulary.event_types.iter())
    };

    event_types
        .filter(|event_type| event_type.starts_with(prefix))
        .map(|event_type| {
            let value = format!("type:{event_type}");
            let mut candidate = CompletionCandidate::new(
                value.clone(),
                "query-field-value",
                "Event types",
                source.map_or_else(
                    || "payload inventory event type".to_string(),
                    |source| format!("event type for {source}"),
                ),
                replace_start,
                replace_end,
                if source.is_some() { 110 } else { 90 },
            )
            .preview(format!("sinexctl events query {value}"));
            if let Some(source) = source {
                candidate = candidate.source(source);
            }
            candidate
        })
        .collect()
}

fn payload_key_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
    source: Option<&str>,
    event_type: Option<&str>,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidate> {
    let Some(source) = source else {
        return Vec::new();
    };
    let Some(event_type) = event_type else {
        return Vec::new();
    };
    vocabulary
        .payload_keys_by_pair
        .get(&(source.to_string(), event_type.to_string()))
        .into_iter()
        .flat_map(|keys| keys.iter())
        .filter(|key| key.starts_with(prefix))
        .map(|key| {
            CompletionCandidate::new(
                format!("payload.{key}"),
                "payload-key",
                "Payload keys",
                format!("{source}/{event_type} payload key"),
                replace_start,
                replace_end,
                120,
            )
            .source(source)
            .preview(format!(
                "sinexctl events query source:{source} type:{event_type} payload.{key}:"
            ))
        })
        .collect()
}

fn grammar_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
) -> Vec<CompletionCandidate> {
    const ROOTS: &[(&str, &str)] = &[
        ("events", "event query, inspect, trace, watch, and annotate"),
        ("sources", "source material inventory and readiness"),
        ("runtime", "module lifecycle and automata status"),
        ("ops", "operation records and jobs"),
        ("privacy", "private mode and policy posture"),
        ("tasks", "task projection and lifecycle"),
        ("record", "manual canonical health and task records"),
        ("docs", "document search, retrieval, and chunk browsing"),
        ("semantic", "semantic epochs and shadow-lane inspection"),
        ("metrics", "telemetry, throughput, and reports"),
        ("config", "local preferences and runtime targets"),
        ("tui", "interactive operator workbench"),
    ];
    const TERMS: &[(&str, &str)] = &[
        ("source:", "filter by event source"),
        ("type:", "filter by event type"),
        ("since:", "filter by relative or absolute start time"),
        ("until:", "filter by relative or absolute end time"),
        ("id:", "select an event/material/operation/task id"),
        ("payload.", "complete payload keys after source/type"),
    ];

    ROOTS
        .iter()
        .map(|(value, description)| ("command-root", "Commands", *value, *description, 70))
        .chain(
            TERMS
                .iter()
                .map(|(value, description)| ("query-token", "Query grammar", *value, *description, 85)),
        )
        .filter(|(_, _, value, _, _)| value.starts_with(prefix))
        .map(|(kind, group, value, description, score)| {
            CompletionCandidate::new(
                value,
                kind,
                group,
                description,
                replace_start,
                replace_end,
                score,
            )
        })
        .collect()
}

fn ops_dlq_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
) -> Vec<CompletionCandidate> {
    const ACTIONS: &[(&str, &str, &str, u16)] = &[
        ("list", "read DLQ entries", "none", 100),
        ("peek", "inspect one DLQ entry", "none", 95),
        ("requeue", "requeue DLQ entries", "write", 75),
        ("purge", "delete DLQ entries", "destructive", 50),
    ];
    ACTIONS
        .iter()
        .filter(|(value, _, _, _)| value.starts_with(prefix))
        .map(|(value, description, danger, score)| {
            CompletionCandidate::new(
                *value,
                "subcommand",
                "DLQ actions",
                *description,
                replace_start,
                replace_end,
                *score,
            )
            .danger(*danger)
            .preview(format!("sinexctl ops dlq {value}"))
        })
        .collect()
}

fn command_context(line: &str, cursor: usize) -> Option<String> {
    let cursor = cursor.min(line.len());
    let mut tokens = line[..cursor]
        .split_whitespace()
        .map(|token| token.to_string())
        .collect::<Vec<_>>();
    if line[..cursor]
        .chars()
        .next_back()
        .is_some_and(char::is_whitespace)
    {
        tokens.push(String::new());
    }
    let active_is_incomplete = tokens.last().is_some_and(|token| !token.is_empty());
    if active_is_incomplete {
        tokens.pop();
    }
    match tokens.as_slice() {
        [root, ops, dlq, ..] if root == "sinexctl" && ops == "ops" && dlq == "dlq" => {
            Some("ops dlq".to_string())
        }
        [ops, dlq, ..] if ops == "ops" && dlq == "dlq" => Some("ops dlq".to_string()),
        _ => None,
    }
}

impl CompletionCandidate {
    fn new(
        value: impl Into<String>,
        kind: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        replace_start: usize,
        replace_end: usize,
        score: u16,
    ) -> Self {
        let value = value.into();
        Self {
            insert: value.clone(),
            display: value.clone(),
            value,
            replace_start,
            replace_end,
            kind: kind.into(),
            group: group.into(),
            description: description.into(),
            source: None,
            stale: false,
            danger: "none".to_string(),
            privacy: "metadata-only".to_string(),
            preview: None,
            score,
        }
    }

    fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    fn stale(mut self, stale: bool) -> Self {
        self.stale = stale;
        self
    }

    fn danger(mut self, danger: impl Into<String>) -> Self {
        self.danger = danger.into();
        self
    }

    fn privacy(mut self, privacy: impl Into<String>) -> Self {
        self.privacy = privacy.into();
        self
    }

    fn preview(mut self, preview: impl Into<String>) -> Self {
        self.preview = Some(preview.into());
        self
    }
}

fn payload_schema_keys(payload: &'static PayloadInfo) -> BTreeSet<String> {
    let Ok(schema) = (payload.schema_fn)() else {
        return BTreeSet::new();
    };
    schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| properties.keys().cloned().collect())
        .unwrap_or_default()
}

fn print_completion_table(response: &CompletionResponse) {
    if response.candidates.is_empty() {
        println!("No completion candidates.");
        return;
    }
    for candidate in &response.candidates {
        println!(
            "{:<36} {:<18} {}",
            candidate.value, candidate.kind, candidate.description
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    async fn response(line: &str) -> CompletionResponse {
        let cmd = CompletionEndpointCommand {
            line: line.to_string(),
            cursor: line.len(),
        };
        cmd.complete(None).await
    }

    #[sinex_test]
    async fn source_completion_uses_inventory_without_gateway() -> TestResult<()> {
        let response = response("sinexctl events source:wm").await;
        let candidate = response
            .candidates
            .iter()
            .find(|candidate| candidate.value == "source:wm.hyprland")
            .expect("wm source should be available from static payload inventory");
        assert_eq!(candidate.insert, "source:wm.hyprland");
        assert_eq!(candidate.replace_start, "sinexctl events ".len());
        assert_eq!(candidate.replace_end, "sinexctl events source:wm".len());
        assert_eq!(candidate.source.as_deref(), Some("wm.hyprland"));
        assert!(candidate.stale, "static inventory candidates are stale fallback data");
        assert_eq!(candidate.danger, "none");
        assert!(candidate.preview.as_deref().is_some_and(|preview| preview.contains("source:wm.hyprland")));
        Ok(())
    }

    #[sinex_test]
    async fn event_type_completion_is_narrowed_by_source() -> TestResult<()> {
        let response = response("sinexctl events source:wm.hyprland type:win").await;
        assert!(
            response
                .candidates
                .iter()
                .any(|candidate| candidate.value == "type:window.focused")
        );
        assert!(
            response
                .candidates
                .iter()
                .all(|candidate| candidate.value != "type:file.created"),
            "source-filtered type completion must not include unrelated event types"
        );
        Ok(())
    }

    #[sinex_test]
    async fn payload_key_completion_requires_source_and_type() -> TestResult<()> {
        let response =
            response("sinexctl events source:wm.hyprland type:window.focused payload.").await;
        assert!(
            response
                .candidates
                .iter()
                .any(|candidate| candidate.value.starts_with("payload."))
        );
        Ok(())
    }

    #[sinex_test]
    async fn grammar_completion_omits_removed_shortcut_roots() -> TestResult<()> {
        let response = response("sinexctl ").await;
        let values: BTreeSet<&str> = response
            .candidates
            .iter()
            .map(|candidate| candidate.value.as_str())
            .collect();
        for removed in [
            "query",
            "recent",
            "errors",
            "watch",
            "timeline",
            "explain",
            "trace",
            "annotate",
            "modules",
            "automata",
            "throughput",
            "telemetry",
            "report",
            "relations",
            "audit",
            "blob",
            "state",
            "admin",
            "declare",
            "curation",
            "llm",
            "instructions",
            "documents",
            "semantics",
            "completions",
        ] {
            assert!(
                !values.contains(removed),
                "removed root `{removed}` must not be suggested"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn grammar_completion_includes_record_root() -> TestResult<()> {
        let response = response("sinexctl rec").await;
        assert!(
            response
                .candidates
                .iter()
                .any(|candidate| candidate.value == "record"),
            "canonical record root must be suggested: {response:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn ops_dlq_completion_marks_destructive_actions() -> TestResult<()> {
        let response = response("sinexctl ops dlq p").await;
        let purge = response
            .candidates
            .iter()
            .find(|candidate| candidate.value == "purge")
            .expect("DLQ purge should be suggested in ops dlq context");
        assert_eq!(purge.kind, "subcommand");
        assert_eq!(purge.danger, "destructive");
        assert_eq!(purge.replace_start, "sinexctl ops dlq ".len());
        assert_eq!(purge.replace_end, "sinexctl ops dlq p".len());
        assert_eq!(purge.preview.as_deref(), Some("sinexctl ops dlq purge"));
        Ok(())
    }
}
