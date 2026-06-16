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
    pub kind: String,
    pub group: String,
    pub description: String,
    pub danger: String,
    pub privacy: String,
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
            self.source_privacy.insert(source.source_id, source.privacy);
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

    let mut candidates = if let Some(prefix) = active.strip_prefix("source:") {
        source_candidates(prefix, vocabulary)
    } else if let Some(prefix) = active
        .strip_prefix("type:")
        .or_else(|| active.strip_prefix("event_type:"))
    {
        event_type_candidates(prefix, source.as_deref(), vocabulary)
    } else if let Some(prefix) = active.strip_prefix("payload.") {
        payload_key_candidates(prefix, source.as_deref(), event_type.as_deref(), vocabulary)
    } else {
        grammar_candidates(active)
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

fn source_candidates(prefix: &str, vocabulary: &CompletionVocabulary) -> Vec<CompletionCandidate> {
    vocabulary
        .sources
        .iter()
        .filter(|source| source.starts_with(prefix))
        .map(|source| CompletionCandidate {
            value: format!("source:{source}"),
            kind: "query-field-value".to_string(),
            group: "Sources".to_string(),
            description: vocabulary
                .source_descriptions
                .get(source)
                .cloned()
                .unwrap_or_else(|| "payload inventory source".to_string()),
            danger: "none".to_string(),
            privacy: vocabulary
                .source_privacy
                .get(source)
                .cloned()
                .unwrap_or_else(|| "metadata-only".to_string()),
            score: if prefix.is_empty() { 80 } else { 100 },
        })
        .collect()
}

fn event_type_candidates(
    prefix: &str,
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
        .map(|event_type| CompletionCandidate {
            value: format!("type:{event_type}"),
            kind: "query-field-value".to_string(),
            group: "Event types".to_string(),
            description: source.map_or_else(
                || "payload inventory event type".to_string(),
                |source| format!("event type for {source}"),
            ),
            danger: "none".to_string(),
            privacy: "metadata-only".to_string(),
            score: if source.is_some() { 110 } else { 90 },
        })
        .collect()
}

fn payload_key_candidates(
    prefix: &str,
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
        .map(|key| CompletionCandidate {
            value: format!("payload.{key}"),
            kind: "payload-key".to_string(),
            group: "Payload keys".to_string(),
            description: format!("{source}/{event_type} payload key"),
            danger: "none".to_string(),
            privacy: "metadata-only".to_string(),
            score: 120,
        })
        .collect()
}

fn grammar_candidates(prefix: &str) -> Vec<CompletionCandidate> {
    const ROOTS: &[(&str, &str)] = &[
        ("events", "event query, inspect, trace, watch, and annotate"),
        ("sources", "source material inventory and readiness"),
        ("runtime", "module lifecycle and automata status"),
        ("ops", "operation records and jobs"),
        ("privacy", "private mode and policy posture"),
        ("tasks", "task projection and lifecycle"),
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
        .map(|(kind, group, value, description, score)| CompletionCandidate {
            value: value.to_string(),
            kind: kind.to_string(),
            group: group.to_string(),
            description: description.to_string(),
            danger: "none".to_string(),
            privacy: "metadata-only".to_string(),
            score,
        })
        .collect()
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
        assert!(
            response
                .candidates
                .iter()
                .any(|candidate| candidate.value == "source:wm.hyprland")
        );
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
        ] {
            assert!(
                !values.contains(removed),
                "removed root `{removed}` must not be suggested"
            );
        }
        Ok(())
    }
}
