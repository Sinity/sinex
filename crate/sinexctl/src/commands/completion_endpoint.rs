use std::collections::{BTreeMap, BTreeSet};

use clap::Args;
use sinex_primitives::events::schema_registry::{PayloadInfo, get_all_payloads};
use sinex_primitives::query_units::{QueryUnitId, query_unit_descriptor, query_unit_descriptors};
use sinex_primitives::views::{
    CompletionCandidateView, CompletionResponseView, SourceCoverageListView,
};

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

    async fn complete(&self, client: Option<&GatewayClient>) -> CompletionResponseView {
        let active_token = active_token(&self.line, self.cursor).to_string();
        let mut vocabulary = CompletionVocabulary::from_payload_inventory();
        if let Some(client) = client
            && let Ok(runtime) = RuntimeCompletionVocabulary::load(client).await
        {
            vocabulary.merge_runtime(runtime);
        }

        let candidates = build_candidates(&self.line, self.cursor, &active_token, &vocabulary);
        CompletionResponseView::new(self.line.clone(), self.cursor, active_token, candidates)
    }
}

#[derive(Debug, Default)]
struct CompletionVocabulary {
    sources: BTreeSet<String>,
    event_types: BTreeSet<String>,
    event_types_by_source: BTreeMap<String, BTreeSet<String>>,
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
    prefix
        .rsplit_once(char::is_whitespace)
        .map_or(prefix, |(_, token)| token)
}

fn active_token_start(line: &str, cursor: usize, active: &str) -> usize {
    let cursor = cursor.min(line.len());
    cursor.saturating_sub(active.len())
}

fn build_candidates(
    line: &str,
    cursor: usize,
    active: &str,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidateView> {
    let replace_start = active_token_start(line, cursor, active);
    let replace_end = cursor.min(line.len());

    let mut candidates = if command_context(line, cursor).as_deref() == Some("ops dlq") {
        ops_dlq_candidates(line, cursor, active, replace_start, replace_end)
    } else if command_context(line, cursor).as_deref() == Some("query") {
        query_unit_candidates(line, cursor, active, replace_start, replace_end, vocabulary)
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

fn query_source_value_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidateView> {
    let prefix = prefix.trim_matches('"');
    vocabulary
        .sources
        .iter()
        .filter(|source| source.starts_with(prefix))
        .map(|source| {
            let value = format!("\"{source}\"");
            CompletionCandidateView::new(
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
            .preview(format!("sinexctl query 'events where source = {value}'"))
        })
        .collect()
}

fn query_event_type_value_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
    source: Option<&str>,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidateView> {
    let prefix = prefix.trim_matches('"');
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
            let value = format!("\"{event_type}\"");
            let mut candidate = CompletionCandidateView::new(
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
            .preview(format!(
                "sinexctl query 'events where event_type = {value}'"
            ));
            if let Some(source) = source {
                candidate = candidate.source(source);
            }
            candidate
        })
        .collect()
}

fn grammar_candidates(
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
) -> Vec<CompletionCandidateView> {
    const ROOTS: &[(&str, &str)] = &[
        (
            "query",
            "shared query units over events, sources, debt, ops, and runtime",
        ),
        ("events", "event query, inspect, trace, watch, and annotate"),
        ("sources", "source material inventory and readiness"),
        ("show", "resolve and inspect one public Sinex object ref"),
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
        ("where", "start a descriptor-backed query predicate"),
        ("sort", "start descriptor-backed query ordering"),
        ("limit", "bound finite query results"),
    ];

    ROOTS
        .iter()
        .map(|(value, description)| ("command-root", "Commands", *value, *description, 70))
        .chain(
            TERMS.iter().map(|(value, description)| {
                ("query-token", "Query grammar", *value, *description, 85)
            }),
        )
        .filter(|(_, _, value, _, _)| value.starts_with(prefix))
        .map(|(kind, group, value, description, score)| {
            CompletionCandidateView::new(
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

fn query_unit_candidates(
    line: &str,
    cursor: usize,
    active: &str,
    replace_start: usize,
    replace_end: usize,
    vocabulary: &CompletionVocabulary,
) -> Vec<CompletionCandidateView> {
    let cursor = cursor.min(line.len());
    let expression = line[..cursor]
        .strip_prefix("sinexctl query")
        .unwrap_or(&line[..cursor])
        .trim();
    let tokens = expression
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tokens.len() <= 1 && !active.is_empty() {
        return query_unit_descriptors()
            .iter()
            .filter(|descriptor| descriptor.unit.as_str().starts_with(active))
            .map(|descriptor| {
                CompletionCandidateView::new(
                    descriptor.unit.as_str(),
                    "query-unit",
                    "Query units",
                    format!(
                        "{} fields, backed by {}",
                        descriptor.fields.len(),
                        descriptor.backing_rpc_methods.join(", ")
                    ),
                    replace_start,
                    replace_end,
                    100,
                )
                .preview(format!("sinexctl query '{} where '", descriptor.unit))
            })
            .collect();
    }

    let Ok(unit) = tokens[0].parse::<QueryUnitId>() else {
        return Vec::new();
    };
    let descriptor = query_unit_descriptor(unit);
    let lower = active.to_ascii_lowercase();
    let last = tokens.last().map(String::as_str).unwrap_or_default();

    if tokens.len() == 1 {
        return query_clause_candidates(unit, "", replace_start, replace_end);
    }

    if lower.ends_with(" sort") || lower.ends_with(" sort ") || last == "sort" {
        return descriptor
            .sort_keys
            .iter()
            .map(|sort| {
                CompletionCandidateView::new(
                    sort.key,
                    "query-sort-key",
                    "Query sort keys",
                    format!("sort key for {}", descriptor.unit),
                    replace_start,
                    replace_end,
                    95,
                )
            })
            .collect();
    }

    if tokens.len() >= 2
        && tokens
            .get(tokens.len().saturating_sub(2))
            .is_some_and(|token| token == "sort")
        && descriptor.sort_keys.iter().any(|sort| sort.key == last)
    {
        return ["asc", "desc"]
            .into_iter()
            .map(|direction| {
                CompletionCandidateView::new(
                    direction,
                    "query-sort-direction",
                    "Query sort direction",
                    "sort direction",
                    replace_start,
                    replace_end,
                    90,
                )
            })
            .collect();
    }

    if lower.ends_with(" where") || lower.ends_with(" and") || lower.ends_with(" or") {
        return descriptor
            .fields
            .iter()
            .map(|field| {
                CompletionCandidateView::new(
                    field.name,
                    "query-field",
                    "Query fields",
                    format!("{:?} field", field.field_type),
                    replace_start,
                    replace_end,
                    95,
                )
            })
            .collect();
    }

    if let Some(field) = descriptor.fields.iter().find(|field| field.name == last) {
        return field
            .operators
            .iter()
            .map(|operator| {
                CompletionCandidateView::new(
                    operator.as_str(),
                    "query-operator",
                    "Query operators",
                    format!("operator for {}", field.name),
                    replace_start,
                    replace_end,
                    90,
                )
            })
            .collect();
    }

    if let Some(field_name) = tokens.get(tokens.len().saturating_sub(3))
        && let Some(field) = descriptor
            .fields
            .iter()
            .find(|field| field.name == field_name)
        && !field.enum_values.is_empty()
    {
        return field
            .enum_values
            .iter()
            .filter(|value| value.starts_with(last))
            .map(|value| {
                CompletionCandidateView::new(
                    *value,
                    "query-enum",
                    "Query enum values",
                    format!("{} value", field.name),
                    replace_start,
                    replace_end,
                    88,
                )
            })
            .collect();
    }

    if let Some(field_name) = tokens.get(tokens.len().saturating_sub(3)) {
        match field_name.as_str() {
            "source" if unit == QueryUnitId::Events => {
                return query_source_value_candidates(
                    active,
                    replace_start,
                    replace_end,
                    vocabulary,
                );
            }
            "event_type" if unit == QueryUnitId::Events => {
                let source = selected_query_value(&tokens, "source");
                return query_event_type_value_candidates(
                    active,
                    replace_start,
                    replace_end,
                    source.as_deref(),
                    vocabulary,
                );
            }
            _ => {}
        }
    }

    descriptor
        .fields
        .iter()
        .filter(|field| field.name.starts_with(last))
        .map(|field| {
            CompletionCandidateView::new(
                field.name,
                "query-field",
                "Query fields",
                format!("{:?} field", field.field_type),
                replace_start,
                replace_end,
                85,
            )
        })
        .chain(query_clause_candidates(
            unit,
            last,
            replace_start,
            replace_end,
        ))
        .collect()
}

fn query_clause_candidates(
    unit: QueryUnitId,
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
) -> Vec<CompletionCandidateView> {
    let mut clauses = vec![
        (
            "where",
            "query-clause",
            "start a descriptor-backed predicate",
            88,
        ),
        ("limit", "query-clause", "bound finite query results", 82),
    ];
    if !query_unit_descriptor(unit).sort_keys.is_empty() {
        clauses.push((
            "sort",
            "query-clause",
            "start descriptor-backed ordering",
            84,
        ));
    }
    if unit == QueryUnitId::Events {
        clauses.push(("since", "query-clause", "filter recent events", 87));
        clauses.push(("after", "query-cursor", "page after an event id cursor", 86));
        clauses.push((
            "before",
            "query-cursor",
            "page before an event id cursor",
            86,
        ));
    }

    clauses
        .into_iter()
        .filter(|(value, _, _, _)| value.starts_with(prefix))
        .map(|(value, kind, description, score)| {
            CompletionCandidateView::new(
                value,
                kind,
                "Query clauses",
                description,
                replace_start,
                replace_end,
                score,
            )
        })
        .collect()
}

fn selected_query_value(tokens: &[String], field: &str) -> Option<String> {
    tokens.windows(3).find_map(|window| {
        (window[0] == field && window[1] == "=")
            .then(|| window[2].trim_matches('"').trim_matches('\'').to_string())
    })
}

fn ops_dlq_candidates(
    line: &str,
    cursor: usize,
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
) -> Vec<CompletionCandidateView> {
    if let Some(subcommand) = ops_dlq_subcommand(line, cursor) {
        return ops_dlq_option_candidates(&subcommand, prefix, replace_start, replace_end);
    }

    const ACTIONS: &[(&str, &str, &str, u16)] = &[
        ("list", "read DLQ entries", "none", 100),
        ("peek", "inspect one DLQ entry", "none", 95),
        ("triage", "summarize DLQ buckets", "none", 90),
        ("cleanup-plan", "plan safe DLQ cleanup", "none", 85),
        ("requeue", "requeue DLQ entries", "write", 75),
        ("purge", "delete DLQ entries", "destructive", 50),
    ];
    ACTIONS
        .iter()
        .filter(|(value, _, _, _)| value.starts_with(prefix))
        .map(|(value, description, danger, score)| {
            CompletionCandidateView::new(
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

fn ops_dlq_subcommand(line: &str, cursor: usize) -> Option<String> {
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
    if tokens.last().is_some_and(|token| !token.is_empty()) {
        tokens.pop();
    }
    match tokens.as_slice() {
        [root, ops, dlq, subcommand, ..] if root == "sinexctl" && ops == "ops" && dlq == "dlq" => {
            Some(subcommand.clone())
        }
        [ops, dlq, subcommand, ..] if ops == "ops" && dlq == "dlq" => Some(subcommand.clone()),
        _ => None,
    }
}

fn ops_dlq_option_candidates(
    subcommand: &str,
    prefix: &str,
    replace_start: usize,
    replace_end: usize,
) -> Vec<CompletionCandidateView> {
    let options: &[(&str, &str, &str, u16)] = match subcommand {
        "peek" => &[
            ("--tail", "inspect newest retained DLQ messages", "none", 95),
            (
                "--start-sequence",
                "start peeking at a DLQ stream sequence",
                "none",
                90,
            ),
            ("--limit", "number of messages to peek", "none", 85),
            (
                "--payload-preview-chars",
                "maximum sanitized payload preview characters",
                "none",
                80,
            ),
        ],
        "triage" | "cleanup-plan" => &[
            (
                "--all-retained",
                "inspect the full retained DLQ sequence span",
                "none",
                100,
            ),
            (
                "--tail",
                "number of newest retained messages to inspect",
                "none",
                90,
            ),
        ],
        "purge" => &[
            (
                "--start-sequence",
                "inclusive first DLQ stream sequence to delete",
                "destructive",
                85,
            ),
            (
                "--end-sequence",
                "inclusive last DLQ stream sequence to delete",
                "destructive",
                84,
            ),
            (
                "--confirm",
                "confirm destructive DLQ purge",
                "destructive",
                70,
            ),
        ],
        "requeue" => &[
            ("--event-id", "specific event ID to requeue", "write", 85),
            (
                "--start-sequence",
                "first DLQ stream sequence to requeue",
                "write",
                82,
            ),
            (
                "--end-sequence",
                "last DLQ stream sequence to requeue",
                "write",
                81,
            ),
            ("--all", "requeue all DLQ messages", "write", 70),
        ],
        _ => &[],
    };

    options
        .iter()
        .filter(|(value, _, _, _)| value.starts_with(prefix))
        .map(|(value, description, danger, score)| {
            CompletionCandidateView::new(
                *value,
                "option",
                "DLQ options",
                *description,
                replace_start,
                replace_end,
                *score,
            )
            .danger(*danger)
            .preview(format!("sinexctl ops dlq {subcommand} {value}"))
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
        [root, query, ..] if root == "sinexctl" && query == "query" => Some("query".to_string()),
        [query, ..] if query == "query" => Some("query".to_string()),
        [root, ops, dlq, ..] if root == "sinexctl" && ops == "ops" && dlq == "dlq" => {
            Some("ops dlq".to_string())
        }
        [ops, dlq, ..] if ops == "ops" && dlq == "dlq" => Some("ops dlq".to_string()),
        _ => None,
    }
}

fn print_completion_table(response: &CompletionResponseView) {
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
#[path = "completion_endpoint_test.rs"]
mod tests;
