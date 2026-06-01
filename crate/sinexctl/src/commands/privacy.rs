use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde::Serialize;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::Provenance;
use sinex_primitives::privacy::{PrivateModeReasonClass, RuntimePrivateModeState};
use sinex_primitives::query::{
    Cursor, EventQuery, EventQueryResult, PayloadFilter, QueryResultEvent, SortDirection, TimeRange,
};
use sinex_primitives::rpc::dlq::DlqListResponse;
use sinex_primitives::rpc::privacy::{
    PrivacyPolicyAddDictionaryTermRequest, PrivacyPolicyBindRuleRequest,
    PrivacyPolicyCreateBackendRequest, PrivacyPolicyCreateDictionaryRequest,
    PrivacyPolicyCreateKeyRequest, PrivacyPolicyCreateRuleRequest, PrivacyPolicyIdResponse,
    PrivacyPolicyListResponse, PrivacyPolicySeedCatalogResponse,
};
use sinex_primitives::rpc::sources::{
    CaveatSeverity, SourceCaveat, SourceReadiness, SourceReadinessStatus,
    SourcesReadinessListRequest, SourcesReadinessListResponse,
};
use sinex_primitives::temporal::Timestamp;
use std::path::PathBuf;

use crate::validation::parse_time_input;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl privacy private-mode status -f json
    sinexctl privacy private-mode enable --actor sinity --source-class desktop
    sinexctl privacy private-mode disable
    sinexctl privacy policy list
    sinexctl privacy policy create-backend --name presidio-local --kind presidio
    sinexctl privacy policy create-key --name local-pii
    sinexctl privacy audit
    sinexctl privacy export --since 24h --source terminal --output privacy-export.json -f json
")]
pub struct PrivacyCommand {
    #[command(subcommand)]
    cmd: PrivacySubcommand,
}

#[derive(Debug, Subcommand)]
enum PrivacySubcommand {
    /// Query or toggle runtime private mode.
    PrivateMode {
        #[command(subcommand)]
        cmd: PrivateModeCommand,
    },

    /// Inspect DB-backed privacy policy state.
    Policy {
        #[command(subcommand)]
        cmd: PrivacyPolicyCommand,
    },

    /// Summarize current privacy posture from private-mode, DLQ, and source readiness.
    Audit(PrivacyAuditArgs),

    /// Export event metadata without raw payloads, snippets, or source-material bytes.
    Export(PrivacyExportArgs),
}

#[derive(Debug, Subcommand)]
enum PrivateModeCommand {
    /// Show the gateway-observed private-mode state.
    Status,

    /// Enable runtime private mode.
    Enable(PrivateModeEnableArgs),

    /// Disable runtime private mode.
    Disable,
}

#[derive(Debug, Subcommand)]
enum PrivacyPolicyCommand {
    /// List DB-backed rules, field bindings, recognizers, dictionaries, and keys.
    List(PrivacyPolicyListArgs),

    /// Create a DB-backed dictionary.
    CreateDictionary(PrivacyPolicyCreateDictionaryArgs),

    /// Register an external or local recognizer backend.
    CreateBackend(PrivacyPolicyCreateBackendArgs),

    /// Register an encryption/hash key namespace.
    CreateKey(PrivacyPolicyCreateKeyArgs),

    /// Add a term to a DB-backed dictionary.
    AddDictionaryTerm(PrivacyPolicyAddDictionaryTermArgs),

    /// Create a DB-backed recognizer rule.
    CreateRule(PrivacyPolicyCreateRuleArgs),

    /// Bind a rule to an event field path or global scope.
    BindRule(PrivacyPolicyBindRuleArgs),

    /// Seed the DB policy table from the built-in catalog.
    SeedCatalog,
}

#[derive(Debug, Args)]
struct PrivateModeEnableArgs {
    /// Coarse actor label to persist.
    #[arg(long, default_value = "operator")]
    actor: String,

    /// Coarse reason class. Avoid detailed reasons that weaken deniability.
    #[arg(long, default_value = "operator_private")]
    reason_class: PrivateModeReasonClass,

    /// Source class covered by private mode. Repeatable; omit for all classes.
    #[arg(long = "source-class")]
    source_classes: Vec<String>,

    /// Optional RFC3339 expiry. Expired private-mode state is treated as disabled.
    #[arg(long = "expires-at")]
    expires_at: Option<String>,
}

#[derive(Debug, Args)]
struct PrivacyPolicyListArgs {
    /// Include disabled rules, recognizers, dictionaries, and terms.
    #[arg(long)]
    include_disabled: bool,
}

#[derive(Debug, Args)]
struct PrivacyPolicyCreateDictionaryArgs {
    #[arg(long)]
    name: String,

    #[arg(long, default_value = "")]
    description: String,

    #[arg(long)]
    language: Option<String>,

    #[arg(long = "source-kind", default_value = "user")]
    source_kind: String,

    #[arg(long)]
    tag: Vec<String>,
}

#[derive(Debug, Args)]
struct PrivacyPolicyCreateBackendArgs {
    #[arg(long)]
    name: String,

    #[arg(long)]
    kind: String,

    #[arg(long = "endpoint-url")]
    endpoint_url: Option<String>,

    #[arg(long = "config-json", default_value = "{}")]
    config_json: serde_json::Value,
}

#[derive(Debug, Args)]
struct PrivacyPolicyCreateKeyArgs {
    #[arg(long)]
    name: String,

    #[arg(long, default_value = "")]
    description: String,
}

#[derive(Debug, Args)]
struct PrivacyPolicyAddDictionaryTermArgs {
    #[arg(long = "dictionary-id")]
    dictionary_id: Uuid,

    #[arg(long)]
    term: String,
}

#[derive(Debug, Args)]
struct PrivacyPolicyCreateRuleArgs {
    #[arg(long)]
    name: String,

    #[arg(long, default_value = "")]
    description: String,

    #[arg(long = "recognizer-kind", default_value = "local_pattern")]
    recognizer_kind: String,

    #[arg(long = "recognizer-backend-id")]
    recognizer_backend_id: Option<Uuid>,

    #[arg(long = "matcher-type")]
    matcher_type: String,

    #[arg(long = "matcher-value")]
    matcher_value: String,

    #[arg(long = "matcher-config-json", default_value = "{}")]
    matcher_config_json: serde_json::Value,

    #[arg(long = "case-sensitive")]
    case_sensitive: bool,

    #[arg(long)]
    action: String,

    #[arg(long = "action-label")]
    action_label: Option<String>,

    #[arg(long = "key-namespace", default_value = "default")]
    key_namespace: String,
}

#[derive(Debug, Args)]
struct PrivacyPolicyBindRuleArgs {
    #[arg(long = "rule-name")]
    rule_name: String,

    #[arg(long = "event-source")]
    event_source: Option<String>,

    #[arg(long = "event-type")]
    event_type: Option<String>,

    #[arg(long = "field-path")]
    field_path: Option<String>,

    #[arg(long, default_value_t = 0)]
    priority: i32,
}

#[derive(Debug, Args)]
struct PrivacyAuditArgs {
    /// Optional source family filter (e.g. "terminal", "browser", "chat").
    #[arg(long)]
    source_family: Option<String>,

    /// Treat last-success older than this many seconds as `Stale`.
    /// Defaults to the gateway readiness default.
    #[arg(long = "stale-after-seconds")]
    stale_after_seconds: Option<i64>,
}

#[derive(Debug, Args)]
struct PrivacyExportArgs {
    /// Filter by source. Repeatable; omit to include all sources.
    #[arg(long)]
    source: Vec<EventSource>,

    /// Filter by event type. Repeatable; omit to include all event types.
    #[arg(long)]
    event_type: Vec<EventType>,

    /// Time range start: "1h", "2d", "2026-05-19", or RFC3339.
    #[arg(long, short = 's')]
    since: Option<String>,

    /// Time range end. Defaults to open-ended.
    #[arg(long, short = 'u')]
    until: Option<String>,

    /// Free-text search used only for selecting events; snippets are not exported.
    #[arg(short = 'q', long)]
    query: Option<String>,

    /// Maximum number of event envelopes to export.
    #[arg(long, short = 'n', default_value_t = 100)]
    limit: i64,

    /// Write the sanitized export artifact to this path instead of stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

impl PrivacyCommand {
    #[must_use]
    pub fn command_path(&self) -> &'static str {
        match &self.cmd {
            PrivacySubcommand::PrivateMode { cmd } => match cmd {
                PrivateModeCommand::Status => "privacy private-mode status",
                PrivateModeCommand::Enable(_) => "privacy private-mode enable",
                PrivateModeCommand::Disable => "privacy private-mode disable",
            },
            PrivacySubcommand::Policy { cmd } => match cmd {
                PrivacyPolicyCommand::List(_) => "privacy policy list",
                PrivacyPolicyCommand::CreateBackend(_) => "privacy policy create-backend",
                PrivacyPolicyCommand::CreateKey(_) => "privacy policy create-key",
                PrivacyPolicyCommand::CreateDictionary(_) => "privacy policy create-dictionary",
                PrivacyPolicyCommand::AddDictionaryTerm(_) => "privacy policy add-dictionary-term",
                PrivacyPolicyCommand::CreateRule(_) => "privacy policy create-rule",
                PrivacyPolicyCommand::BindRule(_) => "privacy policy bind-rule",
                PrivacyPolicyCommand::SeedCatalog => "privacy policy seed-catalog",
            },
            PrivacySubcommand::Audit(_) => "privacy audit",
            PrivacySubcommand::Export(_) => "privacy export",
        }
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            PrivacySubcommand::PrivateMode { cmd } => cmd.execute(client, format).await,
            PrivacySubcommand::Policy { cmd } => cmd.execute(client, format).await,
            PrivacySubcommand::Audit(args) => args.execute(client, format).await,
            PrivacySubcommand::Export(args) => args.execute(client, format).await,
        }
    }
}

impl PrivateModeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let state = match self {
            Self::Status => client.private_mode_status().await?.state,
            Self::Enable(args) => {
                client
                    .private_mode_enable(
                        args.actor.clone(),
                        args.reason_class.clone(),
                        args.source_classes.clone(),
                        args.expires_at
                            .as_deref()
                            .map(Timestamp::parse_rfc3339)
                            .transpose()?,
                    )
                    .await?
                    .state
            }
            Self::Disable => client.private_mode_disable().await?.state,
        };

        CommandOutput::single(state, format_private_mode_state).display(&format)?;
        Ok(())
    }
}

impl PrivacyPolicyCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = match self {
            Self::List(args) => {
                let policy = client.privacy_policy_list(args.include_disabled).await?;
                CommandOutput::single(policy, format_privacy_policy_list).display(&format)?;
                return Ok(());
            }
            Self::CreateDictionary(args) => {
                client
                    .privacy_policy_create_dictionary(PrivacyPolicyCreateDictionaryRequest {
                        name: args.name.clone(),
                        description: args.description.clone(),
                        language: args.language.clone(),
                        source_kind: args.source_kind.clone(),
                        tags: args.tag.clone(),
                    })
                    .await?
            }
            Self::CreateBackend(args) => {
                client
                    .privacy_policy_create_backend(PrivacyPolicyCreateBackendRequest {
                        name: args.name.clone(),
                        kind: args.kind.clone(),
                        endpoint_url: args.endpoint_url.clone(),
                        config: args.config_json.clone(),
                    })
                    .await?
            }
            Self::CreateKey(args) => {
                client
                    .privacy_policy_create_key(PrivacyPolicyCreateKeyRequest {
                        name: args.name.clone(),
                        description: args.description.clone(),
                    })
                    .await?
            }
            Self::AddDictionaryTerm(args) => {
                client
                    .privacy_policy_add_dictionary_term(PrivacyPolicyAddDictionaryTermRequest {
                        dictionary_id: args.dictionary_id,
                        term: args.term.clone(),
                        metadata: serde_json::Value::Object(serde_json::Map::new()),
                    })
                    .await?
            }
            Self::CreateRule(args) => {
                client
                    .privacy_policy_create_rule(PrivacyPolicyCreateRuleRequest {
                        name: args.name.clone(),
                        description: args.description.clone(),
                        recognizer_backend_id: args.recognizer_backend_id,
                        recognizer_kind: args.recognizer_kind.clone(),
                        matcher_type: args.matcher_type.clone(),
                        matcher_value: args.matcher_value.clone(),
                        matcher_config: args.matcher_config_json.clone(),
                        case_sensitive: args.case_sensitive,
                        action: args.action.clone(),
                        action_label: args.action_label.clone(),
                        key_namespace: args.key_namespace.clone(),
                    })
                    .await?
            }
            Self::BindRule(args) => {
                client
                    .privacy_policy_bind_rule(PrivacyPolicyBindRuleRequest {
                        rule_name: args.rule_name.clone(),
                        event_source: args.event_source.clone(),
                        event_type: args.event_type.clone(),
                        field_path: args.field_path.clone(),
                        priority: args.priority,
                    })
                    .await?
            }
            Self::SeedCatalog => {
                let response = client.privacy_policy_seed_catalog().await?;
                CommandOutput::single(response, format_privacy_policy_seed_catalog)
                    .display(&format)?;
                return Ok(());
            }
        };
        CommandOutput::single(response, format_privacy_policy_id_response).display(&format)?;
        Ok(())
    }
}

impl PrivacyAuditArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let private_mode = client.private_mode_status().await?.state;
        let dlq = client.dlq_list().await?;
        let readiness = client
            .sources_readiness_list(SourcesReadinessListRequest {
                source_family: self.source_family.clone(),
                stale_after_seconds: self.stale_after_seconds,
            })
            .await?;
        let report = build_privacy_audit_report(private_mode, &dlq, &readiness);
        CommandOutput::single(report, format_privacy_audit_report).display(&format)?;
        Ok(())
    }
}

fn format_privacy_policy_list(policy: &PrivacyPolicyListResponse) -> String {
    let mut lines = vec![
        "Privacy Policy".to_string(),
        format!("Rules: {}", policy.rules.len()),
        format!("Field bindings: {}", policy.field_rules.len()),
        format!("Recognizer backends: {}", policy.recognizer_backends.len()),
        format!(
            "Dictionaries: {} ({} terms)",
            policy.dictionaries.len(),
            policy.dictionary_terms.len()
        ),
        format!("Key namespaces: {}", policy.key_namespaces.len()),
    ];

    if !policy.rules.is_empty() {
        lines.push("Rule detail:".to_string());
        for rule in &policy.rules {
            let action = rule.action_label.as_deref().unwrap_or(rule.action.as_str());
            lines.push(format!(
                "  {} [{}] {}:{} -> {}",
                rule.name, rule.recognizer_kind, rule.matcher_type, rule.matcher_value, action
            ));
        }
    }

    if !policy.recognizer_backends.is_empty() {
        lines.push("Recognizer backend detail:".to_string());
        for backend in &policy.recognizer_backends {
            let endpoint = backend.endpoint_url.as_deref().unwrap_or("local");
            lines.push(format!(
                "  {} [{}] {}",
                backend.name, backend.kind, endpoint
            ));
        }
    }

    if !policy.dictionaries.is_empty() {
        lines.push("Dictionary detail:".to_string());
        for dictionary in &policy.dictionaries {
            let term_count = policy
                .dictionary_terms
                .iter()
                .filter(|term| term.dictionary_id == dictionary.id)
                .count();
            lines.push(format!(
                "  {} [{}] {} terms",
                dictionary.name, dictionary.source_kind, term_count
            ));
        }
    }

    lines.join("\n")
}

fn format_privacy_policy_id_response(response: &PrivacyPolicyIdResponse) -> String {
    format!("Policy object: {}", response.id)
}

fn format_privacy_policy_seed_catalog(response: &PrivacyPolicySeedCatalogResponse) -> String {
    format!("Seeded catalog rules: {}", response.seeded_rules)
}

impl PrivacyExportArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let query = self.to_event_query()?;
        let response = client.query_events(query).await?;
        let report = build_privacy_export_report(response, self.to_export_scope());

        if let Some(path) = &self.output {
            let content = render_privacy_export_report(&report, format)?;
            std::fs::write(path, content)?;
            let receipt = PrivacyExportReceipt {
                output_path: path.display().to_string(),
                exported_events: report.events.len(),
                next_cursor: report.next_cursor.clone(),
                payload_policy: report.payload_policy,
            };
            CommandOutput::single(receipt, format_privacy_export_receipt).display(&format)?;
        } else {
            CommandOutput::single(report, format_privacy_export_report).display(&format)?;
        }

        Ok(())
    }

    fn to_event_query(&self) -> Result<EventQuery> {
        self.ensure_explicit_scope()?;
        let start_time = self.since.as_deref().map(parse_time_input).transpose()?;
        let end_time = self.until.as_deref().map(parse_time_input).transpose()?;
        let time_range = match (start_time, end_time) {
            (None, None) => None,
            (start, end) => Some(TimeRange::new(start, end)?),
        };

        Ok(EventQuery {
            sources: self.source.clone(),
            event_types: self.event_type.clone(),
            time_range,
            payload: self
                .query
                .as_ref()
                .map(|text| PayloadFilter::TextSearch { text: text.clone() }),
            limit: self
                .limit
                .clamp(1, sinex_primitives::query::Pagination::MAX_LIMIT),
            direction: SortDirection::Desc,
            include_total_estimate: true,
            ..Default::default()
        })
    }

    fn ensure_explicit_scope(&self) -> Result<()> {
        if self.source.is_empty()
            && self.event_type.is_empty()
            && self.since.is_none()
            && self.until.is_none()
            && self.query.is_none()
        {
            return Err(eyre!(
                "privacy export requires an explicit scope: pass --source, --event-type, \
                 --since, --until, or --query"
            ));
        }
        Ok(())
    }

    fn to_export_scope(&self) -> PrivacyExportScope {
        PrivacyExportScope {
            sources: self.source.iter().map(ToString::to_string).collect(),
            event_types: self.event_type.iter().map(ToString::to_string).collect(),
            since: self.since.clone(),
            until: self.until.clone(),
            text_search_used: self.query.is_some(),
            limit: self
                .limit
                .clamp(1, sinex_primitives::query::Pagination::MAX_LIMIT),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyAuditReport {
    private_mode: PrivacyAuditPrivateMode,
    dlq: PrivacyAuditDlq,
    sources: PrivacyAuditSources,
    findings: Vec<PrivacyAuditFinding>,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyAuditPrivateMode {
    enabled: bool,
    reason_class: String,
    actor: String,
    started_at: Option<String>,
    source_classes: Vec<String>,
    updated_by_operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyAuditDlq {
    total_messages: u64,
    total_bytes: u64,
    has_backlog: bool,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyAuditSources {
    total: usize,
    available: usize,
    blocked: usize,
    degraded_or_error: usize,
    privacy_caveats: usize,
    blocking_caveats: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyAuditFinding {
    code: String,
    severity: &'static str,
    surface: &'static str,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyExportReport {
    schema_version: u32,
    payload_policy: &'static str,
    scope: PrivacyExportScope,
    exported_events: usize,
    total_estimate: Option<i64>,
    next_cursor: Option<Cursor>,
    events: Vec<PrivacyExportEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyExportScope {
    sources: Vec<String>,
    event_types: Vec<String>,
    since: Option<String>,
    until: Option<String>,
    text_search_used: bool,
    limit: i64,
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyExportEvent {
    id: Option<String>,
    source: String,
    event_type: String,
    ts_orig: Option<String>,
    host: String,
    provenance: PrivacyExportProvenance,
    associated_blob_count: usize,
    payload_schema_id: Option<String>,
    source_run_id: Option<String>,
    created_by_operation_id: Option<String>,
    scope_key: Option<String>,
    equivalence_key: Option<String>,
    relevance_score: Option<f64>,
    payload_redacted: bool,
    snippet_redacted: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PrivacyExportProvenance {
    Material {
        source_material_id: String,
        anchor_byte: i64,
        offset_kind: &'static str,
    },
    Derived {
        parent_event_count: usize,
        operation_id: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
struct PrivacyExportReceipt {
    output_path: String,
    exported_events: usize,
    next_cursor: Option<Cursor>,
    payload_policy: &'static str,
}

fn build_privacy_audit_report(
    private_mode: RuntimePrivateModeState,
    dlq: &DlqListResponse,
    readiness: &SourcesReadinessListResponse,
) -> PrivacyAuditReport {
    let sources = summarize_sources(&readiness.sources);
    let mut findings = Vec::new();

    if private_mode.enabled {
        let scope = if private_mode.affected_source_classes.is_empty() {
            "all source classes".to_string()
        } else {
            private_mode.affected_source_classes.join(", ")
        };
        findings.push(PrivacyAuditFinding {
            code: "privacy.private_mode_enabled".to_string(),
            severity: "warning",
            surface: "runtime",
            message: format!("private mode is enabled for {scope}"),
        });
    }

    if dlq.total_messages > 0 {
        findings.push(PrivacyAuditFinding {
            code: "privacy.dlq_backlog".to_string(),
            severity: "warning",
            surface: "dlq",
            message: format!(
                "{} raw-ingest DLQ messages may need privacy-aware review before requeue",
                dlq.total_messages
            ),
        });
    }

    for source in &readiness.sources {
        for caveat in source
            .caveats
            .iter()
            .filter(|c| caveat_is_privacy_relevant(c))
        {
            findings.push(PrivacyAuditFinding {
                code: caveat.code.clone(),
                severity: severity_label(caveat.severity),
                surface: "source_readiness",
                message: format!(
                    "{} source family reports {}",
                    source.source_family, caveat.code
                ),
            });
        }
    }

    PrivacyAuditReport {
        private_mode: PrivacyAuditPrivateMode {
            enabled: private_mode.enabled,
            reason_class: private_mode.reason_class.to_string(),
            actor: private_mode.actor,
            started_at: private_mode.started_at.map(|ts| ts.to_string()),
            source_classes: private_mode.affected_source_classes,
            updated_by_operation_id: private_mode.updated_by_operation_id,
        },
        dlq: PrivacyAuditDlq {
            total_messages: dlq.total_messages,
            total_bytes: dlq.total_bytes,
            has_backlog: dlq.total_messages > 0,
        },
        sources,
        findings,
    }
}

fn build_privacy_export_report(
    result: EventQueryResult,
    scope: PrivacyExportScope,
) -> PrivacyExportReport {
    let (events, next_cursor, total_estimate) = match result {
        EventQueryResult::Events {
            events,
            next_cursor,
            total_estimate,
        } => (events, next_cursor, total_estimate),
        _ => (Vec::new(), None, None),
    };
    let events = sanitize_privacy_export_events(events);

    PrivacyExportReport {
        schema_version: 1,
        payload_policy: "metadata_only_payloads_and_snippets_omitted",
        scope,
        exported_events: events.len(),
        total_estimate,
        next_cursor,
        events,
    }
}

fn sanitize_privacy_export_events(events: Vec<QueryResultEvent>) -> Vec<PrivacyExportEvent> {
    events
        .into_iter()
        .map(sanitize_privacy_export_event)
        .collect()
}

fn sanitize_privacy_export_event(event: QueryResultEvent) -> PrivacyExportEvent {
    let source = event.event.source.to_string();
    let event_type = event.event.event_type.to_string();
    let provenance = match event.event.provenance {
        Provenance::Material {
            id,
            anchor_byte,
            offset_kind,
            ..
        } => PrivacyExportProvenance::Material {
            source_material_id: id.as_uuid().to_string(),
            anchor_byte,
            offset_kind: offset_kind.as_wire_str(),
        },
        Provenance::Derived {
            source_event_ids,
            operation_id,
        } => PrivacyExportProvenance::Derived {
            parent_event_count: source_event_ids.len(),
            operation_id: operation_id.map(|id| id.as_uuid().to_string()),
        },
    };

    PrivacyExportEvent {
        id: event.event.id.map(|id| id.as_uuid().to_string()),
        source,
        event_type,
        ts_orig: event.event.ts_orig.map(|ts| ts.to_string()),
        host: event.event.host.to_string(),
        provenance,
        associated_blob_count: event.event.associated_blob_ids.as_ref().map_or(0, Vec::len),
        payload_schema_id: event.event.payload_schema_id.map(|id| id.to_string()),
        source_run_id: event.event.source_run_id.map(|id| id.to_string()),
        created_by_operation_id: event.event.created_by_operation_id.map(|id| id.to_string()),
        scope_key: event.event.scope_key,
        equivalence_key: event.event.equivalence_key,
        relevance_score: event.relevance_score,
        payload_redacted: true,
        snippet_redacted: event.snippet.is_some(),
    }
}

fn render_privacy_export_report(
    report: &PrivacyExportReport,
    format: OutputFormat,
) -> Result<String> {
    match format {
        OutputFormat::Table => Ok(format_privacy_export_report(report)),
        OutputFormat::Json | OutputFormat::Dot => format_json(report),
        OutputFormat::Yaml => format_yaml(report),
    }
}

fn format_privacy_export_receipt(receipt: &PrivacyExportReceipt) -> String {
    let mut lines = vec![
        "Privacy Export".to_string(),
        format!("Output: {}", receipt.output_path),
        format!("Events: {}", receipt.exported_events),
        format!("Payload policy: {}", receipt.payload_policy),
    ];
    if receipt.next_cursor.is_some() {
        lines.push("Next cursor: present".to_string());
    }
    lines.join("\n")
}

fn format_privacy_export_report(report: &PrivacyExportReport) -> String {
    let mut lines = vec![
        "Privacy Export".to_string(),
        format!("Events: {}", report.exported_events),
        format!("Payload policy: {}", report.payload_policy),
    ];
    if let Some(total) = report.total_estimate {
        lines.push(format!("Approximate total matches: {total}"));
    }
    if report.next_cursor.is_some() {
        lines.push("Next cursor: present".to_string());
    }
    if report.events.is_empty() {
        lines.push("No events found.".to_string());
    } else {
        for event in &report.events {
            lines.push(format!(
                "  {} {} {} {}",
                event.id.as_deref().unwrap_or("-"),
                event.ts_orig.as_deref().unwrap_or("-"),
                event.source,
                event.event_type
            ));
        }
    }
    lines.join("\n")
}

fn summarize_sources(sources: &[SourceReadiness]) -> PrivacyAuditSources {
    let mut summary = PrivacyAuditSources {
        total: sources.len(),
        available: 0,
        blocked: 0,
        degraded_or_error: 0,
        privacy_caveats: 0,
        blocking_caveats: 0,
    };

    for source in sources {
        match source.status {
            SourceReadinessStatus::Available => summary.available += 1,
            SourceReadinessStatus::Blocked => summary.blocked += 1,
            SourceReadinessStatus::Partial
            | SourceReadinessStatus::Stale
            | SourceReadinessStatus::Error
            | SourceReadinessStatus::Missing
            | SourceReadinessStatus::Unknown => summary.degraded_or_error += 1,
            SourceReadinessStatus::Disabled => {}
        }

        summary.privacy_caveats += source
            .caveats
            .iter()
            .filter(|c| caveat_is_privacy_relevant(c))
            .count();
        summary.blocking_caveats += source
            .caveats
            .iter()
            .filter(|c| c.severity == CaveatSeverity::Blocking)
            .count();
    }

    summary
}

fn caveat_is_privacy_relevant(caveat: &SourceCaveat) -> bool {
    caveat.code.starts_with("policy.") || caveat.code.starts_with("privacy.")
}

fn severity_label(severity: CaveatSeverity) -> &'static str {
    match severity {
        CaveatSeverity::Info => "info",
        CaveatSeverity::Warning => "warning",
        CaveatSeverity::Degraded => "degraded",
        CaveatSeverity::Blocking => "blocking",
    }
}

fn format_privacy_audit_report(report: &PrivacyAuditReport) -> String {
    let source_classes = if report.private_mode.source_classes.is_empty() {
        "all".to_string()
    } else {
        report.private_mode.source_classes.join(",")
    };
    let started_at = report.private_mode.started_at.as_deref().unwrap_or("-");
    let mut lines = vec![
        "Privacy Audit".to_string(),
        format!(
            "Private mode: {}",
            if report.private_mode.enabled {
                "enabled"
            } else {
                "disabled"
            }
        ),
        format!("Reason: {}", report.private_mode.reason_class),
        format!("Actor: {}", report.private_mode.actor),
        format!("Started: {started_at}"),
        format!("Source classes: {source_classes}"),
        format!(
            "DLQ: {} messages / {} bytes",
            report.dlq.total_messages, report.dlq.total_bytes
        ),
        format!(
            "Sources: {} total, {} available, {} blocked, {} degraded/error",
            report.sources.total,
            report.sources.available,
            report.sources.blocked,
            report.sources.degraded_or_error
        ),
        format!(
            "Caveats: {} privacy-relevant, {} blocking",
            report.sources.privacy_caveats, report.sources.blocking_caveats
        ),
    ];

    if report.findings.is_empty() {
        lines.push("Findings: none".to_string());
    } else {
        lines.push(format!("Findings: {}", report.findings.len()));
        for finding in &report.findings {
            lines.push(format!(
                "  [{}] {} ({}) - {}",
                finding.severity, finding.code, finding.surface, finding.message
            ));
        }
    }

    lines.join("\n")
}

fn format_private_mode_state(state: &RuntimePrivateModeState) -> String {
    let scope = if state.affected_source_classes.is_empty() {
        "all".to_string()
    } else {
        state.affected_source_classes.join(",")
    };
    let started = state
        .started_at
        .as_ref()
        .map_or_else(|| "-".to_string(), ToString::to_string);
    let expires = state
        .expires_at
        .as_ref()
        .map_or_else(|| "-".to_string(), ToString::to_string);
    format!(
        "Private mode: {}\nReason: {}\nActor: {}\nStarted: {}\nExpires: {}\nSource classes: {}",
        if state.enabled { "enabled" } else { "disabled" },
        state.reason_class,
        state.actor,
        started,
        expires,
        scope
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::domain::HostName;
    use sinex_primitives::events::{Event, SourceMaterial};
    use sinex_primitives::temporal::Timestamp;
    use sinex_primitives::{Id, Uuid};
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn private_mode_table_summary_keeps_coarse_scope() -> xtask::sandbox::TestResult<()> {
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["clipboard".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        let summary = format_private_mode_state(&state);

        assert!(summary.contains("Private mode: enabled"));
        assert!(summary.contains("Actor: sinity"));
        assert!(summary.contains("Source classes: clipboard"));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_audit_summarizes_posture_without_source_identifier_leak()
    -> xtask::sandbox::TestResult<()> {
        let report = build_privacy_audit_report(
            RuntimePrivateModeState::enabled_by(
                "sinity",
                vec!["desktop".to_string()],
                Timestamp::UNIX_EPOCH,
            ),
            &DlqListResponse {
                total_messages: 2,
                total_bytes: 128,
                first_seq: 1,
                last_seq: 2,
            },
            &SourcesReadinessListResponse {
                sources: vec![SourceReadiness {
                    binding_id: None,
                    source_family: "desktop".to_string(),
                    source_unit_id: None,
                    parser_id: None,
                    source_identifier: "/home/sinity/private/window.log".to_string(),
                    status: SourceReadinessStatus::Blocked,
                    cost: sinex_primitives::rpc::sources::SourceReadinessCost::Unavailable,
                    freshness_seconds: None,
                    material_count: 1,
                    parsed_event_count: None,
                    last_success_at: None,
                    caveats: vec![SourceCaveat {
                        code: "policy.raw_material_blocked".to_string(),
                        severity: CaveatSeverity::Blocking,
                        message: "blocked by private mode".to_string(),
                        evidence_ref: Some("/home/sinity/private/window.log".to_string()),
                    }],
                    evidence: json!({"raw_path": "/home/sinity/private/window.log"}),
                }],
            },
        );

        assert!(report.private_mode.enabled);
        assert!(report.dlq.has_backlog);
        assert_eq!(report.sources.blocked, 1);
        assert_eq!(report.sources.privacy_caveats, 1);
        assert_eq!(report.sources.blocking_caveats, 1);
        assert_eq!(report.findings.len(), 3);

        let table = format_privacy_audit_report(&report);
        assert!(table.contains("privacy.private_mode_enabled"));
        assert!(table.contains("privacy.dlq_backlog"));
        assert!(table.contains("policy.raw_material_blocked"));
        assert!(!table.contains("/home/sinity/private/window.log"));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_export_omits_payload_and_snippet_material() -> xtask::sandbox::TestResult<()> {
        let event = Event {
            id: Some(Id::from_uuid(Uuid::from_u128(1))),
            source: EventSource::from_static("terminal"),
            event_type: EventType::from_static("shell.command"),
            payload: json!({
                "command": "export TOKEN=secret",
                "cwd": "/home/sinity/private"
            }),
            ts_orig: Some(Timestamp::UNIX_EPOCH),
            host: HostName::from_static("sinnix-prime"),
            source_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::from_material(
                Id::<SourceMaterial>::from_uuid(Uuid::from_u128(2)),
                42,
                None,
                None,
            ),
            associated_blob_ids: Some(vec![Uuid::from_u128(3)]),
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
            anchor_payload_hash: None,
        };
        let report = build_privacy_export_report(
            EventQueryResult::Events {
                events: vec![QueryResultEvent {
                    event,
                    relevance_score: Some(0.8),
                    snippet: Some("TOKEN=secret".to_string()),
                }],
                next_cursor: None,
                total_estimate: Some(1),
            },
            PrivacyExportScope {
                sources: vec!["terminal".to_string()],
                event_types: vec!["shell.command".to_string()],
                since: Some("24h".to_string()),
                until: None,
                text_search_used: true,
                limit: 100,
            },
        );

        let encoded = serde_json::to_string(&report)?;
        assert_eq!(report.exported_events, 1);
        assert_eq!(report.scope.sources, vec!["terminal".to_string()]);
        assert!(report.scope.text_search_used);
        assert!(encoded.contains("metadata_only_payloads_and_snippets_omitted"));
        assert!(encoded.contains("\"payload_redacted\":true"));
        assert!(encoded.contains("\"snippet_redacted\":true"));
        assert!(encoded.contains("\"associated_blob_count\":1"));
        assert!(!encoded.contains("TOKEN=secret"));
        assert!(!encoded.contains("/home/sinity/private"));
        assert!(!encoded.contains("\"payload\""));
        assert!(!encoded.contains("\"snippet\""));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_export_requires_explicit_scope() -> xtask::sandbox::TestResult<()> {
        let args = PrivacyExportArgs {
            source: Vec::new(),
            event_type: Vec::new(),
            since: None,
            until: None,
            query: None,
            limit: 100,
            output: None,
        };

        let error = args
            .to_event_query()
            .expect_err("unscoped privacy export should be refused");
        assert!(
            format!("{error:#}").contains("requires an explicit scope"),
            "error should explain scope requirement: {error:#}"
        );
        Ok(())
    }
}
