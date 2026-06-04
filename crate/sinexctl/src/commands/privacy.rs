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
    PrivacyPolicyBackendAddRequest, PrivacyPolicyDictionaryAddRequest, PrivacyPolicyListResponse,
    PrivacyPolicyMutationResponse, PrivacyPolicyRule, PrivacyPolicyRuleAddRequest,
    PrivacyPolicyScopeBindRequest, PrivacyPolicySeedBuiltinRequest,
    PrivacyPolicySeedBuiltinResponse,
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
    sinexctl privacy policy backend add --name presidio-local --kind presidio --endpoint-url http://127.0.0.1:5001/analyze
    sinexctl privacy policy dictionary add --name local-projects --term sinex --tag project
    sinexctl privacy policy seed builtin --enabled
    sinexctl privacy policy rule add --name api-token --matcher-type regex --matcher-value 'TOKEN=[^ ]+' --action redact
    sinexctl privacy policy scope bind --rule-name api-token --event-source terminal --field-path command
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

    /// Inspect DB-backed admission policy.
    Policy {
        #[command(subcommand)]
        cmd: PolicyCommand,
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
enum PolicyCommand {
    /// List DB-backed policy rules, scopes, recognizer backends, and dictionaries.
    List(PolicyListArgs),

    /// Manage recognizer backend bindings.
    Backend {
        #[command(subcommand)]
        cmd: PolicyBackendCommand,
    },

    /// Manage imported or user-local dictionary assets.
    Dictionary {
        #[command(subcommand)]
        cmd: PolicyDictionaryCommand,
    },

    /// Manage DB-backed policy rules.
    Rule {
        #[command(subcommand)]
        cmd: PolicyRuleCommand,
    },

    /// Seed DB-backed policy from catalog data.
    Seed {
        #[command(subcommand)]
        cmd: PolicySeedCommand,
    },

    /// Bind policy rules to source/type/field scopes.
    Scope {
        #[command(subcommand)]
        cmd: PolicyScopeCommand,
    },
}

#[derive(Debug, Args)]
struct PolicyListArgs {
    /// Include disabled policy objects.
    #[arg(long)]
    include_disabled: bool,
}

#[derive(Debug, Subcommand)]
enum PolicyBackendCommand {
    /// Add a recognizer backend binding.
    Add(PolicyBackendAddArgs),
}

#[derive(Debug, Args)]
struct PolicyBackendAddArgs {
    /// Unique backend name.
    #[arg(long)]
    name: String,

    /// Backend kind: local, presidio, gitleaks, trufflehog, or external_http.
    #[arg(long)]
    kind: String,

    /// Optional HTTP endpoint URL for remote recognizers.
    #[arg(long)]
    endpoint_url: Option<String>,

    /// Backend-specific JSON config.
    #[arg(long, default_value = "{}")]
    config: String,

    /// Register the backend disabled.
    #[arg(long)]
    disabled: bool,
}

#[derive(Debug, Subcommand)]
enum PolicyDictionaryCommand {
    /// Add an imported or user-local dictionary.
    Add(PolicyDictionaryAddArgs),
}

#[derive(Debug, Args)]
struct PolicyDictionaryAddArgs {
    /// Unique dictionary name.
    #[arg(long)]
    name: String,

    /// Operator-facing description.
    #[arg(long, default_value = "")]
    description: String,

    /// Optional BCP-47-ish language tag.
    #[arg(long)]
    language: Option<String>,

    /// Source kind: user, seed, imported, or generated.
    #[arg(long, default_value = "user")]
    source_kind: String,

    /// Dictionary tag. Repeatable.
    #[arg(long)]
    tag: Vec<String>,

    /// Dictionary term. Repeatable.
    #[arg(long)]
    term: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum PolicyRuleCommand {
    /// Add a DB-backed policy rule.
    Add(PolicyRuleAddArgs),
}

#[derive(Debug, Subcommand)]
enum PolicySeedCommand {
    /// Upsert the built-in catalog into DB policy rows.
    Builtin(PolicySeedBuiltinArgs),
}

#[derive(Debug, Args)]
struct PolicySeedBuiltinArgs {
    /// Seed rows enabled. By default seeded rules are present but disabled.
    #[arg(long)]
    enabled: bool,
}

#[derive(Debug, Args)]
struct PolicyRuleAddArgs {
    /// Unique rule name.
    #[arg(long)]
    name: String,

    /// Operator-facing description.
    #[arg(long, default_value = "")]
    description: String,

    /// Matcher type: regex, literal, dictionary, structural, presidio_entity,
    /// presidio_analyzer, secret_scanner, or external.
    #[arg(long)]
    matcher_type: String,

    /// Matcher value. Stored in policy DB; omitted from table output.
    #[arg(long)]
    matcher_value: String,

    /// JSON matcher config for dictionary/backend recognizers.
    #[arg(long, default_value = "{}")]
    matcher_config: String,

    /// Presidio context word (repeatable): a term whose presence near a
    /// candidate span boosts the recognizer's confidence. Folded into
    /// matcher_config["context"]. Ignored by non-Presidio recognizers.
    #[arg(long = "context-word")]
    context_word: Vec<String>,

    /// Optional recognizer backend UUID.
    #[arg(long)]
    recognizer_backend_id: Option<Uuid>,

    /// Recognizer kind: local_pattern, dictionary, presidio_entity,
    /// secret_scanner, or external.
    #[arg(long, default_value = "local_pattern")]
    recognizer_kind: String,

    /// Treat string matching as case sensitive.
    #[arg(long)]
    case_sensitive: bool,

    /// Action: redact, hash, encrypt, suppress, or mask.
    #[arg(long)]
    action: String,

    /// Optional label used by redaction/masking actions.
    #[arg(long)]
    action_label: Option<String>,

    /// Encryption/hash key namespace.
    #[arg(long, default_value = "default")]
    key_namespace: String,
}

#[derive(Debug, Subcommand)]
enum PolicyScopeCommand {
    /// Bind a rule to an optional source/type/field scope.
    Bind(PolicyScopeBindArgs),
}

#[derive(Debug, Args)]
struct PolicyScopeBindArgs {
    /// Existing policy rule name.
    #[arg(long)]
    rule_name: String,

    /// Optional event source scope; omitted means all sources.
    #[arg(long)]
    event_source: Option<String>,

    /// Optional event type scope; omitted means all event types.
    #[arg(long)]
    event_type: Option<String>,

    /// Optional payload field path; omitted means all fields.
    #[arg(long)]
    field_path: Option<String>,

    /// Scope priority; higher values win earlier when scopes overlap.
    #[arg(long, default_value_t = 0)]
    priority: i32,
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
                PolicyCommand::List(_) => "privacy policy list",
                PolicyCommand::Backend {
                    cmd: PolicyBackendCommand::Add(_),
                } => "privacy policy backend add",
                PolicyCommand::Dictionary {
                    cmd: PolicyDictionaryCommand::Add(_),
                } => "privacy policy dictionary add",
                PolicyCommand::Rule {
                    cmd: PolicyRuleCommand::Add(_),
                } => "privacy policy rule add",
                PolicyCommand::Seed {
                    cmd: PolicySeedCommand::Builtin(_),
                } => "privacy policy seed builtin",
                PolicyCommand::Scope {
                    cmd: PolicyScopeCommand::Bind(_),
                } => "privacy policy scope bind",
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

impl PolicyCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List(args) => args.execute(client, format).await,
            Self::Backend { cmd } => cmd.execute(client, format).await,
            Self::Dictionary { cmd } => cmd.execute(client, format).await,
            Self::Rule { cmd } => cmd.execute(client, format).await,
            Self::Seed { cmd } => cmd.execute(client, format).await,
            Self::Scope { cmd } => cmd.execute(client, format).await,
        }
    }
}

impl PolicyBackendCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Add(args) => args.execute(client, format).await,
        }
    }
}

impl PolicyDictionaryCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Add(args) => args.execute(client, format).await,
        }
    }
}

impl PolicyRuleCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Add(args) => args.execute(client, format).await,
        }
    }
}

impl PolicySeedCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Builtin(args) => args.execute(client, format).await,
        }
    }
}

impl PolicyScopeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Bind(args) => args.execute(client, format).await,
        }
    }
}

impl PolicyListArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client.privacy_policy_list(self.include_disabled).await?;
        CommandOutput::single(response, format_privacy_policy_list).display(&format)?;
        Ok(())
    }
}

impl PolicyBackendAddArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .privacy_policy_backend_add(self.to_request()?)
            .await?;
        CommandOutput::single(response, format_privacy_policy_mutation).display(&format)?;
        Ok(())
    }

    fn to_request(&self) -> Result<PrivacyPolicyBackendAddRequest> {
        let config = serde_json::from_str(&self.config)
            .map_err(|e| eyre!("--config must be valid JSON: {e}"))?;
        Ok(PrivacyPolicyBackendAddRequest {
            name: self.name.clone(),
            kind: self.kind.clone(),
            endpoint_url: self.endpoint_url.clone(),
            config,
            enabled: !self.disabled,
        })
    }
}

impl PolicyDictionaryAddArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .privacy_policy_dictionary_add(self.to_request())
            .await?;
        CommandOutput::single(response, format_privacy_policy_mutation).display(&format)?;
        Ok(())
    }

    fn to_request(&self) -> PrivacyPolicyDictionaryAddRequest {
        PrivacyPolicyDictionaryAddRequest {
            name: self.name.clone(),
            description: self.description.clone(),
            language: self.language.clone(),
            source_kind: self.source_kind.clone(),
            tags: self.tag.clone(),
            terms: self.term.clone(),
        }
    }
}

impl PolicyRuleAddArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client.privacy_policy_rule_add(self.to_request()?).await?;
        CommandOutput::single(response, format_privacy_policy_mutation).display(&format)?;
        Ok(())
    }

    fn to_request(&self) -> Result<PrivacyPolicyRuleAddRequest> {
        let matcher_config = serde_json::from_str(&self.matcher_config)
            .map_err(|e| eyre!("--matcher-config must be valid JSON: {e}"))?;
        Ok(PrivacyPolicyRuleAddRequest {
            name: self.name.clone(),
            description: self.description.clone(),
            matcher_type: self.matcher_type.clone(),
            matcher_value: self.matcher_value.clone(),
            matcher_config,
            context_words: self.context_word.clone(),
            recognizer_backend_id: self.recognizer_backend_id,
            recognizer_kind: self.recognizer_kind.clone(),
            case_sensitive: self.case_sensitive,
            action: self.action.clone(),
            action_label: self.action_label.clone(),
            key_namespace: self.key_namespace.clone(),
        })
    }
}

impl PolicySeedBuiltinArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .privacy_policy_seed_builtin(PrivacyPolicySeedBuiltinRequest {
                enabled: self.enabled,
            })
            .await?;
        CommandOutput::single(response, format_privacy_policy_seed).display(&format)?;
        Ok(())
    }
}

impl PolicyScopeBindArgs {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client.privacy_policy_scope_bind(self.to_request()).await?;
        CommandOutput::single(response, format_privacy_policy_mutation).display(&format)?;
        Ok(())
    }

    fn to_request(&self) -> PrivacyPolicyScopeBindRequest {
        PrivacyPolicyScopeBindRequest {
            rule_name: self.rule_name.clone(),
            event_source: self.event_source.clone(),
            event_type: self.event_type.clone(),
            field_path: self.field_path.clone(),
            priority: self.priority,
        }
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

fn format_privacy_policy_list(report: &PrivacyPolicyListResponse) -> String {
    let mut lines = vec![
        "Privacy Policy".to_string(),
        format!(
            "Rules: {} ({} enabled)",
            report.rules.len(),
            report.rules.iter().filter(|rule| rule.enabled).count()
        ),
        format!("Field scopes: {}", report.field_scopes.len()),
        format!("Key namespaces: {}", report.key_namespaces.len()),
        format!(
            "Recognizer backends: {} ({} enabled)",
            report.recognizer_backends.len(),
            report
                .recognizer_backends
                .iter()
                .filter(|backend| backend.enabled)
                .count()
        ),
        format!(
            "Dictionaries: {} ({} enabled)",
            report.dictionaries.len(),
            report
                .dictionaries
                .iter()
                .filter(|dictionary| dictionary.enabled)
                .count()
        ),
    ];

    if report.rules.is_empty() {
        lines.push("Rules: none".to_string());
    } else {
        lines.push("Rules:".to_string());
        for rule in &report.rules {
            lines.push(format_privacy_rule_line(rule, report));
        }
    }

    lines.join("\n")
}

fn format_privacy_rule_line(
    rule: &PrivacyPolicyRule,
    report: &PrivacyPolicyListResponse,
) -> String {
    let scope_count = report
        .field_scopes
        .iter()
        .filter(|scope| scope.rule_id == rule.id)
        .count();
    format!(
        "  {} [{}] matcher={} recognizer={} action={} key={} scopes={}",
        rule.name,
        if rule.enabled { "enabled" } else { "disabled" },
        rule.matcher_type,
        rule.recognizer_kind,
        rule.action,
        rule.key_namespace,
        scope_count
    )
}

fn format_privacy_policy_mutation(response: &PrivacyPolicyMutationResponse) -> String {
    format!(
        "Privacy Policy Mutation\nKind: {}\nName: {}\nID: {}",
        response.kind, response.name, response.id
    )
}

fn format_privacy_policy_seed(response: &PrivacyPolicySeedBuiltinResponse) -> String {
    format!(
        "Privacy Policy Seed\nInserted: {}\nUpdated: {}\nUnchanged: {}\nTotal: {}",
        response.inserted, response.updated, response.unchanged, response.total
    )
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
                    source_id: None,
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
            ts_quality: None,
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

    #[sinex_test]
    async fn privacy_policy_table_summarizes_without_matcher_value_leak()
    -> xtask::sandbox::TestResult<()> {
        let rule_id = Uuid::new_v4();
        let report = PrivacyPolicyListResponse {
            rules: vec![PrivacyPolicyRule {
                id: rule_id,
                name: "api-token".to_string(),
                description: "token fixture".to_string(),
                matcher_type: "regex".to_string(),
                matcher_value: "SECRET_TOKEN_SHOULD_NOT_RENDER".to_string(),
                matcher_config: json!({}),
                context_words: vec![],
                recognizer_backend_id: None,
                recognizer_kind: "local_pattern".to_string(),
                case_sensitive: false,
                action: "redact".to_string(),
                action_label: Some("<TOKEN>".to_string()),
                key_namespace: "default".to_string(),
                enabled: true,
            }],
            field_scopes: vec![],
            key_namespaces: vec![],
            recognizer_backends: vec![],
            dictionaries: vec![],
        };

        let table = format_privacy_policy_list(&report);

        assert!(table.contains("Privacy Policy"));
        assert!(table.contains("api-token"));
        assert!(table.contains("matcher=regex"));
        assert!(!table.contains("SECRET_TOKEN_SHOULD_NOT_RENDER"));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_policy_rule_add_parses_matcher_config_without_receipt_leak()
    -> xtask::sandbox::TestResult<()> {
        let args = PolicyRuleAddArgs {
            name: "local-secret".to_string(),
            description: "fixture".to_string(),
            matcher_type: "regex".to_string(),
            matcher_value: "SECRET_TOKEN_SHOULD_NOT_RENDER".to_string(),
            matcher_config: r#"{"entity":"API_KEY","score_threshold":0.8}"#.to_string(),
            context_word: vec![],
            recognizer_backend_id: None,
            recognizer_kind: "local_pattern".to_string(),
            case_sensitive: false,
            action: "redact".to_string(),
            action_label: Some("<SECRET>".to_string()),
            key_namespace: "default".to_string(),
        };

        let request = args.to_request()?;
        assert_eq!(request.matcher_type, "regex");
        assert_eq!(request.matcher_config["entity"], "API_KEY");
        assert_eq!(request.matcher_config["score_threshold"], 0.8);

        let receipt = PrivacyPolicyMutationResponse {
            id: Uuid::new_v4(),
            kind: "rule".to_string(),
            name: request.name,
        };
        let table = format_privacy_policy_mutation(&receipt);
        assert!(table.contains("Privacy Policy Mutation"));
        assert!(table.contains("local-secret"));
        assert!(!table.contains("SECRET_TOKEN_SHOULD_NOT_RENDER"));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_policy_seed_builtin_formats_idempotent_counts()
    -> xtask::sandbox::TestResult<()> {
        let args = PolicySeedBuiltinArgs { enabled: false };
        let response = PrivacyPolicySeedBuiltinResponse {
            inserted: 37,
            updated: 0,
            unchanged: 0,
            total: 37,
        };

        let table = format_privacy_policy_seed(&response);
        assert!(!args.enabled);
        assert!(table.contains("Privacy Policy Seed"));
        assert!(table.contains("Inserted: 37"));
        assert!(table.contains("Total: 37"));
        Ok(())
    }

    #[sinex_test]
    async fn privacy_policy_backend_add_parses_config_and_enabled_state()
    -> xtask::sandbox::TestResult<()> {
        let args = PolicyBackendAddArgs {
            name: "presidio-local".to_string(),
            kind: "presidio".to_string(),
            endpoint_url: Some("http://127.0.0.1:5001/analyze".to_string()),
            config: r#"{"language":"en","entities":["EMAIL_ADDRESS"]}"#.to_string(),
            disabled: true,
        };

        let request = args.to_request()?;
        assert_eq!(request.name, "presidio-local");
        assert_eq!(request.kind, "presidio");
        assert_eq!(
            request.endpoint_url.as_deref(),
            Some("http://127.0.0.1:5001/analyze")
        );
        assert_eq!(request.config["language"], "en");
        assert_eq!(request.config["entities"][0], "EMAIL_ADDRESS");
        assert!(!request.enabled);
        Ok(())
    }

    #[sinex_test]
    async fn privacy_policy_dictionary_add_preserves_terms_and_tags()
    -> xtask::sandbox::TestResult<()> {
        let args = PolicyDictionaryAddArgs {
            name: "local-projects".to_string(),
            description: "project deny-list".to_string(),
            language: Some("en".to_string()),
            source_kind: "user".to_string(),
            tag: vec!["project".to_string(), "local".to_string()],
            term: vec!["sinex".to_string(), "sinity".to_string()],
        };

        let request = args.to_request();
        assert_eq!(request.name, "local-projects");
        assert_eq!(request.description, "project deny-list");
        assert_eq!(request.language.as_deref(), Some("en"));
        assert_eq!(request.source_kind, "user");
        assert_eq!(
            request.tags,
            vec!["project".to_string(), "local".to_string()]
        );
        assert_eq!(
            request.terms,
            vec!["sinex".to_string(), "sinity".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn privacy_policy_scope_bind_preserves_field_hint_scope() -> xtask::sandbox::TestResult<()>
    {
        let args = PolicyScopeBindArgs {
            rule_name: "window-title-sensitive".to_string(),
            event_source: Some("desktop".to_string()),
            event_type: Some("window.focus".to_string()),
            field_path: Some("title".to_string()),
            priority: 20,
        };

        let request = args.to_request();
        assert_eq!(request.rule_name, "window-title-sensitive");
        assert_eq!(request.event_source.as_deref(), Some("desktop"));
        assert_eq!(request.event_type.as_deref(), Some("window.focus"));
        assert_eq!(request.field_path.as_deref(), Some("title"));
        assert_eq!(request.priority, 20);
        Ok(())
    }
}
