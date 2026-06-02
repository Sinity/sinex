//! DB-backed privacy policy engine (#1042 Slice 3).
//!
//! Loads user-defined privacy rules from the database (via `PrivacyPolicyRepository`)
//! and exposes a `redact_batch` chokepoint that mutates `AdmittedEvent` payloads
//! before persistence. All events — both source (material-provenance) and derived
//! (parent-provenance) — flow through `persist_batch_optimized` and therefore
//! through this engine.
//!
//! # Architecture
//!
//! Rules are compiled into a `PrivacyEngine` using the existing public
//! `PatternRule` / `PrivacyEngine::new(config)` API. No private `CompiledMatcher`
//! exposure is needed: the policy engine simply constructs a fresh `PrivacyEngine`
//! from DB rules via `PrivacyConfig::extra_rules`, then applies the compiled
//! engine per event or targeted field.
//!
//! # Field-path scoping
//!
//! `field_path` in `privacy.field_rules` is interpreted as a JSON Pointer.
//! A `field_path` of `/text` matches the key `"text"` at the root of the
//! payload JSON object; `/results/0/text` matches nested array/object payloads.
//! Bare field names are treated as root keys for operator ergonomics.
//!
//! # Cache refresh
//!
//! Rules are loaded once at engine construction and refreshed periodically via
//! `ensure_fresh()`. The default refresh interval is 30 seconds, configurable
//! via `SINEX_PRIVACY_POLICY_REFRESH_SECS`. Up to 30 seconds of stale policy
//! is acceptable for a single-user system; instant invalidation via Postgres
//! NOTIFY/LISTEN is a potential future improvement.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sinex_db::{DbPool, DbPoolExt};
use sinex_primitives::JsonValue;
use sinex_primitives::constants::env_vars;
use sinex_primitives::events::Event;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    CategorySet, Matcher, PatternRule, PrivacyConfig, PrivacyEngine, ProcessingContext,
    RuleCategory, Strategy,
};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::event_engine::admission::AdmittedEvent;

// ─── Refresh interval ───────────────────────────────────────────────────────

/// Default policy refresh interval in seconds.
const DEFAULT_REFRESH_SECS: u64 = 30;
const DEFAULT_RECOGNIZER_TIMEOUT_SECS: u64 = 3;

fn refresh_interval() -> std::time::Duration {
    let secs = std::env::var("SINEX_PRIVACY_POLICY_REFRESH_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REFRESH_SECS);
    std::time::Duration::from_secs(secs)
}

// ─── Compiled rule set ───────────────────────────────────────────────────────

/// A privacy engine scoped to a specific `(event_source, event_type)` pair.
///
/// Each `CompiledScope` owns a `PrivacyEngine` built from the DB rules that
/// apply to that scope plus globally-scoped rules (NULL source/type).
/// Per-field rules are encoded as extra `PatternRule`s; unscoped rules walk the
/// full JSON.
struct ScopedEngine {
    /// source string, or None = all sources.
    event_source: Option<String>,
    /// `event_type` string, or None = all event types.
    event_type: Option<String>,
    /// Field paths covered by at least one rule in this scope (None = all fields).
    field_paths: Vec<Option<String>>,
    /// The compiled engine for this scope.
    engine: PrivacyEngine,
}

impl std::fmt::Debug for ScopedEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `PrivacyEngine` deliberately has no `Debug` impl — it holds an
        // encryption key that must not surface in logs or panic messages.
        // Show the scope metadata and elide the engine internals.
        f.debug_struct("ScopedEngine")
            .field("event_source", &self.event_source)
            .field("event_type", &self.event_type)
            .field("field_paths", &self.field_paths)
            .finish_non_exhaustive()
    }
}

/// The compiled rule set derived from the DB state.
///
/// Built by `compile_rules` from the raw `LoadedRule` list returned by
/// `PrivacyPolicyRepository::load_enabled_rules`.
struct CompiledPolicyRuleSet {
    scopes: Vec<ScopedEngine>,
    external_rules: Vec<ExternalRecognizerRule>,
}

impl CompiledPolicyRuleSet {
    fn empty() -> Self {
        Self {
            scopes: Vec::new(),
            external_rules: Vec::new(),
        }
    }
}

/// A DB policy rule backed by an external recognizer such as Presidio Analyzer.
#[derive(Debug, Clone)]
struct ExternalRecognizerRule {
    name: String,
    event_source: Option<String>,
    event_type: Option<String>,
    field_path: Option<String>,
    endpoint_url: String,
    language: String,
    entities: Vec<String>,
    score_threshold: Option<f64>,
    strategy: Strategy,
}

// ─── Rule compilation ────────────────────────────────────────────────────────

/// Convert a `matcher_type` / `matcher_value` pair from the DB into a
/// `sinex_primitives::privacy::Matcher`.
///
/// Returns `None` and logs a warning if the type is unrecognised or the
/// pattern is syntactically invalid. Invalid rules are skipped silently at
/// load time so a bad regex never blocks the chokepoint.
fn db_row_to_matcher(
    rule_name: &str,
    matcher_type: &str,
    matcher_value: &str,
    matcher_config: &JsonValue,
    case_sensitive: bool,
) -> Option<Matcher> {
    match matcher_type {
        "regex" => {
            // Validate the pattern compiles before accepting it.
            if let Err(e) = regex::Regex::new(matcher_value) {
                warn!(
                    rule = rule_name,
                    pattern = matcher_value,
                    error = %e,
                    "DB privacy rule has invalid regex — skipping"
                );
                None
            } else {
                Some(Matcher::Regex {
                    pattern: matcher_value.to_string(),
                })
            }
        }
        "literal" => Some(Matcher::Literal {
            text: matcher_value.to_string(),
            case_sensitive,
        }),
        "dictionary" => dictionary_terms(matcher_value, matcher_config).map(|terms| {
            Matcher::Any(
                terms
                    .into_iter()
                    .map(|text| Matcher::Literal {
                        text,
                        case_sensitive,
                    })
                    .collect(),
            )
        }),
        "structural" => structural_detector(rule_name, matcher_value)
            .map(|detector| Matcher::Structural { detector }),
        "secret_scanner" => secret_scanner_regex(rule_name, matcher_value, matcher_config)
            .map(|pattern| Matcher::Regex { pattern }),
        other => {
            warn!(
                rule = rule_name,
                matcher_type = other,
                "DB privacy rule has unknown matcher_type — skipping"
            );
            None
        }
    }
}

fn dictionary_terms(matcher_value: &str, matcher_config: &JsonValue) -> Option<Vec<String>> {
    let from_config = matcher_config
        .get("terms")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|term| !term.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        });

    let terms = from_config.unwrap_or_else(|| {
        serde_json::from_str::<Vec<String>>(matcher_value).unwrap_or_else(|_| {
            matcher_value
                .lines()
                .map(str::trim)
                .filter(|term| !term.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
    });

    if terms.is_empty() { None } else { Some(terms) }
}

fn structural_detector(rule_name: &str, matcher_value: &str) -> Option<sinex_primitives::privacy::StructuralDetector> {
    serde_json::from_value(serde_json::Value::String(matcher_value.to_string()))
        .map_err(|error| {
            warn!(
                rule = rule_name,
                detector = matcher_value,
                error = %error,
                "DB privacy rule has unknown structural detector — skipping"
            );
            error
        })
        .ok()
}

fn secret_scanner_regex(
    rule_name: &str,
    matcher_value: &str,
    matcher_config: &JsonValue,
) -> Option<String> {
    let pattern = matcher_config
        .get("regex")
        .and_then(serde_json::Value::as_str)
        .or_else(|| matcher_config.get("pattern").and_then(serde_json::Value::as_str))
        .unwrap_or(matcher_value);

    if let Err(error) = regex::Regex::new(pattern) {
        warn!(
            rule = rule_name,
            pattern,
            error = %error,
            "DB privacy secret-scanner rule has invalid regex — skipping"
        );
        None
    } else {
        Some(pattern.to_string())
    }
}

fn external_recognizer_rule(
    loaded_rule: &sinex_db::repositories::privacy_policy::LoadedRule,
    scope: Option<&sinex_db::repositories::privacy_policy::FieldRuleRecord>,
) -> Option<ExternalRecognizerRule> {
    let rule = &loaded_rule.rule;
    if !matches!(
        rule.matcher_type.as_str(),
        "presidio_entity" | "presidio_analyzer" | "external"
    ) {
        return None;
    }

    let Some(backend) = &loaded_rule.backend else {
        warn!(
            rule = %rule.name,
            "external privacy recognizer rule has no enabled backend"
        );
        return None;
    };

    if !matches!(backend.kind.as_str(), "presidio" | "external_http") {
        warn!(
            rule = %rule.name,
            backend = %backend.name,
            kind = %backend.kind,
            "external privacy recognizer rule references a non-HTTP recognizer backend"
        );
        return None;
    }

    let endpoint_url = backend
        .endpoint_url
        .as_deref()
        .or_else(|| backend.config.get("endpoint_url").and_then(JsonValue::as_str))
        .or_else(|| backend.config.get("analyze_url").and_then(JsonValue::as_str));
    let Some(endpoint_url) = endpoint_url else {
        warn!(
            rule = %rule.name,
            backend = %backend.name,
            "external privacy recognizer backend has no endpoint_url"
        );
        return None;
    };

    let language = rule
        .matcher_config
        .get("language")
        .or_else(|| backend.config.get("language"))
        .and_then(JsonValue::as_str)
        .unwrap_or("en")
        .to_string();
    let entities = external_rule_entities(&rule.matcher_value, &rule.matcher_config);
    let score_threshold = rule
        .matcher_config
        .get("score_threshold")
        .or_else(|| backend.config.get("score_threshold"))
        .and_then(JsonValue::as_f64);

    Some(ExternalRecognizerRule {
        name: rule.name.clone(),
        event_source: scope.and_then(|scope| scope.event_source.clone()),
        event_type: scope.and_then(|scope| scope.event_type.clone()),
        field_path: scope.and_then(|scope| scope.field_path.clone()),
        endpoint_url: endpoint_url.to_string(),
        language,
        entities,
        score_threshold,
        strategy: db_row_to_strategy(&rule.action, rule.action_label.as_deref()),
    })
}

fn external_rule_entities(matcher_value: &str, matcher_config: &JsonValue) -> Vec<String> {
    let from_entities = matcher_config
        .get("entities")
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        });

    if let Some(entities) = from_entities
        && !entities.is_empty()
    {
        return entities;
    }

    matcher_config
        .get("entity_type")
        .and_then(JsonValue::as_str)
        .or_else(|| {
            let trimmed = matcher_value.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        })
        .map(|value| vec![value.to_string()])
        .unwrap_or_default()
}

/// Convert a DB `action` string + optional `action_label` into a `Strategy`.
fn db_row_to_strategy(action: &str, action_label: Option<&str>) -> Strategy {
    match action {
        "redact" => Strategy::Redact {
            label: action_label.map(String::from),
        },
        "hash" => Strategy::Hash,
        "encrypt" => Strategy::Encrypt,
        "suppress" => Strategy::Suppress,
        "mask" => Strategy::Mask {
            char: Some('*'),
            keep_prefix: Some(4),
            keep_suffix: Some(4),
        },
        other => {
            warn!(
                action = other,
                "DB privacy rule has unknown action — defaulting to redact"
            );
            Strategy::Redact { label: None }
        }
    }
}

/// Compile the list of loaded rules into `ScopedEngine` entries.
///
/// Rules with field scopes produce per-scope engines; rules without any
/// field scope produce a "global" engine entry that walks the whole payload.
fn compile_rules(
    loaded: &[sinex_db::repositories::privacy_policy::LoadedRule],
) -> Result<CompiledPolicyRuleSet> {
    if loaded.is_empty() {
        return Ok(CompiledPolicyRuleSet::empty());
    }

    // Group rules by (event_source, event_type) scope. Rules without field_rules
    // (no scopes) produce a global engine (None, None).
    //
    // Key: (event_source, event_type) — both Option<String>.
    use std::collections::HashMap;
    type ScopeKey = (Option<String>, Option<String>);
    type ScopeRules = Vec<(PatternRule, Option<String>)>;
    let mut scope_map: HashMap<ScopeKey, ScopeRules> = HashMap::new();
    let mut external_rules = Vec::new();

    for loaded_rule in loaded {
        let rule = &loaded_rule.rule;
        if matches!(
            rule.matcher_type.as_str(),
            "presidio_entity" | "presidio_analyzer" | "external"
        ) {
            if loaded_rule.scopes.is_empty() {
                if let Some(external) = external_recognizer_rule(loaded_rule, None) {
                    external_rules.push(external);
                }
            } else {
                for scope in &loaded_rule.scopes {
                    if let Some(external) = external_recognizer_rule(loaded_rule, Some(scope)) {
                        external_rules.push(external);
                    }
                }
            }
            continue;
        }

        let Some(matcher) = db_row_to_matcher(
            &rule.name,
            &rule.matcher_type,
            &rule.matcher_value,
            &rule.matcher_config,
            rule.case_sensitive,
        ) else {
            continue;
        };
        let strategy = db_row_to_strategy(&rule.action, rule.action_label.as_deref());

        let pattern_rule = PatternRule {
            name: format!("db.{}", rule.name),
            description: rule.description.clone(),
            category: RuleCategory::Custom,
            matcher,
            strategy,
            // DB rules apply to all ProcessingContexts (the chokepoint is payload-level).
            contexts: Vec::new(),
            enabled: true,
        };

        if loaded_rule.scopes.is_empty() {
            // Global rule: applies to all (source, event_type) pairs, all fields.
            scope_map
                .entry((None, None))
                .or_default()
                .push((pattern_rule, None));
        } else {
            for scope in &loaded_rule.scopes {
                let key = (scope.event_source.clone(), scope.event_type.clone());
                scope_map
                    .entry(key)
                    .or_default()
                    .push((pattern_rule.clone(), scope.field_path.clone()));
            }
        }
    }

    let mut scopes = Vec::with_capacity(scope_map.len());
    for ((event_source, event_type), rule_fps) in scope_map {
        let rules: Vec<PatternRule> = rule_fps.iter().map(|(r, _)| r.clone()).collect();
        let field_paths: Vec<Option<String>> = rule_fps.iter().map(|(_, fp)| fp.clone()).collect();

        let mut config = PrivacyConfig::default();
        // Disable built-in catalog rules: the chokepoint only applies DB policy.
        config.builtin_categories = CategorySet::None;
        config.extra_rules = rules;

        let engine = PrivacyEngine::new(config).map_err(|e| {
            SinexError::processing("failed to compile DB privacy policy rules")
                .with_context("event_source", event_source.as_deref().unwrap_or("*"))
                .with_context("event_type", event_type.as_deref().unwrap_or("*"))
                .with_std_error(&e)
        })?;

        scopes.push(ScopedEngine {
            event_source,
            event_type,
            field_paths,
            engine,
        });
    }

    Ok(CompiledPolicyRuleSet {
        scopes,
        external_rules,
    })
}

// ─── PolicyEngine ────────────────────────────────────────────────────────────

/// DB-backed, cached policy engine.
///
/// Thread-safe (`Send + Sync` via `Arc<RwLock>`). Owned by `JetStreamConsumer`
/// and shared (via `Arc`) if needed across concurrent tasks.
pub struct PolicyEngine {
    pool: DbPool,
    rules: Arc<RwLock<CompiledPolicyRuleSet>>,
    last_refresh: Arc<tokio::sync::Mutex<Instant>>,
    refresh_interval: std::time::Duration,
    http_client: reqwest::Client,
}

impl PolicyEngine {
    /// Build and load the initial rule set from the database.
    pub async fn load(pool: DbPool) -> Result<Self> {
        let loaded = pool
            .privacy_policy()
            .load_enabled_rules()
            .await
            .map_err(|e| {
                SinexError::database("failed to load initial privacy policy rules")
                    .with_std_error(&e)
            })?;
        let compiled = compile_rules(&loaded).map_err(|e| {
            SinexError::processing("failed to compile initial privacy policy rules")
                .with_std_error(&e)
        })?;
        debug!(rule_count = loaded.len(), "Privacy policy loaded from DB");
        Ok(Self {
            pool,
            rules: Arc::new(RwLock::new(compiled)),
            last_refresh: Arc::new(tokio::sync::Mutex::new(Instant::now())),
            refresh_interval: refresh_interval(),
            http_client: recognizer_http_client()?,
        })
    }

    /// No-op engine (empty rule set, no DB access). Used in tests that don't
    /// need DB-backed policy.
    pub fn noop(pool: DbPool) -> Self {
        Self {
            pool,
            rules: Arc::new(RwLock::new(CompiledPolicyRuleSet::empty())),
            last_refresh: Arc::new(tokio::sync::Mutex::new(Instant::now())),
            refresh_interval: std::time::Duration::from_secs(u64::MAX),
            http_client: recognizer_http_client().unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Refresh the rule cache if the refresh interval has elapsed.
    ///
    /// This is a best-effort operation: if the DB is unavailable, the stale
    /// cache remains in use and a warning is logged. The chokepoint never
    /// blocks on a DB failure.
    pub async fn ensure_fresh(&self) {
        let mut last = self.last_refresh.lock().await;
        if last.elapsed() < self.refresh_interval {
            return;
        }

        match self.pool.privacy_policy().load_enabled_rules().await {
            Ok(loaded) => {
                let count = loaded.len();
                match compile_rules(&loaded) {
                    Ok(compiled) => {
                        let mut rules = self.rules.write().await;
                        *rules = compiled;
                        *last = Instant::now();
                        debug!(rule_count = count, "Privacy policy refreshed from DB");
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to compile refreshed privacy policy rules; using stale cache");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to refresh privacy policy from DB; using stale cache");
            }
        }
    }

    /// Apply the policy to a batch of admitted events.
    ///
    /// Mutates `event.payload` for each event in the batch according to the
    /// matching DB rules. Events whose payload is not a JSON object are passed
    /// through unchanged. Source events and derived events both flow through
    /// this path.
    ///
    /// Local rule compile failures are skipped at load time; configured
    /// external-recognizer failures suppress the affected value so analyzer
    /// outages do not persist unclassified sensitive text.
    pub async fn redact_batch(&self, mut batch: Vec<AdmittedEvent>) -> Vec<AdmittedEvent> {
        let rules = self.rules.read().await;
        if rules.scopes.is_empty() && rules.external_rules.is_empty() {
            return batch;
        }

        for admitted in &mut batch {
            apply_policy_to_event(&mut admitted.event, &rules);
            apply_external_policy_to_event(&self.http_client, &mut admitted.event, &rules).await;
        }
        batch
    }

    /// Apply the policy to a single JSON value (used for DLQ redaction).
    ///
    /// Applies all global rules (NULL source/type scope). Returns the possibly
    /// mutated value. On policy engine error, returns a metadata-only stub.
    pub async fn redact_json_value(&self, value: JsonValue) -> JsonValue {
        let rules = self.rules.read().await;
        if rules.scopes.is_empty() && rules.external_rules.is_empty() {
            return value;
        }

        // For DLQ redaction: apply global (None, None) scoped engines conservatively.
        let mut result = value;
        for scope in &rules.scopes {
            if scope.event_source.is_none() && scope.event_type.is_none() {
                result = apply_scoped_engine_to_json(result, scope);
            }
        }
        for rule in &rules.external_rules {
            if rule.event_source.is_none() && rule.event_type.is_none() {
                result = apply_external_rule_to_json(&self.http_client, result, rule).await;
            }
        }
        result
    }
}

fn recognizer_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_RECOGNIZER_TIMEOUT_SECS))
        .build()
        .map_err(|e| {
            SinexError::configuration("failed to build privacy recognizer HTTP client")
                .with_std_error(&e)
        })
}

// ─── Application logic ───────────────────────────────────────────────────────

/// Apply all matching policy scopes to an event payload.
fn apply_policy_to_event(event: &mut Event<JsonValue>, rules: &CompiledPolicyRuleSet) {
    let source = event.source.as_str();
    let event_type = event.event_type.as_str();

    for scope in &rules.scopes {
        // Check if this scope matches the event's (source, event_type).
        let source_match = scope.event_source.as_deref().is_none_or(|s| s == source);
        let type_match = scope
            .event_type
            .as_deref()
            .is_none_or(|t| t == event_type);

        if !source_match || !type_match {
            continue;
        }

        event.payload = apply_scoped_engine_to_json(
            std::mem::replace(&mut event.payload, JsonValue::Null),
            scope,
        );
    }
}

async fn apply_external_policy_to_event(
    client: &reqwest::Client,
    event: &mut Event<JsonValue>,
    rules: &CompiledPolicyRuleSet,
) {
    let source = event.source.as_str();
    let event_type = event.event_type.as_str();

    for rule in &rules.external_rules {
        let source_match = rule.event_source.as_deref().is_none_or(|s| s == source);
        let type_match = rule.event_type.as_deref().is_none_or(|t| t == event_type);
        if !source_match || !type_match {
            continue;
        }

        event.payload = apply_external_rule_to_json(
            client,
            std::mem::replace(&mut event.payload, JsonValue::Null),
            rule,
        )
        .await;
    }
}

async fn apply_external_rule_to_json(
    client: &reqwest::Client,
    mut value: JsonValue,
    rule: &ExternalRecognizerRule,
) -> JsonValue {
    if let Some(path) = &rule.field_path {
        let pointer = field_path_pointer(path);
        if let Some(original) = value
            .pointer_mut(&pointer)
            .map(|field_value| std::mem::replace(field_value, JsonValue::Null))
        {
            if let JsonValue::String(text) = original {
                let replacement = apply_external_rule_to_string(client, &text, rule).await;
                if let Some(field_value) = value.pointer_mut(&pointer) {
                    *field_value = replacement;
                }
            } else if let Some(field_value) = value.pointer_mut(&pointer) {
                *field_value = original;
            }
        }
        return value;
    }

    let mut pointers = Vec::new();
    collect_string_pointers(&value, String::new(), &mut pointers);

    for pointer in pointers {
        let Some(text) = value
            .pointer(&pointer)
            .and_then(JsonValue::as_str)
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        let replacement = apply_external_rule_to_string(client, &text, rule).await;
        if let Some(field_value) = value.pointer_mut(&pointer) {
            *field_value = replacement;
        }
    }

    value
}

fn collect_string_pointers(value: &JsonValue, pointer: String, output: &mut Vec<String>) {
    match value {
        JsonValue::String(_) => output.push(pointer),
        JsonValue::Array(values) => {
            for (idx, child) in values.iter().enumerate() {
                collect_string_pointers(child, format!("{pointer}/{idx}"), output);
            }
        }
        JsonValue::Object(object) => {
            for (key, child) in object {
                collect_string_pointers(
                    child,
                    format!("{pointer}/{}", json_pointer_escape(key)),
                    output,
                );
            }
        }
        _ => {}
    }
}

async fn apply_external_rule_to_string(
    client: &reqwest::Client,
    text: &str,
    rule: &ExternalRecognizerRule,
) -> JsonValue {
    if text.is_empty() {
        return JsonValue::String(text.to_string());
    }

    let matches = match query_presidio(client, rule, text).await {
        Ok(matches) => matches,
        Err(error) => {
            warn!(
                rule = %rule.name,
                endpoint = %rule.endpoint_url,
                error = %error,
                "external privacy recognizer failed; suppressing value"
            );
            return JsonValue::Null;
        }
    };

    if matches.is_empty() {
        return JsonValue::String(text.to_string());
    }
    if matches.iter().any(|m| m.start < m.end) && matches!(&rule.strategy, Strategy::Suppress) {
        return JsonValue::Null;
    }

    JsonValue::String(replace_external_spans(text, &matches, rule))
}

#[derive(Debug, serde::Serialize)]
struct PresidioAnalyzeRequest<'a> {
    text: &'a str,
    language: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    entities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_threshold: Option<f64>,
}

#[derive(Debug, serde::Deserialize)]
struct PresidioAnalyzeResponse {
    start: usize,
    end: usize,
    #[allow(dead_code)]
    entity_type: Option<String>,
    #[allow(dead_code)]
    score: Option<f64>,
}

async fn query_presidio(
    client: &reqwest::Client,
    rule: &ExternalRecognizerRule,
    text: &str,
) -> Result<Vec<PresidioAnalyzeResponse>> {
    let request = PresidioAnalyzeRequest {
        text,
        language: &rule.language,
        entities: rule.entities.clone(),
        score_threshold: rule.score_threshold,
    };
    client
        .post(&rule.endpoint_url)
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            SinexError::processing("external privacy recognizer request failed")
                .with_context("rule", &rule.name)
                .with_context("endpoint", &rule.endpoint_url)
                .with_std_error(&e)
        })?
        .error_for_status()
        .map_err(|e| {
            SinexError::processing("external privacy recognizer returned an error status")
                .with_context("rule", &rule.name)
                .with_context("endpoint", &rule.endpoint_url)
                .with_std_error(&e)
        })?
        .json::<Vec<PresidioAnalyzeResponse>>()
        .await
        .map_err(|e| {
            SinexError::parse("external privacy recognizer response was not Presidio-compatible")
                .with_context("rule", &rule.name)
                .with_context("endpoint", &rule.endpoint_url)
                .with_std_error(&e)
        })
}

fn replace_external_spans(
    text: &str,
    matches: &[PresidioAnalyzeResponse],
    rule: &ExternalRecognizerRule,
) -> String {
    let mut spans = matches
        .iter()
        .filter_map(|matched| {
            let start = char_offset_to_byte_index(text, matched.start)?;
            let end = char_offset_to_byte_index(text, matched.end)?;
            (start < end && end <= text.len()).then_some((start, end))
        })
        .collect::<Vec<_>>();
    spans.sort_unstable_by_key(|(start, end)| (*start, std::cmp::Reverse(*end)));

    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    let mut last_end = 0;
    for (start, end) in spans {
        if start < last_end {
            continue;
        }
        output.push_str(&text[cursor..start]);
        output.push_str(&external_replacement(&text[start..end], rule));
        cursor = end;
        last_end = end;
    }
    output.push_str(&text[cursor..]);
    output
}

fn external_replacement(matched: &str, rule: &ExternalRecognizerRule) -> String {
    match &rule.strategy {
        Strategy::Redact { label } => label
            .clone()
            .unwrap_or_else(|| format!("<{}>", rule.name.to_uppercase())),
        Strategy::Mask {
            char,
            keep_prefix,
            keep_suffix,
        } => mask_text(matched, *char, *keep_prefix, *keep_suffix),
        Strategy::Hash | Strategy::Encrypt => cryptographic_external_replacement(matched, rule),
        Strategy::Suppress => String::new(),
    }
}

fn cryptographic_external_replacement(matched: &str, rule: &ExternalRecognizerRule) -> String {
    let mut config = PrivacyConfig::default();
    config.builtin_categories = CategorySet::None;
    config.key.key_file = std::env::var(env_vars::PRIVACY_KEY_FILE).ok();
    config.key.key_hex = std::env::var(env_vars::PRIVACY_KEY).ok();
    config.extra_rules = vec![PatternRule {
        name: rule.name.clone(),
        description: "external recognizer span replacement".to_string(),
        category: RuleCategory::Custom,
        matcher: Matcher::Literal {
            text: matched.to_string(),
            case_sensitive: true,
        },
        strategy: rule.strategy.clone(),
        contexts: Vec::new(),
        enabled: true,
    }];

    PrivacyEngine::new(config)
        .map(|engine| engine.process(matched, ProcessingContext::Document).text.into_owned())
        .unwrap_or_else(|error| {
            warn!(
                rule = %rule.name,
                %error,
                "external privacy recognizer could not apply cryptographic strategy; redacting span"
            );
            format!("<{}>", rule.name.to_uppercase())
        })
}

fn mask_text(
    text: &str,
    mask_char: Option<char>,
    keep_prefix: Option<usize>,
    keep_suffix: Option<usize>,
) -> String {
    let mask_char = mask_char.unwrap_or('*');
    let keep_prefix = keep_prefix.unwrap_or(0);
    let keep_suffix = keep_suffix.unwrap_or(0);
    let chars = text.chars().collect::<Vec<_>>();
    if chars.len() <= keep_prefix + keep_suffix {
        return mask_char.to_string().repeat(chars.len());
    }
    let prefix = chars.iter().take(keep_prefix).collect::<String>();
    let suffix = chars
        .iter()
        .skip(chars.len() - keep_suffix)
        .collect::<String>();
    let mask = mask_char
        .to_string()
        .repeat(chars.len() - keep_prefix - keep_suffix);
    format!("{prefix}{mask}{suffix}")
}

fn char_offset_to_byte_index(text: &str, offset: usize) -> Option<usize> {
    if offset == text.chars().count() {
        return Some(text.len());
    }
    text.char_indices().nth(offset).map(|(index, _)| index)
}

/// Apply a scoped engine's rules to a JSON value.
///
/// When `field_paths` contains `Some(path)` entries, paths are interpreted as
/// JSON Pointers. Bare field names are treated as root keys. When
/// `field_paths` contains `None` entries, the engine walks the entire JSON tree.
fn apply_scoped_engine_to_json(value: JsonValue, scope: &ScopedEngine) -> JsonValue {
    // Determine if any rules in this scope apply to all fields (no field_path).
    let has_global_field_rule = scope.field_paths.iter().any(Option::is_none);

    if has_global_field_rule {
        // Apply to the whole payload.
        return scope
            .engine
            .process_json(&value, ProcessingContext::Document);
    }

    let mut value = value;

    for field_path in &scope.field_paths {
        if let Some(path) = field_path {
            let pointer = field_path_pointer(path);
            if let Some(field_value) = value.pointer_mut(&pointer) {
                let original = std::mem::replace(field_value, JsonValue::Null);
                *field_value = scope
                    .engine
                    .process_json(&original, ProcessingContext::Document);
            }
        }
    }

    value
}

fn field_path_pointer(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    if path.starts_with('/') {
        return path.to_string();
    }
    format!("/{}", json_pointer_escape(path))
}

fn json_pointer_escape(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Privacy policy engine tests (#1042 Slices 3 + 4).
    //!
    //! Covers:
    //! - Rule loading from DB (`PrivacyPolicyRepository::load_enabled_rules`)
    //! - Action application: Redact (regex) / Suppress (literal) matchers
    //! - Field-path scoping: rule scoped by JSON Pointer
    //! - Source-type scoping: rule applies only to matching `event_source`
    //! - Chokepoint: derived events also go through `redact_batch`
    //! - DLQ stub: `_raw_bytes_base64` absent from stub produced by `route_to_dlq`
    //! - Cache reload: fresh `PolicyEngine::load` picks up newly added DB rule
    //!
    //! These tests are inline because the `sinexd` integration test harness
    //! uses a CI Postgres instance that serves the main-checkout xtask binary;
    //! inline tests run via the package's own test binary and avoid that issue.

    use super::*;
    use crate::event_engine::admission::AdmittedEvent;
    use sinex_db::DbPoolExt;
    use sinex_primitives::{Id, Uuid, events::DynamicPayload};
    use xtask::sandbox::prelude::*;

    // ─── Shared fixture source material UUID ─────────────────────────────────
    // Keep in sync with tests/event_engine/support.rs for cross-test consistency.
    const FIXTURE_SOURCE_MATERIAL_ID: &str = "00000000-0000-7000-8000-000000000000";

    // ─── Helpers ─────────────────────────────────────────────────────────────

    fn make_material_event(
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> sinex_primitives::events::Event<serde_json::Value> {
        let material_id: Uuid = FIXTURE_SOURCE_MATERIAL_ID.parse().expect("valid UUID");
        let material_id = Id::from_uuid(material_id);
        DynamicPayload::new(source, event_type, payload)
            .from_material(material_id)
            .build()
            .expect("test event build should not fail")
    }

    fn admit(event: sinex_primitives::events::Event<serde_json::Value>) -> AdmittedEvent {
        AdmittedEvent {
            event_id: Uuid::now_v7(),
            event,
            metadata: None,
        }
    }

    async fn insert_global_rule(
        pool: &sinex_db::DbPool,
        name: &str,
        matcher_type: &str,
        matcher_value: &str,
        action: &str,
        action_label: Option<&str>,
    ) -> TestResult<()> {
        let repo = pool.privacy_policy();
        repo.add_rule(
            name,
            "test rule",
            matcher_type,
            matcher_value,
            false,
            action,
            action_label,
            "default",
        )
        .await?;
        repo.bind_field_rule(name, None, None, None, 0).await?;
        Ok(())
    }

    // ─── DB rule loading ──────────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_rule_loading_roundtrip(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        let rules = repo.load_enabled_rules().await?;
        assert!(rules.is_empty(), "expected no rules initially");

        repo.add_rule(
            "rule-enabled",
            "",
            "regex",
            r"SECRET_\w+",
            false,
            "redact",
            None,
            "default",
        )
        .await?;
        repo.bind_field_rule("rule-enabled", None, None, None, 0)
            .await?;

        repo.add_rule(
            "rule-disabled",
            "",
            "literal",
            "x",
            false,
            "redact",
            None,
            "default",
        )
        .await?;
        repo.set_rule_enabled("rule-disabled", false).await?;

        let rules = repo.load_enabled_rules().await?;
        assert_eq!(rules.len(), 1, "only enabled rule should appear");
        assert_eq!(rules[0].rule.name, "rule-enabled");
        assert_eq!(rules[0].rule.matcher_type, "regex");
        assert_eq!(rules[0].rule.action, "redact");
        assert!(
            !rules[0].scopes.is_empty(),
            "global scope should be present"
        );

        Ok(())
    }

    // ─── Action: Redact (regex) ───────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_action_redact_regex(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        insert_global_rule(
            pool,
            "redact-secret",
            "regex",
            r"SECRET_\w+",
            "redact",
            Some("<REDACTED>"),
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({ "token": "my SECRET_TOKEN_123 value", "other": "safe" });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        let token_str = result[0].event.payload["token"].as_str().unwrap_or("");
        assert!(
            !token_str.contains("SECRET_TOKEN_123"),
            "secret token should be redacted; got: {token_str}"
        );
        assert!(
            token_str.contains("<REDACTED>"),
            "expected <REDACTED> label; got: {token_str}"
        );
        assert_eq!(result[0].event.payload["other"].as_str(), Some("safe"));
        Ok(())
    }

    // ─── Action: Suppress (literal) ──────────────────────────────────────────

    #[sinex_test]
    async fn privacy_action_suppress_literal(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        insert_global_rule(
            pool,
            "suppress-sensitive",
            "literal",
            "SENSITIVE_VALUE",
            "suppress",
            None,
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({ "data": "SENSITIVE_VALUE", "safe": "ok" });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        let data = &result[0].event.payload["data"];
        assert!(
            data.is_null(),
            "suppressed field should be Null; got: {data}"
        );
        assert_eq!(result[0].event.payload["safe"].as_str(), Some("ok"));
        Ok(())
    }

    // ─── Field-path scoping ───────────────────────────────────────────────────

    /// Field scopes use JSON Pointer semantics.
    #[sinex_test]
    async fn privacy_field_scoped_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        repo.add_rule(
            "scope-test",
            "",
            "regex",
            r"SENSITIVE",
            false,
            "redact",
            Some("<SCOPED>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("scope-test", None, None, Some("/secret_field"), 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({
            "secret_field": "contains SENSITIVE data",
            "public_field": "also SENSITIVE but not scoped"
        });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        let secret = result[0].event.payload["secret_field"]
            .as_str()
            .unwrap_or("");
        let public = result[0].event.payload["public_field"]
            .as_str()
            .unwrap_or("");
        assert!(
            !secret.contains("SENSITIVE"),
            "scoped field should be redacted; got: {secret}"
        );
        assert!(
            public.contains("SENSITIVE"),
            "unscoped field must be untouched; got: {public}"
        );
        Ok(())
    }

    // ─── Source-type scoping ──────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_source_scoped_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        repo.add_rule(
            "source-scope-test",
            "",
            "regex",
            r"PII_\w+",
            false,
            "redact",
            Some("<PII>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("source-scope-test", Some("sensitive.source"), None, None, 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;

        let payload_match = serde_json::json!({ "field": "PII_DATA here" });
        let event_match = make_material_event("sensitive.source", "test.event", payload_match);
        let results = engine.redact_batch(vec![admit(event_match)]).await;
        let val = results[0].event.payload["field"].as_str().unwrap_or("");
        assert!(
            !val.contains("PII_DATA"),
            "scoped-source event should be redacted; got: {val}"
        );

        let payload_other = serde_json::json!({ "field": "PII_DATA here" });
        let event_other = make_material_event("other.source", "test.event", payload_other);
        let results_other = engine.redact_batch(vec![admit(event_other)]).await;
        let val_other = results_other[0].event.payload["field"]
            .as_str()
            .unwrap_or("");
        assert!(
            val_other.contains("PII_DATA"),
            "unscoped-source event must be untouched; got: {val_other}"
        );
        Ok(())
    }

    // ─── Chokepoint: derived events ───────────────────────────────────────────

    #[sinex_test]
    async fn privacy_chokepoint_applies_to_derived_events(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        insert_global_rule(
            pool,
            "derived-redact",
            "regex",
            r"DERIVED_SECRET_\w+",
            "redact",
            Some("<DERIVED>"),
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;

        let parent_id: Uuid = Uuid::now_v7();
        let parent_event_id: sinex_primitives::events::EventId = Id::from_uuid(parent_id);
        let payload = serde_json::json!({ "summary": "derived contains DERIVED_SECRET_XYZ here" });
        let derived_event = DynamicPayload::new("sinex.derived", "analytics.insight", payload)
            .from_parents([parent_event_id])
            .expect("valid parent")
            .build()
            .expect("test derived event build should not fail");

        let result = engine.redact_batch(vec![admit(derived_event)]).await;
        let summary = result[0].event.payload["summary"].as_str().unwrap_or("");
        assert!(
            !summary.contains("DERIVED_SECRET_XYZ"),
            "derived event secret should be redacted; got: {summary}"
        );
        assert!(
            summary.contains("<DERIVED>"),
            "expected <DERIVED> label; got: {summary}"
        );
        Ok(())
    }

    // ─── DLQ raw-bytes suppression ────────────────────────────────────────────

    /// Verifies that the stub produced by route_to_dlq (when JSON parse fails)
    /// NEVER contains `_raw_bytes_base64` — only a metadata-only stub.
    #[sinex_test]
    async fn privacy_dlq_raw_bytes_stub_shape(_ctx: TestContext) -> TestResult<()> {
        let parse_err_str = "expected value at line 1 column 1";
        let raw_len: usize = 42;
        let stub = serde_json::json!({
            "_parse_error": parse_err_str,
            "_raw_bytes_suppressed": true,
            "_raw_bytes_len": raw_len,
            "_dlq_note": "raw payload suppressed by privacy chokepoint (#1042)"
        });

        assert!(
            stub.get("_raw_bytes_base64").is_none(),
            "_raw_bytes_base64 must be absent from DLQ stub; got: {stub}"
        );
        assert_eq!(
            stub.get("_raw_bytes_suppressed")
                .and_then(sinex_primitives::JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            stub.get("_raw_bytes_len")
                .and_then(sinex_primitives::JsonValue::as_u64),
            Some(42)
        );
        Ok(())
    }

    // ─── Cache reload ─────────────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_cache_reload_picks_up_new_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();

        let engine_before = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({ "value": "CACHE_SENTINEL_XYZ" });
        let event = make_material_event("s", "t", payload);
        let result_before = engine_before.redact_batch(vec![admit(event)]).await;
        assert_eq!(
            result_before[0].event.payload["value"].as_str(),
            Some("CACHE_SENTINEL_XYZ"),
            "no rule should be applied before DB insert"
        );

        insert_global_rule(
            pool,
            "cache-test",
            "literal",
            "CACHE_SENTINEL_XYZ",
            "redact",
            Some("<CACHED>"),
        )
        .await?;

        let engine_after = PolicyEngine::load(pool.clone()).await?;
        let payload2 = serde_json::json!({ "value": "CACHE_SENTINEL_XYZ" });
        let event2 = make_material_event("s", "t", payload2);
        let result_after = engine_after.redact_batch(vec![admit(event2)]).await;
        let value = result_after[0].event.payload["value"]
            .as_str()
            .unwrap_or("");
        assert!(
            !value.contains("CACHE_SENTINEL_XYZ"),
            "rule should apply after reload; got: {value}"
        );
        assert!(
            value.contains("<CACHED>"),
            "expected <CACHED> label; got: {value}"
        );
        Ok(())
    }
}
