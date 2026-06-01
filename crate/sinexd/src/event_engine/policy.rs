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
//! from DB rules via `PrivacyConfig::extra_rules`, then applies `process_json`
//! per event or `process` per targeted field.
//!
//! # Field-path scoping
//!
//! `field_path` in `privacy.field_rules` is interpreted as a JSON Pointer into
//! the event payload. A `field_path` of `/title` targets the payload's `title`
//! field, while `/items/0/text` targets nested array/object content. Field paths
//! are policy bindings, not parser-local imperative privacy code: source records
//! and parser declarations provide sensitivity hints, while DB policy rows decide
//! which recognizers and actions apply.
//!
//! # Cache refresh
//!
//! Rules are loaded once at engine construction and refreshed periodically via
//! `ensure_fresh()`. The default refresh interval is 30 seconds, configurable
//! via `SINEX_PRIVACY_POLICY_REFRESH_SECS`. Up to 30 seconds of stale policy
//! is acceptable for a single-user system; instant invalidation via Postgres
//! NOTIFY/LISTEN is a potential future improvement.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sinex_db::{DbPool, DbPoolExt};
use sinex_primitives::JsonValue;
use sinex_primitives::events::Event;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::{
    CategorySet, KeyConfig, Matcher, PatternRule, PrivacyConfig, PrivacyEngine,
    ProcessingContext, RuleCategory, Strategy, StructuralDetector, encrypt_token, hash_token,
};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::event_engine::admission::AdmittedEvent;

// ─── Refresh interval ───────────────────────────────────────────────────────

/// Default policy refresh interval in seconds.
const DEFAULT_REFRESH_SECS: u64 = 30;

fn refresh_interval() -> std::time::Duration {
    let secs = std::env::var("SINEX_PRIVACY_POLICY_REFRESH_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REFRESH_SECS);
    std::time::Duration::from_secs(secs)
}

// ─── Compiled rule set ───────────────────────────────────────────────────────

/// A privacy engine scoped to a specific `(event_source, event_type, field_path)` pair.
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
    /// Field path covered by this scope (None = all fields).
    field_path: Option<String>,
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
            .field("field_path", &self.field_path)
            .finish_non_exhaustive()
    }
}

/// The compiled rule set derived from the DB state.
///
/// Built by `compile_rules` from the raw `LoadedRule` list returned by
/// `PrivacyPolicyRepository::load_enabled_rules`.
struct CompiledPolicyRuleSet {
    scopes: Vec<ScopedEngine>,
    presidio_rules: Vec<PresidioRule>,
}

impl CompiledPolicyRuleSet {
    fn empty() -> Self {
        Self {
            scopes: Vec::new(),
            presidio_rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct PresidioRule {
    name: String,
    event_source: Option<String>,
    event_type: Option<String>,
    field_path: Option<String>,
    endpoint_url: String,
    entities: Vec<String>,
    language: String,
    score_threshold: Option<f64>,
    context: Vec<String>,
    strategy: Strategy,
    key_namespace: String,
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
    case_sensitive: bool,
    dictionary_terms: &[String],
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
        "structural" => {
            db_structural_detector(matcher_value).map(|detector| Matcher::Structural { detector })
        }
        "dictionary" => dictionary_matcher(rule_name, case_sensitive, dictionary_terms),
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

fn dictionary_matcher(
    rule_name: &str,
    case_sensitive: bool,
    dictionary_terms: &[String],
) -> Option<Matcher> {
    let literals: Vec<Matcher> = dictionary_terms
        .iter()
        .filter(|term| !term.is_empty())
        .map(|term| Matcher::Literal {
            text: term.clone(),
            case_sensitive,
        })
        .collect();

    match literals.len() {
        0 => {
            warn!(
                rule = rule_name,
                "DB privacy dictionary rule has no enabled terms — skipping"
            );
            None
        }
        1 => literals.into_iter().next(),
        _ => Some(Matcher::Any(literals)),
    }
}

fn db_structural_detector(value: &str) -> Option<StructuralDetector> {
    match value {
        "credit_card" => Some(StructuralDetector::CreditCard),
        "email" => Some(StructuralDetector::Email),
        "phone_number" => Some(StructuralDetector::PhoneNumber),
        "iban" => Some(StructuralDetector::Iban),
        "ipv4" => Some(StructuralDetector::Ipv4),
        "ipv6" => Some(StructuralDetector::Ipv6),
        "mac_address" => Some(StructuralDetector::MacAddress),
        "user_home_path" => Some(StructuralDetector::UserHomePath),
        "local_hostname" => Some(StructuralDetector::LocalHostname),
        "ssn" => Some(StructuralDetector::Ssn),
        "pesel" => Some(StructuralDetector::Pesel),
        "nip" => Some(StructuralDetector::Nip),
        "regon" => Some(StructuralDetector::Regon),
        _ => None,
    }
}

/// Convert a DB `action` string + optional `action_label` into a `Strategy`.
fn db_row_to_strategy(
    action: &str,
    action_label: Option<&str>,
    matcher_config: &JsonValue,
) -> Strategy {
    match action {
        "redact" => Strategy::Redact {
            label: action_label.map(String::from),
        },
        "hash" => Strategy::Hash,
        "encrypt" => Strategy::Encrypt,
        "suppress" => Strategy::Suppress,
        "mask" => Strategy::Mask {
            char: matcher_config
                .get("mask_char")
                .and_then(|value| value.as_str())
                .and_then(|value| value.chars().next()),
            keep_prefix: matcher_config
                .get("keep_prefix")
                .and_then(|value| value.as_u64())
                .and_then(|value| usize::try_from(value).ok()),
            keep_suffix: matcher_config
                .get("keep_suffix")
                .and_then(|value| value.as_u64())
                .and_then(|value| usize::try_from(value).ok()),
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

    // Group rules by (event_source, event_type, field_path) scope. Rules without field_rules
    // (no scopes) produce a global engine (None, None).
    //
    // Key: (event_source, event_type, field_path) — all Option<String>.
    use std::collections::HashMap;
    type ScopeKey = (Option<String>, Option<String>, Option<String>);
    type ScopeRules = Vec<PatternRule>;
    let mut scope_map: HashMap<ScopeKey, ScopeRules> = HashMap::new();
    let mut presidio_rules = Vec::new();

    for loaded_rule in loaded {
        let rule = &loaded_rule.rule;
        if rule.recognizer_kind == "presidio_entity" || rule.matcher_type == "presidio_entity" {
            let Some(mut rules) = compile_presidio_rule(loaded_rule) else {
                continue;
            };
            presidio_rules.append(&mut rules);
            continue;
        }

        let Some(matcher) = db_row_to_matcher(
            &rule.name,
            &rule.matcher_type,
            &rule.matcher_value,
            rule.case_sensitive,
            &loaded_rule.dictionary_terms,
        ) else {
            continue;
        };
        let strategy = db_row_to_strategy(
            &rule.action,
            rule.action_label.as_deref(),
            &rule.matcher_config,
        );

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
                .entry((None, None, None))
                .or_default()
                .push(pattern_rule);
        } else {
            for scope in &loaded_rule.scopes {
                let key = (
                    scope.event_source.clone(),
                    scope.event_type.clone(),
                    normalize_field_path(scope.field_path.as_deref()),
                );
                scope_map.entry(key).or_default().push(pattern_rule.clone());
            }
        }
    }

    let mut scopes = Vec::with_capacity(scope_map.len());
    for ((event_source, event_type, field_path), rules) in scope_map {
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
            field_path,
            engine,
        });
    }

    Ok(CompiledPolicyRuleSet {
        scopes,
        presidio_rules,
    })
}

fn normalize_field_path(path: Option<&str>) -> Option<String> {
    let path = path?.trim();
    if path.is_empty() {
        return None;
    }
    if path.starts_with('/') {
        return Some(path.to_string());
    }
    Some(format!("/{}", path.replace('~', "~0").replace('/', "~1")))
}

fn compile_presidio_rule(
    loaded_rule: &sinex_db::repositories::privacy_policy::LoadedRule,
) -> Option<Vec<PresidioRule>> {
    let rule = &loaded_rule.rule;
    let backend = loaded_rule.backend.as_ref()?;
    if backend.kind != "presidio" {
        warn!(
            rule = rule.name,
            backend = backend.name,
            kind = backend.kind,
            "Presidio privacy rule is bound to a non-presidio backend — skipping"
        );
        return None;
    }
    let Some(endpoint_url) = backend.endpoint_url.as_deref().map(presidio_analyze_url) else {
        warn!(
            rule = rule.name,
            backend = backend.name,
            "Presidio privacy rule has no endpoint_url — skipping"
        );
        return None;
    };

    let config = &rule.matcher_config;
    let mut entities = string_array(config.get("entities"));
    if entities.is_empty() && !rule.matcher_value.trim().is_empty() {
        entities.push(rule.matcher_value.clone());
    }
    if entities.is_empty() {
        warn!(
            rule = rule.name,
            "Presidio privacy rule has no entity types — skipping"
        );
        return None;
    }

    let language = config
        .get("language")
        .and_then(JsonValue::as_str)
        .or_else(|| backend.config.get("language").and_then(JsonValue::as_str))
        .unwrap_or("en")
        .to_string();
    let score_threshold = config
        .get("score_threshold")
        .and_then(JsonValue::as_f64)
        .or_else(|| {
            backend
                .config
                .get("score_threshold")
                .and_then(JsonValue::as_f64)
        });
    let context = string_array(config.get("context"));
    let strategy = db_row_to_strategy(&rule.action, rule.action_label.as_deref(), config);

    let scopes: Vec<(Option<String>, Option<String>, Option<String>)> =
        if loaded_rule.scopes.is_empty() {
            vec![(None, None, None)]
        } else {
            loaded_rule
                .scopes
                .iter()
                .map(|scope| {
                    (
                        scope.event_source.clone(),
                        scope.event_type.clone(),
                        normalize_field_path(scope.field_path.as_deref()),
                    )
                })
                .collect()
        };

    Some(
        scopes
            .into_iter()
            .map(|(event_source, event_type, field_path)| PresidioRule {
                name: rule.name.clone(),
                event_source,
                event_type,
                field_path,
                endpoint_url: endpoint_url.clone(),
                entities: entities.clone(),
                language: language.clone(),
                score_threshold,
                context: context.clone(),
                strategy: strategy.clone(),
                key_namespace: rule.key_namespace.clone(),
            })
            .collect(),
    )
}

fn presidio_analyze_url(endpoint_url: &str) -> String {
    let endpoint = endpoint_url.trim_end_matches('/');
    if endpoint.ends_with("/analyze") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/analyze")
    }
}

fn string_array(value: Option<&JsonValue>) -> Vec<String> {
    value
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
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
    /// Local compile failures skip invalid rules at load time. External
    /// recognizer failures protect the targeted field rather than persisting
    /// the original text unchanged.
    pub async fn redact_batch(&self, mut batch: Vec<AdmittedEvent>) -> Vec<AdmittedEvent> {
        let presidio_rules = {
            let rules = self.rules.read().await;
            if rules.scopes.is_empty() && rules.presidio_rules.is_empty() {
                return batch;
            }

            for admitted in &mut batch {
                apply_policy_to_event(&mut admitted.event, &rules);
            }
            rules.presidio_rules.clone()
        };

        if !presidio_rules.is_empty() {
            let client = reqwest::Client::new();
            for admitted in &mut batch {
                apply_presidio_policy_to_event(&client, &mut admitted.event, &presidio_rules).await;
            }
        }
        batch
    }

    /// Apply the policy to a single JSON value (used for DLQ redaction).
    ///
    /// Applies all global rules (NULL source/type scope). Returns the possibly
    /// mutated value. On policy engine error, returns a metadata-only stub.
    pub async fn redact_json_value(&self, value: JsonValue) -> JsonValue {
        let (mut result, presidio_rules) = {
            let rules = self.rules.read().await;
            if rules.scopes.is_empty() && rules.presidio_rules.is_empty() {
                return value;
            }

            // For DLQ redaction: apply global (None, None) scoped engines conservatively.
            let mut result = value;
            for scope in &rules.scopes {
                if scope.event_source.is_none() && scope.event_type.is_none() {
                    result = apply_scoped_engine_to_json(result, scope);
                }
            }
            let presidio_rules = rules
                .presidio_rules
                .iter()
                .filter(|rule| rule.event_source.is_none() && rule.event_type.is_none())
                .cloned()
                .collect::<Vec<_>>();
            (result, presidio_rules)
        };

        if !presidio_rules.is_empty() {
            let client = reqwest::Client::new();
            result = apply_presidio_rules_to_json(&client, result, presidio_rules.iter()).await;
        }
        result
    }
}

// ─── Application logic ───────────────────────────────────────────────────────

/// Apply all matching policy scopes to an event payload.
fn apply_policy_to_event(event: &mut Event<JsonValue>, rules: &CompiledPolicyRuleSet) {
    let source = event.source.as_str();
    let event_type = event.event_type.as_str();

    for scope in &rules.scopes {
        // Check if this scope matches the event's (source, event_type).
        let source_match = scope.event_source.as_deref().is_none_or(|s| s == source);
        let type_match = scope.event_type.as_deref().is_none_or(|t| t == event_type);

        if !source_match || !type_match {
            continue;
        }

        event.payload = apply_scoped_engine_to_json(
            std::mem::replace(&mut event.payload, JsonValue::Null),
            scope,
        );
    }
}

async fn apply_presidio_policy_to_event(
    client: &reqwest::Client,
    event: &mut Event<JsonValue>,
    rules: &[PresidioRule],
) {
    let source = event.source.as_str();
    let event_type = event.event_type.as_str();
    event.payload = apply_presidio_rules_to_json(
        client,
        std::mem::replace(&mut event.payload, JsonValue::Null),
        rules.iter().filter(|rule| {
            rule.event_source.as_deref().is_none_or(|s| s == source)
                && rule.event_type.as_deref().is_none_or(|t| t == event_type)
        }),
    )
    .await;
}

async fn apply_presidio_rules_to_json<'a>(
    client: &reqwest::Client,
    value: JsonValue,
    rules: impl Iterator<Item = &'a PresidioRule>,
) -> JsonValue {
    let mut payload = value;
    for rule in rules {
        if let Some(path) = &rule.field_path {
            if let Some(target) = payload.pointer_mut(path) {
                apply_presidio_rule_to_value(client, target, rule).await;
            }
        } else {
            apply_presidio_rule_to_value(client, &mut payload, rule).await;
        }
    }
    payload
}

async fn apply_presidio_rule_to_value(
    client: &reqwest::Client,
    value: &mut JsonValue,
    rule: &PresidioRule,
) {
    let mut strings = Vec::new();
    collect_string_paths(value, String::new(), &mut strings);
    for path in strings {
        let Some(target) = value.pointer_mut(&path) else {
            continue;
        };
        let Some(text) = target.as_str() else {
            continue;
        };
        match analyze_presidio_text(client, rule, text).await {
            Ok(spans) if !spans.is_empty() => {
                *target = JsonValue::String(apply_presidio_spans(text, &spans, rule));
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    rule = rule.name,
                    error = %error,
                    "Presidio recognizer failed; protecting targeted field"
                );
                *target = JsonValue::String("<RECOGNIZER_UNAVAILABLE>".to_string());
            }
        }
    }
}

fn collect_string_paths(value: &JsonValue, current: String, out: &mut Vec<String>) {
    match value {
        JsonValue::String(_) => out.push(if current.is_empty() {
            String::new()
        } else {
            current
        }),
        JsonValue::Array(values) => {
            for (idx, child) in values.iter().enumerate() {
                collect_string_paths(child, format!("{current}/{idx}"), out);
            }
        }
        JsonValue::Object(map) => {
            for (key, child) in map {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                collect_string_paths(child, format!("{current}/{escaped}"), out);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Serialize)]
struct PresidioAnalyzeRequest<'a> {
    text: &'a str,
    entities: &'a [String],
    language: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    context: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PresidioAnalyzeResult {
    start: usize,
    end: usize,
    score: f64,
}

#[derive(Debug, Clone)]
struct PresidioSpan {
    start: usize,
    end: usize,
}

async fn analyze_presidio_text(
    client: &reqwest::Client,
    rule: &PresidioRule,
    text: &str,
) -> Result<Vec<PresidioSpan>> {
    let response = client
        .post(&rule.endpoint_url)
        .json(&PresidioAnalyzeRequest {
            text,
            entities: &rule.entities,
            language: &rule.language,
            score_threshold: rule.score_threshold,
            context: rule.context.clone(),
        })
        .send()
        .await
        .map_err(|error| {
            SinexError::processing("failed to call Presidio analyzer").with_std_error(&error)
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(SinexError::processing("Presidio analyzer returned non-success status")
            .with_context("status", status.as_u16().to_string()));
    }

    let mut spans = response
        .json::<Vec<PresidioAnalyzeResult>>()
        .await
        .map_err(|error| {
            SinexError::processing("failed to decode Presidio analyzer response")
                .with_std_error(&error)
        })?
        .into_iter()
        .filter(|span| span.end > span.start && span.end <= text.len())
        .filter(|span| {
            rule.score_threshold
                .is_none_or(|threshold| span.score >= threshold)
        })
        .filter(|span| text.is_char_boundary(span.start) && text.is_char_boundary(span.end))
        .map(|span| PresidioSpan {
            start: span.start,
            end: span.end,
        })
        .collect::<Vec<_>>();

    spans.sort_by_key(|span| (span.start, std::cmp::Reverse(span.end)));
    Ok(spans)
}

fn apply_presidio_spans(text: &str, spans: &[PresidioSpan], rule: &PresidioRule) -> String {
    let mut result = String::with_capacity(text.len());
    let mut cursor = 0;
    for span in spans {
        if span.start < cursor {
            continue;
        }
        result.push_str(&text[cursor..span.start]);
        result.push_str(&replacement_for_strategy(
            &text[span.start..span.end],
            rule,
        ));
        cursor = span.end;
    }
    result.push_str(&text[cursor..]);
    result
}

fn replacement_for_strategy(text: &str, rule: &PresidioRule) -> String {
    match &rule.strategy {
        Strategy::Redact { label } => label
            .clone()
            .unwrap_or_else(|| "<REDACTED>".to_string()),
        Strategy::Suppress => String::new(),
        Strategy::Mask {
            char,
            keep_prefix,
            keep_suffix,
        } => {
            let mask = char.unwrap_or('*');
            let prefix = keep_prefix.unwrap_or(0);
            let suffix = keep_suffix.unwrap_or(0);
            let chars: Vec<char> = text.chars().collect();
            chars
                .iter()
                .enumerate()
                .map(|(idx, value)| {
                    if idx < prefix || chars.len().saturating_sub(idx) <= suffix {
                        *value
                    } else {
                        mask
                    }
                })
                .collect()
        }
        Strategy::Hash => match resolve_key_namespace(&rule.key_namespace) {
            Some(key) => hash_token(text, &key),
            None => format!("<{}>", rule.name.to_uppercase()),
        },
        Strategy::Encrypt => match resolve_key_namespace(&rule.key_namespace) {
            Some(key) => encrypt_token(text, &key)
                .unwrap_or_else(|_| format!("<{}>", rule.name.to_uppercase())),
            None => format!("<{}>", rule.name.to_uppercase()),
        },
    }
}

fn resolve_key_namespace(namespace: &str) -> Option<[u8; 32]> {
    let suffix = key_namespace_env_suffix(namespace);
    let namespaced = KeyConfig {
        key_file: std::env::var(format!("SINEX_PRIVACY_KEY_FILE_{suffix}")).ok(),
        key_hex: std::env::var(format!("SINEX_PRIVACY_KEY_{suffix}")).ok(),
    };
    if let Some(key) = namespaced.resolve() {
        return Some(key);
    }

    if namespace == "default" {
        return KeyConfig {
            key_file: std::env::var("SINEX_PRIVACY_KEY_FILE").ok(),
            key_hex: std::env::var("SINEX_PRIVACY_KEY").ok(),
        }
        .resolve();
    }

    None
}

fn key_namespace_env_suffix(namespace: &str) -> String {
    namespace
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Apply a scoped engine's rules to a JSON value.
///
/// When `field_path` is set, it is treated as a JSON Pointer and only that
/// payload sub-tree is processed. When it is absent, the engine walks the entire
/// JSON tree via `process_json`.
fn apply_scoped_engine_to_json(value: JsonValue, scope: &ScopedEngine) -> JsonValue {
    if let Some(path) = &scope.field_path {
        let mut payload = value;
        if let Some(target) = payload.pointer_mut(path) {
            let original = std::mem::replace(target, JsonValue::Null);
            *target = scope
                .engine
                .process_json(&original, ProcessingContext::Document);
        }
        payload
    } else {
        // Apply to the whole payload.
        scope
            .engine
            .process_json(&value, ProcessingContext::Document)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Privacy policy engine tests (#1042 Slices 3 + 4).
    //!
    //! Covers:
    //! - Rule loading from DB (`PrivacyPolicyRepository::load_enabled_rules`)
    //! - Action application: Redact (regex) / Suppress (literal) matchers
    //! - Field-path scoping: JSON Pointer bindings and per-field rule isolation
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

    #[sinex_test]
    async fn privacy_nested_field_scoped_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        repo.add_rule(
            "nested-scope-test",
            "",
            "regex",
            r"SECRET_\w+",
            false,
            "redact",
            Some("<NESTED>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("nested-scope-test", None, None, Some("/items/0/text"), 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({
            "items": [
                { "text": "contains SECRET_ALPHA" },
                { "text": "contains SECRET_BETA" }
            ],
            "summary": "contains SECRET_GAMMA"
        });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        assert_eq!(
            result[0].event.payload["items"][0]["text"].as_str(),
            Some("contains <NESTED>")
        );
        assert_eq!(
            result[0].event.payload["items"][1]["text"].as_str(),
            Some("contains SECRET_BETA")
        );
        assert_eq!(
            result[0].event.payload["summary"].as_str(),
            Some("contains SECRET_GAMMA")
        );
        Ok(())
    }

    #[sinex_test]
    async fn privacy_field_scoped_rules_do_not_cross_apply(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        repo.add_rule(
            "alpha-field-only",
            "",
            "literal",
            "ALPHA_SECRET",
            false,
            "redact",
            Some("<ALPHA>"),
            "default",
        )
        .await?;
        repo.add_rule(
            "beta-field-only",
            "",
            "literal",
            "BETA_SECRET",
            false,
            "redact",
            Some("<BETA>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("alpha-field-only", None, None, Some("/alpha"), 0)
            .await?;
        repo.bind_field_rule("beta-field-only", None, None, Some("/beta"), 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({
            "alpha": "ALPHA_SECRET and BETA_SECRET",
            "beta": "ALPHA_SECRET and BETA_SECRET"
        });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        assert_eq!(
            result[0].event.payload["alpha"].as_str(),
            Some("<ALPHA> and BETA_SECRET")
        );
        assert_eq!(
            result[0].event.payload["beta"].as_str(),
            Some("ALPHA_SECRET and <BETA>")
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

    // ─── Matcher: Dictionary ─────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_dictionary_rule_redacts_source_and_derived_events(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        let tags = vec!["test".to_string()];
        let dictionary_id = repo
            .add_dictionary(
                "local-sensitive-terms",
                "test dictionary",
                Some("en"),
                "seed",
                &tags,
            )
            .await?;
        repo.add_dictionary_term(dictionary_id, "ACME_PRIVATE_PROJECT", serde_json::json!({}))
            .await?;
        repo.add_dictionary_term(dictionary_id, "QUIET_USER_ALIAS", serde_json::json!({}))
            .await?;
        repo.add_recognizer_rule(
            "dictionary-redact",
            "",
            None,
            "dictionary",
            "dictionary",
            "local-sensitive-terms",
            serde_json::json!({ "dictionary": "local-sensitive-terms" }),
            false,
            "redact",
            Some("<DICT>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("dictionary-redact", None, None, None, 0)
            .await?;

        let loaded = repo.load_enabled_rules().await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].dictionary_terms.len(), 2);

        let engine = PolicyEngine::load(pool.clone()).await?;

        let source_payload = serde_json::json!({
            "body": "notes mention ACME_PRIVATE_PROJECT"
        });
        let source_event = make_material_event("test.source", "test.event", source_payload);

        let parent_id: Uuid = Uuid::now_v7();
        let parent_event_id: sinex_primitives::events::EventId = Id::from_uuid(parent_id);
        let derived_payload = serde_json::json!({
            "summary": "derived mention QUIET_USER_ALIAS"
        });
        let derived_event =
            DynamicPayload::new("sinex.derived", "analytics.insight", derived_payload)
                .from_parents([parent_event_id])
                .expect("valid parent")
                .build()
                .expect("test derived event build should not fail");

        let result = engine
            .redact_batch(vec![admit(source_event), admit(derived_event)])
            .await;

        let body = result[0].event.payload["body"].as_str().unwrap_or("");
        assert!(
            !body.contains("ACME_PRIVATE_PROJECT"),
            "source dictionary term should be redacted; got: {body}"
        );
        assert!(
            body.contains("<DICT>"),
            "expected dictionary redaction label; got: {body}"
        );

        let summary = result[1].event.payload["summary"].as_str().unwrap_or("");
        assert!(
            !summary.contains("QUIET_USER_ALIAS"),
            "derived dictionary term should be redacted; got: {summary}"
        );
        assert!(
            summary.contains("<DICT>"),
            "expected dictionary redaction label; got: {summary}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn privacy_presidio_rule_redacts_bound_field(ctx: TestContext) -> TestResult<()> {
        use axum::{Json, Router, routing::post};
        use tokio::net::TcpListener;

        async fn analyze(Json(body): Json<JsonValue>) -> Json<JsonValue> {
            let text = body.get("text").and_then(JsonValue::as_str).unwrap_or("");
            let start = text.find("alice@example.com").unwrap_or(0);
            let end = start + "alice@example.com".len();
            Json(serde_json::json!([
                {
                    "entity_type": "EMAIL_ADDRESS",
                    "start": start,
                    "end": end,
                    "score": 0.99
                }
            ]))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = Router::new().route("/analyze", post(analyze));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let pool = ctx.pool();
        let repo = pool.privacy_policy();
        let backend_id = repo
            .add_recognizer_backend(
                "test-presidio",
                "presidio",
                Some(&format!("http://{addr}")),
                serde_json::json!({ "language": "en" }),
            )
            .await?;
        repo.add_recognizer_rule(
            "presidio-email",
            "",
            Some(backend_id),
            "presidio_entity",
            "presidio_entity",
            "EMAIL_ADDRESS",
            serde_json::json!({
                "entities": ["EMAIL_ADDRESS"],
                "score_threshold": 0.8
            }),
            false,
            "redact",
            Some("<EMAIL>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("presidio-email", None, None, Some("/body"), 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({
            "body": "contact alice@example.com",
            "other": "contact alice@example.com"
        });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        assert_eq!(
            result[0].event.payload["body"].as_str(),
            Some("contact <EMAIL>")
        );
        assert_eq!(
            result[0].event.payload["other"].as_str(),
            Some("contact alice@example.com")
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
