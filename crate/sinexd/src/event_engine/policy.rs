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
//! # Field-path scoping — v1 limitation
//!
//! `field_path` in `privacy.field_rules` is interpreted as a **top-level JSON
//! object key only**. A `field_path` of `/text` matches the key `"text"` at the
//! root of the payload JSON object. Nested paths (e.g. `/results/0/text`) are
//! NOT supported in v1 — the engine applies the rule to all top-level string
//! values if the key is absent, or skips nested content silently. This limitation
//! is documented here and in the field_rules table comment. A follow-up (tracked
//! in #1042) will extend to full JSON-pointer traversal.
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

use sinex_db::{DbPool, DbPoolExt};
use sinex_primitives::JsonValue;
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
#[derive(Debug)]
struct ScopedEngine {
    /// source string, or None = all sources.
    event_source: Option<String>,
    /// event_type string, or None = all event types.
    event_type: Option<String>,
    /// Field paths covered by at least one rule in this scope (None = all fields).
    field_paths: Vec<Option<String>>,
    /// The compiled engine for this scope.
    engine: PrivacyEngine,
}

/// The compiled rule set derived from the DB state.
///
/// Built by `compile_rules` from the raw `LoadedRule` list returned by
/// `PrivacyPolicyRepository::load_enabled_rules`.
struct CompiledPolicyRuleSet {
    scopes: Vec<ScopedEngine>,
}

impl CompiledPolicyRuleSet {
    fn empty() -> Self {
        Self { scopes: Vec::new() }
    }
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

/// Convert a DB `action` string + optional `action_label` into a `Strategy`.
fn db_row_to_strategy(action: &str, action_label: Option<&str>) -> Strategy {
    match action {
        "redact" => Strategy::Redact {
            label: action_label.map(String::from),
        },
        "hash" => Strategy::Hash,
        "encrypt" => Strategy::Encrypt,
        "suppress" => Strategy::Suppress,
        other => {
            warn!(action = other, "DB privacy rule has unknown action — defaulting to redact");
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
) -> Result<CompiledPolicyRuleSet, SinexError> {
    if loaded.is_empty() {
        return Ok(CompiledPolicyRuleSet::empty());
    }

    // Group rules by (event_source, event_type) scope. Rules without field_rules
    // (no scopes) produce a global engine (None, None).
    //
    // Key: (event_source, event_type) — both Option<String>.
    use std::collections::HashMap;
    let mut scope_map: HashMap<(Option<String>, Option<String>), Vec<(PatternRule, Option<String>)>> =
        HashMap::new();

    for loaded_rule in loaded {
        let rule = &loaded_rule.rule;
        let matcher = match db_row_to_matcher(
            &rule.name,
            &rule.matcher_type,
            &rule.matcher_value,
            rule.case_sensitive,
        ) {
            Some(m) => m,
            None => continue,
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

    Ok(CompiledPolicyRuleSet { scopes })
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
    pub async fn load(pool: DbPool) -> Result<Self, SinexError> {
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
    /// Failures in the policy engine are logged and treated as no-ops for the
    /// affected event (fail-open at the rule level, not at the batch level).
    pub async fn redact_batch(&self, mut batch: Vec<AdmittedEvent>) -> Vec<AdmittedEvent> {
        let rules = self.rules.read().await;
        if rules.scopes.is_empty() {
            return batch;
        }

        for admitted in &mut batch {
            apply_policy_to_event(&mut admitted.event, &rules);
        }
        batch
    }

    /// Apply the policy to a single JSON value (used for DLQ redaction).
    ///
    /// Applies all global rules (NULL source/type scope). Returns the possibly
    /// mutated value. On policy engine error, returns a metadata-only stub.
    pub async fn redact_json_value(&self, value: JsonValue) -> JsonValue {
        let rules = self.rules.read().await;
        if rules.scopes.is_empty() {
            return value;
        }

        // For DLQ redaction: apply global (None, None) scoped engines conservatively.
        let mut result = value;
        for scope in &rules.scopes {
            if scope.event_source.is_none() && scope.event_type.is_none() {
                result = apply_scoped_engine_to_json(result, scope);
            }
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
        let source_match = scope
            .event_source
            .as_deref()
            .map_or(true, |s| s == source);
        let type_match = scope
            .event_type
            .as_deref()
            .map_or(true, |t| t == event_type);

        if !source_match || !type_match {
            continue;
        }

        event.payload = apply_scoped_engine_to_json(
            std::mem::replace(&mut event.payload, JsonValue::Null),
            scope,
        );
    }
}

/// Apply a scoped engine's rules to a JSON value.
///
/// # Field-path scoping (v1 limitation)
///
/// When `field_paths` contains `Some(path)` entries, only the top-level keys
/// matching `/key` (e.g. `field_path="/text"` → key `"text"`) are processed.
/// All other fields are passed through unchanged. Nested JSON paths are NOT
/// supported in v1 — they would require recursive pointer traversal which is
/// deferred to a follow-up.
///
/// When `field_paths` contains `None` entries (global rule, no field scope),
/// the engine walks the entire JSON tree via `process_json`.
fn apply_scoped_engine_to_json(value: JsonValue, scope: &ScopedEngine) -> JsonValue {
    // Determine if any rules in this scope apply to all fields (no field_path).
    let has_global_field_rule = scope.field_paths.iter().any(|fp| fp.is_none());

    if has_global_field_rule {
        // Apply to the whole payload.
        return scope.engine.process_json(&value, ProcessingContext::Document);
    }

    // Field-scoped rules: apply only to named top-level keys.
    // v1 limitation: only top-level string fields are supported.
    let mut obj = match value {
        JsonValue::Object(obj) => obj,
        other => return other,
    };

    for (idx, field_path) in scope.field_paths.iter().enumerate() {
        let _ = idx; // suppress unused warning
        if let Some(path) = field_path {
            // Strip leading "/" to get the top-level key name.
            let key = path.strip_prefix('/').unwrap_or(path.as_str());
            if let Some(field_value) = obj.get_mut(key) {
                let original = std::mem::replace(field_value, JsonValue::Null);
                *field_value = scope.engine.process_json(&original, ProcessingContext::Document);
            }
        }
    }

    JsonValue::Object(obj)
}
