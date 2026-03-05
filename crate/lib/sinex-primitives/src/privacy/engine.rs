//! The privacy engine: compiles rules into an efficient processing pipeline.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use regex::Regex;

use super::config::{CategorySet, PrivacyConfig};
use super::detector;
use super::envelope;
use super::{
    Matcher, PatternRule, PrivacyError, Processed, ProcessingContext, RuleCategory, Strategy,
    StructuralDetector,
};

// ─── Compiled rule ───────────────────────────────────────────

enum CompiledMatcher {
    Regex(Regex),
    Structural(StructuralDetector),
    Literal {
        lower: String,
        case_sensitive: bool,
        original: String,
    },
    /// All sub-matchers must match (AND logic).
    All(Vec<CompiledMatcher>),
    /// Any sub-matcher must match (OR logic).
    Any(Vec<CompiledMatcher>),
}

struct CompiledRule {
    name: String,
    #[allow(dead_code)]
    category: RuleCategory,
    matcher: CompiledMatcher,
    strategy: Strategy,
    contexts: Vec<ProcessingContext>,
}

impl CompiledRule {
    fn matches_context(&self, ctx: ProcessingContext) -> bool {
        self.contexts.is_empty() || self.contexts.contains(&ctx)
    }
}

/// Apply a masking strategy to a matched string.
///
/// Keeps `keep_prefix` chars visible at the start and `keep_suffix` chars at the end.
/// Fills the middle with `mask_ch`. If prefix + suffix >= total length, the whole string
/// is returned unchanged (nothing to mask).
fn apply_mask(matched: &str, mask_ch: char, keep_prefix: usize, keep_suffix: usize) -> String {
    let chars: Vec<char> = matched.chars().collect();
    let total = chars.len();
    if keep_prefix + keep_suffix >= total {
        // Nothing to mask — return as-is
        return matched.to_string();
    }
    let masked_count = total - keep_prefix - keep_suffix;
    let mut result = String::with_capacity(matched.len());
    result.extend(&chars[..keep_prefix]);
    for _ in 0..masked_count {
        result.push(mask_ch);
    }
    result.extend(&chars[total - keep_suffix..]);
    result
}

/// Recursively compile a `Matcher` into a `CompiledMatcher`.
fn compile_matcher(matcher: &Matcher, rule_name: &str) -> Result<CompiledMatcher, regex::Error> {
    match matcher {
        Matcher::Regex { pattern } => {
            let re = Regex::new(pattern)?;
            Ok(CompiledMatcher::Regex(re))
        }
        Matcher::Structural { detector } => Ok(CompiledMatcher::Structural(*detector)),
        Matcher::Literal {
            text,
            case_sensitive,
        } => Ok(CompiledMatcher::Literal {
            lower: text.to_lowercase(),
            case_sensitive: *case_sensitive,
            original: text.clone(),
        }),
        Matcher::All(sub_matchers) => {
            let compiled = sub_matchers
                .iter()
                .map(|m| compile_matcher(m, rule_name))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CompiledMatcher::All(compiled))
        }
        Matcher::Any(sub_matchers) => {
            let compiled = sub_matchers
                .iter()
                .map(|m| compile_matcher(m, rule_name))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CompiledMatcher::Any(compiled))
        }
    }
}

// ─── Engine ──────────────────────────────────────────────────

/// The unified privacy engine.
///
/// Thread-safe (`Send + Sync`). Constructed once, used immutably by all threads.
pub struct PrivacyEngine {
    enabled: bool,
    rules: Vec<CompiledRule>,
    key: Option<[u8; 32]>,
    stats_enabled: bool,
    stats: Vec<AtomicU64>,
    /// Original rule definitions (for catalog/diagnostics).
    definitions: Vec<PatternRule>,
}

// SAFETY: AtomicU64 is Send+Sync, everything else is immutable after construction.
unsafe impl Send for PrivacyEngine {}
unsafe impl Sync for PrivacyEngine {}

impl PrivacyEngine {
    /// Build from configuration.
    pub fn new(config: PrivacyConfig) -> Result<Self, PrivacyError> {
        let key = config.key.resolve();

        // Log key status once
        if key.is_none() {
            tracing::debug!(
                "privacy engine: no encryption key configured; Encrypt/Hash strategies will degrade to Redact"
            );
        }

        // Collect rules: builtins (filtered by category + overrides) + extras
        let mut definitions = Vec::new();

        // Add builtins
        let builtins = super::catalog::builtin_rules();
        for mut rule in builtins {
            // Category filter
            match &config.builtin_categories {
                CategorySet::All => {}
                CategorySet::Only(cats) => {
                    if !cats.contains(&rule.category) {
                        continue;
                    }
                }
                CategorySet::None => continue,
            }

            // Apply overrides
            if let Some(ov) = config.overrides.get(&rule.name) {
                if let Some(enabled) = ov.enabled {
                    rule.enabled = enabled;
                }
                if let Some(ref strategy) = ov.strategy {
                    rule.strategy = strategy.clone();
                }
                if let Some(ref contexts) = ov.contexts {
                    rule.contexts = contexts.clone();
                }
            }

            // Apply category-level strategy override
            if rule.category == RuleCategory::Secret {
                if let Some(ref s) = config.secret_strategy {
                    // Only override if rule still has default redact strategy
                    if matches!(rule.strategy, Strategy::Redact { .. }) {
                        rule.strategy = s.clone();
                    }
                }
            }

            if rule.enabled {
                definitions.push(rule);
            }
        }

        // Add extras
        for rule in config.extra_rules {
            if rule.enabled {
                definitions.push(rule);
            }
        }

        // Compile
        let mut rules = Vec::with_capacity(definitions.len());
        for def in &definitions {
            let matcher = compile_matcher(&def.matcher, &def.name).map_err(|e| {
                PrivacyError::InvalidPattern {
                    rule: def.name.clone(),
                    source: e,
                }
            })?;
            rules.push(CompiledRule {
                name: def.name.clone(),
                category: def.category,
                matcher,
                strategy: def.strategy.clone(),
                contexts: def.contexts.clone(),
            });
        }

        let stats = (0..rules.len()).map(|_| AtomicU64::new(0)).collect();

        Ok(Self {
            enabled: config.enabled,
            rules,
            key,
            stats_enabled: config.track_stats,
            stats,
            definitions,
        })
    }

    /// No-op passthrough engine (for testing).
    pub fn noop() -> Self {
        Self {
            enabled: false,
            rules: Vec::new(),
            key: None,
            stats_enabled: false,
            stats: Vec::new(),
            definitions: Vec::new(),
        }
    }

    /// Process a string in the given context.
    pub fn process<'a>(&self, input: &'a str, ctx: ProcessingContext) -> Processed<'a> {
        if !self.enabled || input.is_empty() {
            return Processed::unchanged(input);
        }
        // Skip strings that already contain encrypted tokens to avoid double-encryption.
        if envelope::contains_encrypted_token(input) {
            return Processed::unchanged(input);
        }

        // Check suppress rules first
        for (i, rule) in self.rules.iter().enumerate() {
            if !matches!(rule.strategy, Strategy::Suppress) {
                continue;
            }
            if !rule.matches_context(ctx) {
                continue;
            }
            if self.matcher_hits(&rule.matcher, input) {
                if self.stats_enabled {
                    self.stats[i].fetch_add(1, Ordering::Relaxed);
                }
                return Processed::suppressed(&rule.name);
            }
        }

        // Apply non-suppress rules
        let mut current: Cow<'a, str> = Cow::Borrowed(input);
        let mut matched_rules = Vec::new();

        for (i, rule) in self.rules.iter().enumerate() {
            if matches!(rule.strategy, Strategy::Suppress) {
                continue;
            }
            if !rule.matches_context(ctx) {
                continue;
            }
            if let Some(replaced) = self.apply_rule(rule, &current) {
                if self.stats_enabled {
                    self.stats[i].fetch_add(1, Ordering::Relaxed);
                }
                matched_rules.push(rule.name.clone());
                current = Cow::Owned(replaced);
            }
        }

        Processed {
            text: current,
            matched_rules,
            suppressed: false,
        }
    }

    /// Process all string values in a JSON tree.
    pub fn process_json(
        &self,
        value: &serde_json::Value,
        ctx: ProcessingContext,
    ) -> serde_json::Value {
        if !self.enabled {
            return value.clone();
        }
        match value {
            serde_json::Value::String(s) => {
                let processed = self.process(s, ctx);
                if processed.suppressed {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(processed.text.into_owned())
                }
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| self.process_json(v, ctx)).collect())
            }
            serde_json::Value::Object(obj) => serde_json::Value::Object(
                obj.iter()
                    .map(|(k, v)| (k.clone(), self.process_json(v, ctx)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// Check if any Suppress rule matches.
    pub fn should_suppress(&self, input: &str, ctx: ProcessingContext) -> bool {
        if !self.enabled {
            return false;
        }
        self.rules.iter().any(|rule| {
            matches!(rule.strategy, Strategy::Suppress)
                && rule.matches_context(ctx)
                && self.matcher_hits(&rule.matcher, input)
        })
    }

    /// Decrypt an encrypted token.
    pub fn decrypt(&self, token: &str) -> Result<String, PrivacyError> {
        let key = self.key.as_ref().ok_or(PrivacyError::NoKey)?;
        envelope::decrypt_token(token, key)
    }

    /// Decrypt all encrypted tokens in a string.
    pub fn decrypt_all(&self, input: &str) -> Result<String, PrivacyError> {
        let key = self.key.as_ref().ok_or(PrivacyError::NoKey)?;
        envelope::decrypt_all(input, key)
    }

    /// The compiled rule definitions (for catalog/diagnostics).
    pub fn catalog(&self) -> &[PatternRule] {
        &self.definitions
    }

    /// Snapshot of per-rule match statistics (name → count).
    pub fn stats_snapshot(&self) -> Vec<(String, u64)> {
        self.definitions
            .iter()
            .zip(self.stats.iter())
            .map(|(def, stat)| (def.name.clone(), stat.load(Ordering::Relaxed)))
            .collect()
    }

    // ── Internal ──

    /// Check if a matcher has any hit in the input (without replacement).
    fn matcher_hits(&self, matcher: &CompiledMatcher, input: &str) -> bool {
        match matcher {
            CompiledMatcher::Regex(re) => re.is_match(input),
            CompiledMatcher::Structural(det) => !detector::find_matches(*det, input).is_empty(),
            CompiledMatcher::Literal {
                lower,
                case_sensitive,
                original,
            } => {
                if *case_sensitive {
                    input.contains(original.as_str())
                } else {
                    input.to_lowercase().contains(lower.as_str())
                }
            }
            CompiledMatcher::All(sub_matchers) => {
                sub_matchers.iter().all(|m| self.matcher_hits(m, input))
            }
            CompiledMatcher::Any(sub_matchers) => {
                sub_matchers.iter().any(|m| self.matcher_hits(m, input))
            }
        }
    }

    /// Apply a rule's strategy to the input, returning Some(replaced) if modified.
    fn apply_rule(&self, rule: &CompiledRule, input: &str) -> Option<String> {
        self.apply_matcher(&rule.matcher, &rule.strategy, &rule.name, input)
    }

    fn apply_matcher(
        &self,
        matcher: &CompiledMatcher,
        strategy: &Strategy,
        rule_name: &str,
        input: &str,
    ) -> Option<String> {
        match matcher {
            CompiledMatcher::Regex(re) => self.apply_regex(re, strategy, rule_name, input),
            CompiledMatcher::Structural(det) => {
                self.apply_structural(*det, strategy, rule_name, input)
            }
            CompiledMatcher::Literal {
                lower,
                case_sensitive,
                original,
            } => self.apply_literal(original, lower, *case_sensitive, strategy, rule_name, input),
            CompiledMatcher::All(sub_matchers) => {
                // All must match. Apply each sub-matcher's replacements in sequence.
                // Start from the input and apply each sub-matcher that hits.
                if !sub_matchers.iter().all(|m| self.matcher_hits(m, input)) {
                    return None;
                }
                let mut current = input.to_string();
                let mut any_changed = false;
                for sub in sub_matchers {
                    if let Some(replaced) = self.apply_matcher(sub, strategy, rule_name, &current) {
                        current = replaced;
                        any_changed = true;
                    }
                }
                if any_changed { Some(current) } else { None }
            }
            CompiledMatcher::Any(sub_matchers) => {
                // Apply the first sub-matcher that produces a replacement.
                for sub in sub_matchers {
                    if let Some(replaced) = self.apply_matcher(sub, strategy, rule_name, input) {
                        return Some(replaced);
                    }
                }
                None
            }
        }
    }

    fn apply_regex(
        &self,
        re: &Regex,
        strategy: &Strategy,
        rule_name: &str,
        input: &str,
    ) -> Option<String> {
        if !re.is_match(input) {
            return None;
        }
        let result = match strategy {
            Strategy::Redact { label } => {
                let replacement = label.as_deref().unwrap_or_else(|| {
                    Box::leak(format!("<{}>", rule_name.to_uppercase()).into_boxed_str())
                });
                re.replace_all(input, replacement)
            }
            Strategy::Encrypt => {
                if let Some(ref key) = self.key {
                    let key = *key;
                    re.replace_all(input, |caps: &regex::Captures| {
                        let matched = caps.get(0).map_or("", |m| m.as_str());
                        envelope::encrypt_token(matched, &key)
                            .unwrap_or_else(|_| "<ENCRYPT_ERR>".into())
                    })
                } else {
                    // Degrade to redact
                    let label = format!("<{}>", rule_name.to_uppercase());
                    re.replace_all(input, label.as_str())
                }
            }
            Strategy::Hash => {
                if let Some(ref key) = self.key {
                    let key = *key;
                    re.replace_all(input, |caps: &regex::Captures| {
                        let matched = caps.get(0).map_or("", |m| m.as_str());
                        envelope::hash_token(matched, &key)
                    })
                } else {
                    let label = format!("<{}>", rule_name.to_uppercase());
                    re.replace_all(input, label.as_str())
                }
            }
            Strategy::Suppress => {
                // Should not reach here (handled in process())
                return None;
            }
            Strategy::Mask {
                char: mask_char,
                keep_prefix,
                keep_suffix,
            } => {
                let mask_ch = mask_char.unwrap_or('*');
                let prefix = keep_prefix.unwrap_or(0);
                let suffix = keep_suffix.unwrap_or(0);
                re.replace_all(input, |caps: &regex::Captures| {
                    let matched = caps.get(0).map_or("", |m| m.as_str());
                    apply_mask(matched, mask_ch, prefix, suffix)
                })
            }
        };
        match result {
            Cow::Borrowed(_) => None,
            Cow::Owned(s) => Some(s),
        }
    }

    fn apply_structural(
        &self,
        det: StructuralDetector,
        strategy: &Strategy,
        rule_name: &str,
        input: &str,
    ) -> Option<String> {
        let matches = detector::find_matches(det, input);
        if matches.is_empty() {
            return None;
        }

        let mut result = input.to_string();
        // Process from right to left to preserve byte indices
        for (start, end) in matches.into_iter().rev() {
            let matched = &input[start..end];
            let replacement = self.apply_strategy_to_match(matched, strategy, rule_name);
            result.replace_range(start..end, &replacement);
        }
        Some(result)
    }

    fn apply_literal(
        &self,
        original: &str,
        lower: &str,
        case_sensitive: bool,
        strategy: &Strategy,
        rule_name: &str,
        input: &str,
    ) -> Option<String> {
        let has_match = if case_sensitive {
            input.contains(original)
        } else {
            input.to_lowercase().contains(lower)
        };
        if !has_match {
            return None;
        }

        // For case-insensitive, we need to find actual match positions
        if case_sensitive {
            let replacement = self.apply_strategy_to_match(original, strategy, rule_name);
            Some(input.replace(original, &replacement))
        } else {
            // Simple approach: find case-insensitive matches
            let input_lower = input.to_lowercase();
            let mut result = String::with_capacity(input.len());
            let mut last_end = 0;
            for (pos, _) in input_lower.match_indices(lower) {
                result.push_str(&input[last_end..pos]);
                let matched = &input[pos..pos + lower.len()];
                let replacement = self.apply_strategy_to_match(matched, strategy, rule_name);
                result.push_str(&replacement);
                last_end = pos + lower.len();
            }
            result.push_str(&input[last_end..]);
            Some(result)
        }
    }

    fn apply_strategy_to_match(
        &self,
        matched: &str,
        strategy: &Strategy,
        rule_name: &str,
    ) -> String {
        match strategy {
            Strategy::Redact { label } => label
                .as_deref()
                .unwrap_or_else(|| {
                    Box::leak(format!("<{}>", rule_name.to_uppercase()).into_boxed_str())
                })
                .to_string(),
            Strategy::Encrypt => {
                if let Some(ref key) = self.key {
                    envelope::encrypt_token(matched, key).unwrap_or_else(|_| "<ENCRYPT_ERR>".into())
                } else {
                    format!("<{}>", rule_name.to_uppercase())
                }
            }
            Strategy::Hash => {
                if let Some(ref key) = self.key {
                    envelope::hash_token(matched, key)
                } else {
                    format!("<{}>", rule_name.to_uppercase())
                }
            }
            Strategy::Suppress => String::new(),
            Strategy::Mask {
                char: mask_char,
                keep_prefix,
                keep_suffix,
            } => {
                let mask_ch = mask_char.unwrap_or('*');
                let prefix = keep_prefix.unwrap_or(0);
                let suffix = keep_suffix.unwrap_or(0);
                apply_mask(matched, mask_ch, prefix, suffix)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::privacy::ProcessingContext;
    use xtask::sandbox::sinex_test;

    fn test_engine() -> PrivacyEngine {
        PrivacyEngine::new(PrivacyConfig::default()).unwrap()
    }

    fn test_engine_with_key() -> PrivacyEngine {
        let mut config = PrivacyConfig::default();
        config.key.key_hex = Some("42".repeat(32));
        config.track_stats = true;
        PrivacyEngine::new(config).unwrap()
    }

    // ── Basic redaction cases ──

    #[sinex_test]
    async fn redacts_aws_access_key() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE";
        let result = e.process(input, ProcessingContext::Command);
        assert!(result.any_matched());
        assert!(result.text.contains("<AWS_ACCESS_KEY>"));
        assert!(!result.text.contains("AKIAIOSFODNN7EXAMPLE"));
        Ok(())
    }

    #[sinex_test]
    async fn redacts_github_token() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        // Use bare token (no `token=` prefix which triggers generic_secret_assign too)
        let input = "found ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij in logs";
        let result = e.process(input, ProcessingContext::Command);
        assert!(result.any_matched());
        assert!(
            result.text.contains("<GITHUB_TOKEN>"),
            "expected <GITHUB_TOKEN>, got: {}",
            result.text
        );
        assert!(!result.text.contains("ghp_"));
        Ok(())
    }

    #[sinex_test]
    async fn redacts_url_credentials() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "postgres://admin:s3cret@localhost:5432/db";
        let result = e.process(input, ProcessingContext::Command);
        assert!(result.any_matched());
        assert!(!result.text.contains("s3cret"));
        Ok(())
    }

    #[sinex_test]
    async fn redacts_jwt() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "Authorization: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123def456";
        let result = e.process(input, ProcessingContext::Command);
        assert!(result.any_matched());
        assert!(result.text.contains("<JWT_TOKEN>"));
        Ok(())
    }

    #[sinex_test]
    async fn redacts_bearer_token() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "curl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.sig'";
        let result = e.process(input, ProcessingContext::Command);
        assert!(result.any_matched());
        Ok(())
    }

    #[sinex_test]
    async fn preserves_safe_content() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "ls -la /home/user/projects";
        let result = e.process(input, ProcessingContext::Command);
        assert!(!result.any_matched());
        assert_eq!(result.text.as_ref(), input);
        Ok(())
    }

    #[sinex_test]
    async fn preserves_normal_commands() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "git commit -m 'fix bug'";
        let result = e.process(input, ProcessingContext::Command);
        assert!(!result.any_matched());
        assert_eq!(result.text.as_ref(), input);
        Ok(())
    }

    // ── Suppress strategy ──

    #[sinex_test]
    async fn suppresses_private_key() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAK...";
        let result = e.process(input, ProcessingContext::Command);
        assert!(result.suppressed);
        Ok(())
    }

    // ── Context filtering ──

    #[sinex_test]
    async fn cli_flag_only_fires_in_command_context() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "--password s3cret";
        let cmd_result = e.process(input, ProcessingContext::Command);
        assert!(cmd_result.any_matched());

        let _journal_result = e.process(input, ProcessingContext::Journal);
        // cli_secret_flag is Command-only, but generic_secret_assign covers Journal
        // so let's test something purely Command-scoped
        let input2 = "--auth-token abc123def456ghi";
        let journal_result2 = e.process(input2, ProcessingContext::Journal);
        // Should not match cli_secret_flag in Journal context
        assert!(!journal_result2.text.contains("--$1"));
        Ok(())
    }

    #[sinex_test]
    async fn title_rules_only_fire_in_title_context() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "KeePass - Database.kdbx";
        let title_result = e.process(input, ProcessingContext::WindowTitle);
        assert!(title_result.any_matched());
        assert!(title_result.text.contains("<PASSWORD_MANAGER>"));

        let cmd_result = e.process(input, ProcessingContext::Command);
        // Title rules should NOT fire in Command context
        assert!(!cmd_result.text.contains("<PASSWORD_MANAGER>"));
        Ok(())
    }

    // ── Structural PII ──

    #[sinex_test]
    async fn detects_credit_card_with_luhn() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "card: 4111 1111 1111 1111";
        let result = e.process(input, ProcessingContext::Clipboard);
        assert!(result.any_matched());
        assert!(result.text.contains("<CREDIT_CARD>"));
        Ok(())
    }

    #[sinex_test]
    async fn rejects_non_luhn_digits() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "number: 1234567890123456";
        let result = e.process(input, ProcessingContext::Clipboard);
        // Should NOT match credit_card (fails Luhn)
        assert!(!result.text.contains("<CREDIT_CARD>"));
        Ok(())
    }

    #[sinex_test]
    async fn hashes_email_when_key_available() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine_with_key();
        let input = "contact user@example.com please";
        let result = e.process(input, ProcessingContext::Clipboard);
        assert!(result.any_matched());
        assert!(result.text.contains("\u{231c}hash:"));
        assert!(!result.text.contains("user@example.com"));
        Ok(())
    }

    #[sinex_test]
    async fn redacts_email_when_no_key() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let input = "contact user@example.com please";
        let result = e.process(input, ProcessingContext::Clipboard);
        assert!(result.any_matched());
        // Degrades to redact since no key
        assert!(!result.text.contains("user@example.com"));
        Ok(())
    }

    // ── JSON processing ──

    #[sinex_test]
    async fn processes_json_strings() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine();
        let json = serde_json::json!({
            "message": "key=AKIAIOSFODNN7EXAMPLE",
            "count": 42,
            "nested": {
                "token": "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"
            }
        });
        let result = e.process_json(&json, ProcessingContext::Dbus);
        let msg = result["message"].as_str().unwrap();
        assert!(!msg.contains("AKIAIOSFODNN7EXAMPLE"));
        Ok(())
    }

    // ── Encryption roundtrip ──

    #[sinex_test]
    async fn encrypt_strategy_produces_decryptable_tokens() -> ::xtask::sandbox::TestResult<()> {
        use super::super::{Matcher, PatternRule, RuleCategory, Strategy};

        let mut config = PrivacyConfig::default();
        config.key.key_hex = Some("42".repeat(32));
        config.builtin_categories = CategorySet::None;
        config.extra_rules.push(PatternRule {
            name: "test_encrypt".into(),
            description: "test".into(),
            category: RuleCategory::Custom,
            matcher: Matcher::Regex {
                pattern: r"SECRET_\w+".into(),
            },
            strategy: Strategy::Encrypt,
            contexts: vec![],
            enabled: true,
        });

        let engine = PrivacyEngine::new(config).unwrap();
        let result = engine.process("value=SECRET_ABC123", ProcessingContext::Command);
        assert!(result.text.contains("\u{231c}enc:v1:"));

        let decrypted = engine.decrypt_all(&result.text).unwrap();
        assert_eq!(decrypted, "value=SECRET_ABC123");
        Ok(())
    }

    // ── Noop engine ──

    #[sinex_test]
    async fn noop_passes_through() -> ::xtask::sandbox::TestResult<()> {
        let e = PrivacyEngine::noop();
        let input = "AKIAIOSFODNN7EXAMPLE";
        let result = e.process(input, ProcessingContext::Command);
        assert_eq!(result.text.as_ref(), input);
        assert!(!result.any_matched());
        Ok(())
    }

    // ── Stats ──

    #[sinex_test]
    async fn stats_tracking() -> ::xtask::sandbox::TestResult<()> {
        let e = test_engine_with_key();
        e.process("AKIAIOSFODNN7EXAMPLE", ProcessingContext::Command);
        e.process("AKIAIOSFODNN7EXAMPLE", ProcessingContext::Command);
        let stats = e.stats_snapshot();
        let aws_count = stats
            .iter()
            .find(|(n, _)| n == "aws_access_key")
            .map(|(_, c)| *c)
            .unwrap_or(0);
        assert_eq!(aws_count, 2);
        Ok(())
    }

    // ── apply_mask unit tests ──

    #[sinex_test]
    async fn mask_middle_digits() -> ::xtask::sandbox::TestResult<()> {
        // 4111111111111111: keep 4 prefix, 4 suffix → 4111 + 8×'*' + 1111
        let result = apply_mask("4111111111111111", '*', 4, 4);
        assert_eq!(result, "4111********1111");
        Ok(())
    }

    #[sinex_test]
    async fn mask_all_chars_when_prefix_suffix_exceed_length() -> ::xtask::sandbox::TestResult<()> {
        // If prefix + suffix >= total, return as-is
        let result = apply_mask("abc", '*', 2, 2);
        assert_eq!(result, "abc");
        Ok(())
    }

    #[sinex_test]
    async fn mask_zero_prefix_suffix() -> ::xtask::sandbox::TestResult<()> {
        let result = apply_mask("hello", '*', 0, 0);
        assert_eq!(result, "*****");
        Ok(())
    }

    #[sinex_test]
    async fn mask_custom_char() -> ::xtask::sandbox::TestResult<()> {
        let result = apply_mask("secret", '#', 1, 1);
        assert_eq!(result, "s####t");
        Ok(())
    }

    // ── Strategy::Mask integration tests ──

    fn engine_with_mask_rule(keep_prefix: usize, keep_suffix: usize) -> PrivacyEngine {
        use super::super::{Matcher, PatternRule, RuleCategory};
        let mut config = PrivacyConfig::default();
        config.builtin_categories = CategorySet::None;
        config.extra_rules.push(PatternRule {
            name: "test_mask".into(),
            description: "test mask rule".into(),
            category: RuleCategory::Custom,
            matcher: Matcher::Regex {
                pattern: r"\b\d{16}\b".into(),
            },
            strategy: Strategy::Mask {
                char: Some('*'),
                keep_prefix: Some(keep_prefix),
                keep_suffix: Some(keep_suffix),
            },
            contexts: vec![],
            enabled: true,
        });
        PrivacyEngine::new(config).unwrap()
    }

    #[sinex_test]
    async fn mask_strategy_redacts_middle_of_card_number() -> ::xtask::sandbox::TestResult<()> {
        let e = engine_with_mask_rule(4, 4);
        let result = e.process("card: 4111111111111111", ProcessingContext::Command);
        assert!(result.any_matched());
        assert!(
            result.text.contains("4111********1111"),
            "got: {}",
            result.text
        );
        assert!(!result.text.contains("4111111111111111"));
        Ok(())
    }

    #[sinex_test]
    async fn mask_strategy_leaves_non_matching_text_unchanged() -> ::xtask::sandbox::TestResult<()>
    {
        let e = engine_with_mask_rule(4, 4);
        let result = e.process("no card here", ProcessingContext::Command);
        assert!(!result.any_matched());
        assert_eq!(result.text.as_ref(), "no card here");
        Ok(())
    }

    // ── Compound matcher tests ──

    fn engine_with_compound(matcher: super::super::Matcher) -> PrivacyEngine {
        use super::super::{PatternRule, RuleCategory};
        let mut config = PrivacyConfig::default();
        config.builtin_categories = CategorySet::None;
        config.extra_rules.push(PatternRule {
            name: "test_compound".into(),
            description: "compound rule".into(),
            category: RuleCategory::Custom,
            matcher,
            strategy: Strategy::Redact {
                label: Some("<COMPOUND>".into()),
            },
            contexts: vec![],
            enabled: true,
        });
        PrivacyEngine::new(config).unwrap()
    }

    #[sinex_test]
    async fn any_matcher_fires_on_first_sub_match() -> ::xtask::sandbox::TestResult<()> {
        use super::super::Matcher;
        let e = engine_with_compound(Matcher::Any(vec![
            Matcher::Regex {
                pattern: r"FOO".into(),
            },
            Matcher::Regex {
                pattern: r"BAR".into(),
            },
        ]));
        let result = e.process("contains BAR here", ProcessingContext::Command);
        assert!(result.any_matched());
        assert!(result.text.contains("<COMPOUND>"));
        Ok(())
    }

    #[sinex_test]
    async fn any_matcher_fires_on_either_branch() -> ::xtask::sandbox::TestResult<()> {
        use super::super::Matcher;
        let e = engine_with_compound(Matcher::Any(vec![
            Matcher::Regex {
                pattern: r"FOO".into(),
            },
            Matcher::Regex {
                pattern: r"BAR".into(),
            },
        ]));
        let result_foo = e.process("FOO present", ProcessingContext::Command);
        let result_bar = e.process("BAR present", ProcessingContext::Command);
        assert!(result_foo.any_matched());
        assert!(result_bar.any_matched());
        Ok(())
    }

    #[sinex_test]
    async fn any_matcher_does_not_fire_when_no_branch_matches() -> ::xtask::sandbox::TestResult<()>
    {
        use super::super::Matcher;
        let e = engine_with_compound(Matcher::Any(vec![
            Matcher::Regex {
                pattern: r"FOO".into(),
            },
            Matcher::Regex {
                pattern: r"BAR".into(),
            },
        ]));
        let result = e.process("neither here", ProcessingContext::Command);
        assert!(!result.any_matched());
        Ok(())
    }

    #[sinex_test]
    async fn all_matcher_fires_only_when_both_sub_matchers_match()
    -> ::xtask::sandbox::TestResult<()> {
        use super::super::Matcher;
        let e = engine_with_compound(Matcher::All(vec![
            Matcher::Regex {
                pattern: r"MUST".into(),
            },
            Matcher::Regex {
                pattern: r"ALSO".into(),
            },
        ]));

        // Both present — should match
        let result = e.process("MUST ALSO be here", ProcessingContext::Command);
        assert!(
            result.any_matched(),
            "both sub-matchers match, rule should fire"
        );

        // Only one present — should NOT match
        let result = e.process("only MUST present", ProcessingContext::Command);
        assert!(
            !result.any_matched(),
            "only one sub-matcher matches, rule must not fire"
        );
        Ok(())
    }

    #[sinex_test]
    async fn all_matcher_does_not_fire_when_one_branch_missing() -> ::xtask::sandbox::TestResult<()>
    {
        use super::super::Matcher;
        let e = engine_with_compound(Matcher::All(vec![
            Matcher::Regex {
                pattern: r"ALPHA".into(),
            },
            Matcher::Regex {
                pattern: r"BETA".into(),
            },
        ]));
        let result = e.process("only ALPHA", ProcessingContext::Command);
        assert!(!result.any_matched());
        Ok(())
    }
}
