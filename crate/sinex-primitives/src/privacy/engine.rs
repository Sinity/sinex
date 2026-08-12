//! The privacy engine: compiles rules into an efficient processing pipeline.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use regex::Regex;

use super::config::{CategorySet, PrivacyConfig};
use super::detector;
use super::envelope;
use super::{
    Matcher, PatternRule, PrivacyError, PrivacyMatchFinding, Processed, ProcessingContext,
    RuleCategory, Strategy, StructuralDetector,
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

/// Find case-insensitive match spans without applying offsets from a folded
/// string to the original input. Each folded character retains the byte span
/// of the original character that produced it, so case mappings that expand
/// (`İ` → `i` + combining dot) still redact the complete original character.
fn find_case_insensitive_spans(input: &str, lower: &str) -> Vec<(usize, usize)> {
    if lower.is_empty() {
        return Vec::new();
    }

    let mut folded = String::with_capacity(input.len());
    let mut folded_spans = Vec::new();
    for (original_start, ch) in input.char_indices() {
        let original_end = original_start + ch.len_utf8();
        for folded_ch in ch.to_lowercase() {
            let folded_start = folded.len();
            folded.push(folded_ch);
            folded_spans.push((folded_start, folded.len(), original_start, original_end));
        }
    }

    let mut spans = Vec::new();
    for (folded_start, _) in folded.match_indices(lower) {
        let folded_end = folded_start + lower.len();
        let Some((_, _, original_start, _)) = folded_spans
            .iter()
            .find(|(_, folded_end_for_char, _, _)| *folded_end_for_char > folded_start)
        else {
            continue;
        };
        let Some((_, _, _, original_end)) = folded_spans
            .iter()
            .rev()
            .find(|(folded_start_for_char, _, _, _)| *folded_start_for_char < folded_end)
        else {
            continue;
        };
        spans.push((*original_start, *original_end));
    }

    // A case expansion can make two folded matches refer to the same
    // original character. Coalesce those spans before slicing the input.
    let mut merged = Vec::with_capacity(spans.len());
    for (start, end) in spans {
        if let Some((_, previous_end)) = merged.last_mut()
            && start < *previous_end
        {
            *previous_end = (*previous_end).max(end);
        } else {
            merged.push((start, end));
        }
    }
    merged
}

/// Recursively compile a `Matcher` into a `CompiledMatcher`.
fn compile_matcher(matcher: &Matcher, _rule_name: &str) -> Result<CompiledMatcher, regex::Error> {
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
                .map(|m| compile_matcher(m, _rule_name))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CompiledMatcher::All(compiled))
        }
        Matcher::Any(sub_matchers) => {
            let compiled = sub_matchers
                .iter()
                .map(|m| compile_matcher(m, _rule_name))
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
                    rule.contexts.clone_from(contexts);
                }
            }

            // Apply category-level strategy override
            if rule.category == RuleCategory::Secret
                && let Some(ref s) = config.secret_strategy
            {
                // Only override if rule still has default redact strategy
                if matches!(rule.strategy, Strategy::Redact { .. }) {
                    rule.strategy = s.clone();
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

        // Pre-compute default redact labels so the hot path never allocates.
        for def in &mut definitions {
            if let Strategy::Redact {
                label: ref mut l @ None,
            } = def.strategy
            {
                *l = Some(format!("<{}>", def.name.to_uppercase()));
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
    #[must_use]
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
    #[must_use]
    pub fn process<'a>(&self, input: &'a str, ctx: ProcessingContext) -> Processed<'a> {
        if !self.enabled || input.is_empty() {
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
            if self.matcher_hits_unencrypted(&rule.matcher, input) {
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
    #[must_use]
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

    /// Detect matches without applying a strategy or retaining matched text.
    ///
    /// This is the report-only counterpart to [`Self::process`]. It is used by
    /// bounded privacy audits so the recognizer catalog can measure cleartext
    /// posture without ever constructing a redacted or raw-value report.
    #[must_use]
    pub fn detect_matches(
        &self,
        input: &str,
        ctx: ProcessingContext,
    ) -> Vec<PrivacyMatchFinding> {
        if !self.enabled || input.is_empty() {
            return Vec::new();
        }

        self.rules
            .iter()
            .zip(self.definitions.iter())
            .filter_map(|(rule, definition)| {
                if !rule.matches_context(ctx) {
                    return None;
                }
                let match_count = self.matcher_match_count(&rule.matcher, input);
                (match_count > 0).then(|| PrivacyMatchFinding {
                    rule_name: definition.name.clone(),
                    category: definition.category,
                    match_count,
                })
            })
            .collect()
    }

    /// Check if any Suppress rule matches.
    #[must_use]
    pub fn should_suppress(&self, input: &str, ctx: ProcessingContext) -> bool {
        if !self.enabled {
            return false;
        }
        self.rules.iter().any(|rule| {
            matches!(rule.strategy, Strategy::Suppress)
                && rule.matches_context(ctx)
                && self.matcher_hits_unencrypted(&rule.matcher, input)
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
    #[must_use]
    pub fn catalog(&self) -> &[PatternRule] {
        &self.definitions
    }

    /// Snapshot of per-rule match statistics (name → count).
    #[must_use]
    pub fn stats_snapshot(&self) -> Vec<(String, u64)> {
        self.definitions
            .iter()
            .zip(self.stats.iter())
            .map(|(def, stat)| (def.name.clone(), stat.load(Ordering::Relaxed)))
            .collect()
    }

    // ── Internal ──

    /// Check if a matcher has any hit in the input (without replacement).
    #[allow(
        clippy::self_only_used_in_recursion,
        reason = "Recursive matcher traversal: `&self` keeps the call symmetric with sibling methods that do use engine state"
    )]
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
                    !find_case_insensitive_spans(input, lower).is_empty()
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

    #[allow(
        clippy::self_only_used_in_recursion,
        reason = "Recursive matcher traversal shares the engine's case-folding and structural detector helpers"
    )]
    fn matcher_match_count(&self, matcher: &CompiledMatcher, input: &str) -> u64 {
        match matcher {
            CompiledMatcher::Regex(re) => re.find_iter(input).count() as u64,
            CompiledMatcher::Structural(det) => detector::find_matches(*det, input).len() as u64,
            CompiledMatcher::Literal {
                lower,
                case_sensitive,
                original,
            } => {
                if *case_sensitive {
                    input.match_indices(original.as_str()).count() as u64
                } else {
                    find_case_insensitive_spans(input, lower).len() as u64
                }
            }
            CompiledMatcher::All(sub_matchers) => {
                if sub_matchers
                    .iter()
                    .all(|matcher| self.matcher_match_count(matcher, input) > 0)
                {
                    1
                } else {
                    0
                }
            }
            CompiledMatcher::Any(sub_matchers) => sub_matchers
                .iter()
                .map(|matcher| self.matcher_match_count(matcher, input))
                .sum(),
        }
    }

    fn matcher_hits_unencrypted(&self, matcher: &CompiledMatcher, input: &str) -> bool {
        let token_spans = envelope::find_encrypted_token_spans(input, self.key.as_ref());
        if token_spans.is_empty() {
            return self.matcher_hits(matcher, input);
        }

        let mut segment_start = 0;
        for (token_start, token_end) in token_spans {
            if self.matcher_hits(matcher, &input[segment_start..token_start]) {
                return true;
            }
            segment_start = token_end;
        }
        self.matcher_hits(matcher, &input[segment_start..])
    }

    /// Apply a rule's strategy to the input, returning Some(replaced) if modified.
    fn apply_rule(&self, rule: &CompiledRule, input: &str) -> Option<String> {
        let token_spans = envelope::find_encrypted_token_spans(input, self.key.as_ref());
        if token_spans.is_empty() {
            return self.apply_matcher(&rule.matcher, &rule.strategy, &rule.name, input);
        }

        let mut result = String::with_capacity(input.len());
        let mut segment_start = 0;
        let mut changed = false;
        for (token_start, token_end) in token_spans {
            let segment = &input[segment_start..token_start];
            if let Some(replaced) =
                self.apply_matcher(&rule.matcher, &rule.strategy, &rule.name, segment)
            {
                result.push_str(&replaced);
                changed = true;
            } else {
                result.push_str(segment);
            }
            result.push_str(&input[token_start..token_end]);
            segment_start = token_end;
        }
        let segment = &input[segment_start..];
        if let Some(replaced) =
            self.apply_matcher(&rule.matcher, &rule.strategy, &rule.name, segment)
        {
            result.push_str(&replaced);
            changed = true;
        } else {
            result.push_str(segment);
        }

        changed.then_some(result)
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
                // sinex-24es: apply EVERY matching sub-matcher, not just the
                // first. `Any` compiles DB dictionary rules (event_engine/
                // policy.rs) -- a dictionary of N terms must redact every
                // term present, not just whichever term happens to be first
                // in the list. Chain replacements sequentially, same pattern
                // as `All` above, so a later sub-matcher still sees (and can
                // redact) content the earlier ones didn't touch.
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
                // Label is always pre-computed during engine construction.
                let replacement = label.as_deref().unwrap_or("<UNKNOWN>");
                re.replace_all(input, replacement)
            }
            Strategy::Encrypt => {
                if let Some(ref key) = self.key {
                    let key = *key;
                    re.replace_all(input, |caps: &regex::Captures| {
                        let matched = caps.get(0).map_or("", |m| m.as_str());
                        envelope::encrypt_token(matched, &key)
                            .unwrap_or_else(|error| {
                                tracing::warn!(rule = %rule_name, %error, "PII encryption failed; token redacted");
                                format!("<ENCRYPT_ERR:{}>", rule_name.to_uppercase())
                            })
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
        // For case-insensitive, we need to find actual match positions
        if case_sensitive {
            if !input.contains(original) {
                return None;
            }
            let replacement = self.apply_strategy_to_match(original, strategy, rule_name);
            Some(input.replace(original, &replacement))
        } else {
            // sinex-24es: find case-insensitive match spans WITHOUT indexing
            // `input` using byte offsets computed against a separately
            // lowercased copy. `str::to_lowercase()` is not byte-length-
            // preserving for all Unicode (e.g. U+0130 'İ', 2 bytes, lowercases
            // to "i" + combining-dot-above, 3 bytes) -- reusing `input_lower`'s
            // offsets against `input` can land mid-codepoint (panic:
            // "byte index N is not a char boundary") or silently redact the
            // wrong byte span (data still leaks). `find_case_insensitive_spans`
            // only ever slices `input` at its own real char boundaries.
            let spans = find_case_insensitive_spans(input, lower);
            if spans.is_empty() {
                // No match in the folded-to-original span map means there is
                // no safe original span to replace.
                return None;
            }
            let mut result = String::with_capacity(input.len());
            let mut last_end = 0;
            for (start, end) in spans {
                result.push_str(&input[last_end..start]);
                let matched = &input[start..end];
                let replacement = self.apply_strategy_to_match(matched, strategy, rule_name);
                result.push_str(&replacement);
                last_end = end;
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
            Strategy::Redact { label } => label.as_deref().unwrap_or("<UNKNOWN>").to_string(),
            Strategy::Encrypt => {
                if let Some(ref key) = self.key {
                    envelope::encrypt_token(matched, key).unwrap_or_else(|error| {
                        tracing::warn!(rule = %rule_name, %error, "PII encryption failed; token redacted");
                        format!("<ENCRYPT_ERR:{}>", rule_name.to_uppercase())
                    })
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
#[path = "engine_test.rs"]
mod tests;
