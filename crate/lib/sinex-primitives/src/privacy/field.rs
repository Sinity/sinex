//! Per-field privacy decision logging.
//!
//! Used by declarative parsers (via the `#[derive(SourceRecord)]` macro) to
//! record what the privacy engine did to each field in an emitted event,
//! so #1072 audit/export/redact CLI can answer "why is this field blank?"
//! and "was this event suppressed?".
//!
//! Imperative parsers don't populate this — they leave
//! `ParsedEventIntent.field_privacy_log = None` and behave identically to
//! their pre-#1100 selves.
//!
//! # Backward-compat invariant
//!
//! No existing call to `privacy::process()` changes behavior. The macro emits
//! its own helper invocations alongside the engine call to *record* the
//! decision; the engine itself is unchanged.

use serde::{Deserialize, Serialize};

use crate::privacy::{ProcessingContext, Processed, Strategy};

// Note: not deriving `JsonSchema` because `Strategy` doesn't implement it.
// `FieldPrivacyDecision` is consumed via `serde_json` not via JSON schema
// generation; if a future consumer needs it, add JsonSchema to Strategy
// in privacy/mod.rs and uncomment here.

/// One field's privacy outcome at parse time.
///
/// Populated per field by the macro-generated parser code. Multiple entries
/// per event accumulate into `ParsedEventIntent.field_privacy_log`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldPrivacyDecision {
    /// Field name in the parser's payload struct (e.g. `"command"`).
    pub field: String,

    /// Privacy context the engine ran under.
    pub context: ProcessingContext,

    /// The strategy that fired, if any rule matched. `None` means the engine
    /// ran but no rule matched (the value passed through unchanged).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<Strategy>,

    /// Names of rules that matched, in application order. Empty if no match.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matched_rules: Vec<String>,

    /// Whether the value the parser placed in the payload differs from the
    /// raw input (i.e. some redaction/encryption/hash/mask occurred).
    pub redacted: bool,

    /// Whether the field was dropped from the payload entirely (a `Suppress`
    /// strategy fired).
    pub suppressed: bool,

    /// Whether the entire event was suppressed because of this field
    /// (set when a `#[suppress_if]` attribute drops the whole event).
    pub whole_event_suppressed: bool,
}

impl FieldPrivacyDecision {
    /// Build a decision from the engine's `Processed` result for a non-suppressed
    /// field. The `redacted` flag reflects whether the engine returned a
    /// modified value.
    #[must_use]
    pub fn from_processed(
        field: impl Into<String>,
        context: ProcessingContext,
        processed: &Processed<'_>,
    ) -> Self {
        Self {
            field: field.into(),
            context,
            strategy: None, // strategy is per-rule; engine doesn't expose which fired
            matched_rules: processed.matched_rules.clone(),
            redacted: !processed.matched_rules.is_empty() && !processed.suppressed,
            suppressed: processed.suppressed,
            whole_event_suppressed: false,
        }
    }

    /// Build a decision for a field dropped via `#[suppress_if]` (parser-level
    /// suppression, not engine-level).
    #[must_use]
    pub fn suppressed_by_predicate(
        field: impl Into<String>,
        context: ProcessingContext,
    ) -> Self {
        Self {
            field: field.into(),
            context,
            strategy: Some(Strategy::Suppress),
            matched_rules: Vec::new(),
            redacted: false,
            suppressed: true,
            whole_event_suppressed: false,
        }
    }

    /// Mark this decision as having dropped the entire event. Used when a
    /// `#[suppress_if(whole_event = true)]` predicate fires.
    #[must_use]
    pub fn into_whole_event_suppressor(mut self) -> Self {
        self.whole_event_suppressed = true;
        self.suppressed = true;
        self
    }

    /// Build a decision for a field that was not run through the engine at all
    /// (e.g. a non-sensitive numeric field). Useful for completeness when an
    /// audit trail wants to assert "yes, we considered this field; no, no
    /// redaction applied."
    #[must_use]
    pub fn not_processed(field: impl Into<String>, context: ProcessingContext) -> Self {
        Self {
            field: field.into(),
            context,
            strategy: None,
            matched_rules: Vec::new(),
            redacted: false,
            suppressed: false,
            whole_event_suppressed: false,
        }
    }
}

/// Convenience helper used by macro-generated parser code.
///
/// Wraps `privacy::process(value, context)` and returns both the processed
/// text and the decision record in one call.
///
/// # Errors
///
/// Returns the same `&'static PrivacyError` as `privacy::process()` if the
/// engine failed to initialize.
pub fn parser_field_privacy<'a>(
    field: &str,
    value: &'a str,
    context: ProcessingContext,
) -> Result<(Processed<'a>, FieldPrivacyDecision), &'static crate::privacy::PrivacyError> {
    let processed = crate::privacy::process(value, context)?;
    let decision = FieldPrivacyDecision::from_processed(field, context, &processed);
    Ok((processed, decision))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppressed_by_predicate_records_suppression() {
        let d = FieldPrivacyDecision::suppressed_by_predicate(
            "command",
            ProcessingContext::Command,
        );
        assert!(d.suppressed);
        assert!(!d.redacted);
        assert!(!d.whole_event_suppressed);
        assert_eq!(d.field, "command");
        assert!(matches!(d.strategy, Some(Strategy::Suppress)));
    }

    #[test]
    fn into_whole_event_suppressor_propagates() {
        let d = FieldPrivacyDecision::suppressed_by_predicate(
            "command",
            ProcessingContext::Command,
        )
        .into_whole_event_suppressor();
        assert!(d.whole_event_suppressed);
        assert!(d.suppressed);
    }

    #[test]
    fn not_processed_is_a_no_op_record() {
        let d = FieldPrivacyDecision::not_processed("count", ProcessingContext::Metadata);
        assert!(!d.suppressed);
        assert!(!d.redacted);
        assert!(!d.whole_event_suppressed);
        assert!(d.matched_rules.is_empty());
    }
}
