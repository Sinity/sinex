//! Declared rules attach tags to events, including events with unknown time.
//! Every matching tag is an attributed derived record; rules never change
//! the input payload. A rule change gives a new derivation version.
use crate::derived_event::{derived, record_identity};
use crate::{AnyPerEvent, Env, Input, Reads};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::event_types::TagApplied;
use sinex_model::{Event, EventPayload};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagRule {
    pub tag: String,
    pub event_type: String,
    /// Every condition must hold. No conditions match every event of the type.
    #[serde(default)]
    pub when: Vec<Condition>,
}

/// One payload value, addressed by a JSON pointer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Value")]
pub struct Condition {
    pub field: String,
    #[serde(flatten)]
    pub test: Test,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Test {
    Is(Value),
    /// Literal text comparisons lowercase both sides.
    Contains(String),
    StartsWith(String),
    EndsWith(String),
    /// Present and not null, or absent or null.
    Exists(bool),
}

impl Condition {
    fn holds(&self, payload: &Value) -> bool {
        let value = payload.pointer(&self.field);
        let text = || value.and_then(Value::as_str).map(str::to_lowercase);
        match &self.test {
            Test::Is(expected) => value == Some(expected),
            Test::Contains(part) => text().is_some_and(|t| t.contains(&part.to_lowercase())),
            Test::StartsWith(prefix) => {
                text().is_some_and(|t| t.starts_with(&prefix.to_lowercase()))
            }
            Test::EndsWith(suffix) => text().is_some_and(|t| t.ends_with(&suffix.to_lowercase())),
            Test::Exists(present) => value.is_some_and(|v| !v.is_null()) == *present,
        }
    }
}

fn valid_pointer(pointer: &str) -> bool {
    if !pointer.is_empty() && !pointer.starts_with('/') {
        return false;
    }
    let mut chars = pointer.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' && !matches!(chars.next(), Some('0' | '1')) {
            return false;
        }
    }
    true
}

impl TryFrom<Value> for Condition {
    type Error = String;
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let Value::Object(mut fields) = value else {
            return Err("a condition must be an object".into());
        };
        let field = fields
            .remove("field")
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or("a condition field must be a JSON pointer string")?;
        let test =
            serde_json::from_value(Value::Object(fields)).map_err(|e| format!("condition: {e}"))?;
        Ok(Self { field, test })
    }
}

/// Every registered kind except this derivation's output and telemetry.
fn taggable() -> &'static [&'static str] {
    static TYPES: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
        let mut types: Vec<_> = sinex_model::event_types::ALL
            .iter()
            .map(|kind| kind.event_type)
            .filter(|kind| {
                *kind != TagApplied::EVENT_TYPE && !sinex_model::event_types::is_telemetry(kind)
            })
            .collect();
        types.sort_unstable();
        types
    });
    &TYPES
}

pub struct TagApplier {
    rules: Vec<TagRule>,
    version: u32,
}

impl TagApplier {
    pub const NAME: &'static str = "tag.applier";

    pub fn new(mut rules: Vec<TagRule>) -> Result<Self, String> {
        for rule in &rules {
            if rule.tag.trim().is_empty() {
                return Err("a tag rule has an empty tag".into());
            }
            if !taggable().contains(&rule.event_type.as_str()) {
                return Err(format!(
                    "tag {}: {} is not a type a tag can read",
                    rule.tag, rule.event_type
                ));
            }
            if let Some(condition) = rule.when.iter().find(|c| !valid_pointer(&c.field)) {
                return Err(format!(
                    "tag {}: field {} is not a JSON pointer",
                    rule.tag, condition.field
                ));
            }
        }
        // Rule order has no meaning; one tag lists every matching declaration.
        rules.sort_by_cached_key(|rule| serde_json::to_string(rule).expect("rules serialize"));
        rules.dedup();
        let hash = blake3::hash(&serde_json::to_vec(&rules).expect("rules serialize"));
        let bytes: [u8; 4] = hash.as_bytes()[..4].try_into().expect("four bytes");
        let version = if rules.is_empty() {
            1
        } else {
            2 + (u32::from_le_bytes(bytes) >> 2)
        };
        Ok(Self { rules, version })
    }
}

impl AnyPerEvent for TagApplier {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn version(&self) -> u32 {
        self.version
    }
    fn inputs_types(&self) -> &'static [&'static str] {
        taggable()
    }
    fn output_types(&self) -> &'static [&'static str] {
        &[TagApplied::EVENT_TYPE]
    }
    fn reads(&self, _: &Event) -> Reads {
        Reads::Nothing
    }

    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let event = input.event;
        let mut assignments: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for rule in self.rules.iter().filter(|rule| {
            rule.event_type == event.event_type
                && rule
                    .when
                    .iter()
                    .all(|condition| condition.holds(&event.payload))
        }) {
            assignments
                .entry(&rule.tag)
                .or_default()
                .push(serde_json::to_string(rule).expect("rules serialize"));
        }
        Ok(assignments
            .into_iter()
            .map(|(tag, rules)| {
                let mut output = derived(
                    &TagApplied {
                        tag: tag.to_owned(),
                        record: serde_json::to_string(&(
                            &event.source,
                            event.event_type,
                            record_identity(event),
                        ))
                        .expect("record identity serializes"),
                        event_type: event.event_type.to_owned(),
                        rules,
                        at: event.ts_orig.at,
                    },
                    Self::NAME,
                    [event],
                );
                output.ts_orig = event.ts_orig;
                output
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
