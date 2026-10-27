//! Immutable declarations and their rules. Evaluation uses normalized payloads
//! and has no database, clock or source dependencies.
use crate::{
    Class, EquivalenceKey, OwnedBlob, RecordState, ReplaceRule, TimeQuality, Timestamp, TsOrig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindRef {
    pub name: String,
    pub version: u64,
}

impl KindRef {
    pub fn database_version(&self) -> Result<i64, String> {
        i64::try_from(self.version)
            .ok()
            .filter(|v| *v > 0)
            .ok_or_else(|| "kind version must be a positive bigint".into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::DbCheck)]
#[serde(rename_all = "snake_case")]
pub enum Validation {
    Validated,
    Unvalidated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Normalization {
    Rust,
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum KeyRule {
    Location,
    Fields {
        fields: Vec<String>,
    },
    WhenPresent {
        field: String,
        fields: Vec<String>,
    },
    WhenEqual {
        field: String,
        value: Value,
        then: Box<KeyRule>,
        otherwise: Box<KeyRule>,
    },
    /// Select fields only when identity text is supplied. Empty provider IDs
    /// never identify two otherwise unrelated records.
    WhenNonempty {
        field: String,
        trim: bool,
        then: Box<KeyRule>,
        otherwise: Box<KeyRule>,
    },
}

impl KeyRule {
    fn fields<'a>(&'a self, payload: &Value) -> Option<&'a Vec<String>> {
        match self {
            Self::Location => None,
            Self::Fields { fields } => Some(fields),
            Self::WhenPresent { field, fields } => payload
                .get(field)
                .filter(|value| !value.is_null())
                .map(|_| fields),
            Self::WhenEqual {
                field,
                value,
                then,
                otherwise,
            } => {
                if payload.get(field) == Some(value) {
                    then.fields(payload)
                } else {
                    otherwise.fields(payload)
                }
            }
            Self::WhenNonempty {
                field,
                trim,
                then,
                otherwise,
            } => {
                if payload
                    .get(field)
                    .and_then(Value::as_str)
                    .is_some_and(|value| !if *trim { value.trim() } else { value }.is_empty())
                {
                    then.fields(payload)
                } else {
                    otherwise.fields(payload)
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum TimeRule {
    Unknown,
    FirstPresent {
        fields: Vec<String>,
        quality: TimeQuality,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum RemovedRule {
    Never,
    WhenPresent { field: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedBlobRule {
    pub field: String,
    pub size: String,
    pub when: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindDeclaration {
    pub kind: KindRef,
    pub declaring_producer: String,
    pub payload_schema: Option<Value>,
    pub normalization: Normalization,
    pub key_rule: Option<KeyRule>,
    pub replace_rule: Option<ReplaceRule>,
    pub time_rule: Option<TimeRule>,
    pub least_class: Class,
    pub removed_rule: Option<RemovedRule>,
    pub keep_known: Vec<String>,
    pub owned_blobs: Vec<OwnedBlobRule>,
    pub index_text: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Evaluated {
    pub equivalence_key: Option<EquivalenceKey>,
    pub ts_orig: TsOrig,
    pub record_state: RecordState,
    pub owned_blobs: Vec<OwnedBlob>,
    pub class: Class,
}

impl KindDeclaration {
    pub fn historical(name: String) -> Self {
        Self {
            kind: KindRef { name, version: 1 },
            declaring_producer: "sinex".into(),
            payload_schema: None,
            normalization: Normalization::Json,
            key_rule: None,
            replace_rule: None,
            time_rule: None,
            least_class: Class::Open,
            removed_rule: None,
            keep_known: vec![],
            owned_blobs: vec![],
            index_text: true,
        }
    }

    pub fn schemaless(kind: KindRef, producer: String) -> Self {
        Self {
            kind,
            declaring_producer: producer,
            payload_schema: None,
            normalization: Normalization::Json,
            key_rule: Some(KeyRule::Location),
            replace_rule: Some(ReplaceRule::SuppressRepeat),
            time_rule: Some(TimeRule::Unknown),
            least_class: Class::Open,
            removed_rule: Some(RemovedRule::Never),
            keep_known: vec![],
            owned_blobs: vec![],
            index_text: true,
        }
    }

    pub fn definition_hash(&self) -> String {
        let value = serde_json::to_value(self).expect("declarations serialize");
        blake3::hash(&serde_json::to_vec(&sorted_json(&value)).expect("JSON serializes"))
            .to_hex()
            .to_string()
    }

    pub fn evaluate(&self, payload: &Value, class: Class) -> Result<Evaluated, String> {
        if !payload.is_object() {
            return Err("payload must be an object".into());
        }
        let present = |field: &str| payload.get(field).filter(|v| !v.is_null());
        let fields = self
            .key_rule
            .as_ref()
            .ok_or("historical declaration has no rules")?
            .fields(payload);
        let equivalence_key = fields.map(|fields| {
            let fields: Vec<&str> = fields.iter().map(String::as_str).collect();
            EquivalenceKey::of(&self.kind.name, &fields, payload)
        });
        let ts_orig = match self
            .time_rule
            .as_ref()
            .ok_or("historical declaration has no rules")?
        {
            TimeRule::Unknown => TsOrig::unknown(),
            TimeRule::FirstPresent { fields, quality } => {
                match fields
                    .iter()
                    .find_map(|field| present(field).map(|v| (field, v)))
                {
                    None => TsOrig::unknown(),
                    Some((field, value)) => {
                        let at: Timestamp = serde_json::from_value(value.clone())
                            .map_err(|_| format!("invalid date-time at /{field}"))?;
                        if *quality == TimeQuality::Unknown {
                            return Err("known time needs a quality".into());
                        }
                        TsOrig::known(at, *quality)
                    }
                }
            }
        };
        let record_state = match self
            .removed_rule
            .as_ref()
            .ok_or("historical declaration has no rules")?
        {
            RemovedRule::WhenPresent { field } if present(field).is_some() => RecordState::Removed,
            _ => RecordState::Present,
        };
        let mut owned_blobs = Vec::new();
        for rule in &self.owned_blobs {
            if let Some(field) = &rule.when {
                match present(field) {
                    None | Some(Value::Bool(false)) => continue,
                    Some(Value::Bool(true)) => {}
                    _ => return Err(format!("expected boolean at /{field}")),
                }
            }
            let blob = present(&rule.field)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("expected blob name at /{}", rule.field))?;
            let size = present(&rule.size)
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("expected blob size at /{}", rule.size))?;
            owned_blobs.push(OwnedBlob {
                blob: blob.into(),
                size,
            });
        }
        Ok(Evaluated {
            equivalence_key,
            ts_orig,
            record_state,
            owned_blobs,
            class: class.max(self.least_class),
        })
    }
}

pub fn sorted_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: std::collections::BTreeMap<_, _> = object
                .iter()
                .map(|(k, v)| (k.clone(), sorted_json(v)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.iter().map(sorted_json).collect()),
        value => value.clone(),
    }
}

#[cfg(test)]
mod tests;
