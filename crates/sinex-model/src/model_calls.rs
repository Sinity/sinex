//! Identity and configured accounting for recorded model effects. Provider
//! execution stays disabled; replay can reuse an already recorded response.
mod receipt;
use crate::{Event, EventId, Origin, Timestamp, json_identity};
pub use receipt::{BudgetBasis, ReceiptOnly, receipt_identity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct InputHash(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct OutputHash(pub String);

impl InputHash {
    pub fn of(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).to_hex().to_string())
    }
    pub fn validate(&self) -> Result<(), String> {
        digest(&self.0)
    }
}
impl OutputHash {
    pub fn of(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).to_hex().to_string())
    }
    pub fn for_value(value: &Value) -> Self {
        Self(json_identity::hash(value))
    }
    pub fn validate(&self) -> Result<(), String> {
        digest(&self.0)
    }
}

pub fn digest(value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("expected a lowercase BLAKE3 digest".into());
    }
    Ok(())
}

/// A reading ID identifies its current exact parent; this stable record
/// identity and selected content identify the effect across an ordinary replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordIdentity {
    Key {
        key: String,
    },
    Material {
        material: crate::MaterialId,
        offset: u64,
        output: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputKind {
    pub name: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "selection", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputSelection {
    Payload,
    Text {
        pointer: String,
    },
    Bytes {
        field: String,
        blob: String,
        offset: u64,
        length: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputIdentity {
    pub source: String,
    pub kind: InputKind,
    pub record: RecordIdentity,
    pub reading_hash: String,
    pub selection: InputSelection,
    pub input_hash: InputHash,
    pub selected_bytes: u64,
}

impl InputIdentity {
    pub fn for_event(
        event: &Event,
        selection: InputSelection,
        bytes: Option<&[u8]>,
    ) -> Result<Self, String> {
        let record = if let Some(key) = &event.equivalence_key {
            RecordIdentity::Key { key: key.0.clone() }
        } else if let Origin::Record {
            location,
            output_index,
        } = &event.origin
        {
            RecordIdentity::Material {
                material: location.material,
                offset: location.anchor,
                output: *output_index,
            }
        } else {
            return Err("a model input needs a stable record identity".into());
        };
        let selected = match &selection {
            InputSelection::Payload => json_identity::bytes(&event.payload),
            InputSelection::Text { pointer } => event
                .payload
                .pointer(pointer)
                .and_then(Value::as_str)
                .ok_or("model input text pointer is not text")?
                .as_bytes()
                .to_vec(),
            InputSelection::Bytes {
                field,
                blob,
                offset,
                length,
            } => {
                let owns = event.owned_blobs.iter().any(|owned| &owned.blob == blob)
                    && event.payload.pointer(field).and_then(Value::as_str) == Some(blob.as_str());
                if !owns && !(field == "material" && matches!(&event.origin, Origin::Record { .. }))
                {
                    return Err("model input bytes are not owned by the cited event".into());
                }
                let bytes = bytes.ok_or("model input bytes were not supplied")?;
                if format!("blake3:{}", blake3::hash(bytes)) != *blob {
                    return Err("model input bytes differ from the declared content hash".into());
                }
                let start = usize::try_from(*offset)
                    .map_err(|_| "model input offset is outside the stored bytes")?;
                let end = offset
                    .checked_add(*length)
                    .and_then(|end| usize::try_from(end).ok())
                    .ok_or("model input range is outside the stored bytes")?;
                bytes
                    .get(start..end)
                    .ok_or("model input range is outside the stored bytes")?
                    .to_vec()
            }
        };
        Ok(Self {
            source: event.source.clone(),
            kind: InputKind {
                name: event.event_type.clone(),
                version: event.kind_version,
            },
            record,
            reading_hash: blake3::Hash::from(event.record_hash).to_hex().to_string(),
            selection,
            input_hash: InputHash::of(&selected),
            selected_bytes: u64::try_from(selected.len())
                .map_err(|_| "model selected content length exceeds the accounting quantity")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InputTarget {
    pub event: EventId,
    pub identity: InputIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Instructions {
    pub version: String,
    pub prompt: String,
    pub prompt_hash: String,
    pub rubric: String,
    pub rubric_hash: String,
    pub settings_version: String,
    pub settings: Value,
    pub settings_hash: String,
}

impl Instructions {
    pub fn validate(&self) -> Result<(), String> {
        nonempty(&self.version, "instruction version")?;
        nonempty(&self.prompt, "prompt")?;
        nonempty(&self.settings_version, "settings version")?;
        digest(&self.prompt_hash)?;
        digest(&self.rubric_hash)?;
        digest(&self.settings_hash)?;
        if self.prompt_hash != InputHash::of(self.prompt.as_bytes()).0
            || self.rubric_hash != InputHash::of(self.rubric.as_bytes()).0
            || self.settings_hash != json_identity::hash(&self.settings)
        {
            return Err("model instruction content differs from its recorded hashes".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub name: String,
    pub operation: String,
    pub provider: String,
    pub provider_account: String,
    pub model: String,
    pub revision: String,
    pub instructions: Instructions,
    pub maximum_output_units: u64,
    pub prices: Prices,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Prices {
    pub call: u64,
    pub input_byte: u64,
    pub output_unit: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UsageEstimate {
    pub input_units: u64,
    pub maximum_output_units: u64,
}

impl Prices {
    pub fn estimate(&self, usage: &UsageEstimate) -> Result<u64, String> {
        self.input_byte
            .checked_mul(usage.input_units)
            .and_then(|input| {
                self.output_unit
                    .checked_mul(usage.maximum_output_units)
                    .and_then(|output| input.checked_add(output))
            })
            .and_then(|total| total.checked_add(self.call))
            .ok_or_else(|| "configured model cost estimate exceeds the accounting quantity".into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub scope: String,
    pub currency: String,
    pub unit: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    pub limit_units: u64,
}

impl Budget {
    pub fn validate(&self) -> Result<(), String> {
        nonempty(&self.scope, "accounting scope")?;
        nonempty(&self.currency, "accounting currency")?;
        nonempty(&self.unit, "accounting unit")?;
        if self.period_end <= self.period_start {
            return Err("model accounting period must have positive duration".into());
        }
        Ok(())
    }
    /// Policy versions and changing limits cannot create a fresh account.
    pub fn bucket(&self) -> String {
        BudgetBasis::from(self).bucket()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub name: String,
    pub version: String,
    pub budget: Budget,
    pub routes: Vec<Route>,
}

impl Policy {
    pub fn validate(&self) -> Result<(), String> {
        nonempty(&self.name, "model policy name")?;
        nonempty(&self.version, "model policy version")?;
        self.budget.validate()?;
        let mut operations = std::collections::BTreeSet::new();
        for route in &self.routes {
            for (value, name) in [
                (&route.name, "route name"),
                (&route.operation, "operation"),
                (&route.provider, "provider"),
                (&route.provider_account, "provider account"),
                (&route.model, "model"),
                (&route.revision, "model revision"),
            ] {
                nonempty(value, name)?;
            }
            route.instructions.validate()?;
            if !operations.insert(&route.operation) {
                return Err("each model operation needs one configured route".into());
            }
        }
        Ok(())
    }
    pub fn route(&self, operation: &str) -> Result<&Route, String> {
        self.routes
            .iter()
            .find(|route| route.operation == operation)
            .ok_or_else(|| format!("no model route is configured for {operation}"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub operation: String,
    pub provider: String,
    pub provider_account: String,
    pub model: String,
    pub revision: String,
    pub instructions: Instructions,
    pub maximum_output_units: u64,
    pub inputs: Vec<InputTarget>,
}

impl Request {
    pub fn validate(&self) -> Result<(), String> {
        for (value, name) in [
            (&self.operation, "operation"),
            (&self.provider, "provider"),
            (&self.provider_account, "provider account"),
            (&self.model, "model"),
            (&self.revision, "model revision"),
        ] {
            nonempty(value, name)?;
        }
        self.instructions.validate()?;
        if self.inputs.is_empty() {
            return Err("a model request needs admitted input evidence".into());
        }
        for target in &self.inputs {
            target.identity.input_hash.validate()?;
            digest(&target.identity.reading_hash)?;
        }
        Ok(())
    }
    pub fn identity(&self) -> String {
        let inputs: Vec<_> = self
            .inputs
            .iter()
            .map(|target| {
                serde_json::to_value(&target.identity).expect("input identity serializes")
            })
            .collect();
        let identity = (
            &self.operation,
            &self.provider,
            &self.provider_account,
            &self.model,
            &self.revision,
            &self.instructions,
            self.maximum_output_units,
            inputs,
        );
        json_identity::hash(&serde_json::to_value(identity).expect("request identity serializes"))
    }
    pub fn matches(&self, route: &Route) -> bool {
        self.operation == route.operation
            && self.provider == route.provider
            && self.provider_account == route.provider_account
            && self.model == route.model
            && self.revision == route.revision
            && self.instructions == route.instructions
            && self.maximum_output_units == route.maximum_output_units
    }
    pub fn usage(&self) -> Result<UsageEstimate, String> {
        let input_units = self
            .inputs
            .iter()
            .try_fold(0_u64, |sum, input| {
                sum.checked_add(input.identity.selected_bytes)
            })
            .ok_or("model selected input length exceeds the accounting quantity")?;
        Ok(UsageEstimate {
            input_units,
            maximum_output_units: self.maximum_output_units,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Capabilities {
    pub actual_receipts: bool,
}

#[derive(Clone, Debug)]
pub enum Action {
    FindRecord {
        source: String,
        kind: crate::KindRef,
        key: String,
        current: bool,
    },
    AuthorizePolicy {
        policy: EventId,
        at: Timestamp,
    },
    FindRequest {
        policy: EventId,
        response: EventId,
    },
    Reserve {
        request: EventId,
        at: Timestamp,
    },
    RecordActual {
        receipt: EventId,
        at: Timestamp,
    },
    Reconcile {
        request: EventId,
        response: EventId,
        at: Timestamp,
    },
    Uncertain {
        request: String,
        reason: String,
        at: Timestamp,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Reserved,
    Uncertain,
    Recorded,
    ActualUnbound,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub request: String,
    pub policy: EventId,
    pub bucket: String,
    pub currency: String,
    pub unit: String,
    pub state: State,
    pub reused: bool,
    pub reserved_units: u64,
    pub actual_units: Option<u64>,
    pub response: Option<EventId>,
    pub output_hash: Option<OutputHash>,
    pub overrun: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActualDecision {
    pub receipt: String,
    pub evidence: EventId,
    pub policy: EventId,
    pub bucket: String,
    pub state: State,
    pub bound_request: Option<String>,
    pub reused: bool,
    pub overrun: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum Refusal {
    ProviderDisabled,
    ReceiptAccountingDisabled,
    ContradictoryReceipt {
        receipt: String,
    },
    PolicyUnavailable,
    PolicyUnauthorized,
    InsufficientConfiguredBudget {
        policy: EventId,
        currency: String,
        unit: String,
        required_units: u64,
        limit_units: u64,
        committed_units: String,
    },
    Invalid {
        detail: String,
    },
    ContradictoryResponse {
        request: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    RecordLookup {
        event: Option<EventId>,
    },
    Authorized {
        policy: EventId,
        reused: bool,
    },
    RequestLookup {
        request: Option<EventId>,
        policy_current: bool,
    },
    Accepted {
        decision: Decision,
    },
    ActualAccepted {
        decision: ActualDecision,
    },
    Refused {
        refusal: Refusal,
    },
}

/// No provider or credential reader is constructed, and no input selector is
/// evaluated, while execution is disabled. Recorded effects use admission.
pub fn execute<I, O>(_: impl FnOnce() -> I, _: impl FnOnce(I) -> O) -> Result<O, Refusal> {
    Err(Refusal::ProviderDisabled)
}

fn nonempty(value: &str, name: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("model {name} must be configured"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
