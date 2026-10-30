//! Operator model policy and supplied recorded effects. These are evidence;
//! importing them never constructs or invokes a provider.
use crate::model_calls::{OutputHash, Policy, Request};
use crate::{EventId, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "model.policy", key = ["policy_name"], replace = newer, time = configured_at, quality = captured)]
pub struct ModelPolicy {
    pub policy_name: String,
    pub policy: Policy,
    pub configured_at: Timestamp,
    pub configured_by: String,
}

impl ModelPolicy {
    pub fn validate(&self) -> Result<(), String> {
        self.policy.validate()?;
        if self.policy_name != self.policy.name {
            return Err("model policy name differs from its configured identity".into());
        }
        if self.configured_by.trim().is_empty() {
            return Err("model policy author is missing".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "model.request", key = ["request_id", "request", "policy", "recorded_response"], replace = newer, time = requested_at, quality = captured)]
pub struct ModelRequest {
    pub request_id: String,
    pub policy: EventId,
    pub request: Request,
    /// Explicit reuse of an existing effect; this is outside spend identity.
    pub recorded_response: Option<EventId>,
    pub requested_at: Timestamp,
    pub requested_by: String,
}

impl ModelRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.request.validate()?;
        if self.request_id != self.request.identity() {
            return Err(
                "model request differs from its typed input or instruction identity".into(),
            );
        }
        if self.requested_by.trim().is_empty() {
            return Err("model request author is missing".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Charge {
    pub currency: String,
    pub unit: String,
    pub units: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "model.response", key = ["request_id", "output_hash", "actual", "provider_receipt"], replace = repeat, time = received_at, quality = captured)]
pub struct RecordedResponse {
    pub request_id: String,
    pub request: Request,
    pub output: Value,
    pub output_hash: OutputHash,
    pub actual: Charge,
    pub provider_receipt: Option<String>,
    pub received_at: Timestamp,
}

/// The computational projection excludes financial fields, whose display
/// rules cannot turn an otherwise available text effect into a missing value.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct RecordedEffect {
    pub request_id: String,
    pub request: Request,
    pub output: Value,
    pub output_hash: OutputHash,
}

impl RecordedEffect {
    pub fn validate(&self) -> Result<(), String> {
        validate_effect(
            &self.request_id,
            &self.request,
            &self.output,
            &self.output_hash,
        )
    }
}

impl RecordedResponse {
    pub fn validate(&self) -> Result<(), String> {
        validate_effect(
            &self.request_id,
            &self.request,
            &self.output,
            &self.output_hash,
        )?;
        if self.actual.currency.trim().is_empty() || self.actual.unit.trim().is_empty() {
            return Err("recorded response needs its actual accounting currency and unit".into());
        }
        if self
            .provider_receipt
            .as_ref()
            .is_some_and(|receipt| receipt.trim().is_empty())
        {
            return Err("a supplied provider receipt identity must not be empty".into());
        }
        Ok(())
    }
    pub fn receipt_identity(&self) -> Option<String> {
        self.provider_receipt.as_ref().map(|receipt| {
            crate::model_calls::receipt_identity(
                &self.request.provider,
                &self.request.provider_account,
                receipt,
            )
        })
    }
}

fn validate_effect(
    request_id: &str,
    request: &Request,
    output: &Value,
    output_hash: &OutputHash,
) -> Result<(), String> {
    request.validate()?;
    if request.identity() != request_id {
        return Err("recorded response has a different typed input or instruction identity".into());
    }
    output_hash.validate()?;
    if OutputHash::for_value(output) != *output_hash {
        return Err("recorded response output hash differs from its output".into());
    }
    Ok(())
}

/// An actual receipt makes no assertion that a request or input was verified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version=2,type="model.receipt",key=["policy","receipt"],replace=repeat,time=recorded_at,quality=captured)]
pub struct ModelReceipt {
    pub policy: EventId,
    pub receipt: crate::model_calls::ReceiptOnly,
    pub recorded_at: Timestamp,
    pub recorded_by: String,
}
impl ModelReceipt {
    pub fn validate(&self) -> Result<(), String> {
        self.receipt.validate()?;
        if self.recorded_by.trim().is_empty() {
            return Err("model receipt author is missing".into());
        }
        Ok(())
    }
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum ModelRecord { Policy(ModelPolicy), Request(ModelRequest), Response(RecordedResponse), Receipt(ModelReceipt) }
}
