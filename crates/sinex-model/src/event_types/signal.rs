//! Shared signal rules and their exact delivery identities are ordinary readings.
use crate::{Payloads, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.rule",key=["name"],replace=newer,time=written_at,quality=operator)]
pub struct RuleWritten {
    pub name: String,
    pub active: signals::ActiveRule,
    pub withdrawn: bool,
    pub grants: Vec<crate::Class>,
    pub written_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.intent",key=["delivery_id"],replace=newer,time=received_at,quality=captured)]
pub struct IntentRecorded {
    pub active_rule: Option<signals::ActiveRule>,
    pub inputs: Vec<crate::EventId>,
    pub delivery_id: String,
    /// The authenticated capture identity; logical product ownership is in the intent.
    pub producer: String,
    pub intent: signals::FiredIntent,
    pub notice: signals::notifications::Notice,
    pub received_at: Timestamp,
}

/// Receipt of an authenticated occurrence, independent of its copied query evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.received",key=location,replace=repeat,time=received_at,quality=captured)]
pub struct ReceivedRecorded {
    pub delivery_id: String,
    pub received_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.handoff",key=["delivery_id"],replace=repeat,time=handed_at,quality=captured)]
pub struct HandoffRecorded {
    pub delivery_id: String,
    pub channel: signals::Channel,
    pub handed_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.delivery",key=["delivery_id"],replace=repeat,time=handed_at,quality=captured)]
pub struct DeliveryRecorded {
    pub delivery_id: String,
    pub delivery: signals::Delivery,
    pub handed_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.receipt",key=["delivery_id","route"],replace=newer,time=recorded_at,quality=captured)]
pub struct ReceiptRecorded {
    pub delivery_id: String,
    pub route: String,
    pub receipt: signals::DeliveryReceipt,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.desktop_receipt",key=["delivery_id","route"],replace=newer,time=recorded_at,quality=captured)]
pub struct DesktopReceiptRecorded {
    pub delivery_id: String,
    pub route: String,
    pub receipt: signals::notifications::NotificationReceipt,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.checkpoint",key=["host"],replace=newer,time=written_at,quality=captured)]
pub struct CheckpointRecorded {
    pub host: String,
    pub revision: u64,
    pub settings_identity: String,
    pub read_as: i64,
    pub written_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.evaluation",key=["rule","row_identity"],replace=newer,time=observed_at,quality=captured)]
pub struct EvaluationRecorded {
    pub rule: String,
    pub row_identity: String,
    pub occurrence: signals::Occurrence,
    pub query: String,
    pub read_as: i64,
    pub settings_identity: String,
    pub day_setting: query_language::time::DaySettingWire,
    pub result: serde_json::Value,
    pub complete: bool,
    pub reason: Option<String>,
    pub observed_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.emergency",key=["delivery_id"],replace=repeat,time=completed_at,quality=captured)]
pub struct EmergencyCompleted {
    pub inputs: Vec<crate::EventId>,
    pub delivery_id: String,
    pub host: String,
    pub context_token: String,
    pub intent: signals::FiredIntent,
    pub notice: signals::notifications::Notice,
    pub receipts: Vec<signals::notifications::NotificationReceipt>,
    pub attempt: signals::notifications::Attempt,
    /// Authenticated producer observation of the actual delivery invocation,
    /// separate from evaluation and receipt clocks. A failed attempt attests
    /// no completed platform effect.
    pub invoked_at: Timestamp,
    pub completed_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", content = "value", rename_all = "snake_case")]
pub enum SignalRecord {
    Rule(Box<RuleWritten>),
    Checkpoint(CheckpointRecorded),
    Evaluation(Box<EvaluationRecorded>),
    Intent(Box<IntentRecorded>),
    Emergency(Box<EmergencyCompleted>),
    Delivery(Box<DeliveryRecorded>),
    Handoff(HandoffRecorded),
    Received(ReceivedRecorded),
    Receipt(ReceiptRecorded),
    DesktopReceipt(DesktopReceiptRecorded),
}

impl crate::Payloads for SignalRecord {
    fn build(
        &self,
        source: &str,
        origin: crate::Origin,
        order: crate::ReadingOrder,
        hash: [u8; 32],
        class: crate::Class,
    ) -> Result<crate::Event, crate::BuildError> {
        match self {
            Self::Rule(value) => value.as_ref().build(source, origin, order, hash, class),
            Self::Evaluation(value) => value.as_ref().build(source, origin, order, hash, class),
            Self::Intent(value) => value.as_ref().build(source, origin, order, hash, class),
            Self::Emergency(value) => value.as_ref().build(source, origin, order, hash, class),
            Self::Delivery(value) => value.as_ref().build(source, origin, order, hash, class),
            Self::Checkpoint(value) => value.build(source, origin, order, hash, class),
            Self::Received(value) => value.build(source, origin, order, hash, class),
            Self::Handoff(value) => value.build(source, origin, order, hash, class),
            Self::Receipt(value) => value.build(source, origin, order, hash, class),
            Self::DesktopReceipt(value) => value.build(source, origin, order, hash, class),
        }
    }
}
