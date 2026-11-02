//! Signal rules, the host deliverer's deliveries and their outcomes, and the
//! evaluator reports other products forward. The evaluator checkpoint is
//! daemon state, not events.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=3,type="signal.rule",key=["name"],replace=newer,time=written_at,quality=operator)]
pub struct RuleWritten {
    pub name: String,
    pub active: signals::ActiveRule,
    pub withdrawn: bool,
    pub grants: Vec<crate::Class>,
    pub written_at: Timestamp,
}

/// One handoff by the host deliverer: the firing, its route decision, the
/// holds it waited through and the attention the decision was taken under.
/// It never changes; later news is an outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=3,type="signal.delivery",key=["delivery_id"],replace=repeat,time=handed_at,quality=captured)]
pub struct DeliveryRecorded {
    pub delivery_id: String,
    pub delivery: signals::Delivery,
    pub handed_at: Timestamp,
}

/// What happened to a delivery after its handoff: an outlet's attempt, shown,
/// opened, acknowledged, snoozed, released, cleared, closed or seen.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.outcome",key=location,replace=repeat,time=recorded_at,quality=captured)]
pub struct OutcomeRecorded {
    pub delivery_id: String,
    pub outcome: signals::Outcome,
    pub recorded_at: Timestamp,
}

/// An evaluator report another product forwarded to this host's deliverer,
/// with the authenticated producer and family that sent it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version=2,type="signal.report",key=location,replace=repeat,time=received_at,quality=captured)]
pub struct ReportReceived {
    pub producer: String,
    /// The producer's authenticated family: the only product it may report for.
    pub family: String,
    pub report: signals::Report,
    pub received_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", content = "value", rename_all = "snake_case")]
pub enum SignalRecord {
    Rule(Box<RuleWritten>),
    Outcome(Box<OutcomeRecorded>),
    Report(Box<ReportReceived>),
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
            Self::Outcome(value) => value.as_ref().build(source, origin, order, hash, class),
            Self::Report(value) => value.as_ref().build(source, origin, order, hash, class),
        }
    }
}
