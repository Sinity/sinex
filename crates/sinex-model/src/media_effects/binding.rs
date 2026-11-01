//! Computational projections of retained request bindings. Financial and audit
//! fields are intentionally absent: their output rules cannot change text.
use super::{MediaEffectIdentity, MediaUnavailableReason, TargetAvailability, UnavailableTarget};
use crate::event_types::model_call::RecordedEffect;
use crate::model_calls::{InputIdentity, InputSelection, Request};
use crate::{Event, EventId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Optional configured equality conditions for a recorded interpretation.
/// An absent condition makes no choice between two otherwise eligible effects.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MediaSelector {
    pub policy: Option<EventId>,
    pub request_id: Option<String>,
    pub instruction_version: Option<String>,
    pub settings_version: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MediaSelectors {
    pub screen: Option<MediaSelector>,
    pub speech: Option<MediaSelector>,
}

impl MediaSelectors {
    pub fn validate(&self) -> Result<(), String> {
        for selector in [&self.screen, &self.speech].into_iter().flatten() {
            selector.validate()?;
        }
        Ok(())
    }
}

impl MediaSelector {
    pub fn validate(&self) -> Result<(), String> {
        for value in [
            &self.request_id,
            &self.instruction_version,
            &self.settings_version,
        ] {
            if value.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err("a configured media selection condition is empty".into());
            }
        }
        if let Some(request_id) = &self.request_id {
            crate::model_calls::InputHash(request_id.clone()).validate()?;
        }
        Ok(())
    }

    pub fn matches(&self, binding: &RecordedBinding) -> bool {
        self.policy.is_none_or(|policy| policy == binding.policy)
            && self
                .request_id
                .as_ref()
                .is_none_or(|id| id == &binding.request_id)
            && self
                .instruction_version
                .as_ref()
                .is_none_or(|version| version == &binding.request.instructions.version)
            && self
                .settings_version
                .as_ref()
                .is_none_or(|version| version == &binding.request.instructions.settings_version)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct RecordedBinding {
    pub request_id: String,
    pub targets: Vec<EventId>,
    pub policy: EventId,
    pub request: Request,
    pub recorded_response: Option<EventId>,
}

impl RecordedBinding {
    pub fn validate(&self) -> Result<(), String> {
        self.request.validate()?;
        if self.request_id != self.request.identity()
            || self.targets
                != self
                    .request
                    .inputs
                    .iter()
                    .map(|input| input.event)
                    .collect::<Vec<_>>()
        {
            return Err("recorded media binding differs from its exact ordered inputs".into());
        }
        Ok(())
    }

    /// Requested addresses to resolve through the ordinary exact-input reader.
    /// They are not evidence until that reader returns the actual current rows.
    pub fn requested(&self) -> Vec<EventId> {
        let mut ids = self.targets.clone();
        ids.push(self.policy);
        ids.extend(self.recorded_response);
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub fn verify<'a>(
        &self,
        segment: &Event,
        request: &Event,
        inputs: &'a [Event],
        bytes: impl Fn(EventId) -> Option<&'a [u8]>,
        operation: &str,
    ) -> Result<VerifiedBinding, BindingFailure> {
        if request.record_state==crate::RecordState::Removed || segment.record_state==crate::RecordState::Removed {
            return Err(BindingFailure{reason:MediaUnavailableReason::SourceRemoved,requested:vec![]});
        }
        self.validate().map_err(|_| BindingFailure::invalid())?;
        let current: Self = serde_json::from_value(request.payload.clone())
            .map_err(|_| BindingFailure::invalid())?;
        if request.event_type != "model.request"
            || request.kind_version != 2
            || &current != self
            || self.request.operation != operation
            || !self.targets.contains(&segment.id)
        {
            return Err(BindingFailure::invalid());
        }
        let find = |id| inputs.iter().find(|event| event.id == id);
        let policy = find(self.policy).ok_or_else(|| BindingFailure::missing(self.policy,MediaUnavailableReason::PolicyUnavailable))?;
        let response_id = self.recorded_response.ok_or_else(BindingFailure::invalid)?;
        let response = find(response_id).ok_or_else(|| BindingFailure::missing(response_id,MediaUnavailableReason::ReceiptUnavailable))?;
        if policy.record_state==crate::RecordState::Removed || response.record_state==crate::RecordState::Removed {
            return Err(BindingFailure{reason:MediaUnavailableReason::SourceRemoved,requested:vec![]});
        }
        if policy.event_type != "model.policy"
            || policy.kind_version != 2
            || response.event_type != "model.response"
            || response.kind_version != 2
        {
            return Err(BindingFailure::invalid());
        }
        let effect: RecordedEffect = serde_json::from_value(response.payload.clone())
            .map_err(|_| BindingFailure::invalid())?;
        effect.validate().map_err(|_| BindingFailure::invalid())?;
        if effect.request_id != self.request_id || !route_matches(&policy.payload, &self.request) {
            return Err(BindingFailure::invalid());
        }
        let mut segment_identity = None;
        let mut parents = vec![request.id, policy.id, response.id];
        for target in &self.request.inputs {
            let event = find(target.event).ok_or_else(|| BindingFailure::missing(target.event,MediaUnavailableReason::MissingTarget))?;
            if event.record_state==crate::RecordState::Removed {
                return Err(BindingFailure{reason:MediaUnavailableReason::SourceRemoved,requested:vec![]});
            }
            let selected = match &target.identity.selection {
                InputSelection::Bytes { field, .. } if field == "material" => bytes(event.id),
                InputSelection::Bytes { .. } => return Err(BindingFailure::invalid()),
                _ => None,
            };
            let actual =
                InputIdentity::for_event(event, target.identity.selection.clone(), selected)
                    .map_err(|_| BindingFailure::invalid())?;
            if actual != target.identity {
                return Err(BindingFailure::invalid());
            }
            if event.id == segment.id {
                if segment_identity
                    .as_ref()
                    .is_some_and(|identity| identity != &actual)
                {
                    return Err(BindingFailure::invalid());
                }
                segment_identity = Some(actual);
            }
            parents.push(event.id);
        }
        let input = segment_identity.ok_or_else(BindingFailure::invalid)?;
        let identity =
            MediaEffectIdentity::from_recorded(policy.id, request.id, response.id, &effect, &input)
                .map_err(|_| BindingFailure::invalid())?;
        parents.sort_unstable();
        parents.dedup();
        Ok(VerifiedBinding {
            effect,
            identity,
            parents,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedBinding {
    pub effect: RecordedEffect,
    pub identity: MediaEffectIdentity,
    pub parents: Vec<EventId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingFailure {
    pub reason: MediaUnavailableReason,
    pub requested: Vec<UnavailableTarget>,
}

impl BindingFailure {
    fn invalid() -> Self {
        Self {
            reason: MediaUnavailableReason::InvalidReceipt,
            requested: vec![],
        }
    }
    fn missing(event: EventId, reason:MediaUnavailableReason) -> Self {
        Self {
            reason,
            requested: vec![UnavailableTarget {
                event,
                availability: TargetAvailability::Unknown,
            }],
        }
    }
}

/// Only computational route fields are used. Financial fields remain governed
/// by their own output rules and are never reconstructed from canonical data.
fn route_matches(policy: &Value, request: &Request) -> bool {
    policy
        .pointer("/policy/routes")
        .and_then(Value::as_array)
        .is_some_and(|routes| {
            let mut matching = routes
                .iter()
                .filter(|route| route["operation"] == request.operation);
            let Some(route) = matching.next() else {
                return false;
            };
            matching.next().is_none()
                && route["provider"] == request.provider
                && route["provider_account"] == request.provider_account
                && route["model"] == request.model
                && route["revision"] == request.revision
                && route["maximum_output_units"].as_u64() == Some(request.maximum_output_units)
                && serde_json::from_value::<crate::model_calls::Instructions>(
                    route["instructions"].clone(),
                )
                .is_ok_and(|instructions| instructions == request.instructions)
        })
}

#[cfg(test)]
mod tests;
