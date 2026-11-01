//! Selection uses owner-indexed candidates and the ordinary current query.
//! Requested addresses are resolved by the runner, never treated as parents.
use super::*;
use sinex_derivations::selected::{InputBytes, SelectedInputs, SelectedQuery};
use sinex_model::media_effects::{BindingFailure, MediaSelector, RecordedBinding, VerifiedBinding};
use sinex_model::{EventId, InputRequirement, KindRef, ReferenceInput};

pub(super) fn reference(selector: Option<&MediaSelector>) -> ReferenceInput {
    let kind = KindRef {
        name: "model.request".into(),
        version: 2,
    };
    let mut paths = vec!["/targets", "/request/operation", "/recorded_response"];
    if let Some(selector) = selector {
        if selector.policy.is_some() {
            paths.push("/policy");
        }
        if selector.request_id.is_some() {
            paths.push("/request_id");
        }
        if selector.instruction_version.is_some() {
            paths.push("/request/instructions/version");
        }
        if selector.settings_version.is_some() {
            paths.push("/request/instructions/settings_version");
        }
    }
    ReferenceInput {
        owner_kind: kind.clone(),
        target_kind: KindRef {
            name: "media.segment".into(),
            version: 2,
        },
        key_field: "segment".into(),
        selector: vec![InputRequirement::new(kind, paths)],
    }
}

pub(super) fn query(
    segment: EventId,
    operation: &str,
    selector: Option<&MediaSelector>,
) -> SelectedQuery {
    let mut text = format!(
        "events where event_type = model.request AND kind_version = 2 AND payload.targets = \"{segment}\" AND payload.request.operation = {}",
        quoted(operation)
    );
    if let Some(selector) = selector {
        if let Some(policy) = selector.policy {
            text.push_str(&format!(
                " AND payload.policy = {}",
                quoted(&policy.to_string())
            ));
        }
        for (path, value) in [
            ("request_id", &selector.request_id),
            (
                "request.instructions.version",
                &selector.instruction_version,
            ),
            (
                "request.instructions.settings_version",
                &selector.settings_version,
            ),
        ] {
            if let Some(value) = value {
                text.push_str(&format!(" AND payload.{path} = {}", quoted(value)));
            }
        }
    }
    SelectedQuery::Referenced {
        text,
        reference: reference(selector),
        target: segment,
    }
}
fn quoted(value: &str) -> String {
    serde_json::to_string(value).expect("query literal serializes")
}

fn candidates(
    selected: Option<&SelectedInputs>,
) -> Result<Vec<(EventId, RecordedBinding)>, BindingFailure> {
    let Some(selected) = selected else {
        return Ok(vec![]);
    };
    let Some(rows) = selected.value["rows"].as_array() else {
        return Err(invalid());
    };
    let mut candidates = Vec::new();
    for row in rows {
        let id = row["id"]
            .as_str()
            .and_then(|id| id.parse::<uuid::Uuid>().ok())
            .map(EventId)
            .ok_or_else(invalid)?;
        let binding: RecordedBinding =
            serde_json::from_value(row["payload"].clone()).map_err(|_| invalid())?;
        binding.validate().map_err(|_| invalid())?;
        candidates.push((id, binding));
    }
    candidates.sort_by_key(|(id, _)| *id);
    candidates.dedup_by_key(|(id, _)| *id);
    Ok(candidates)
}

pub(super) fn follow(selected: Option<&SelectedInputs>) -> Vec<EventId> {
    let mut ids = selected.map_or_else(Vec::new, |selected| selected.parents.clone());
    if let Ok(candidates) = candidates(selected) {
        for (id, binding) in candidates {
            ids.push(id);
            ids.extend(binding.requested());
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

pub(super) fn choose(
    selected: Option<&SelectedInputs>,
    inputs:&[Event],
    segment: EventId,
    operation: &str,
    selector: Option<&MediaSelector>,
) -> Result<Option<(EventId, RecordedBinding)>, BindingFailure> {
    let matching = candidates(selected)?.into_iter().filter(|(_, binding)| {
        binding.targets.contains(&segment)
            && binding.request.operation == operation
            && selector.is_none_or(|selector| selector.matches(binding))
            && binding.recorded_response.is_some()
    });
    let mut current=Vec::new();let mut retired=false;
    for (id,binding) in matching {
        let Some(event)=inputs.iter().find(|event|event.id==id) else{return Err(invalid())};
        if event.record_state==sinex_model::RecordState::Removed {retired=true;}else{current.push((id,binding));}
    }
    let mut current=current.into_iter();
    let first = current.next();
    if current.next().is_some() {
        return Err(BindingFailure {
            reason: Reason::SelectionAmbiguous,
            requested: vec![],
        });
    }
    if first.is_none()&&retired{return Err(BindingFailure{reason:Reason::SourceRemoved,requested:vec![]})}
    if first.is_none() && selector.is_some() {
        return Err(BindingFailure {
            reason: Reason::NoRecordedBinding,
            requested: vec![],
        });
    }
    Ok(first)
}

pub(super) fn reads(binding: &RecordedBinding) -> Vec<(EventId, Reads)> {
    binding
        .request
        .inputs
        .iter()
        .filter_map(|target| match &target.identity.selection {
            sinex_model::model_calls::InputSelection::Bytes { field, .. }
                if field == "material" =>
            {
                Some((target.event, Reads::Material))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn verify(
    binding: &RecordedBinding,
    segment: &Event,
    request: EventId,
    inputs: &[Event],
    bytes: &InputBytes,
    operation: &str,
) -> Result<VerifiedBinding, BindingFailure> {
    if binding.request.inputs.iter().any(|target| {
        matches!(
            &target.identity.selection,
            sinex_model::model_calls::InputSelection::Bytes { .. }
        ) && bytes.get(&target.event).is_none_or(Result::is_err)
    }) {
        return Err(BindingFailure {
            reason: Reason::MaterialUnavailable,
            requested: vec![],
        });
    }
    let request = inputs
        .iter()
        .find(|event| event.id == request)
        .ok_or_else(invalid)?;
    binding.verify(
        segment,
        request,
        inputs,
        |id| bytes.get(&id).and_then(|bytes| bytes.as_deref().ok()),
        operation,
    )
}

pub(super) fn requirements(media_kind: &str) -> Vec<InputRequirement> {
    let kind = KindRef {
        name: "media.segment".into(),
        version: 2,
    };
    let media = InputRequirement::new(kind.clone(), ["/kind"]);
    let paths = if media_kind == "screen" {
        vec!["/started_at", "/accessibility_text"]
    } else {
        vec!["/started_at", "/ended_at"]
    };
    let mut branch = InputRequirement::new(kind, paths);
    branch.when.push(("/kind".into(), json!(media_kind)));
    vec![
        media,
        branch,
        InputRequirement::new(
            KindRef {
                name: "model.request".into(),
                version: 2,
            },
            [
                "/request_id",
                "/request",
                "/targets",
                "/policy",
                "/recorded_response",
            ],
        ),
        InputRequirement::new(
            KindRef {
                name: "model.response".into(),
                version: 2,
            },
            ["/request_id", "/request", "/output", "/output_hash"],
        ),
        InputRequirement::new(
            KindRef {
                name: "model.policy".into(),
                version: 2,
            },
            [
                "/policy/routes/*/operation",
                "/policy/routes/*/provider",
                "/policy/routes/*/provider_account",
                "/policy/routes/*/model",
                "/policy/routes/*/revision",
                "/policy/routes/*/instructions",
                "/policy/routes/*/maximum_output_units",
            ],
        ),
    ]
}

fn invalid() -> BindingFailure {
    BindingFailure {
        reason: Reason::InvalidReceipt,
        requested: vec![],
    }
}
