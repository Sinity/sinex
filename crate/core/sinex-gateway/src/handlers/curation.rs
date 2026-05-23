//! Curation proposal/judgment RPC handlers.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::{CurationJudgmentPayload, CurationProposalPayload};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::query::{EventQuery, EventQueryResult, PayloadFilter};
use sinex_primitives::rpc::curation::{
    CurationListProposalsRequest, CurationRecordJudgmentRequest, CurationRecordJudgmentResponse,
};
use sinex_primitives::{Id, JsonValue, Result, SinexError, Timestamp, Uuid};
use sqlx::PgPool;
use std::str::FromStr;

use crate::rpc_server::RpcAuthContext;

pub async fn handle_curation_list_proposals(
    pool: &PgPool,
    req: CurationListProposalsRequest,
) -> Result<EventQueryResult> {
    let mut query = EventQuery {
        sources: vec![EventSource::from_static("curation")],
        event_types: vec![EventType::from_static("curation.proposal")],
        payload: Some(PayloadFilter::Contains {
            value: json!({ "status": req.status }),
        }),
        limit: req.limit,
        ..Default::default()
    };
    query.validate()?;

    pool.events().query(query).await
}

pub async fn handle_curation_record_judgment(
    pool: &PgPool,
    req: CurationRecordJudgmentRequest,
    auth: &RpcAuthContext,
) -> Result<CurationRecordJudgmentResponse> {
    let proposal_event_uuid = Uuid::from_str(&req.proposal_event_id).map_err(|error| {
        SinexError::validation("curation.judgments.record: invalid proposal_event_id")
            .with_context("proposal_event_id", &req.proposal_event_id)
            .with_std_error(&error)
    })?;
    let proposal_event_id = Id::<Event<JsonValue>>::from_uuid(proposal_event_uuid);
    let proposal_event = pool
        .events()
        .get_by_id(proposal_event_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("curation.judgments.record: proposal event not found")
                .with_context("proposal_event_id", &req.proposal_event_id)
        })?;
    if proposal_event.source.as_str() != "curation"
        || proposal_event.event_type.as_str() != "curation.proposal"
    {
        return Err(SinexError::validation(
            "curation.judgments.record: event is not a curation proposal",
        )
        .with_context("proposal_event_id", &req.proposal_event_id)
        .with_context("source", proposal_event.source.as_str())
        .with_context("event_type", proposal_event.event_type.as_str()));
    }
    let proposal: CurationProposalPayload = serde_json::from_value(proposal_event.payload.clone())
        .map_err(|error| {
            SinexError::serialization("curation.judgments.record: invalid proposal payload")
                .with_std_error(&error)
        })?;

    let actor_id = req
        .actor_id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| auth.actor_id().to_string());
    let judgment = CurationJudgmentPayload {
        judgment_id: Uuid::now_v7(),
        proposal_id: proposal.proposal_id,
        actor_kind: req.actor_kind,
        actor_id,
        decision: req.decision,
        corrected_payload: req.corrected_payload,
        comment: req.comment,
        judged_at: Timestamp::now(),
        authorization_context: req.authorization_context,
    };
    let parent = proposal_event.id.ok_or_else(|| {
        SinexError::invalid_state("curation.judgments.record: persisted proposal event missing id")
    })?;
    let event = judgment
        .clone()
        .from_parents([parent])?
        .at_time(judgment.judged_at)
        .build()?;
    let inserted = pool.events().insert(event).await?;

    Ok(CurationRecordJudgmentResponse {
        judgment,
        event: inserted,
    })
}
