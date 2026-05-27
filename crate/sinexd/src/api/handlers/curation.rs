//! Curation proposal/judgment RPC handlers.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::{
    CurationFinalizedPayload, CurationJudgmentDecision, CurationJudgmentPayload,
    CurationProposalPayload, CurationProposalStatus,
};
use sinex_primitives::events::{Event, EventId, EventPayload, Provenance};
use sinex_primitives::query::{EventQuery, EventQueryResult, PayloadFilter};
use sinex_primitives::rpc::curation::{
    CurationDuplicateAction, CurationDuplicateCandidateCluster, CurationDuplicateCandidateEvent,
    CurationFinalizeRequest, CurationFinalizeResponse, CurationListDuplicateCandidatesRequest,
    CurationListDuplicateCandidatesResponse, CurationListProposalsRequest,
    CurationRecordDuplicateJudgmentRequest, CurationRecordDuplicateJudgmentResponse,
    CurationRecordJudgmentRequest, CurationRecordJudgmentResponse,
};
use sinex_primitives::{Id, JsonValue, Result, SinexError, Timestamp, Uuid};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use std::str::FromStr;

use crate::api::rpc_server::RpcAuthContext;

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

pub async fn handle_curation_list_duplicate_candidates(
    pool: &PgPool,
    req: CurationListDuplicateCandidatesRequest,
) -> Result<CurationListDuplicateCandidatesResponse> {
    let limit = req.limit.clamp(1, 1000);
    let events_per_cluster = req.events_per_cluster.clamp(1, 50);
    let source = req
        .source
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    let event_type = req
        .event_type
        .as_deref()
        .filter(|value| !value.trim().is_empty());

    let cluster_rows = sqlx::query(
        r"
        WITH keyed AS (
            SELECT
                source,
                event_type,
                source_material_id,
                COALESCE(NULLIF(payload->>'natural_key_hash', ''), NULLIF(payload->>'natural_key', ''), equivalence_key) AS natural_key_hash,
                ts_orig
            FROM core.events
            WHERE source_material_id IS NOT NULL
              AND ($1::text IS NULL OR source = $1)
              AND ($2::text IS NULL OR event_type = $2)
        )
        SELECT
            source,
            event_type,
            natural_key_hash,
            COUNT(*)::bigint AS event_count,
            COUNT(DISTINCT source_material_id)::bigint AS material_count
        FROM keyed
        WHERE natural_key_hash IS NOT NULL AND natural_key_hash <> ''
        GROUP BY source, event_type, natural_key_hash
        HAVING COUNT(*) > 1 AND COUNT(DISTINCT source_material_id) > 1
        ORDER BY MAX(ts_orig) DESC
        LIMIT $3
        ",
    )
    .bind(source)
    .bind(event_type)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to list duplicate candidate clusters")
            .with_std_error(&error)
    })?;

    let mut clusters = Vec::with_capacity(cluster_rows.len());
    for row in cluster_rows {
        let source: String = row.try_get("source").map_err(cluster_row_error)?;
        let event_type: String = row.try_get("event_type").map_err(cluster_row_error)?;
        let natural_key_hash: String =
            row.try_get("natural_key_hash").map_err(cluster_row_error)?;
        let event_count: i64 = row.try_get("event_count").map_err(cluster_row_error)?;
        let material_count: i64 = row.try_get("material_count").map_err(cluster_row_error)?;

        let event_rows = sqlx::query(
            r"
            SELECT id, source_material_id, ts_orig
            FROM core.events
            WHERE source = $1
              AND event_type = $2
              AND source_material_id IS NOT NULL
              AND COALESCE(NULLIF(payload->>'natural_key_hash', ''), NULLIF(payload->>'natural_key', ''), equivalence_key) = $3
            ORDER BY ts_orig DESC, id DESC
            LIMIT $4
            ",
        )
        .bind(&source)
        .bind(&event_type)
        .bind(&natural_key_hash)
        .bind(events_per_cluster)
        .fetch_all(pool)
        .await
        .map_err(|error| {
            SinexError::database("failed to list duplicate candidate events")
                .with_context("source", &source)
                .with_context("event_type", &event_type)
                .with_context("natural_key_hash", &natural_key_hash)
                .with_std_error(&error)
        })?;

        let mut events = Vec::with_capacity(event_rows.len());
        for event_row in event_rows {
            events.push(CurationDuplicateCandidateEvent {
                event_id: event_row.try_get("id").map_err(cluster_row_error)?,
                source_material_id: event_row
                    .try_get("source_material_id")
                    .map_err(cluster_row_error)?,
                ts_orig: event_row.try_get("ts_orig").map_err(cluster_row_error)?,
            });
        }

        clusters.push(CurationDuplicateCandidateCluster {
            cluster_id: duplicate_cluster_id(&source, &event_type, &natural_key_hash),
            source,
            event_type,
            natural_key_hash,
            event_count,
            material_count,
            events,
        });
    }

    Ok(CurationListDuplicateCandidatesResponse { clusters })
}

pub async fn handle_curation_record_duplicate_judgment(
    pool: &PgPool,
    req: CurationRecordDuplicateJudgmentRequest,
    auth: &RpcAuthContext,
) -> Result<CurationRecordDuplicateJudgmentResponse> {
    validate_duplicate_judgment_request(&req)?;

    let event_ids: Vec<EventId> = req
        .event_ids
        .iter()
        .copied()
        .map(EventId::from_uuid)
        .collect();
    let events = pool.events().get_by_ids(&event_ids).await?;
    if events.len() != event_ids.len() {
        return Err(SinexError::not_found(
            "curation.duplicate_judgments.record: not all candidate events were found",
        )
        .with_context("requested", event_ids.len().to_string())
        .with_context("found", events.len().to_string()));
    }

    let mut evidence_material_ids = Vec::with_capacity(events.len());
    for event in &events {
        if event.source.as_str() != req.source || event.event_type.as_str() != req.event_type {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record: candidate event is outside requested cluster",
            )
            .with_context("event_id", persisted_event_id(event)?.to_string())
            .with_context("expected_source", &req.source)
            .with_context("actual_source", event.source.as_str())
            .with_context("expected_event_type", &req.event_type)
            .with_context("actual_event_type", event.event_type.as_str()));
        }
        let key = duplicate_logical_key(event).ok_or_else(|| {
            SinexError::validation(
                "curation.duplicate_judgments.record: candidate event has no logical key",
            )
            .with_context(
                "event_id",
                persisted_event_id(event)
                    .map_or_else(|_| "<missing-id>".to_string(), |id| id.to_string()),
            )
        })?;
        if key != req.natural_key_hash {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record: candidate event logical key mismatch",
            )
            .with_context("event_id", persisted_event_id(event)?.to_string())
            .with_context("expected_natural_key_hash", &req.natural_key_hash)
            .with_context("actual_natural_key_hash", key));
        }
        let Provenance::Material { id, .. } = &event.provenance else {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record: candidate event is not material-provenance",
            )
            .with_context("event_id", persisted_event_id(event)?.to_string()));
        };
        evidence_material_ids.push(id.to_uuid());
    }
    evidence_material_ids.sort_unstable();
    evidence_material_ids.dedup();
    if evidence_material_ids.len() < 2 {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires candidate events from at least two source materials",
        )
        .with_context("material_count", evidence_material_ids.len().to_string()));
    }

    let action_value = serde_json::to_value(req.action).map_err(|error| {
        SinexError::serialization("failed to serialize duplicate curation action")
            .with_std_error(&error)
    })?;
    let action_label = action_value
        .as_str()
        .ok_or_else(|| {
            SinexError::serialization("duplicate curation action did not serialize as string")
        })?
        .to_string();
    let candidate_payload = json!({
        "source": req.source.clone(),
        "event_type": req.event_type.clone(),
        "natural_key_hash": req.natural_key_hash.clone(),
        "action": action_label,
        "preferred_event_id": req.preferred_event_id,
        "candidate_event_ids": req.event_ids.clone(),
        "candidate_material_ids": evidence_material_ids.clone(),
    });
    let cluster_id = duplicate_cluster_id(&req.source, &req.event_type, &req.natural_key_hash);
    let proposal = CurationProposalPayload {
        proposal_id: Uuid::now_v7(),
        proposal_key: format!("duplicate-resolution:{cluster_id}"),
        proposal_kind: "curation.duplicate_resolution".to_string(),
        target_ref: Some(cluster_id),
        candidate_source: "curation".to_string(),
        candidate_event_type: "curation.duplicate_resolution".to_string(),
        candidate_payload,
        evidence_event_ids: req.event_ids.clone(),
        evidence_material_ids: evidence_material_ids.clone(),
        producer: "sinex-gateway.duplicate-workbench@1".to_string(),
        confidence: 1.0,
        rationale: "operator duplicate-resolution action over cross-material candidate events"
            .to_string(),
        status: CurationProposalStatus::Pending,
    };
    let proposal_event = proposal
        .clone()
        .from_parents(event_ids.clone())?
        .at_time(Timestamp::now())
        .build()?;
    let proposal_event = pool.events().insert(proposal_event).await?;
    let proposal_event_id = proposal_event.id.ok_or_else(|| {
        SinexError::invalid_state(
            "curation.duplicate_judgments.record: persisted proposal event missing id",
        )
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
        decision: duplicate_action_decision(req.action),
        corrected_payload: None,
        comment: req.comment,
        judged_at: Timestamp::now(),
        authorization_context: Some(json!({
            "duplicate_action": action_label,
            "cluster_id": proposal.target_ref.clone(),
            "preferred_event_id": req.preferred_event_id,
        })),
    };
    let judgment_event = judgment
        .clone()
        .from_parents([proposal_event_id])?
        .at_time(judgment.judged_at)
        .build()?;
    let judgment_event = pool.events().insert(judgment_event).await?;

    Ok(CurationRecordDuplicateJudgmentResponse {
        proposal,
        proposal_event,
        judgment,
        judgment_event,
    })
}

pub async fn handle_curation_finalize(
    pool: &PgPool,
    req: CurationFinalizeRequest,
) -> Result<CurationFinalizeResponse> {
    let judgment_event_uuid = Uuid::from_str(&req.judgment_event_id).map_err(|error| {
        SinexError::validation("curation.finalize: invalid judgment_event_id")
            .with_context("judgment_event_id", &req.judgment_event_id)
            .with_std_error(&error)
    })?;
    let judgment_event_id = Id::<Event<JsonValue>>::from_uuid(judgment_event_uuid);
    let judgment_event = pool
        .events()
        .get_by_id(judgment_event_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("curation.finalize: judgment event not found")
                .with_context("judgment_event_id", &req.judgment_event_id)
        })?;
    if judgment_event.source.as_str() != "curation"
        || judgment_event.event_type.as_str() != "curation.judgment"
    {
        return Err(
            SinexError::validation("curation.finalize: event is not a curation judgment")
                .with_context("judgment_event_id", &req.judgment_event_id)
                .with_context("source", judgment_event.source.as_str())
                .with_context("event_type", judgment_event.event_type.as_str()),
        );
    }
    let judgment: CurationJudgmentPayload = serde_json::from_value(judgment_event.payload.clone())
        .map_err(|error| {
            SinexError::serialization("curation.finalize: invalid judgment payload")
                .with_std_error(&error)
        })?;
    let parent_ids = judgment_event.get_source_event_ids().ok_or_else(|| {
        SinexError::invalid_state("curation.finalize: judgment event has no proposal parent")
            .with_context("judgment_event_id", &req.judgment_event_id)
    })?;
    let proposal_event_id = parent_ids.first().copied().ok_or_else(|| {
        SinexError::invalid_state("curation.finalize: judgment event has no proposal parent")
            .with_context("judgment_event_id", &req.judgment_event_id)
    })?;
    let proposal_event = pool
        .events()
        .get_by_id(proposal_event_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("curation.finalize: proposal parent not found")
                .with_context("judgment_event_id", &req.judgment_event_id)
                .with_context("proposal_event_id", proposal_event_id.to_string())
        })?;
    if proposal_event.source.as_str() != "curation"
        || proposal_event.event_type.as_str() != "curation.proposal"
    {
        return Err(SinexError::validation(
            "curation.finalize: judgment parent is not a curation proposal",
        )
        .with_context("judgment_event_id", &req.judgment_event_id)
        .with_context("proposal_event_id", proposal_event_id.to_string())
        .with_context("source", proposal_event.source.as_str())
        .with_context("event_type", proposal_event.event_type.as_str()));
    }
    let proposal: CurationProposalPayload = serde_json::from_value(proposal_event.payload.clone())
        .map_err(|error| {
            SinexError::serialization("curation.finalize: invalid proposal payload")
                .with_std_error(&error)
        })?;
    let finalized_at = Timestamp::now();
    let finalized = CurationFinalizedPayload::from_judgment(
        Uuid::now_v7(),
        &proposal,
        &judgment,
        finalized_at,
    )?;
    let judgment_parent = judgment_event.id.ok_or_else(|| {
        SinexError::invalid_state("curation.finalize: persisted judgment event missing id")
    })?;
    let parents: [EventId; 2] = [proposal_event_id, judgment_parent];
    let event = finalized
        .clone()
        .from_parents(parents)?
        .at_time(finalized_at)
        .build()?;
    let inserted = pool.events().insert(event).await?;

    Ok(CurationFinalizeResponse {
        finalized,
        event: inserted,
    })
}

fn cluster_row_error(error: sqlx::Error) -> SinexError {
    SinexError::database("failed to decode duplicate candidate row").with_std_error(&error)
}

fn duplicate_cluster_id(source: &str, event_type: &str, natural_key_hash: &str) -> String {
    format!("{source}/{event_type}/{natural_key_hash}")
}

fn duplicate_logical_key(event: &Event<JsonValue>) -> Option<String> {
    event
        .payload
        .get("natural_key_hash")
        .or_else(|| event.payload.get("natural_key"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| event.equivalence_key.clone())
}

fn persisted_event_id(event: &Event<JsonValue>) -> Result<EventId> {
    event.id.ok_or_else(|| {
        SinexError::invalid_state("curation duplicate candidate event missing persisted id")
    })
}

fn validate_duplicate_judgment_request(req: &CurationRecordDuplicateJudgmentRequest) -> Result<()> {
    if req.source.trim().is_empty() {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires source",
        ));
    }
    if req.event_type.trim().is_empty() {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires event_type",
        ));
    }
    if req.natural_key_hash.trim().is_empty() {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires natural_key_hash",
        ));
    }
    if req.event_ids.len() < 2 {
        return Err(SinexError::validation(
            "curation.duplicate_judgments.record requires at least two candidate events",
        ));
    }
    let mut unique = HashSet::with_capacity(req.event_ids.len());
    for id in &req.event_ids {
        if !unique.insert(*id) {
            return Err(SinexError::validation(
                "curation.duplicate_judgments.record candidate event ids must be unique",
            )
            .with_context("event_id", id.to_string()));
        }
    }
    if req.action == CurationDuplicateAction::Prefer {
        let preferred_event_id = req.preferred_event_id.ok_or_else(|| {
            SinexError::validation("prefer duplicate action requires preferred_event_id")
        })?;
        if !unique.contains(&preferred_event_id) {
            return Err(SinexError::validation(
                "preferred_event_id must be one of the candidate event ids",
            )
            .with_context("preferred_event_id", preferred_event_id.to_string()));
        }
    } else if req.preferred_event_id.is_some() {
        return Err(SinexError::validation(
            "preferred_event_id is only valid with prefer duplicate action",
        ));
    }
    Ok(())
}

fn duplicate_action_decision(action: CurationDuplicateAction) -> CurationJudgmentDecision {
    match action {
        CurationDuplicateAction::Merge | CurationDuplicateAction::Prefer => {
            CurationJudgmentDecision::Accept
        }
        CurationDuplicateAction::Ignore => CurationJudgmentDecision::Reject,
    }
}
