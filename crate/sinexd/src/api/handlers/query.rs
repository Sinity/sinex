//! Composable event query, provenance lineage, and event-annotation handlers.

use crate::api::service_container::ServiceContainer;
use crate::event_engine::policy::{
    DisclosureCaveat, DisclosureContext, DisclosureDecision, PolicyEngine,
};
use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::query::{
    EventQuery, EventQueryResult, LineageQuery, LineageResult, QueryResultEvent, TimeRange,
};
use sinex_primitives::relations::{EventRelationExpr, EvidenceWindow, ObservedRange, TimeBasis};
use sinex_primitives::rpc::events::{
    EventsAnnotateRequest, EventsAnnotateResponse, EventsRelationEvidenceRequest,
};
use sinex_primitives::views::{
    CaveatView, EventCardListView, PrivacyStateView, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use sinex_primitives::{Id, JsonValue, Result, SinexError};
use sqlx::PgPool;
use std::future::Future;
use std::str::FromStr;
use time::Duration;

pub async fn handle_events_query(pool: &PgPool, query: EventQuery) -> Result<EventQueryResult> {
    pool.events().query(query).await
}

pub async fn handle_events_lineage(
    services: &ServiceContainer,
    query: LineageQuery,
) -> Result<LineageResult> {
    let result = services.pool().events().lineage(query).await?;
    Ok(lineage_result_with_policy(result, services.privacy_policy()).await)
}

pub async fn handle_events_relation_evidence(
    pool: &PgPool,
    req: EventsRelationEvidenceRequest,
) -> Result<ViewEnvelope<EvidenceWindow>> {
    evaluate_relation_evidence_request(req, |query| async move {
        let result = pool.events().query(query).await?;
        events_from_query_result(result)
    })
    .await
}

/// `events.annotate` (#1172 AC-9): write a typed annotation to
/// `core.event_annotations` against an existing event id.
///
/// Distinct from `sources.annotate` (material-level annotation).
pub async fn handle_events_annotate(
    pool: &PgPool,
    req: EventsAnnotateRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<EventsAnnotateResponse> {
    let event_id_str = req.event_id.as_str();
    let annotation_type = req.annotation_type.as_str();
    let content = req.content.as_str();
    let metadata = req.metadata.unwrap_or_else(|| json!({}));

    if annotation_type.trim().is_empty() {
        return Err(SinexError::validation(
            "events.annotate: annotation_type must not be empty",
        ));
    }
    if content.trim().is_empty() {
        return Err(SinexError::validation(
            "events.annotate: content must not be empty",
        ));
    }

    let event_uuid = sinex_primitives::Uuid::from_str(event_id_str).map_err(|error| {
        SinexError::validation("events.annotate: invalid event_id UUID")
            .with_context("event_id", event_id_str)
            .with_std_error(&error)
    })?;
    let event_id =
        Id::<sinex_primitives::events::Event<sinex_primitives::JsonValue>>::from_uuid(event_uuid);

    let record = pool
        .events()
        .add_annotation(
            event_id,
            annotation_type,
            content,
            metadata,
            auth.actor_id(),
        )
        .await
        .map_err(|error| {
            SinexError::database("events.annotate: failed to record annotation")
                .with_source(error.to_string())
        })?;

    Ok(EventsAnnotateResponse {
        id: record.id.as_uuid().to_string(),
        event_id: record.event_id.as_uuid().to_string(),
        annotation_type: record.annotation_type,
        content: record.content,
        metadata: record.metadata,
        created_by: record.created_by,
        created_at: record.created_at.format_rfc3339(),
        updated_at: record.updated_at.format_rfc3339(),
    })
}

/// `events.cards` — query events and return them as `EventCardView`s
/// with refs, caveats, privacy state, and action availability preserved.
pub async fn handle_events_cards(
    services: &ServiceContainer,
    query: EventQuery,
) -> Result<EventCardListView> {
    let result = services.pool().events().query(query).await?;
    match result {
        EventQueryResult::Events {
            events,
            next_cursor,
            total_estimate,
        } => Ok(
            event_card_list_with_policy(&events, services.privacy_policy())
                .await
                .with_query_metadata(next_cursor, total_estimate),
        ),
        other => Err(SinexError::validation(format!(
            "events.cards requires an Events query result, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

pub(crate) async fn event_card_list_with_policy(
    events: &[QueryResultEvent],
    policy: &PolicyEngine,
) -> EventCardListView {
    let mut disclosed_events = Vec::with_capacity(events.len());
    let mut disclosure_meta = Vec::with_capacity(events.len());

    for result in events {
        let mut disclosed = result.clone();
        let payload_decision = policy
            .disclose_event_payload(&result.event, DisclosureContext::View)
            .await;
        disclosed.event.payload = payload_decision.value.clone();

        let snippet_decision = if let Some(snippet) = result.snippet.as_deref() {
            let decision = policy
                .disclose_event_text(&result.event, snippet, DisclosureContext::View)
                .await;
            disclosed.snippet = match decision.value.as_str() {
                Some(text) => Some(text.to_string()),
                None if decision.changed => Some("[suppressed by privacy policy]".to_string()),
                None => disclosed.snippet,
            };
            Some(decision)
        } else {
            None
        };

        let privacy_state = merged_privacy_state(&payload_decision, snippet_decision.as_ref());
        let caveats = disclosure_caveats(&payload_decision, snippet_decision.as_ref());
        disclosed_events.push(disclosed);
        disclosure_meta.push((privacy_state, caveats));
    }

    let mut view = EventCardListView::from_query_events(&disclosed_events);
    for (card, (privacy_state, caveats)) in view.cards.iter_mut().zip(disclosure_meta) {
        if let Some(privacy_state) = privacy_state {
            card.privacy_state = privacy_state;
        }
        card.caveats.extend(caveats);
    }
    view
}

async fn lineage_result_with_policy(
    mut result: LineageResult,
    policy: &PolicyEngine,
) -> LineageResult {
    disclose_lineage_event_payload(&mut result.root, policy).await;
    for node in &mut result.ancestors {
        disclose_lineage_event_payload(&mut node.event, policy).await;
    }
    for node in &mut result.descendants {
        disclose_lineage_event_payload(&mut node.event, policy).await;
    }
    result
}

async fn disclose_lineage_event_payload(event: &mut Event<JsonValue>, policy: &PolicyEngine) {
    let decision = policy
        .disclose_event_payload(event, DisclosureContext::View)
        .await;
    event.payload = decision.value;
}

fn merged_privacy_state(
    payload: &DisclosureDecision,
    snippet: Option<&DisclosureDecision>,
) -> Option<PrivacyStateView> {
    if payload.changed {
        return Some(payload.privacy_state.clone());
    }
    snippet
        .filter(|decision| decision.changed)
        .map(|decision| decision.privacy_state.clone())
}

fn disclosure_caveats(
    payload: &DisclosureDecision,
    snippet: Option<&DisclosureDecision>,
) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    append_disclosure_caveats(&mut caveats, &payload.caveats);
    if let Some(snippet) = snippet {
        append_disclosure_caveats(&mut caveats, &snippet.caveats);
    }
    caveats
}

fn append_disclosure_caveats(output: &mut Vec<CaveatView>, caveats: &[DisclosureCaveat]) {
    for caveat in caveats {
        if output
            .iter()
            .any(|existing| existing.id == caveat.code && existing.message == caveat.message)
        {
            continue;
        }
        output.push(CaveatView {
            id: caveat.code.clone(),
            message: caveat.message.clone(),
            ref_: Some(
                SinexObjectRef::new(SinexObjectKind::Policy, caveat.policy_ref.clone())
                    .with_label("privacy policy")
                    .with_command_hint("sinexctl privacy policy list")
                    .with_rpc_method("privacy.policy.list"),
            ),
        });
    }
}

async fn evaluate_relation_evidence_request<F, Fut>(
    req: EventsRelationEvidenceRequest,
    mut resolve: F,
) -> Result<ViewEnvelope<EvidenceWindow>>
where
    F: FnMut(EventQuery) -> Fut,
    Fut: Future<Output = Result<Vec<Event<JsonValue>>>>,
{
    validate_relation_event_query(&req.seed_query, "seed_query")?;
    if let Some(candidate_query) = &req.candidate_query {
        validate_relation_event_query(candidate_query, "candidate_query")?;
    }

    let seed_events = resolve(req.seed_query.clone()).await?;
    let candidate_query = req
        .candidate_query
        .clone()
        .unwrap_or_else(|| default_candidate_query(&req.relation, &seed_events));
    let candidate_events = resolve(candidate_query.clone()).await?;

    let window = req.relation.evaluate(&seed_events, &candidate_events);
    Ok(window
        .into_view("sinexd.events.relation_evidence")
        .with_query_echo(json!({
            "seed_query": req.seed_query,
            "candidate_query": candidate_query,
            "candidate_query_defaulted": req.candidate_query.is_none(),
            "relation": req.relation,
        })))
}

fn validate_relation_event_query(query: &EventQuery, field: &'static str) -> Result<()> {
    if query.aggregation.is_some() {
        return Err(SinexError::validation(format!(
            "events.relation_evidence: {field} must select events, not an aggregation"
        )));
    }
    Ok(())
}

fn events_from_query_result(result: EventQueryResult) -> Result<Vec<Event<JsonValue>>> {
    match result {
        EventQueryResult::Events { events, .. } => {
            Ok(events.into_iter().map(|row| row.event).collect())
        }
        other => Err(SinexError::validation(format!(
            "events.relation_evidence requires event-list query results, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

fn default_candidate_query(relation: &EventRelationExpr, seeds: &[Event<JsonValue>]) -> EventQuery {
    let mut query = EventQuery {
        limit: sinex_primitives::Pagination::MAX_LIMIT,
        ..EventQuery::default()
    };

    let seed_range = seeds
        .iter()
        .map(ObservedRange::from_event)
        .reduce(|acc, range| acc.union(&range))
        .unwrap_or_else(|| ObservedRange::unknown(TimeBasis::AtemporalAnchor));

    query.time_range = default_candidate_time_range(relation, seed_range);
    query
}

fn default_candidate_time_range(
    relation: &EventRelationExpr,
    seed_range: ObservedRange,
) -> Option<TimeRange> {
    let start = seed_range.start.or(seed_range.end)?;
    let end = seed_range.end.or(seed_range.start)?;
    let (start, end) = match relation {
        EventRelationExpr::Within { within_secs } => {
            let bound = Duration::seconds(*within_secs);
            (start - bound, end + bound)
        }
        EventRelationExpr::Before { max_gap_secs } => {
            (start - Duration::seconds(*max_gap_secs), end)
        }
        EventRelationExpr::After { max_gap_secs } => {
            (start, end + Duration::seconds(*max_gap_secs))
        }
        EventRelationExpr::Overlaps | EventRelationExpr::Sequence { .. } => (start, end),
        EventRelationExpr::Same { .. } => return None,
    };

    TimeRange::new(Some(start), Some(end)).ok()
}

#[cfg(test)]
#[path = "query_test.rs"]
mod tests;
