use super::super::conversions::extract_provenance;
use crate::JsonValue;
use crate::SinexError;
use crate::models::Event;
use crate::repositories::common::{DbResult, db_error};
use sinex_primitives::Id;
use sinex_primitives::events::EventId;
use sqlx::{Executor, Postgres};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Validate that a derived event does not directly reference itself.
///
/// # Why only the direct self-reference check here?
///
/// Events are identified by `UUIDv7`, which is monotonically increasing in
/// time. A newly-created event ID is unique and cannot yet exist in the
/// database. Therefore:
///
/// - A cycle of the form `NEW → A → NEW` is impossible: `NEW` has never
///   been persisted, so no existing event can have `NEW` in its
///   `source_event_ids`.
/// - The only reachable existing-graph cycle case is `NEW → NEW` (the event
///   listing itself as its own parent), which this function detects with an
///   O(n) scan.
///
/// The previous implementation ran a `WITH RECURSIVE` CTE to walk the full
/// ancestry graph on every derived insert. That check added a full
/// recursive DB round-trip per batch row for a condition that `UUIDv7`
/// monotonicity already makes structurally impossible. It has been removed.
///
/// Batch-local cycles are still possible when a caller inserts multiple new
/// derived events with explicit IDs in the same batch. Those are rejected by
/// `ensure_no_intra_batch_synthesis_cycles` before insert.
///
/// Array-size limits are retained because large `source_event_ids` arrays have
/// real query-performance implications irrespective of cycles.
pub(super) fn ensure_no_synthesis_cycles<'e, E>(
    _executor: E,
    event_id: &Id<Event<JsonValue>>,
    source_event_ids: &[EventId],
) -> DbResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    if source_event_ids.is_empty() {
        return Ok(());
    }

    // Array-size guards: large parent arrays degrade lineage query performance.
    const WARN_THRESHOLD: usize = 100;
    const HARD_LIMIT: usize = 1000;

    if source_event_ids.len() > HARD_LIMIT {
        return Err(SinexError::database(format!(
            "source_event_ids array exceeds hard limit of {} parents (got {}). \
             This indicates a pathological derived pattern that will cause performance issues.",
            HARD_LIMIT,
            source_event_ids.len()
        )));
    }

    if source_event_ids.len() > WARN_THRESHOLD {
        tracing::warn!(
            event_id = %event_id,
            parent_count = source_event_ids.len(),
            threshold = WARN_THRESHOLD,
            hard_limit = HARD_LIMIT,
            "Event has unusually large number of parent events. \
             This may indicate a derived anti-pattern and will impact query performance."
        );
    }

    // Direct self-reference: the one cycle the UUIDv7 argument cannot rule out.
    if source_event_ids
        .iter()
        .any(|source_id| source_id == event_id)
    {
        return Err(SinexError::database("cycle detected in derived provenance"));
    }

    Ok(())
}

pub(super) async fn ensure_source_event_ids_are_live<'e, E>(
    executor: E,
    event_id: &Id<Event<JsonValue>>,
    source_event_ids: &[EventId],
    batch_event_ids: Option<&HashSet<Uuid>>,
) -> DbResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    if source_event_ids.is_empty() {
        return Ok(());
    }

    let source_uuids = source_event_ids
        .iter()
        .map(EventId::to_uuid)
        .filter(|source_id| {
            batch_event_ids
                .map(|batch_ids| !batch_ids.contains(source_id))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if source_uuids.is_empty() {
        return Ok(());
    }
    if let Some(invalid_source_id) = source_uuids.iter().find(|source_id| !is_uuid_v7(source_id)) {
        return Err(SinexError::validation(format!(
            "derived event {event_id} references non-UUIDv7 source_event_id {invalid_source_id}; \
             source_event_ids must reference live core.events IDs"
        )));
    }

    let live_ids = sqlx::query_scalar::<_, Uuid>(
        r"
        SELECT id::uuid
        FROM core.events
        WHERE id = ANY($1::uuid[])
        FOR KEY SHARE
        ",
    )
    .bind(&source_uuids)
    .fetch_all(executor)
    .await
    .map_err(|e| db_error(e, "validate live derived event parents"))?;

    let live_set = live_ids.into_iter().collect::<HashSet<_>>();
    let missing_ids = source_uuids
        .iter()
        .copied()
        .filter(|source_id| !live_set.contains(source_id))
        .collect::<Vec<_>>();

    if !missing_ids.is_empty() {
        return Err(SinexError::validation(format!(
            "derived event {event_id} references {} non-live source_event_ids: {}",
            missing_ids.len(),
            missing_ids
                .iter()
                .map(Uuid::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    Ok(())
}

fn is_uuid_v7(value: &Uuid) -> bool {
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

pub(super) fn ensure_no_intra_batch_synthesis_cycles(
    synthesis_checks: &[(Id<Event<JsonValue>>, Vec<EventId>)],
) -> DbResult<()> {
    if synthesis_checks.len() < 2 {
        return Ok(());
    }

    let batch_ids: HashSet<Uuid> = synthesis_checks
        .iter()
        .map(|(event_id, _)| *event_id.as_uuid())
        .collect();
    let local_edges: HashMap<Uuid, Vec<Uuid>> = synthesis_checks
        .iter()
        .filter_map(|(event_id, source_ids)| {
            let local_parents = source_ids
                .iter()
                .map(EventId::to_uuid)
                .filter(|source_id| batch_ids.contains(source_id))
                .collect::<Vec<_>>();
            if local_parents.is_empty() {
                None
            } else {
                Some((*event_id.as_uuid(), local_parents))
            }
        })
        .collect();

    if local_edges.is_empty() {
        return Ok(());
    }

    let mut finished = HashSet::new();
    let mut stack = Vec::new();
    let mut nodes = local_edges.keys().copied().collect::<Vec<_>>();
    nodes.sort_unstable();

    for node in nodes {
        if let Some(cycle) =
            detect_intra_batch_synthesis_cycle(node, &local_edges, &mut finished, &mut stack)
        {
            let cycle = cycle
                .into_iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(" -> ");
            return Err(SinexError::database(format!(
                "cycle detected in derived provenance within batch: {cycle}"
            )));
        }
    }

    Ok(())
}

fn detect_intra_batch_synthesis_cycle(
    node: Uuid,
    local_edges: &HashMap<Uuid, Vec<Uuid>>,
    finished: &mut HashSet<Uuid>,
    stack: &mut Vec<Uuid>,
) -> Option<Vec<Uuid>> {
    if finished.contains(&node) {
        return None;
    }

    if let Some(position) = stack.iter().position(|current| *current == node) {
        let mut cycle = stack[position..].to_vec();
        cycle.push(node);
        return Some(cycle);
    }

    stack.push(node);
    if let Some(parents) = local_edges.get(&node) {
        for parent in parents {
            if let Some(cycle) =
                detect_intra_batch_synthesis_cycle(*parent, local_edges, finished, stack)
            {
                return Some(cycle);
            }
        }
    }
    stack.pop();
    finished.insert(node);
    None
}

pub(super) fn ensure_batch_event_ids(events: &mut [Event<JsonValue>]) {
    for event in events {
        if event.id.is_none() {
            event.id = Some(Id::<Event<JsonValue>>::new());
        }
    }
}

type SynthesisChecks = Vec<(Id<Event<JsonValue>>, Vec<EventId>)>;

pub(super) fn collect_synthesis_checks(events: &[Event<JsonValue>]) -> DbResult<SynthesisChecks> {
    let mut synthesis_checks = Vec::new();

    for event in events {
        let Some(event_id) = event.id.as_ref() else {
            return Err(db_error(
                sqlx::Error::Protocol("batch insert event missing id".into()),
                "insert batch",
            ));
        };

        let (source_event_ids_raw, _, _, _, _, _) = extract_provenance(event)?;
        if let Some(source_ids) = source_event_ids_raw.filter(|source_ids| !source_ids.is_empty()) {
            synthesis_checks.push((
                Id::<Event<JsonValue>>::from_uuid(*event_id.as_uuid()),
                source_ids,
            ));
        }
    }

    Ok(synthesis_checks)
}

pub(super) fn resolved_created_by_operation_id(event: &Event<JsonValue>) -> DbResult<Option<Uuid>> {
    let provenance_operation_id = event.provenance.operation_uuid();

    match (event.created_by_operation_id, provenance_operation_id) {
        (Some(event_level), Some(provenance_level)) if event_level != provenance_level => {
            Err(SinexError::invalid_state(format!(
                "operation lineage mismatch: event.created_by_operation_id={event_level} \
                 but provenance.operation_id={provenance_level}"
            )))
        }
        (Some(event_level), _) => Ok(Some(event_level)),
        (None, Some(provenance_level)) => Ok(Some(provenance_level)),
        (None, None) => Ok(None),
    }
}

/// Validate a cascade session table name produced by `prepare_cascade_session`.
///
/// Table names must contain only ASCII alphanumerics, underscores, and at most
/// one dot (for schema qualification). This prevents format!()-based SQL injection
/// in the dynamic cascade queries.
pub(super) fn validate_cascade_table_name(table_name: &str) -> DbResult<()> {
    if table_name.is_empty()
        || table_name.starts_with('.')
        || table_name.ends_with('.')
        || table_name.contains("..")
        || !table_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(SinexError::validation(format!(
            "invalid cascade table name: {table_name:?}"
        )));
    }
    Ok(())
}
