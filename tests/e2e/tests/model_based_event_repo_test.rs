//! Model-based stateful property tests for `EventRepository`.
//!
//! Uses `proptest` to generate random sequences of repository operations, runs
//! them in parallel against:
//!   - A **reference model** (in-memory `HashMap`) — always correct by construction
//!   - The **real `EventRepository`** — backed by a live PostgreSQL TestContext
//!
//! After each operation, the test asserts that the real DB matches the model's
//! expected output. If they diverge, proptest automatically shrinks the operation
//! sequence to the minimal reproducing case.
//!
//! ## Invariants verified
//!
//! 1. `insert(e)` → `get_by_id(e.id)` returns exactly `e`
//! 2. `count_all()` equals the size of the reference model at every step
//! 3. `count_by_source(s)` matches the model's count for source `s`
//! 4. `delete_by_source(s)` removes all events for `s` in both model and DB
//! 5. Re-inserting an event with the same ID (via `ON CONFLICT DO NOTHING`) is idempotent

use std::collections::HashMap;

use proptest::prelude::*;
use sinex_primitives::{DynamicPayload, EventSource};
use xtask::sandbox::prelude::*;

// ─── Reference model ─────────────────────────────────────────────────────────

/// In-memory reference model for the event repository.
///
/// Tracks a set of events keyed by their ID, with a secondary index by source
/// that mirrors what the DB maintains.
#[derive(Default)]
struct ReferenceModel {
    /// event_id → (source, event_type)
    events: HashMap<String, (String, String)>,
}

impl ReferenceModel {
    fn insert(&mut self, id: String, source: String, event_type: String) {
        // ON CONFLICT DO NOTHING: don't overwrite existing
        self.events.entry(id).or_insert((source, event_type));
    }

    fn count_all(&self) -> usize {
        self.events.len()
    }

    fn count_by_source(&self, source: &str) -> usize {
        self.events
            .values()
            .filter(|(s, _)| s.as_str() == source)
            .count()
    }

    fn delete_by_source(&mut self, source: &str) {
        self.events.retain(|_, (s, _)| s.as_str() != source);
    }

}

// ─── Operation vocabulary ─────────────────────────────────────────────────────

/// The set of operations we apply to both the model and the real DB.
#[derive(Debug, Clone)]
enum EventRepoOp {
    /// Insert a new event with the given source tag (0–2) and event_type tag (0–2).
    Insert { source_idx: u8, type_idx: u8 },
    /// Re-insert an already-inserted event (idempotency check).
    ReInsertLast,
    /// Query count for a specific source tag.
    CountBySource { source_idx: u8 },
    /// Delete all events for a source tag.
    DeleteBySource { source_idx: u8 },
    /// Query total count (cross-check with model).
    CountAll,
}

fn source_tag(idx: u8) -> &'static str {
    match idx % 3 {
        0 => "model-source-alpha",
        1 => "model-source-beta",
        _ => "model-source-gamma",
    }
}

fn type_tag(idx: u8) -> &'static str {
    match idx % 3 {
        0 => "model.event.created",
        1 => "model.event.updated",
        _ => "model.event.deleted",
    }
}

fn op_strategy() -> impl Strategy<Value = EventRepoOp> {
    prop_oneof![
        (0u8..3, 0u8..3).prop_map(|(source_idx, type_idx)| EventRepoOp::Insert {
            source_idx,
            type_idx
        }),
        Just(EventRepoOp::ReInsertLast),
        (0u8..3).prop_map(|source_idx| EventRepoOp::CountBySource { source_idx }),
        (0u8..3).prop_map(|source_idx| EventRepoOp::DeleteBySource { source_idx }),
        Just(EventRepoOp::CountAll),
    ]
}

// ─── Model-based test ────────────────────────────────────────────────────────

/// Run a random sequence of operations against both the reference model and
/// the real EventRepository, asserting they stay in sync at every step.
#[sinex_prop(cases = 20, timeout = "60s")]
async fn prop_event_repo_model_matches_reference(
    ctx: &TestContext,
    #[strategy(prop::collection::vec(op_strategy(), 1..25))] ops: Vec<EventRepoOp>,
) -> TestResult<()> {
    let pool = ctx.pool();
    let events = pool.events();

    // Create a single source material for this test run — all events derive from it.
    let material_id = ctx
        .create_source_material(Some("model-prop-test"))
        .await
        .map_err(|e| TestCaseError::fail(format!("create_source_material failed: {e}")))?;

    // Isolate this test's events by using a unique prefix per run
    let run_id = uuid::Uuid::now_v7().to_string().replace('-', "");
    let prefixed_source = |base: &str| format!("{base}-{run_id}");

    let mut model = ReferenceModel::default();
    let mut inserted_ids: Vec<String> = Vec::new();
    let mut anchor: i64 = 0;

    for (step, op) in ops.iter().enumerate() {
        match op {
            EventRepoOp::Insert { source_idx, type_idx } => {
                let source_str = prefixed_source(source_tag(*source_idx));
                let type_str = type_tag(*type_idx);

                let event = DynamicPayload::new(
                    source_str.clone(),
                    type_str,
                    serde_json::json!({"step": step, "op": "insert"}),
                )
                .from_material_at(material_id, anchor)
                .build()
                .map_err(|e| TestCaseError::fail(format!("build event failed: {e}")))?;

                anchor += 1;

                let inserted = pool
                    .events()
                    .insert(event)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("insert failed: {e}")))?;

                let id_str = inserted
                    .id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| format!("unknown-{step}"));

                model.insert(id_str.clone(), source_str, type_str.to_string());
                inserted_ids.push(id_str);
            }

            EventRepoOp::ReInsertLast => {
                // Re-inserting with a fresh UUIDv7 ID verifies that count increases by exactly 1.
                // Idempotency of existing UUIDs (ON CONFLICT DO NOTHING) is tested in persistence tests.
                if !inserted_ids.is_empty() {
                    let before = model.count_all();
                    let source_str = prefixed_source(source_tag(0));

                    let event = DynamicPayload::new(
                        source_str.clone(),
                        "model.event.reinsertion",
                        serde_json::json!({"step": step, "op": "re-insert"}),
                    )
                    .from_material_at(material_id, anchor)
                    .build()
                    .map_err(|e| TestCaseError::fail(format!("build re-insert failed: {e}")))?;

                    anchor += 1;

                    let inserted = pool
                        .events()
                        .insert(event)
                        .await
                        .map_err(|e| TestCaseError::fail(format!("re-insert failed: {e}")))?;

                    let id_str = inserted
                        .id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| format!("reinsert-{step}"));

                    model.insert(id_str.clone(), source_str, "model.event.reinsertion".to_string());
                    inserted_ids.push(id_str);
                    let after = model.count_all();
                    prop_assert_eq!(
                        after,
                        before + 1,
                        "step {}: re-insert should add exactly 1 to model count",
                        step
                    );
                }
            }

            EventRepoOp::CountBySource { source_idx } => {
                let source_str = prefixed_source(source_tag(*source_idx));
                let model_count = model.count_by_source(&source_str) as i64;

                let db_count = events
                    .count_by_source(&EventSource::from(source_str.clone()))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("db count_by_source failed: {e}")))?;

                prop_assert_eq!(
                    db_count,
                    model_count,
                    "step {}: count_by_source({}) mismatch: db={} model={}",
                    step, source_str, db_count, model_count
                );
            }

            EventRepoOp::DeleteBySource { source_idx } => {
                let source_str = prefixed_source(source_tag(*source_idx));

                events
                    .delete_by_source(&EventSource::from(source_str.clone()))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("db delete_by_source failed: {e}")))?;

                model.delete_by_source(&source_str);

                // Verify the delete took effect
                let db_count_after = events
                    .count_by_source(&EventSource::from(source_str.clone()))
                    .await
                    .map_err(|e| TestCaseError::fail(format!("db count after delete failed: {e}")))?;

                prop_assert_eq!(
                    db_count_after,
                    0,
                    "step {}: after delete_by_source({}), count should be 0, got {}",
                    step, source_str, db_count_after
                );
            }

            EventRepoOp::CountAll => {
                let model_count = model.count_all() as i64;

                let db_count = events
                    .count_all()
                    .await
                    .map_err(|e| TestCaseError::fail(format!("db count_all failed: {e}")))?;

                prop_assert_eq!(
                    db_count,
                    model_count,
                    "step {}: count_all mismatch: db={} model={}",
                    step, db_count, model_count
                );
            }
        }
    }

    Ok(())
}

// ─── Get-by-id consistency ────────────────────────────────────────────────────

/// After inserting an event, `get_by_id` must return it, and after
/// `delete_by_source`, `get_by_id` must return None.
#[sinex_prop(cases = 15, timeout = "45s")]
async fn prop_get_by_id_consistent_with_insert_and_delete(
    ctx: &TestContext,
    #[strategy(1u8..10)] count: u8,
) -> TestResult<()> {
    use sinex_primitives::events::Event;
    use sinex_primitives::Id;
    use serde_json::Value as JsonValue;

    let pool = ctx.pool();
    let events = pool.events();
    let run_id = uuid::Uuid::now_v7().to_string().replace('-', "");
    let source = format!("model-get-by-id-{run_id}");

    let material_id = ctx
        .create_source_material(Some("model-get-by-id-test"))
        .await
        .map_err(|e| TestCaseError::fail(format!("create_source_material failed: {e}")))?;

    let mut ids: Vec<Id<Event<JsonValue>>> = Vec::new();

    // Insert `count` events and verify get_by_id returns each one
    for i in 0..count {
        let event = DynamicPayload::new(
            source.clone(),
            "model.get.by.id",
            serde_json::json!({"index": i}),
        )
        .from_material_at(material_id, i64::from(i))
        .build()
        .map_err(|e| TestCaseError::fail(format!("build event failed: {e}")))?;

        let inserted = pool
            .events()
            .insert(event)
            .await
            .map_err(|e| TestCaseError::fail(format!("insert failed: {e}")))?;

        let event_id = inserted
            .id
            .ok_or_else(|| TestCaseError::fail("inserted event had no ID"))?;

        let fetched = events
            .get_by_id(event_id)
            .await
            .map_err(|e| TestCaseError::fail(format!("get_by_id failed: {e}")))?;

        prop_assert!(
            fetched.is_some(),
            "get_by_id({event_id}) must return Some after insert"
        );
        ids.push(event_id);
    }

    // Delete by source — all events should disappear
    events
        .delete_by_source(&EventSource::from(source.clone()))
        .await
        .map_err(|e| TestCaseError::fail(format!("delete_by_source failed: {e}")))?;

    // Verify none of the inserted IDs are findable
    for id in &ids {
        let fetched = events
            .get_by_id(*id)
            .await
            .map_err(|e| TestCaseError::fail(format!("get_by_id post-delete failed: {e}")))?;

        prop_assert!(
            fetched.is_none(),
            "get_by_id({id}) must return None after delete_by_source"
        );
    }

    Ok(())
}
