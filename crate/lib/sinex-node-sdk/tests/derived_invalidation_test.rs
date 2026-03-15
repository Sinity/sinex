#![cfg(feature = "messaging")]

//! Tests for scope invalidation support in the derived node model family.
//!
//! Verifies:
//! - `DerivedScopeInvalidation` construction and filtering
//! - Transducer nodes ignore invalidation (return empty)
//! - Scope reconciler nodes recompute from working set
//! - Windowed nodes recompute from working set

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedOutput, DerivedScopeInvalidation, DerivedTriggerContext,
};
use sinex_node_sdk::{NodeLogicError, ScopeReconcilerNode, TransducerNode, WindowedNode};
use sinex_primitives::domain::{InvalidationAction, ProcessingMode, TriggerKind};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::testing::event_stub;
use sinex_primitives::{Id, JsonValue, Uuid};
use xtask::sandbox::prelude::*;

// ── Test fixtures ────────────────────────────────────────────────────────

fn make_context() -> DerivedTriggerContext {
    DerivedTriggerContext {
        trigger_event_id: Id::new(),
        source: "test".into(),
        event_type: "test.event".into(),
        ts_orig: Some(Timestamp::now()),
        ts_coided: Timestamp::now(),
        processing_mode: ProcessingMode::Replay,
        trigger_kind: TriggerKind::ScopeInvalidation,
        created_by_operation_id: None,
    }
}

// ── DerivedScopeInvalidation unit tests ──────────────────────────────────

#[sinex_test]
async fn invalidation_signal_construction() -> TestResult<()> {
    let ids = vec![Uuid::now_v7(), Uuid::now_v7()];
    let inv = DerivedScopeInvalidation::archived(ids.clone(), "fs-watcher", "file.created");

    assert_eq!(inv.action, InvalidationAction::Archived);
    assert_eq!(inv.affected_event_ids.len(), 2);
    assert_eq!(inv.event_source, "fs-watcher");
    assert_eq!(inv.event_type, "file.created");
    assert!(inv.operation_id.is_none());
    assert!(inv.affected_scope_keys.is_empty());

    Ok(())
}

#[sinex_test]
async fn invalidation_signal_with_operation_and_scopes() -> TestResult<()> {
    let op_id = Uuid::now_v7();
    let inv =
        DerivedScopeInvalidation::replaced(vec![Uuid::now_v7()], "analytics", "analytics.summary")
            .with_operation(op_id)
            .with_scope_keys(vec!["scope-a".into(), "scope-b".into()]);

    assert_eq!(inv.action, InvalidationAction::Replaced);
    assert_eq!(inv.operation_id, Some(op_id));
    assert_eq!(inv.affected_scope_keys, vec!["scope-a", "scope-b"]);

    Ok(())
}

#[sinex_test]
async fn invalidation_matches_input_filter() -> TestResult<()> {
    let inv =
        DerivedScopeInvalidation::archived(vec![Uuid::now_v7()], "analytics", "analytics.summary");

    assert!(inv.matches_input("analytics.summary"));
    assert!(!inv.matches_input("file.created"));
    assert!(!inv.matches_input("analytics.trend"));

    Ok(())
}

#[sinex_test]
async fn invalidation_inserted_variant() -> TestResult<()> {
    let inv =
        DerivedScopeInvalidation::inserted(vec![Uuid::now_v7()], "fs-watcher", "file.created");
    assert_eq!(inv.action, InvalidationAction::Inserted);

    Ok(())
}

// ── Transducer: ignores invalidation ─────────────────────────────────────

#[derive(Default, Serialize, Deserialize)]
struct TransducerState;

#[derive(Deserialize)]
struct TInput {
    _value: String,
}

#[derive(Serialize)]
struct TOutput {
    _result: String,
}

struct TestTransducer;

impl TransducerNode for TestTransducer {
    type State = TransducerState;
    type Input = TInput;
    type Output = TOutput;

    fn name(&self) -> &'static str {
        "test-transducer"
    }
    fn input_event_type(&self) -> &'static str {
        "file.created"
    }
    fn output_event_type(&self) -> &'static str {
        "file.processed"
    }

    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        Ok(None)
    }
}

#[sinex_test]
async fn transducer_invalidation_returns_empty() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, TransducerWrapper};

    let mut wrapper = TransducerWrapper(TestTransducer);
    let mut state = TransducerState;
    let ctx = make_context();

    let result: Vec<DerivedOutput<JsonValue>> = wrapper
        .process_invalidation_derived(
            &mut state,
            "any-scope",
            Vec::<sinex_primitives::Event<JsonValue>>::new(),
            &ctx,
        )
        .await?;

    assert!(
        result.is_empty(),
        "transducers should return empty on invalidation"
    );
    Ok(())
}

// ── ScopeReconciler: recomputes from working set ─────────────────────────

#[derive(Default, Serialize, Deserialize)]
struct ReconcilerState;

#[derive(Deserialize, Clone)]
struct RInput {
    value: i64,
}

#[derive(Serialize)]
struct ROutput {
    total: i64,
    count: usize,
}

struct TestReconciler;

impl ScopeReconcilerNode for TestReconciler {
    type State = ReconcilerState;
    type Input = RInput;
    type Output = ROutput;

    fn name(&self) -> &'static str {
        "test-reconciler"
    }
    fn input_event_type(&self) -> &'static str {
        "measurement.taken"
    }
    fn output_event_type(&self) -> &'static str {
        "measurement.aggregate"
    }

    fn scope_keys(&self, _input: &Self::Input, _context: &DerivedTriggerContext) -> Vec<String> {
        vec!["default".into()]
    }

    async fn reconcile(
        &mut self,
        _state: &mut Self::State,
        _scope_key: &str,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        Ok(Some(DerivedOutput::reconciled(
            ROutput {
                total: input.value,
                count: 1,
            },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            vec![*context.trigger_event_id.as_uuid()],
            "default".into(),
        )))
    }

    /// Custom recompute: sum all values in the working set.
    async fn recompute_scope(
        &mut self,
        _state: &mut Self::State,
        scope_key: &str,
        working_set: Vec<Self::Input>,
        context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
        if working_set.is_empty() {
            return Ok(Vec::new());
        }

        let total: i64 = working_set.iter().map(|i| i.value).sum();
        let count = working_set.len();

        Ok(vec![DerivedOutput::reconciled(
            ROutput { total, count },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            vec![*context.trigger_event_id.as_uuid()],
            scope_key.to_string(),
        )])
    }
}

#[sinex_test]
async fn reconciler_recomputes_scope_from_working_set() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(TestReconciler);
    let mut state = ReconcilerState;
    let ctx = make_context();

    // Build working set as Event<JsonValue> (the trait method takes events)
    let working_set: Vec<sinex_primitives::Event<JsonValue>> = (1..=3)
        .map(|i| event_stub(serde_json::json!({ "value": i * 10 })))
        .collect();

    let results = wrapper
        .process_invalidation_derived(&mut state, "test-scope", working_set, &ctx)
        .await?;

    assert_eq!(results.len(), 1);

    // Verify the recomputed output summed the values: 10 + 20 + 30 = 60
    let output_payload: serde_json::Value = results[0].payload.clone();
    assert_eq!(output_payload["total"], 60);
    assert_eq!(output_payload["count"], 3);

    Ok(())
}

#[sinex_test]
async fn reconciler_empty_working_set_produces_no_output() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(TestReconciler);
    let mut state = ReconcilerState;
    let ctx = make_context();

    let results = wrapper
        .process_invalidation_derived(
            &mut state,
            "empty-scope",
            Vec::<sinex_primitives::Event<JsonValue>>::new(),
            &ctx,
        )
        .await?;

    assert!(
        results.is_empty(),
        "empty working set should produce no output"
    );
    Ok(())
}

// ── WindowedNode: recomputes from working set ────────────────────────────

#[derive(Default, Serialize, Deserialize)]
struct WindowState {
    values: Vec<i64>,
}

#[derive(Deserialize)]
struct WInput {
    value: i64,
}

#[derive(Serialize)]
struct WOutput {
    sum: i64,
    window_size: usize,
}

struct TestWindowed;

impl WindowedNode for TestWindowed {
    type State = WindowState;
    type Input = WInput;
    type Output = WOutput;

    fn name(&self) -> &'static str {
        "test-windowed"
    }
    fn input_event_type(&self) -> &'static str {
        "metric.sample"
    }
    fn output_event_type(&self) -> &'static str {
        "metric.window"
    }

    async fn accumulate(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &DerivedTriggerContext,
    ) -> Result<(), NodeLogicError> {
        state.values.push(input.value);
        Ok(())
    }

    fn window_complete(&self, state: &Self::State) -> bool {
        // Window is always "complete" for recomputation
        !state.values.is_empty()
    }

    async fn emit(
        &mut self,
        state: &mut Self::State,
        _context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        let sum: i64 = state.values.iter().sum();
        let window_size = state.values.len();
        state.values.clear();

        Ok(Some(DerivedOutput::windowed(
            WOutput { sum, window_size },
            vec![], // simplified for test
        )))
    }
}

#[sinex_test]
async fn windowed_recomputes_from_working_set() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, WindowedWrapper};

    let mut wrapper = WindowedWrapper(TestWindowed);
    let mut state = WindowState::default();
    let ctx = make_context();

    let working_set: Vec<sinex_primitives::Event<JsonValue>> = vec![
        event_stub(serde_json::json!({ "value": 5 })),
        event_stub(serde_json::json!({ "value": 15 })),
    ];

    let results = wrapper
        .process_invalidation_derived(&mut state, "any", working_set, &ctx)
        .await?;

    assert_eq!(results.len(), 1);
    let output: serde_json::Value = results[0].payload.clone();
    assert_eq!(output["sum"], 20);
    assert_eq!(output["window_size"], 2);

    Ok(())
}
