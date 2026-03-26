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
    DerivedOutput, DerivedScopeInvalidation, DerivedTriggerContext, ScopeReconcilerWrapper,
};
use sinex_node_sdk::runtime::stream::{
    EventEmitter, Node, NodeHandles, NodeInitContext, ServiceInfo,
};
use sinex_node_sdk::{
    CheckpointManager, EventTransport, NatsPublisher, NodeLogicError, ScopeReconcilerNode,
    ScopeReconcilerNodeAdapter, TransducerNode, WindowedNode,
};
use sinex_primitives::domain::{InvalidationAction, ProcessingMode, TriggerKind};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::testing::event_stub;
use sinex_primitives::{Id, JsonValue, Pagination, Uuid};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
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

#[derive(Deserialize)]
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

#[derive(Default, Serialize, Deserialize)]
struct DefaultReconcilerState {
    total: i64,
    count: usize,
}

struct DefaultStatefulReconciler;

impl ScopeReconcilerNode for DefaultStatefulReconciler {
    type State = DefaultReconcilerState;
    type Input = RInput;
    type Output = ROutput;

    fn name(&self) -> &'static str {
        "default-stateful-reconciler"
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
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        state.total += input.value;
        state.count += 1;

        Ok(Some(DerivedOutput::reconciled(
            ROutput {
                total: state.total,
                count: state.count,
            },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            vec![*context.trigger_event_id.as_uuid()],
            scope_key.to_string(),
        )))
    }
}

#[sinex_test]
async fn reconciler_live_processing_uses_single_scope_key() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(TestReconciler);
    let mut state = ReconcilerState;
    let ctx = make_context();

    let result = wrapper
        .process_derived(
            &mut state,
            event_stub(serde_json::json!({ "value": 42 })),
            &ctx,
        )
        .await?;

    let output = result.expect("single-scope live processing should emit");
    assert_eq!(output.payload["total"], 42);
    assert_eq!(output.payload["count"], 1);
    assert_eq!(output.scope_key.as_deref(), Some("default"));

    Ok(())
}

struct MultiScopeReconciler;

impl ScopeReconcilerNode for MultiScopeReconciler {
    type State = ReconcilerState;
    type Input = RInput;
    type Output = ROutput;

    fn name(&self) -> &'static str {
        "test-multi-scope-reconciler"
    }
    fn input_event_type(&self) -> &'static str {
        "measurement.taken"
    }
    fn output_event_type(&self) -> &'static str {
        "measurement.aggregate"
    }

    fn scope_keys(&self, _input: &Self::Input, _context: &DerivedTriggerContext) -> Vec<String> {
        vec!["scope-a".into(), "scope-b".into()]
    }

    async fn reconcile(
        &mut self,
        _state: &mut Self::State,
        scope_key: &str,
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
            scope_key.to_string(),
        )))
    }
}

#[sinex_test]
async fn reconciler_live_processing_rejects_multiple_scope_keys() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(MultiScopeReconciler);
    let mut state = ReconcilerState;
    let ctx = make_context();

    let err = wrapper
        .process_derived(
            &mut state,
            event_stub(serde_json::json!({ "value": 42 })),
            &ctx,
        )
        .await
        .expect_err("multi-scope live processing should be rejected");

    assert!(
        matches!(
            err,
            NodeLogicError::Processing(ref message)
                if message.contains("supports at most one scope per trigger")
        ),
        "unexpected error: {err:?}"
    );

    Ok(())
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
async fn default_reconciler_recompute_starts_from_fresh_state() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(DefaultStatefulReconciler);
    let mut state = DefaultReconcilerState {
        total: 1_000,
        count: 99,
    };
    let ctx = make_context();

    let working_set: Vec<sinex_primitives::Event<JsonValue>> = vec![
        event_stub(serde_json::json!({ "value": 10 })),
        event_stub(serde_json::json!({ "value": 20 })),
    ];

    let results = wrapper
        .process_invalidation_derived(&mut state, "fresh-scope", working_set, &ctx)
        .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].payload["total"], 10);
    assert_eq!(results[0].payload["count"], 1);
    assert_eq!(results[1].payload["total"], 30);
    assert_eq!(results[1].payload["count"], 2);
    assert_eq!(state.total, 30);
    assert_eq!(state.count, 2);

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
            Timestamp::now(), // test: no real window to derive from
            vec![],
        )))
    }
}

#[sinex_test]
async fn windowed_recomputes_from_working_set() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, WindowedWrapper};

    let mut wrapper = WindowedWrapper(TestWindowed);
    let mut state = WindowState {
        values: vec![999, 1_000],
    };
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
    assert!(state.values.is_empty(), "rebuilt window state should replace stale data");

    Ok(())
}

// ── Serialization + Wire Format ───────────────────────────────────────

#[sinex_test]
async fn invalidation_signal_serialization_roundtrip() -> TestResult<()> {
    let op_id = Uuid::now_v7();
    let ids = vec![Uuid::now_v7(), Uuid::now_v7()];
    let inv = DerivedScopeInvalidation::archived(ids.clone(), "fs-watcher", "file.created")
        .with_operation(op_id)
        .with_scope_keys(vec!["scope-a".into()]);

    // Serialize to JSON (this is what goes over NATS)
    let json = serde_json::to_vec(&inv).expect("should serialize");

    // Deserialize back (this is what the adapter does on receive)
    let restored: DerivedScopeInvalidation =
        serde_json::from_slice(&json).expect("should deserialize");

    assert_eq!(restored.action, InvalidationAction::Archived);
    assert_eq!(restored.affected_event_ids, ids);
    assert_eq!(restored.event_source, "fs-watcher");
    assert_eq!(restored.event_type, "file.created");
    assert_eq!(restored.operation_id, Some(op_id));
    assert_eq!(restored.affected_scope_keys, vec!["scope-a"]);

    Ok(())
}

// ── Reconciler: multi-scope recomputation ─────────────────────────────

#[sinex_test]
async fn reconciler_recomputes_independent_scopes_correctly() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(TestReconciler);
    let mut state = ReconcilerState;
    let ctx = make_context();

    // Scope A: events summing to 100
    let ws_a: Vec<sinex_primitives::Event<JsonValue>> = (1..=4)
        .map(|i| event_stub(serde_json::json!({ "value": i * 25 })))
        .collect();

    let results_a = wrapper
        .process_invalidation_derived(&mut state, "scope-a", ws_a, &ctx)
        .await?;

    assert_eq!(results_a.len(), 1);
    assert_eq!(results_a[0].payload["total"], 250);
    assert_eq!(results_a[0].payload["count"], 4);

    // Scope B: different working set
    let ws_b: Vec<sinex_primitives::Event<JsonValue>> =
        vec![event_stub(serde_json::json!({ "value": 7 }))];

    let results_b = wrapper
        .process_invalidation_derived(&mut state, "scope-b", ws_b, &ctx)
        .await?;

    assert_eq!(results_b.len(), 1);
    assert_eq!(results_b[0].payload["total"], 7);
    assert_eq!(results_b[0].payload["count"], 1);

    Ok(())
}

#[sinex_test]
async fn reconciler_output_carries_scope_key() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, ScopeReconcilerWrapper};

    let mut wrapper = ScopeReconcilerWrapper(TestReconciler);
    let mut state = ReconcilerState;
    let ctx = make_context();

    let ws: Vec<sinex_primitives::Event<JsonValue>> =
        vec![event_stub(serde_json::json!({ "value": 42 }))];

    let results = wrapper
        .process_invalidation_derived(&mut state, "my-scope", ws, &ctx)
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].scope_key.as_deref(),
        Some("my-scope"),
        "scope key should match the scope passed to process_invalidation_derived"
    );

    Ok(())
}

#[sinex_test]
async fn windowed_output_has_latest_input_temporal_policy() -> TestResult<()> {
    use sinex_node_sdk::derived_node::traits::{DerivedNodeImpl, WindowedWrapper};
    use sinex_primitives::domain::SyntheticTemporalPolicy;

    let mut wrapper = WindowedWrapper(TestWindowed);
    let mut state = WindowState::default();
    let ctx = make_context();

    let ws: Vec<sinex_primitives::Event<JsonValue>> = vec![
        event_stub(serde_json::json!({ "value": 1 })),
        event_stub(serde_json::json!({ "value": 2 })),
    ];

    let results = wrapper
        .process_invalidation_derived(&mut state, "any", ws, &ctx)
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].temporal_policy,
        SyntheticTemporalPolicy::LatestInput,
        "windowed invalidation output should use LatestInput policy"
    );

    Ok(())
}

async fn initialize_scope_reconciler_adapter(
    ctx: &TestContext,
) -> TestResult<ScopeReconcilerNodeAdapter<TestReconciler>> {
    let nats = ctx.nats_handle()?;
    let nats_client = nats.connect().await?;
    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(nats_client.clone())));

    let kv_store = async_nats::jetstream::new(nats_client)
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: format!("KV_derived_invalid_{}", Uuid::now_v7().simple()),
            ..Default::default()
        })
        .await?;

    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv_store,
        "test-reconciler".to_string(),
        "default".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));

    let (event_sender, _event_receiver) = mpsc::channel::<sinex_primitives::Event<JsonValue>>(1024);
    let emitter = EventEmitter::new(event_sender, false);
    let handles = NodeHandles::new(
        ctx.pool().clone(),
        checkpoint_manager,
        emitter,
        transport,
        None,
        None,
    );

    let work_dir = std::env::temp_dir().join(format!(
        "sinex-derived-invalidation-{}",
        Uuid::now_v7().simple()
    ));
    std::fs::create_dir_all(&work_dir)?;

    let init_context = NodeInitContext::new(
        sinex_node_sdk::DerivedNodeConfig::default(),
        HashMap::new(),
        ServiceInfo::new(
            "test-reconciler".to_string(),
            "test-host".to_string(),
            work_dir.clone(),
            false,
        ),
        handles,
        camino::Utf8PathBuf::from_path_buf(work_dir).map_err(|path| {
            color_eyre::eyre::eyre!(
                "temporary derived invalidation path should be utf-8: {}",
                path.display()
            )
        })?,
    );

    let mut adapter = ScopeReconcilerNodeAdapter::new(ScopeReconcilerWrapper(TestReconciler));
    adapter.initialize(init_context).await?;

    Ok(adapter)
}

#[sinex_test]
async fn scope_invalidation_paginates_working_set_and_stale_outputs(ctx: TestContext) -> TestResult<()> {
    use sinex_db::DbPoolExt;
    use sinex_primitives::query::{AggregationMode, EventQuery, EventQueryResult};

    let ctx = ctx.with_nats().shared().await?;
    let mut adapter = initialize_scope_reconciler_adapter(&ctx).await?;
    let material_id = ctx
        .create_source_material(Some("derived-invalidation-pagination"))
        .await?;

    let scope_key = "scope:derived-pagination";
    let expected_count = (Pagination::MAX_LIMIT + 1) as usize;
    let expected_total = (expected_count as i64 - 1) * expected_count as i64 / 2;

    let mut input_events = Vec::with_capacity(expected_count);
    for value in 0..expected_count {
        let mut event = DynamicPayload::new(
            "measurements",
            "measurement.taken",
            serde_json::json!({ "value": value as i64 }),
        )
        .from_material(material_id)
        .build()?;
        event.scope_key = Some(scope_key.to_string());
        input_events.push(event);
    }
    let input_events = ctx.pool().events().insert_batch(input_events).await?;

    let mut stale_outputs = Vec::with_capacity(expected_count);
    for (index, input_event) in input_events.iter().enumerate() {
        let input_id = input_event.id.expect("inserted input should have id");
        let mut derived = DynamicPayload::new(
            "test-reconciler",
            "measurement.aggregate",
            serde_json::json!({ "stale_index": index }),
        )
        .from_parents(vec![input_id])?
        .build()?;
        derived.scope_key = Some(scope_key.to_string());
        stale_outputs.push(derived);
    }
    ctx.pool().events().insert_batch(stale_outputs).await?;

    let first_input_id = input_events
        .first()
        .and_then(|event| event.id)
        .expect("paged input fixture should contain an id");
    let invalidation =
        DerivedScopeInvalidation::replaced(vec![*first_input_id.as_uuid()], "measurements", "measurement.taken")
            .with_scope_keys(vec![scope_key.to_string()]);

    let outputs = adapter.process_invalidation(&invalidation).await?;
    assert_eq!(outputs.len(), 1, "large scope should still recompute one aggregate");
    assert_eq!(outputs[0].payload["count"], serde_json::json!(expected_count));
    assert_eq!(outputs[0].payload["total"], serde_json::json!(expected_total));

    let live_output_count = match ctx
        .pool()
        .events()
        .query(EventQuery {
            sources: vec![sinex_primitives::EventSource::new("test-reconciler")?],
            event_types: vec![sinex_primitives::EventType::new("measurement.aggregate")?],
            scope_key: Some(scope_key.to_string()),
            aggregation: Some(AggregationMode::Count),
            ..EventQuery::default()
        })
        .await?
    {
        EventQueryResult::Count { count } => count,
        other => panic!("expected count result, got {other:?}"),
    };
    assert_eq!(live_output_count, 0, "all stale outputs should be archived");

    let archived_output_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)::bigint as "count!"
        FROM audit.archived_events
        WHERE source = $1 AND event_type = $2 AND scope_key = $3
        "#,
        "test-reconciler",
        "measurement.aggregate",
        scope_key
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(
        archived_output_count,
        expected_count as i64,
        "invalidations must archive every stale scope output, not just the first page"
    );

    Ok(())
}
