    // Inline because these cover a private shutdown-signaling helper.
    #[cfg(feature = "messaging")]
    use super::log_self_observation_failure;
    use super::request_runtime_drain;
    use super::{DerivedNodeAdapter, stale_output_ids_or_fail_scope};
    use crate::derived_node::{
        DerivedNodeConfig, DerivedOutput, DerivedTriggerContext, InputProvenanceFilter,
        ScopeReconcilerWrapper, TransducerWrapper,
    };
    use crate::exploration::ExplorationProvider;
    #[cfg(feature = "messaging")]
    use crate::health_reporter::{HealthReporter, HealthThresholds};
    use crate::runtime::stream::{
        Checkpoint, EventEmitter, Node, NodeHandles, NodeRuntimeState, RuntimeDrainController,
        ScanArgs, ServiceInfo,
    };
    #[cfg(feature = "messaging")]
    use crate::self_observation::{SelfObservationError, SelfObserver, SelfObserverConfig};
    use crate::shutdown::ShutdownConfig;
    use crate::{CheckpointManager, CheckpointState, EventTransport, NatsPublisher, SinexError};
    use crate::{ErrorAction, NodeLogicError, ScopeReconcilerNode, TransducerNode};
    use camino::Utf8PathBuf;
    use futures::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sinex_db::DbPoolExt;
    use sinex_primitives::domain::{EventSource, EventType, ProcessingMode, TriggerKind};
    use sinex_primitives::events::{DynamicPayload, Event};
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::temporal::Timestamp;
    use sinex_primitives::{HostName, Id, JsonValue, Uuid};
    use std::collections::HashMap;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[cfg(feature = "messaging")]
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use xtask::sandbox::prelude::*;

    #[derive(Debug, Default, Serialize, Deserialize)]
    struct TestDerivedState;

    #[derive(Debug, Default, Serialize, Deserialize)]
    struct WildcardMaterialOnlyState {
        processed: usize,
    }

    struct TestDerivedNode;

    impl TransducerNode for TestDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(None)
        }
    }

    struct WildcardMaterialOnlyNode;

    impl TransducerNode for WildcardMaterialOnlyNode {
        type State = WildcardMaterialOnlyState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "wildcard-material-only"
        }

        fn input_event_type(&self) -> &'static str {
            "*"
        }

        fn input_provenance_filter(&self) -> InputProvenanceFilter {
            InputProvenanceFilter::MaterialOnly
        }

        fn output_event_type(&self) -> &'static str {
            "ignored.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            state.processed += 1;
            Ok(None)
        }
    }

    struct RetryDerivedNode {
        seen: Arc<AtomicUsize>,
    }

    impl TransducerNode for RetryDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-retry-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            Err(NodeLogicError::Processing("retry requested".to_string()))
        }

        fn handle_error(&self, _error: &NodeLogicError) -> crate::ErrorAction {
            crate::ErrorAction::Retry
        }
    }

    struct EmittingDerivedNode;

    impl TransducerNode for EmittingDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-emitting-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(Some(DerivedOutput::transduced(
                json!({"ok": true}),
                context.ts_orig.unwrap_or_else(Timestamp::now),
                context.trigger_uuid(),
            )))
        }
    }

    #[derive(Default, Deserialize)]
    struct UnserializableDerivedState;

    impl Serialize for UnserializableDerivedState {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("state serialization exploded"))
        }
    }

    struct UnserializableDerivedNode;

    impl TransducerNode for UnserializableDerivedNode {
        type State = UnserializableDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "adapter-regression-unserializable-checkpoint"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(None)
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    struct TestScopeReconcilerState;

    #[derive(Deserialize)]
    struct ScopeReconcilerInput {
        value: i64,
    }

    #[derive(Serialize)]
    struct ScopeReconcilerOutput {
        total: i64,
        count: usize,
    }

    struct TestScopeReconcilerNode;

    impl ScopeReconcilerNode for TestScopeReconcilerNode {
        type State = TestScopeReconcilerState;
        type Input = ScopeReconcilerInput;
        type Output = ScopeReconcilerOutput;

        fn name(&self) -> &'static str {
            "adapter-regression-scope-reconciler"
        }

        fn input_event_type(&self) -> &'static str {
            "measurement.taken"
        }

        fn output_event_type(&self) -> &'static str {
            "measurement.aggregate"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        fn scope_keys(
            &self,
            _input: &Self::Input,
            _context: &DerivedTriggerContext,
        ) -> Vec<String> {
            vec!["default".into()]
        }

        async fn reconcile(
            &mut self,
            _state: &mut Self::State,
            scope_key: &str,
            input: Self::Input,
            context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(vec![DerivedOutput::reconciled(
                ScopeReconcilerOutput {
                    total: input.value,
                    count: 1,
                },
                context.ts_orig.unwrap_or_else(Timestamp::now),
                vec![*context.trigger_event_id.as_uuid()],
                scope_key.to_string(),
            )])
        }

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

            let total = working_set.iter().map(|input| input.value).sum();
            let count = working_set.len();

            Ok(vec![DerivedOutput::reconciled(
                ScopeReconcilerOutput { total, count },
                context.ts_orig.unwrap_or_else(Timestamp::now),
                vec![*context.trigger_event_id.as_uuid()],
                scope_key.to_string(),
            )])
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    struct StatefulInvalidationState {
        invalidations_applied: u64,
    }

    struct StatefulInvalidationNode;

    impl ScopeReconcilerNode for StatefulInvalidationNode {
        type State = StatefulInvalidationState;
        type Input = ScopeReconcilerInput;
        type Output = ScopeReconcilerOutput;

        fn name(&self) -> &'static str {
            "adapter-regression-stateful-invalidation"
        }

        fn input_event_type(&self) -> &'static str {
            "measurement.taken"
        }

        fn output_event_type(&self) -> &'static str {
            "measurement.aggregate"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        fn scope_keys(
            &self,
            _input: &Self::Input,
            _context: &DerivedTriggerContext,
        ) -> Vec<String> {
            vec!["default".into()]
        }

        async fn reconcile(
            &mut self,
            _state: &mut Self::State,
            _scope_key: &str,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(Vec::new())
        }

        async fn recompute_scope(
            &mut self,
            state: &mut Self::State,
            _scope_key: &str,
            _working_set: Vec<Self::Input>,
            _context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            state.invalidations_applied += 1;
            Ok(Vec::new())
        }
    }

    struct DlqRetryDerivedNode;

    impl TransducerNode for DlqRetryDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-dlq-retry-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Err(NodeLogicError::Processing("route me to dlq".to_string()))
        }

        fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
            ErrorAction::SendToProcessingFailureQueue
        }
    }

    fn make_input_event(value: &str) -> std::result::Result<Event<JsonValue>, SinexError> {
        let mut event = DynamicPayload::new("test.source", "test.input", json!({ "value": value }))
            .from_parents([Id::<Event<JsonValue>>::new()])?
            .build()?;
        event.id = Some(event.id.unwrap_or_else(Id::new));
        Ok(event)
    }

    fn make_material_input_event(
        event_type: &str,
        value: &str,
    ) -> std::result::Result<Event<JsonValue>, SinexError> {
        let mut event = DynamicPayload::new("test.source", event_type, json!({ "value": value }))
            .from_material(Uuid::now_v7())
            .build()?;
        event.id = Some(event.id.unwrap_or_else(Id::new));
        Ok(event)
    }

    async fn make_runtime_state(
        ctx: &TestContext,
        node_name: &str,
        node_run_id: Option<Uuid>,
    ) -> TestResult<NodeRuntimeState> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            node_name.to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, _event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = NodeHandles::new_edge(
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
        })?;
        Ok(NodeRuntimeState::new(
            ServiceInfo::new(
                node_name.to_string(),
                node_name.to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                node_run_id,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ))
    }

    async fn make_runtime_state_with_db(
        ctx: &TestContext,
        node_name: &str,
        node_run_id: Option<Uuid>,
    ) -> TestResult<(NodeRuntimeState, mpsc::Receiver<Event<JsonValue>>)> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            node_name.to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = NodeHandles::new(
            ctx.pool().clone(),
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
        })?;
        Ok((
            NodeRuntimeState::new(
                ServiceInfo::new(
                    node_name.to_string(),
                    node_name.to_string(),
                    HostName::from_static("test-host"),
                    work_dir_path,
                    false,
                    format!("instance-{}", Uuid::now_v7().simple()),
                    env!("CARGO_PKG_VERSION").to_string(),
                    node_run_id,
                ),
                handles,
                HashMap::new(),
                work_dir_utf8,
            ),
            event_receiver,
        ))
    }

    #[cfg(feature = "messaging")]
    async fn make_runtime_state_with_validator(
        ctx: &TestContext,
        node_name: &str,
        node_run_id: Option<Uuid>,
    ) -> TestResult<(NodeRuntimeState, mpsc::Receiver<Event<JsonValue>>, Uuid)> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            node_name.to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
        let validator = Arc::new(crate::schema_validator::NodeSchemaValidator::new());
        let schema_id = Uuid::now_v7();
        validator.register_test_schema(
            schema_id,
            node_name,
            "test.output",
            &json!({
                "type": "object",
                "properties": {
                    "ok": { "type": "boolean" }
                },
                "required": ["ok"]
            }),
        )?;
        let emitter = EventEmitter::with_validator(event_sender, false, validator);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = NodeHandles::new_edge(
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
        })?;
        Ok((
            NodeRuntimeState::new(
                ServiceInfo::new(
                    node_name.to_string(),
                    node_name.to_string(),
                    HostName::from_static("test-host"),
                    work_dir_path,
                    false,
                    format!("instance-{}", Uuid::now_v7().simple()),
                    env!("CARGO_PKG_VERSION").to_string(),
                    node_run_id,
                ),
                handles,
                HashMap::new(),
                work_dir_utf8,
            ),
            event_receiver,
            schema_id,
        ))
    }

    #[sinex_test]
    async fn request_runtime_drain_delivers_to_receiver() -> TestResult<()> {
        let drain = RuntimeDrainController::new();
        let mut rx = drain.subscribe();

        assert!(request_runtime_drain(&drain, "test-derived"));
        rx.changed().await?;
        assert!(*rx.borrow());
        Ok(())
    }

    #[sinex_test]
    async fn request_runtime_drain_is_idempotent() -> TestResult<()> {
        let drain = RuntimeDrainController::new();

        assert!(request_runtime_drain(&drain, "test-derived"));
        assert!(request_runtime_drain(&drain, "test-derived"));
        assert!(drain.is_requested());
        Ok(())
    }

    #[sinex_test]
    async fn stale_output_ids_or_fail_scope_returns_empty_ids_on_success() -> TestResult<()> {
        let stale_ids = stale_output_ids_or_fail_scope("test-derived", "scope-a", Ok(Vec::new()))
            .expect("successful stale query should return ids");
        assert!(stale_ids.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn stale_output_ids_or_fail_scope_surfaces_query_error() -> TestResult<()> {
        let error = stale_output_ids_or_fail_scope(
            "test-derived",
            "scope-a",
            Err(SinexError::invalid_state("corrupt stale output row")),
        )
        .expect_err("stale output query errors must fail the invalidation scope");

        let rendered = error.to_string();
        assert!(rendered.contains("Failed to query stale outputs"));
        assert!(rendered.contains("test-derived"));
        assert!(rendered.contains("scope-a"));
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn log_self_observation_failure_accepts_publish_errors() -> TestResult<()> {
        log_self_observation_failure(
            "test-derived",
            "invalidation.errors",
            &SelfObservationError::Publish("boom".to_string()),
        );
        Ok(())
    }

    #[sinex_test]
    async fn derived_source_state_is_unhealthy_before_runtime_initialization() -> TestResult<()> {
        let adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));

        let state = ExplorationProvider::get_source_state(&adapter)?;

        assert!(!state.is_connected);
        assert!(!state.healthy);
        assert_eq!(state.last_updated, None);
        assert!(state.description.contains("runtime not initialized"));
        assert_eq!(
            state
                .metadata
                .get("runtime_initialized")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn derived_source_state_reflects_failed_health_reporter(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        adapter.runtime = Some(make_runtime_state(&ctx, "test-derived", None).await?);

        let observer = Arc::new(SelfObserver::new(
            ctx.nats_client(),
            SelfObserverConfig {
                component: "derived-source-state".to_string(),
                namespace: None,
                enabled: true,
                min_emission_interval: Duration::from_millis(10),
            },
        ));
        let reporter = Arc::new(HealthReporter::new(
            "derived-source-state".to_string(),
            observer,
            HealthThresholds {
                error_rate_degraded: 0.05,
                error_rate_failed: 0.20,
                window_seconds: 60,
            },
        ));
        reporter.record_error(&SinexError::processing("derived node failure"));
        adapter.health_reporter = Some(reporter);

        let state = ExplorationProvider::get_source_state(&adapter)?;

        assert!(state.is_connected);
        assert!(!state.healthy);
        assert!(state.description.contains("status=failed"));
        assert_eq!(
            state
                .metadata
                .get("health_status")
                .and_then(serde_json::Value::as_str),
            Some("failed")
        );
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn derived_health_check_reflects_failed_health_reporter(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        adapter.runtime = Some(make_runtime_state(&ctx, "test-derived", None).await?);

        let observer = Arc::new(SelfObserver::new(
            ctx.nats_client(),
            SelfObserverConfig {
                component: "derived-health-check".to_string(),
                namespace: None,
                enabled: true,
                min_emission_interval: Duration::from_millis(10),
            },
        ));
        let reporter = Arc::new(HealthReporter::new(
            "derived-health-check".to_string(),
            observer,
            HealthThresholds {
                error_rate_degraded: 0.05,
                error_rate_failed: 0.20,
                window_seconds: 60,
            },
        ));
        reporter.record_error(&SinexError::processing("derived node failure"));
        adapter.health_reporter = Some(reporter);

        assert!(
            !crate::runtime::stream::Node::health_check(&adapter).await?,
            "health_check should fail once the reporter marks the node failed"
        );
        Ok(())
    }

    #[sinex_test]
    async fn try_restore_from_file_rejects_missing_state_payload() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("derived-empty-state.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::None,
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );

        let error = adapter
            .try_restore_from_file()
            .await
            .expect_err("empty hot reload state must not be treated as absent");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("derived-adapter-test"));
        assert!(message.contains(&checkpoint_path.display().to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn load_state_accepts_fresh_kv_checkpoint_without_state_payload(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv,
            "derived-adapter-test".to_string(),
            "test-group".to_string(),
            "fresh-consumer".to_string(),
        );

        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        adapter.checkpoint_manager = Some(Arc::new(manager));
        adapter
            .load_state()
            .await
            .expect("fresh derived checkpoint state should be treated as a clean start");

        assert_eq!(adapter.persisted_state.events_processed, 0);
        assert_eq!(adapter.last_revision, 0);
        Ok(())
    }

    #[sinex_test]
    async fn load_state_rejects_kv_checkpoint_without_state_payload(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv.clone(),
            "derived-adapter-test".to_string(),
            "test-group".to_string(),
            "test-consumer".to_string(),
        );
        manager.save_checkpoint(&CheckpointState::default()).await?;

        let mut keys = kv.keys().await?;
        let key = keys.try_next().await?.expect("checkpoint key should exist");
        let corrupt = serde_json::to_vec(&CheckpointState {
            checkpoint: Checkpoint::stream("restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        })?;
        kv.put(&key, corrupt.into()).await?;

        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        adapter.checkpoint_manager = Some(Arc::new(manager));

        let error = adapter
            .load_state()
            .await
            .expect_err("empty derived checkpoint KV state must not be treated as fresh");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("derived-adapter-test"));
        Ok(())
    }

    #[sinex_test]
    async fn process_batch_halts_on_retry_error() -> TestResult<()> {
        let seen = Arc::new(AtomicUsize::new(0));
        let node = RetryDerivedNode {
            seen: Arc::clone(&seen),
        };
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(node));

        let error = adapter
            .process_batch(vec![
                make_input_event("first")?,
                make_input_event("second")?,
            ])
            .await
            .expect_err("retry errors must stop the batch");

        assert!(
            error.to_string().contains("retry"),
            "retryable batch failure should propagate an explicit error: {error:#}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "batch processing must stop at the first retryable error"
        );
        Ok(())
    }

    #[sinex_test]
    async fn process_batch_halts_after_three_consecutive_checkpoint_save_failures(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let mut adapter = DerivedNodeAdapter::with_config(
            TransducerWrapper(UnserializableDerivedNode),
            DerivedNodeConfig {
                checkpoint_interval: 1,
                ..DerivedNodeConfig::default()
            },
        );
        adapter.runtime = Some(
            make_runtime_state(
                &ctx,
                "adapter-regression-unserializable-checkpoint",
                Some(Uuid::now_v7()),
            )
            .await?,
        );
        adapter.checkpoint_manager = Some(
            adapter
                .runtime
                .as_ref()
                .expect("runtime set")
                .checkpoint_manager(),
        );

        let first = adapter
            .process_batch(vec![make_input_event("checkpoint-1")?])
            .await
            .expect("first checkpoint serialization failure should not halt the batch");
        assert!(
            first.is_empty(),
            "unserializable checkpoint node should not emit output events"
        );

        assert!(
            adapter
                .process_batch(vec![make_input_event("checkpoint-2")?])
                .await
                .expect("second checkpoint serialization failure should not halt the batch")
                .is_empty(),
            "second failed checkpoint should still let batch processing complete"
        );

        let error = adapter
            .process_batch(vec![make_input_event("checkpoint-3")?])
            .await
            .expect_err("third consecutive checkpoint serialization failure must halt the batch");

        assert!(
            error
                .to_string()
                .contains("Checkpoint save failed 3 consecutive times"),
            "batch halt should report the consecutive failure threshold: {error:#}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn derived_outputs_propagate_runtime_node_run_id(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let node_run_id = Uuid::now_v7();
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));
        adapter.runtime = Some(
            make_runtime_state(&ctx, "derived-adapter-emitting-test", Some(node_run_id)).await?,
        );

        let outputs = adapter.process_one(make_input_event("emit")?).await?;
        let output = outputs
            .into_iter()
            .next()
            .expect("emitting node should produce one output event");

        assert_eq!(output.node_run_id, Some(node_run_id));
        Ok(())
    }

    #[sinex_test]
    async fn derived_outputs_use_deterministic_event_ids() -> TestResult<()> {
        let input = make_input_event("deterministic-output")?;
        let mut first_adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));
        let mut second_adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));

        let first_output = first_adapter
            .process_one(input.clone())
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| color_eyre::eyre::eyre!("emitting node should produce an output"))?;
        let second_output = second_adapter
            .process_one(input)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| color_eyre::eyre::eyre!("emitting node should produce an output"))?;

        assert_eq!(first_output.id, second_output.id);
        let id = first_output.id.ok_or_else(|| {
            color_eyre::eyre::eyre!("derived output should carry deterministic id")
        })?;
        assert_eq!(id.as_uuid().get_version_num(), 7);
        assert_eq!(id.as_uuid().get_variant(), uuid::Variant::RFC4122);
        Ok(())
    }

    #[sinex_test]
    async fn process_one_tracks_run_local_processed_count() -> TestResult<()> {
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));

        adapter.process_one(make_input_event("emit")?).await?;
        adapter.process_one(make_input_event("emit")?).await?;

        assert_eq!(adapter.run_events_processed, 2);
        assert_eq!(adapter.persisted_state.events_processed, 2);
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn emitted_derived_outputs_stamp_payload_schema_id_from_runtime_validator(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (runtime, mut event_receiver, schema_id) = make_runtime_state_with_validator(
            &ctx,
            "derived-adapter-emitting-test",
            Some(Uuid::now_v7()),
        )
        .await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));
        adapter.event_emitter = Some(runtime.event_emitter().clone());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let outputs = adapter.process_one(make_input_event("emit")?).await?;
        let emitted = adapter
            .emit_output_events(outputs, "test-emission")
            .await
            .expect("derived output emission should succeed");
        assert_eq!(emitted, 1);

        let event = event_receiver
            .recv()
            .await
            .expect("emitted event should reach the runtime sender");
        assert_eq!(event.payload_schema_id, Some(schema_id));
        Ok(())
    }

    #[sinex_test]
    async fn scope_invalidation_outputs_apply_privacy_filtering() -> TestResult<()> {
        struct PrivacyInvalidationNode;

        impl TransducerNode for PrivacyInvalidationNode {
            type State = TestDerivedState;
            type Input = JsonValue;
            type Output = JsonValue;

            fn name(&self) -> &'static str {
                "derived-adapter-invalidation-privacy-test"
            }

            fn input_event_type(&self) -> &'static str {
                "test.input"
            }

            fn output_event_type(&self) -> &'static str {
                "test.output"
            }

            fn output_privacy_context(&self) -> ProcessingContext {
                ProcessingContext::Command
            }

            async fn process(
                &mut self,
                _state: &mut Self::State,
                _input: Self::Input,
                _context: &DerivedTriggerContext,
            ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError>
            {
                Ok(None)
            }
        }

        let adapter = DerivedNodeAdapter::new(TransducerWrapper(PrivacyInvalidationNode));
        let output = DerivedOutput::reconciled(
            json!({ "value": "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij" }),
            Timestamp::now(),
            vec![Uuid::now_v7()],
            "scope-a".to_string(),
        );
        let context = DerivedTriggerContext {
            trigger_event_id: Id::new(),
            source: EventSource::new("test.source")?,
            event_type: EventType::new("test.invalidation")?,
            ts_orig: None,
            ts_coided: Timestamp::now(),
            processing_mode: ProcessingMode::Replay,
            trigger_kind: TriggerKind::ScopeInvalidation,
            created_by_operation_id: None,
        };

        let event = adapter.build_output_event(output, 0, None, &context)?;

        assert_eq!(event.payload["value"].as_str(), Some("<GITHUB_TOKEN>"));
        Ok(())
    }

    #[sinex_test]
    async fn current_checkpoint_tracks_last_processed_input_event() -> TestResult<()> {
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        let input = make_input_event("checkpoint-me")?;
        let input_id = input.id.expect("test input must have an id");

        let _ = adapter.process_one(input).await?;

        assert_eq!(
            adapter.current_checkpoint_internal(),
            Checkpoint::internal(*input_id.as_uuid(), 1)
        );
        Ok(())
    }

    #[sinex_test]
    async fn load_state_restores_resume_position_from_checkpoint_metadata() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir
            .path()
            .join("derived-legacy-resume-position.checkpoint.json");
        let resume_event_id = Uuid::now_v7();
        let legacy_state = serde_json::json!({
            "state": null,
            "events_processed": 7,
            "last_checkpoint": Timestamp::now(),
            "version": 1
        });
        CheckpointState {
            checkpoint: Checkpoint::internal(resume_event_id, 7),
            processed_count: 7,
            last_activity: Timestamp::now(),
            data: Some(legacy_state),
            version: 2,
            revision: 0,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );

        adapter.load_state().await?;

        assert_eq!(
            adapter.current_checkpoint_internal(),
            Checkpoint::internal(resume_event_id, 7)
        );
        Ok(())
    }

    #[sinex_test]
    async fn load_state_restores_hot_reload_revision_for_followup_save(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = Arc::new(CheckpointManager::new(
            kv,
            "derived-adapter-hot-reload-revision-test".to_string(),
            "test-group".to_string(),
            "hot-reload-consumer".to_string(),
        ));

        let persisted_json = serde_json::json!({
            "state": null,
            "events_processed": 3,
            "last_checkpoint": Timestamp::now(),
            "version": 1,
            "last_input_event_id": Uuid::now_v7(),
        });
        let baseline_revision = manager
            .save_checkpoint(&CheckpointState {
                checkpoint: Checkpoint::internal(Uuid::now_v7(), 3),
                processed_count: 3,
                last_activity: Timestamp::now(),
                data: Some(persisted_json.clone()),
                version: 2,
                revision: 0,
            })
            .await?;

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir
            .path()
            .join("derived-hot-reload-revision.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::internal(Uuid::now_v7(), 3),
            processed_count: 3,
            last_activity: Timestamp::now(),
            data: Some(persisted_json),
            version: 2,
            revision: baseline_revision,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter.load_state().await?;
        assert_eq!(adapter.last_revision, baseline_revision);
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_some(),
            "restored hot reload file must remain until the state is durably re-saved"
        );

        adapter.save_state().await?;
        assert!(
            adapter.last_revision > baseline_revision,
            "restored hot reload state must keep the prior KV revision so the next save updates instead of blind-creating"
        );
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "restored hot reload file should be cleaned up after successful KV sync"
        );
        Ok(())
    }

    #[sinex_test]
    async fn load_state_falls_back_to_kv_when_hot_reload_file_is_corrupt(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = Arc::new(CheckpointManager::new(
            kv,
            "derived-adapter-hot-reload-fallback-test".to_string(),
            "test-group".to_string(),
            "hot-reload-fallback-consumer".to_string(),
        ));

        let persisted_json = serde_json::json!({
            "state": null,
            "events_processed": 9,
            "last_checkpoint": Timestamp::now(),
            "version": 1,
            "last_input_event_id": Uuid::now_v7(),
        });
        let revision = manager
            .save_checkpoint(&CheckpointState {
                checkpoint: Checkpoint::internal(Uuid::now_v7(), 9),
                processed_count: 9,
                last_activity: Timestamp::now(),
                data: Some(persisted_json),
                version: 2,
                revision: 0,
            })
            .await?;

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir
            .path()
            .join("derived-hot-reload-fallback.checkpoint.json");
        tokio::fs::write(&checkpoint_path, "{ definitely not valid json").await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter
            .load_state()
            .await
            .expect("corrupt hot reload file should fall back to healthy KV state");

        assert_eq!(adapter.last_revision, revision);
        assert_eq!(adapter.persisted_state.events_processed, 9);
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "corrupt hot reload file should be discarded after successful KV restore"
        );
        Ok(())
    }

    #[sinex_test]
    async fn historical_replay_resumes_from_internal_checkpoint(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let inserted = ctx
            .pool()
            .events()
            .insert_batch(vec![
                make_input_event("first")?,
                make_input_event("second")?,
                make_input_event("third")?,
            ])
            .await?;
        let second_id = inserted[1].id.expect("inserted event must have an id");
        let third_id = inserted[2].id.expect("inserted event must have an id");

        let (runtime, _event_receiver) =
            make_runtime_state_with_db(&ctx, "derived-history-resume-test", None).await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_emitter = Some(runtime.event_emitter().clone());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let report = adapter
            .run_historical(
                Checkpoint::internal(*second_id.as_uuid(), 2),
                Timestamp::now(),
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.events_processed, 1);
        assert_eq!(
            report.final_checkpoint,
            Checkpoint::internal(*third_id.as_uuid(), 1)
        );
        Ok(())
    }

    #[sinex_test]
    async fn process_event_batch_filters_wildcard_material_only_inputs() -> TestResult<()> {
        let material_event = make_material_input_event("file.created", "material")?;
        let synthesized_event = make_input_event("synthesized")?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(WildcardMaterialOnlyNode));

        let stats = adapter
            .process_event_batch(vec![material_event, synthesized_event])
            .await?;

        assert_eq!(stats.processed, 1);
        assert_eq!(adapter.persisted_state.state.processed, 1);
        assert_eq!(adapter.persisted_state.events_processed, 1);
        Ok(())
    }

    #[sinex_test]
    async fn historical_replay_filters_wildcard_material_only_inputs(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let material_id = ctx
            .create_source_material(Some("wildcard-material-only-history"))
            .await?;

        let mut material_event = DynamicPayload::new(
            "test.source",
            "file.created",
            json!({ "value": "material" }),
        )
        .from_material(material_id)
        .build()?;
        material_event.id = Some(material_event.id.unwrap_or_else(Id::new));

        let synthesized_event = make_input_event("synthesized-history")?;

        ctx.pool()
            .events()
            .insert_batch(vec![material_event, synthesized_event])
            .await?;

        let (runtime, _event_receiver) =
            make_runtime_state_with_db(&ctx, "wildcard-material-only", None).await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(WildcardMaterialOnlyNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_emitter = Some(runtime.event_emitter().clone());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let report = adapter
            .run_historical(Checkpoint::None, Timestamp::now(), ScanArgs::default())
            .await?;

        assert_eq!(report.events_processed, 1);
        assert_eq!(adapter.persisted_state.state.processed, 1);
        assert_eq!(adapter.persisted_state.events_processed, 1);
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn handle_invalidation_message_returns_none_when_output_emit_fails(
        ctx: TestContext,
    ) -> TestResult<()> {
        use super::super::DerivedScopeInvalidation;
        use sinex_db::DbPoolExt;
        use sinex_primitives::events::DynamicPayload;
        use sinex_primitives::query::{AggregationMode, EventQuery, EventQueryResult};
        use sinex_primitives::{EventSource, EventType};

        let ctx = ctx.with_nats().dedicated().await?;
        let material_id = ctx
            .create_source_material(Some("derived-invalidation-output-send-failure"))
            .await?;
        let scope_key = "scope:output-send-failure";

        let mut input = DynamicPayload::new(
            "measurements",
            "measurement.taken",
            serde_json::json!({ "value": 5_i64 }),
        )
        .from_material(material_id)
        .build()?;
        input.scope_key = Some(scope_key.to_string());

        let inserted = ctx.pool().events().insert_batch(vec![input]).await?;
        let input_id = inserted
            .first()
            .and_then(|event| event.id)
            .expect("inserted input should have id");
        let mut stale_output = DynamicPayload::new(
            "adapter-regression-scope-reconciler",
            "measurement.aggregate",
            serde_json::json!({ "total": 5_i64, "count": 1_u64 }),
        )
        .from_parents(vec![input_id])?
        .build()?;
        stale_output.scope_key = Some(scope_key.to_string());
        ctx.pool().events().insert_batch(vec![stale_output]).await?;

        let (runtime, event_receiver) =
            make_runtime_state_with_db(&ctx, "adapter-regression-scope-reconciler", None).await?;
        drop(event_receiver);

        let mut adapter = DerivedNodeAdapter::new(ScopeReconcilerWrapper(TestScopeReconcilerNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_emitter = Some(runtime.event_emitter().clone());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let invalidation = DerivedScopeInvalidation::replaced(
            vec![*input_id.as_uuid()],
            EventSource::from_static("measurements"),
            EventType::from_static("measurement.taken"),
        )
        .with_scope_keys(vec![scope_key.to_string()]);
        let payload = serde_json::to_vec(&invalidation)?;

        let result = adapter.handle_invalidation_message(&payload).await;
        assert!(
            matches!(result, Ok(None)),
            "output send failures must skip the invalidation (Ok(None)), got: {result:?}"
        );
        let live_output_count = match ctx
            .pool()
            .events()
            .query(EventQuery {
                sources: vec![EventSource::new("adapter-regression-scope-reconciler")?],
                event_types: vec![EventType::new("measurement.aggregate")?],
                scope_key: Some(scope_key.to_string()),
                aggregation: Some(AggregationMode::Count),
                ..EventQuery::default()
            })
            .await?
        {
            EventQueryResult::Count { count } => count,
            other => panic!("expected count result, got {other:?}"),
        };
        assert_eq!(
            live_output_count, 1,
            "stale outputs must remain live when replacement emission fails"
        );

        let archived_output_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint as "count!"
            FROM audit.archived_events
            WHERE source = $1 AND event_type = $2 AND scope_key = $3
            "#,
            "adapter-regression-scope-reconciler",
            "measurement.aggregate",
            scope_key
        )
        .fetch_one(ctx.pool())
        .await?;
        assert_eq!(
            archived_output_count, 0,
            "replacement emission failure must not archive stale outputs"
        );
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn handle_invalidation_message_checkpoints_state_only_mutations(
        ctx: TestContext,
    ) -> TestResult<()> {
        use super::super::DerivedScopeInvalidation;
        use sinex_db::DbPoolExt;
        use sinex_primitives::events::DynamicPayload;
        use sinex_primitives::{EventSource, EventType};

        let ctx = ctx.with_nats().dedicated().await?;
        let material_id = ctx
            .create_source_material(Some("derived-invalidation-state-only"))
            .await?;
        let scope_key = "scope:state-only";

        let mut input = DynamicPayload::new(
            "measurements",
            "measurement.taken",
            serde_json::json!({ "value": 7_i64 }),
        )
        .from_material(material_id)
        .build()?;
        input.scope_key = Some(scope_key.to_string());
        let input_id = ctx
            .pool()
            .events()
            .insert_batch(vec![input])
            .await?
            .into_iter()
            .next()
            .and_then(|event| event.id)
            .expect("inserted invalidation input should have an id");

        let (runtime, _event_receiver) =
            make_runtime_state_with_db(&ctx, "adapter-regression-stateful-invalidation", None)
                .await?;

        let mut adapter = DerivedNodeAdapter::with_config(
            ScopeReconcilerWrapper(StatefulInvalidationNode),
            DerivedNodeConfig {
                checkpoint_interval: 1,
                ..DerivedNodeConfig::default()
            },
        );
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_emitter = Some(runtime.event_emitter().clone());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let invalidation = DerivedScopeInvalidation::replaced(
            vec![*input_id.as_uuid()],
            EventSource::from_static("measurements"),
            EventType::from_static("measurement.taken"),
        )
        .with_scope_keys(vec![scope_key.to_string()]);
        let payload = serde_json::to_vec(&invalidation)?;

        let processed = adapter.handle_invalidation_message(&payload).await;
        assert_eq!(
            processed.expect("state-only invalidation must not halt the node"),
            Some(0),
            "state-only invalidation should still be treated as a successful recomputation"
        );
        assert_eq!(adapter.persisted_state.state.invalidations_applied, 1);
        assert!(
            adapter.last_revision > 0,
            "state-only invalidation should force a checkpoint-worthy state save"
        );
        assert_eq!(
            adapter.events_since_checkpoint, 0,
            "successful invalidation checkpoint should clear the dirty counter"
        );
        Ok(())
    }

    #[cfg(feature = "db")]
    #[sinex_test]
    async fn historical_replay_fails_when_dlq_routing_fails(ctx: TestContext) -> TestResult<()> {
        use sinex_db::DbPoolExt;

        let ctx = ctx.with_nats().dedicated().await?;
        let inserted = ctx
            .pool()
            .events()
            .insert_batch(vec![make_input_event("route-to-dlq")?])
            .await?;
        let input_id = inserted[0].id.expect("inserted event should have an id");

        let (runtime, _event_receiver) =
            make_runtime_state_with_db(&ctx, "derived-adapter-dlq-retry-test", None).await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(DlqRetryDerivedNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_emitter = Some(runtime.event_emitter().clone());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let error = adapter
            .run_historical(Checkpoint::None, Timestamp::now(), ScanArgs::default())
            .await
            .expect_err("historical replay must fail when DLQ routing fails");

        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to send derived-node event to processing-failure stream"));
        assert!(rendered.contains("route me to dlq"));
        assert!(rendered.contains("derived-adapter-dlq-retry-test"));
        assert!(
            adapter.events_processed() == 0,
            "failing DLQ routing must not advance replay progress past the bad event"
        );
        assert_eq!(adapter.current_checkpoint_internal(), Checkpoint::None);
        assert_eq!(input_id, inserted[0].id.expect("id should stay available"));
        Ok(())
    }
