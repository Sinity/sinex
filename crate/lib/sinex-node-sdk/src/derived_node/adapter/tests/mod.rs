    #[cfg(test)] mod processing_replay;
    // Inline because these cover a private shutdown-signaling helper.
    #[cfg(feature = "messaging")]
    use super::log_self_observation_failure;
    use super::request_runtime_drain;
    use super::{DerivedNodeAdapter, stale_output_ids_or_fail_scope};
    use crate::derived_node::{
        DerivedNodeConfig, DerivedOutput, DerivedTriggerContext, InputProvenanceFilter,
        ScopeReconcilerWrapper, TransducerWrapper,
    };
    use crate::exploration::{ExplorationProvider, ExportFormat};
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
    use sinex_primitives::domain::{
        EventSource, EventType, ProcessingMode, SanitizedPath, TriggerKind,
    };
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

    #[sinex_test]
    async fn derived_ingestion_history_is_explicitly_unavailable() -> TestResult<()> {
        let adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));

        let error = ExplorationProvider::get_ingestion_history(&adapter, 10)
            .expect_err("derived nodes must not report an empty ingestion history as success");

        assert!(error.to_string().contains("derived nodes"));
        Ok(())
    }

    #[sinex_test]
    async fn derived_export_is_explicitly_unavailable() -> TestResult<()> {
        let adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        let path = SanitizedPath::from_static("/tmp/derived-export.json");

        let error = ExplorationProvider::export_data(&adapter, &path, ExportFormat::Json)
            .expect_err("derived nodes must not report export success without writing data");

        assert!(error.to_string().contains("derived nodes"));
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
