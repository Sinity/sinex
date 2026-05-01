    #[allow(unused_imports)] use super::*;
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
        use super::super::super::DerivedScopeInvalidation;
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
        use super::super::super::DerivedScopeInvalidation;
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
