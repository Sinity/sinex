use super::*;

/// Register a `derivation.product_declarations` row so
/// `derivation.enforce_event_product_declaration()` accepts a test-built
/// derived event that declares `product_class` (sinex-0vx.4). Src-level unit
/// test module (`use super::*`, not `crate/sinexd/tests/**`), so it mirrors
/// rather than reuses `tests/api/common::seed_product_declaration` (same
/// pattern as `cascade_analyzer_test.rs` / `replay_control_test.rs`,
/// sinex-egyf / sinex-li78 quiet-host reverification follow-up).
async fn seed_product_declaration(
    pool: &sqlx::PgPool,
    declaration_id: &str,
    product_class: sinex_primitives::derivation::DerivedProductClass,
    output_source: &str,
    output_event_type: &str,
) -> sinex_primitives::Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        ) VALUES (
            $1, 'sinex-li78-test', $2, 'derived_output',
            $3, $4, 'v1', 'default_canonical_input', '{}'::jsonb, 'true'
        )
        ON CONFLICT (declaration_id) DO NOTHING
        "#,
        declaration_id,
        product_class.as_str(),
        output_source,
        output_event_type,
    )
    .execute(pool)
    .await
    .map_err(|e| {
        sinex_primitives::SinexError::database("seed product declaration").with_source(e)
    })?;
    Ok(())
}

async fn make_db_input_event(
    ctx: &TestContext,
    material_label: &str,
    value: &str,
) -> TestResult<Event<JsonValue>> {
    let material_id = ctx.create_source_material(Some(material_label)).await?;
    let mut event = DynamicPayload::new("test.source", "test.input", json!({ "value": value }))
        .from_material(material_id)
        .build()?;
    event.id = Some(event.id.unwrap_or_else(Id::new));
    Ok(event)
}

#[sinex_test]
#[ignore = "sinex-wb2r open: process_batch's checkpoint-failure halt condition short-circuits \
            on error KIND (Checkpoint/Lifecycle/Configuration/PermissionDenied) so the first \
            Checkpoint-kind failure already halts the batch -- the '3 consecutive failures' \
            tolerance this test asserts never actually applies to this error kind"]
async fn process_batch_halts_after_three_consecutive_checkpoint_save_failures(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut adapter = AutomatonRuntime::with_config(
        TransducerWrapper(UnserializableAutomaton),
        AutomatonAdapterConfig {
            checkpoint_interval: 1,
            ..AutomatonAdapterConfig::default()
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
        "unserializable checkpoint automaton should not emit output events"
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
async fn derived_outputs_propagate_runtime_module_run_id(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let module_run_id = Uuid::now_v7();
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));
    adapter.runtime =
        Some(make_runtime_state(&ctx, "derived-adapter-emitting-test", Some(module_run_id)).await?);

    let outputs = adapter.process_one(make_input_event("emit")?).await?;
    let output = outputs
        .into_iter()
        .next()
        .expect("emitting automaton should produce one output event");

    assert_eq!(output.module_run_id, Some(module_run_id));
    Ok(())
}

#[sinex_test]
async fn derived_outputs_carry_unique_random_uuidv7_ids() -> TestResult<()> {
    // Event IDs are interpretation identity: each processing invocation mints a
    // fresh UUIDv7. Two independent processings of the same input event must
    // produce DIFFERENT output IDs (replay creates new interpretations).
    let input = make_input_event("random-id-output")?;
    let mut first_adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));
    let mut second_adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));

    let first_output = first_adapter
        .process_one(input.clone())
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("emitting automaton should produce an output"))?;
    let second_output = second_adapter
        .process_one(input)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("emitting automaton should produce an output"))?;

    let first_id = first_output
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("derived output must carry an event id"))?;
    let second_id = second_output
        .id
        .ok_or_else(|| color_eyre::eyre::eyre!("derived output must carry an event id"))?;

    // Each invocation yields a new interpretation identity.
    assert_ne!(
        first_id, second_id,
        "re-processing must produce a distinct event id"
    );

    // Both must be valid RFC4122 UUIDv7 (required by the admission gate).
    assert_eq!(first_id.as_uuid().get_version_num(), 7);
    assert_eq!(first_id.as_uuid().get_variant(), uuid::Variant::RFC4122);
    assert_eq!(second_id.as_uuid().get_version_num(), 7);
    assert_eq!(second_id.as_uuid().get_variant(), uuid::Variant::RFC4122);
    Ok(())
}

#[sinex_test]
async fn process_one_tracks_run_local_processed_count() -> TestResult<()> {
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));

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
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));
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
async fn scope_invalidation_outputs_preserve_payload_for_policy_admission() -> TestResult<()> {
    struct PolicyAdmissionInvalidationNode;

    impl Transducer for PolicyAdmissionInvalidationNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-invalidation-policy-admission-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        const OUTPUT_DECLARATIONS:
            &'static [sinex_primitives::derivation::DerivationOutputDeclaration] = &[
            sinex_primitives::derivation::DerivationOutputDeclaration {
                declaration_id: "test.derived-adapter-invalidation-policy-admission-test.test.output",
                owner: "test",
                product_class:
                    sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent,
                write_surface: sinex_primitives::derivation::DerivationWriteSurface::DerivedOutput,
                output_source: None,
                output_event_type: Some("test.output"),
                projection_kind: None,
                artifact_kind: None,
                proposal_kind: None,
                semantics_version: "1.0.0",
                input_eligibility: sinex_primitives::derivation::InputEligibility::ExplicitOnly,
                default_support: sinex_primitives::derivation::ClaimSupportTemplate::new(
                    sinex_primitives::derivation::SupportLevel::Convergent,
                    sinex_primitives::derivation::SourceCoverage::Partial,
                    sinex_primitives::derivation::ClaimTemporalQuality::DeclaredEffective,
                ),
                verification_command: "xtask test -p sinexd -E 'test(scope_invalidation_outputs_preserve_payload)'",
            },
        ];

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &AutomatonContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
            Ok(None)
        }
    }

    let adapter = AutomatonRuntime::new(TransducerWrapper(PolicyAdmissionInvalidationNode));
    let token = ["ghp_", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"].concat();
    let declaration = &PolicyAdmissionInvalidationNode::OUTPUT_DECLARATIONS[0];
    let output = DerivedOutput::reconciled(
        json!({ "value": token }),
        Timestamp::now(),
        vec![Uuid::now_v7()],
        "scope-a".to_string(),
    )
    .with_declaration_id(declaration.declaration_id)
    .with_product_class(declaration.product_class)
    .with_claim_support(sinex_primitives::derivation::ClaimSupport::unknown());
    let context = AutomatonContext {
        trigger_event_id: Id::new(),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.invalidation")?,
        ts_orig: None,
        ts_coided: Timestamp::now(),
        processing_mode: ProcessingMode::Replay,
        trigger_kind: TriggerKind::ScopeInvalidation,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    };

    let event = adapter.build_output_event(output, 0, None, &context)?;

    assert_eq!(event.payload["value"].as_str(), Some(token.as_str()));
    Ok(())
}

#[sinex_test]
async fn current_checkpoint_tracks_last_processed_input_event() -> TestResult<()> {
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(TestAutomaton));
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

    let mut adapter = AutomatonRuntime::with_shutdown_config(
        TransducerWrapper(TestAutomaton),
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

    let mut adapter = AutomatonRuntime::with_shutdown_config(
        TransducerWrapper(TestAutomaton),
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

    let mut adapter = AutomatonRuntime::with_shutdown_config(
        TransducerWrapper(TestAutomaton),
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
async fn historical_replay_resumes_from_internal_checkpoint(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let inserted = ctx
        .pool()
        .events()
        .insert_batch(vec![
            make_db_input_event(&ctx, "history-resume-first", "first").await?,
            make_db_input_event(&ctx, "history-resume-second", "second").await?,
            make_db_input_event(&ctx, "history-resume-third", "third").await?,
        ])
        .await?;
    let second_id = inserted[1].id.expect("inserted event must have an id");
    let third_id = inserted[2].id.expect("inserted event must have an id");

    let (runtime, _event_receiver) =
        make_runtime_state_with_db(&ctx, "derived-history-resume-test", None).await?;
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(EmittingAutomaton));
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
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(WildcardMaterialOnlyNode));

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

    let material_event_id = material_event
        .id
        .expect("material event fixture should carry an id");
    let product_class = sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent;
    let declaration_id = "sinex.test.historical_replay_filters_wildcard_material_only_inputs";
    seed_product_declaration(
        ctx.pool(),
        declaration_id,
        product_class,
        "test.source",
        "test.input",
    )
    .await?;
    let mut synthesized_event = DynamicPayload::new(
        "test.source",
        "test.input",
        json!({ "value": "synthesized-history" }),
    )
    .from_parents([material_event_id])?
    .build()?;
    synthesized_event.id = Some(synthesized_event.id.unwrap_or_else(Id::new));
    synthesized_event.product_class = Some(product_class);
    synthesized_event.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    synthesized_event.derivation_declaration_id = Some(declaration_id.to_string());

    ctx.pool()
        .events()
        .insert_batch(vec![material_event, synthesized_event])
        .await?;

    let (runtime, _event_receiver) =
        make_runtime_state_with_db(&ctx, "wildcard-material-only", None).await?;
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(WildcardMaterialOnlyNode));
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
async fn handle_invalidation_archives_before_output_emit_and_retries_without_duplicates(
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
    let product_class = sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent;
    let declaration_id = "sinex.test.handle_invalidation_archives_before_output_emit";
    seed_product_declaration(
        ctx.pool(),
        declaration_id,
        product_class,
        "adapter-regression-scope-reconciler",
        "measurement.aggregate",
    )
    .await?;
    let mut stale_output = DynamicPayload::new(
        "adapter-regression-scope-reconciler",
        "measurement.aggregate",
        serde_json::json!({ "total": 5_i64, "count": 1_u64 }),
    )
    .from_parents(vec![input_id])?
    .build()?;
    stale_output.scope_key = Some(scope_key.to_string());
    stale_output.product_class = Some(product_class);
    stale_output.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    stale_output.derivation_declaration_id = Some(declaration_id.to_string());
    ctx.pool().events().insert_batch(vec![stale_output]).await?;

    let (runtime, _event_receiver) =
        make_runtime_state_with_db(&ctx, "adapter-regression-scope-reconciler", None).await?;

    let mut adapter = AutomatonRuntime::new(ScopeReconcilerWrapper(TestScopeReconcilerAutomaton));
    adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
    // The runtime helper owns a background settlement receiver, so dropping
    // its returned receiver does not make the shared emitter fail. Use an
    // intentionally receiver-less channel for the first attempt to exercise
    // the archive-before-emission failure window.
    let (failing_sender, failing_receiver) = mpsc::channel(1);
    drop(failing_receiver);
    adapter.event_emitter = Some(EventEmitter::new(failing_sender, false));
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
        live_output_count, 0,
        "stale outputs must be archived before replacement emission begins"
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
        archived_output_count, 1,
        "the archive marker must survive a failed replacement emission"
    );
    let (retry_sender, mut retry_receiver) = mpsc::channel(4);
    adapter.event_emitter = Some(EventEmitter::new(retry_sender, false));
    let retry = adapter.handle_invalidation_message(&payload).await?;
    assert_eq!(
        retry,
        Some(1),
        "redelivery must recompute exactly one replacement"
    );
    let emitted = tokio::time::timeout(std::time::Duration::from_secs(1), retry_receiver.recv())
        .await?
        .expect("redelivery should emit the recomputed replacement");
    assert_eq!(emitted.scope_key.as_deref(), Some(scope_key));
    assert_eq!(
        retry_receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "redelivery must not emit a duplicate replacement"
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
        make_runtime_state_with_db(&ctx, "adapter-regression-stateful-invalidation", None).await?;

    let mut adapter = AutomatonRuntime::with_config(
        ScopeReconcilerWrapper(StatefulInvalidationNode {
            allow_scope_recompute: true,
        }),
        AutomatonAdapterConfig {
            checkpoint_interval: 1,
            ..AutomatonAdapterConfig::default()
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
        processed.expect("state-only invalidation must not halt the automaton"),
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

#[cfg(feature = "messaging")]
#[sinex_test]
async fn global_state_invalidation_does_not_clobber_unrelated_state(
    ctx: TestContext,
) -> TestResult<()> {
    use super::super::super::DerivedScopeInvalidation;
    use sinex_db::DbPoolExt;
    use sinex_primitives::events::DynamicPayload;
    use sinex_primitives::{EventSource, EventType};

    let ctx = ctx.with_nats().dedicated().await?;
    let material_id = ctx
        .create_source_material(Some("derived-invalidation-global-state"))
        .await?;
    let mut input = DynamicPayload::new(
        "measurements",
        "measurement.taken",
        serde_json::json!({ "value": 11_i64 }),
    )
    .from_material(material_id)
    .build()?;
    input.scope_key = Some("scope:foreign".to_string());
    let input_id = ctx
        .pool()
        .events()
        .insert_batch(vec![input])
        .await?
        .into_iter()
        .next()
        .and_then(|event| event.id)
        .expect("invalidation input should have an id");

    let (runtime, _event_receiver) =
        make_runtime_state_with_db(&ctx, "adapter-regression-global-state", None).await?;
    let mut adapter = AutomatonRuntime::with_config(
        ScopeReconcilerWrapper(StatefulInvalidationNode {
            allow_scope_recompute: false,
        }),
        AutomatonAdapterConfig {
            checkpoint_interval: 1,
            ..AutomatonAdapterConfig::default()
        },
    );
    adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
    adapter.event_emitter = Some(runtime.event_emitter().clone());
    adapter.host = runtime.service_info().host().to_string();
    adapter.runtime = Some(runtime);
    adapter.persisted_state.state.invalidations_applied = 41;

    let invalidation = DerivedScopeInvalidation::replaced(
        vec![*input_id.as_uuid()],
        EventSource::from_static("measurements"),
        EventType::from_static("measurement.taken"),
    )
    .with_scope_keys(vec!["scope:foreign".to_string()]);
    let processed = adapter
        .handle_invalidation_message(&serde_json::to_vec(&invalidation)?)
        .await?;

    assert_eq!(processed, Some(0));
    assert_eq!(
        adapter.persisted_state.state.invalidations_applied, 41,
        "a foreign scope must not replace a global accumulator with default state"
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
        .insert_batch(vec![
            make_db_input_event(&ctx, "route-to-dlq", "route-to-dlq").await?,
        ])
        .await?;
    let input_id = inserted[0].id.expect("inserted event should have an id");

    let (runtime, _event_receiver) =
        make_runtime_state_with_db(&ctx, "derived-adapter-dlq-retry-test", None).await?;
    let mut adapter = AutomatonRuntime::new(TransducerWrapper(DlqRetryAutomaton));
    adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
    adapter.event_emitter = Some(runtime.event_emitter().clone());
    adapter.host = runtime.service_info().host().to_string();
    adapter.runtime = Some(runtime);

    let error = adapter
        .run_historical(Checkpoint::None, Timestamp::now(), ScanArgs::default())
        .await
        .expect_err("historical replay must fail when DLQ routing fails");

    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed to send automaton event to processing-failure stream"));
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
