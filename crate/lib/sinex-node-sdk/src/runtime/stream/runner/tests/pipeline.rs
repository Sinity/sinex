//! Pipeline-side `NodeRunner<T>` tests: control-plane encoding, scan ack,
//! drain orchestration, replay forwarder, provisional resolution, ingestor
//! startup, and DLQ fallback paths.

use super::*;

#[sinex_test]
async fn encode_control_message_serializes_scan_ack() -> TestResult<()> {
    let operation_id = Uuid::now_v7();
    let ack = NodeScanAck {
        operation_id,
        node_name: "test-node".to_string(),
        accepted: true,
        error: None,
    };

    let encoded =
        encode_control_message("scan acknowledgement", operation_id, &ack.node_name, &ack)?;
    let decoded: NodeScanAck = serde_json::from_slice(&encoded)?;

    assert_eq!(decoded.operation_id, operation_id);
    assert!(decoded.accepted);
    Ok(())
}

#[sinex_test]
async fn encode_control_message_reports_serialization_failure() -> TestResult<()> {
    let operation_id = Uuid::now_v7();
    let err = encode_control_message(
        "scan acknowledgement",
        operation_id,
        "test-node",
        &FailingSerialize,
    )
    .expect_err("failing serializers must surface explicit control-plane errors");

    let text = err.to_string();
    assert!(text.contains("Failed to serialize scan acknowledgement"));
    assert!(text.contains("test-node"));
    assert!(text.contains(&operation_id.to_string()));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn publish_scan_ack_reports_nats_failures(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let client = ctx.nats_client();

    let operation_id = Uuid::now_v7();
    let ack = NodeScanAck {
        operation_id,
        node_name: "test-node".to_string(),
        accepted: true,
        error: Some("x".repeat(2_000_000)),
    };

    let error = NodeRunner::<RuntimeTestNode>::publish_scan_ack(
        &client,
        Some("sinex.test.reply".into()),
        &ack,
    )
    .await
    .expect_err("oversized control payloads must fail scan acknowledgements honestly");

    let message = error.to_string();
    assert!(message.contains("Failed to publish scan acknowledgement"));
    assert!(message.contains("sinex.test.reply"));
    assert!(message.contains(&operation_id.to_string()));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn run_service_drain_persists_ingestor_checkpoint_and_updates_status(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    ensure_default_bridge_streams(&client).await?;
    let transport = EventTransport::Nats(Arc::new(NatsPublisher::new(client.clone())));
    let work_dir = tempdir()?;

    let ingestor = DrainTestIngestor::default();
    let started = ingestor.started.clone();
    let drain_observed = ingestor.drain_observed.clone();
    let release_exit = ingestor.release_exit.clone();
    let expected_checkpoint = ingestor.final_checkpoint.clone();

    let mut runner = NodeRunner::new(IngestorNodeAdapter::new(ingestor));
    runner
        .initialize_with_transport(
            "runtime-drain-ingestor-service".to_string(),
            HashMap::new(),
            Some(ctx.pool().clone()),
            transport,
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    let runtime = runner
        .runtime_state()
        .ok_or_else(|| color_eyre::eyre::eyre!("runtime state missing after init"))?;
    let control_identity = runtime.control_identity().to_string();
    let drain_controller = runtime.runtime_drain();
    let checkpoint_manager = runtime.checkpoint_manager();
    let node_run_id = runtime
        .node_run_id()
        .ok_or_else(|| color_eyre::eyre::eyre!("node run id missing after db-backed init"))?;
    let drain_complete_subject = sinex_primitives::environment().nats_subject(&format!(
        "sinex.control.nodes.{control_identity}.drain_complete"
    ));
    let mut drain_complete_sub = client.subscribe(drain_complete_subject).await?;

    let run_handle = tokio::spawn(async move { runner.run_service().await });
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestor continuous loop did not start"))?;

    request_drain_until_applied(
        &client,
        &control_identity,
        &drain_controller,
        Some("test drain"),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(3), drain_observed.notified())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestor did not observe runtime drain"))?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if node_run_status(ctx.pool(), node_run_id).await? == "draining" {
                return Ok::<(), color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| color_eyre::eyre::eyre!("node run status never reached draining"))??;

    release_exit.notify_one();

    let drain_complete =
        tokio::time::timeout(Duration::from_secs(3), drain_complete_sub.next())
            .await
            .map_err(|_| color_eyre::eyre::eyre!("drain_complete was not published"))?
            .ok_or_else(|| color_eyre::eyre::eyre!("drain_complete subscription closed"))?;
    let payload: NodeDrainComplete = serde_json::from_slice(&drain_complete.payload)?;
    assert_eq!(payload.node_name, control_identity);
    assert_eq!(
        payload.checkpoint.as_deref(),
        Some(expected_checkpoint.description().as_str())
    );

    let run_result = tokio::time::timeout(Duration::from_secs(3), run_handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("drained ingestor service did not exit"))?;
    run_result??;

    let saved = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(saved.checkpoint, expected_checkpoint);
    assert_eq!(node_run_status(ctx.pool(), node_run_id).await?, "stopped");
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn publish_scan_progress_reports_nats_failures(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let client = ctx.nats_client();

    let operation_id = Uuid::now_v7();
    let progress = NodeScanProgress {
        operation_id,
        node_name: "test-node".to_string(),
        events_processed: 1,
        events_emitted: 2,
        final_report: None,
        error: Some("x".repeat(2_000_000)),
    };

    let error = NodeRunner::<RuntimeTestNode>::publish_scan_progress(
        &client,
        "sinex.test.progress".to_string(),
        &progress,
    )
    .await
    .expect_err("oversized control payloads must fail scan progress honestly");

    let message = error.to_string();
    assert!(message.contains("Failed to publish scan progress update"));
    assert!(message.contains("sinex.test.progress"));
    assert!(message.contains(&operation_id.to_string()));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn finish_replay_forwarder_surfaces_forwarder_error() -> TestResult<()> {
    let emitted_counter = Arc::new(AtomicU64::new(7));
    let handle = tokio::spawn(async {
        Err(SinexError::processing(
            "replay forwarder target channel closed".to_string(),
        ))
    });

    let outcome =
        NodeRunner::<RuntimeTestNode>::finish_replay_forwarder(handle, emitted_counter)
            .await
            .expect_err("forwarder failures must fail the dispatched scan honestly");

    assert_eq!(outcome.events_emitted, 7);
    assert!(
        outcome
            .error
            .to_string()
            .contains("replay forwarder target channel closed")
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn finish_replay_forwarder_surfaces_join_error() -> TestResult<()> {
    let emitted_counter = Arc::new(AtomicU64::new(3));
    let handle: tokio::task::JoinHandle<NodeResult<()>> = tokio::spawn(async move {
        panic!("forwarder panic");
    });

    let outcome =
        NodeRunner::<RuntimeTestNode>::finish_replay_forwarder(handle, emitted_counter)
            .await
            .expect_err("forwarder panics must fail the dispatched scan honestly");

    assert_eq!(outcome.events_emitted, 3);
    assert!(
        outcome
            .error
            .to_string()
            .contains("Replay forwarder join failed")
    );
    Ok(())
}

#[sinex_test]
async fn resolve_provisionals_to_events_surfaces_missing_confirmed_event(
    ctx: TestContext,
) -> TestResult<()> {
    let provisional = ProvisionalEvent {
        event_id: EventId::from(Uuid::now_v7()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let Err(error) = NodeRunner::<RuntimeTestNode>::resolve_provisionals_to_events(
        &[provisional],
        &Some(ctx.pool().clone()),
    )
    .await
    else {
        return Err(color_eyre::eyre::eyre!(
            "missing confirmed events must fail honestly"
        ));
    };

    let message = format!("{error:#}");
    assert!(message.contains("Confirmed event missing from database"));
    Ok(())
}

#[sinex_test]
async fn build_event_from_provisional_rejects_invalid_node_run_id() -> TestResult<()> {
    let provisional = ProvisionalEvent {
        event_id: EventId::from(Uuid::now_v7()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({
            "source": "runtime-test-source",
            "event_type": "runtime.test",
            "host": "runtime-test-host",
            "payload": {"ok": true},
            "source_event_ids": [Uuid::now_v7().to_string()],
            "node_run_id": "not-a-uuid"
        }),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let error = NodeRunner::<RuntimeTestNode>::build_event_from_provisional(&provisional)
        .expect_err("invalid persisted node_run_id must fail honestly");
    assert!(error.to_string().contains("Invalid UUID for node_run_id"));
    Ok(())
}

#[sinex_test]
async fn ingestor_startup_skips_gap_fill_when_only_snapshot_created_checkpoint(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let snapshot_checkpoint = Checkpoint::timestamp(Timestamp::now(), None);
    let node = StartupSequenceTestNode::new(Checkpoint::None, snapshot_checkpoint);
    let scans = node.scans.clone();
    let mut runner = NodeRunner::new(node);
    let work_dir = tempdir()?;
    runner
        .initialize_with_transport(
            "startup-sequence-snapshot-only".to_string(),
            HashMap::new(),
            None,
            EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client()))),
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    runner.run_ingestor_startup_sequence().await?;

    let recorded = scans.lock().await.clone();
    assert_eq!(
        recorded,
        vec![RecordedScan {
            from: Checkpoint::None,
            until: "snapshot",
        }]
    );
    Ok(())
}

#[sinex_test]
async fn ingestor_startup_gap_fill_uses_preexisting_checkpoint(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let preexisting_checkpoint =
        Checkpoint::timestamp(Timestamp::now() - time::Duration::minutes(15), None);
    let snapshot_checkpoint = Checkpoint::timestamp(Timestamp::now(), None);
    let node =
        StartupSequenceTestNode::new(preexisting_checkpoint.clone(), snapshot_checkpoint);
    let scans = node.scans.clone();
    let mut runner = NodeRunner::new(node);
    let work_dir = tempdir()?;
    runner
        .initialize_with_transport(
            "startup-sequence-gap-fill".to_string(),
            HashMap::new(),
            None,
            EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client()))),
            work_dir.path().to_path_buf(),
            false,
        )
        .await?;

    runner.run_ingestor_startup_sequence().await?;

    let recorded = scans.lock().await.clone();
    assert_eq!(
        recorded,
        vec![
            RecordedScan {
                from: Checkpoint::None,
                until: "snapshot",
            },
            RecordedScan {
                from: preexisting_checkpoint,
                until: "historical",
            },
        ]
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn resolve_provisionals_to_events_surfaces_invalid_payload_without_db() -> TestResult<()>
{
    let provisional = ProvisionalEvent {
        event_id: EventId::from(Uuid::now_v7()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({
            "source": "runtime-test-source",
            "event_type": "runtime.test",
            "host": "runtime-test-host",
            "payload": {"ok": true},
            "source_event_ids": [Uuid::now_v7().to_string()],
            "node_run_id": "not-a-uuid"
        }),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };

    let Err(error) =
        NodeRunner::<RuntimeTestNode>::resolve_provisionals_to_events(&[provisional], &None)
            .await
    else {
        return Err(color_eyre::eyre::eyre!(
            "invalid provisional payloads must fail honestly when no db pool is available"
        ));
    };

    let message = format!("{error:#}");
    assert!(
        message.contains("Confirmed event could not be reconstructed from provisional payload")
    );
    assert!(message.contains("Invalid UUID for node_run_id"));
    Ok(())
}

/// Node that returns a checkpoint error from `process_event_batch`. The
/// real adapter does this after 3 consecutive checkpoint CAS failures
/// (see `derived_node::adapter::process_batch`); we shortcut that
/// behaviour to drive the runtime fallback path directly.
struct CheckpointErrorBatchNode;

impl Node for CheckpointErrorBatchNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::ZERO,
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "runtime-checkpoint-error-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_historical: false,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        Err(SinexError::checkpoint(
            "Checkpoint save failed 3 consecutive times; halting to prevent silent progress loss on crash+restart",
        ))
    }
}

/// Regression for #581. The pre-fix code caught `Err` from the batch path
/// and tried per-event DLQ routing; with a checkpoint error every event
/// hit the same KV-revision conflict and the function returned Ok,
/// looping forever and saturating I/O. The fix matches
/// `SinexError::Checkpoint` and propagates immediately.
#[cfg(feature = "messaging")]
#[sinex_test]
async fn process_batch_with_dlq_fallback_propagates_checkpoint_errors(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let transport =
        EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client())));
    let mut node = CheckpointErrorBatchNode;
    let event = Event {
        id: Some(EventId::from(Uuid::now_v7())),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    };

    let error = NodeRunner::<CheckpointErrorBatchNode>::process_batch_with_dlq_fallback(
        &mut node,
        &transport,
        vec![event],
    )
    .await
    .expect_err("checkpoint error must propagate, not be DLQ-fallback'd");

    // The error must remain a Checkpoint variant — the runtime supervisor
    // matches on this to halt the consumer instead of advancing.
    assert!(
        matches!(error, SinexError::Checkpoint(_)),
        "expected SinexError::Checkpoint, got: {error:?}"
    );
    let message = format!("{error:#}");
    assert!(
        message.contains("Checkpoint save failed"),
        "checkpoint error message lost in propagation: {message}"
    );
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn process_batch_with_dlq_fallback_fails_when_dlq_route_fails(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let transport =
        EventTransport::Nats(Arc::new(crate::NatsPublisher::new(ctx.nats_client())));
    let mut node = FailingBatchNode;
    let event = Event {
        id: Some(EventId::from(Uuid::now_v7())),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    };

    let error = NodeRunner::<FailingBatchNode>::process_batch_with_dlq_fallback(
        &mut node,
        &transport,
        vec![event],
    )
    .await
    .expect_err("failed DLQ routing must stop checkpoint advancement");

    let message = format!("{error:#}");
    assert!(message.contains("failed to route failed automaton event to processing-failure stream"));
    assert!(message.contains("batch processing boom"));
    assert!(message.contains("runtime-failing-batch-node"));
    Ok(())
}

#[sinex_test]
async fn load_bridge_checkpoint_state_surfaces_corrupt_kv(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "runtime-test-node".to_string(),
        "runtime-group".to_string(),
        "runtime-consumer".to_string(),
    );
    manager
        .save_checkpoint(&crate::checkpoint::CheckpointState::default())
        .await?;

    let mut keys = kv.keys().await?;
    let key = futures::TryStreamExt::try_next(&mut keys)
        .await?
        .expect("checkpoint key should exist");
    kv.put(&key, b"{ definitely not valid json".as_slice().into())
        .await?;

    let error = NodeRunner::<RuntimeTestNode>::load_bridge_checkpoint_state(&manager)
        .await
        .expect_err("corrupt bridge checkpoint state must surface");
    let message = format!("{error:#}");
    assert!(message.contains("Failed to load checkpoint state for automaton bridge"));
    assert!(message.contains("Failed to decode checkpoint from KV"));
    Ok(())
}
