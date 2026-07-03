    use super::*;
    use crate::runtime::EventTransport;
    use crate::runtime::checkpoint::CheckpointManager;
    use crate::runtime::nats_publisher::NatsPublisher;
    use crate::runtime::stream::{EventEmitter, RuntimeContext, RuntimeHandles, ServiceInfo};
    use camino::Utf8PathBuf;
    use sinex_db::models::Event;
    use sinex_primitives::JsonValue;
    use sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use uuid::Uuid;
    use xtask::sandbox::{
        EnvGuard, EphemeralNats, TestContext, TestResult, sinex_test, timing::WaitHelpers,
    };

    struct TestRuntimeHarness {
        runtime: RuntimeContext,
        _event_rx: mpsc::Receiver<Event<JsonValue>>,
        _nats: Arc<EphemeralNats>,
    }

    async fn build_runtime(
        ctx: &TestContext,
        service_name: &str,
    ) -> TestResult<TestRuntimeHarness> {
        build_runtime_with_identity(ctx, service_name, service_name, None, None).await
    }

    async fn build_runtime_with_identity(
        ctx: &TestContext,
        service_name: &str,
        module_name: &str,
        source_id: Option<&str>,
        runner_pack: Option<&str>,
    ) -> TestResult<TestRuntimeHarness> {
        let nats_client = ctx.ensure_nats().await?;
        let nats = ctx.nats_handle()?;
        let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));

        let (event_tx, event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
        let emitter = EventEmitter::new(event_tx, false);

        let js = async_nats::jetstream::new(nats_client);
        let kv = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "sinex_checkpoints".to_string(),
                history: 1,
                ..Default::default()
            })
            .await?;

        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            service_name.to_string(),
            "test".to_string(),
            format!(
                "{service_name}-{}",
                Uuid::now_v7().to_string().to_lowercase()
            ),
        ));

        let handles = RuntimeHandles::new(
            ctx.pool.clone(),
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );

        let work_dir = Utf8PathBuf::from_path_buf(sinex_primitives::environment().temp_dir())
            .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex-test"));

        let service_info = ServiceInfo::new_with_runtime_identity(
            service_name.to_string(),
            module_name.to_string(),
            source_id.map(ToOwned::to_owned),
            runner_pack.map(ToOwned::to_owned),
            sinex_primitives::events::builder::get_hostname(),
            work_dir.clone().into_std_path_buf(),
            false,
            format!("test-instance-{}", Uuid::now_v7().simple()),
            env!("CARGO_PKG_VERSION").to_string(),
            None,
        );

        let runtime = RuntimeContext::new(service_info, handles, HashMap::new(), work_dir);

        Ok(TestRuntimeHarness {
            runtime,
            _event_rx: event_rx,
            _nats: nats,
        })
    }

    #[sinex_test]
    async fn coordination_failure_counter_increments(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-test").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;

        let before = coordination.leadership_failures.get();
        coordination.record_coordination_failure("test", "simulated");
        let after = coordination.leadership_failures.get();

        assert_eq!(after, before + 1);
        Ok(())
    }

    #[sinex_test]
    async fn instance_metadata_allows_fresh_heartbeat_override() -> TestResult<()> {
        let mut instance = RuntimeInstance::new(
            "coord-test".to_string(),
            ServiceName::new("coordination-heartbeat"),
        )?;
        instance.start_time = SystemTime::UNIX_EPOCH;
        let started_at = instance_started_at(&instance);
        let current_heartbeat = sinex_primitives::temporal::Timestamp::now().unix_timestamp();

        assert!(
            current_heartbeat > started_at,
            "test setup should make the current heartbeat newer than startup"
        );
        let metadata = instance_metadata_at(&instance, Some(current_heartbeat));

        assert_eq!(
            metadata.last_heartbeat, current_heartbeat,
            "metadata should use the fresh heartbeat override"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_coordination_uses_control_identity_for_storage(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime_with_identity(
            &ctx,
            "source-driver-terminal.atuin-history",
            "terminal-watcher",
            Some("terminal.atuin-history"),
            Some("terminal"),
        )
        .await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "source-instance".to_string())?;

        assert_eq!(
            coordination.instance.service_name.as_str(),
            "terminal.atuin-history"
        );

        coordination
            .kv_client
            .register_instance(&coordination.current_metadata())
            .await?;
        assert!(
            coordination
                .kv_client
                .acquire_leadership(&coordination.instance.instance_id)
                .await?,
            "source coordination identity should be a valid NATS KV key"
        );
        Ok(())
    }

    #[sinex_test]
    async fn serialize_handoff_request_round_trips() -> TestResult<()> {
        let request = HandoffRequest {
            requester_instance_id: "requester-1".to_string(),
            requester_version: RuntimeVersion::current()?,
            target_instance_id: "target-1".to_string(),
            target_version: RuntimeVersion::current()?,
            requested_at: SystemTime::now(),
            timeout_seconds: Seconds::from_secs(30),
        };

        let payload = RuntimeCoordination::serialize_handoff_request(&request)?;
        let decoded: HandoffRequest = serde_json::from_slice(&payload)?;

        assert_eq!(decoded.requester_instance_id, request.requester_instance_id);
        assert_eq!(decoded.target_instance_id, request.target_instance_id);
        assert_eq!(decoded.timeout_seconds, request.timeout_seconds);
        Ok(())
    }

    #[sinex_test]
    async fn decode_handoff_request_reports_malformed_payload() -> TestResult<()> {
        let err = RuntimeCoordination::decode_handoff_request(b"{not-json", "handoff request")
            .expect_err("malformed handoff payload should be rejected");
        assert!(
            err.to_string().contains("Failed to decode handoff request"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn forward_handoff_requests_closes_channel_when_subscription_ends() -> TestResult<()> {
        let (handoff_sender, mut handoff_receiver) = mpsc::channel(1);
        let handoff_drops = CoordinationPrimitive::event_counter(0, "coordination_handoff_drops");

        RuntimeCoordination::forward_handoff_requests(
            futures::stream::empty::<async_nats::Message>(),
            "target-instance".to_string(),
            "0.0.1".parse::<RuntimeVersion>().expect("valid version"),
            handoff_sender,
            handoff_drops.clone(),
            ServiceName::new("coordination-test"),
        )
        .await;

        assert!(
            handoff_receiver.recv().await.is_none(),
            "monitor shutdown should close the handoff channel"
        );
        assert_eq!(
            handoff_drops.get(),
            1,
            "subscription shutdown should increment the handoff drop counter"
        );
        Ok(())
    }

    #[sinex_test]
    async fn list_instances_filters_stale_metadata(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-filter").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let fresh = coordination.current_metadata();
        coordination.kv_client.register_instance(&fresh).await?;

        let stale = InstanceMetadata {
            instance_id: "stale-instance".to_string(),
            hostname: fresh.hostname.clone(),
            version: fresh.version.clone(),
            started_at: fresh.started_at,
            last_heartbeat: fresh.last_heartbeat - 600,
        };
        coordination.kv_client.register_instance(&stale).await?;

        let listed = coordination.kv_client.list_instances().await?;
        assert!(
            listed
                .iter()
                .any(|meta| meta.instance_id == fresh.instance_id),
            "fresh instance should remain visible"
        );
        assert!(
            listed
                .iter()
                .all(|meta| meta.instance_id != stale.instance_id),
            "stale instance should be filtered out"
        );
        assert!(
            coordination
                .kv_client
                .get_instance(&stale.instance_id)
                .await?
                .is_none(),
            "stale instance lookup should behave as missing"
        );
        Ok(())
    }

    #[sinex_test]
    async fn maybe_initiate_handoff_ignores_stale_older_version(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-filter").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let fresh = coordination.current_metadata();

        coordination
            .kv_client
            .register_instance(&InstanceMetadata {
                instance_id: "stale-old-version".to_string(),
                hostname: fresh.hostname.clone(),
                version: "0.0.0".to_string(),
                started_at: fresh.started_at - 600,
                last_heartbeat: fresh.last_heartbeat - 600,
            })
            .await?;
        coordination
            .kv_client
            .acquire_leadership("stale-old-version")
            .await?;

        assert!(
            !coordination.maybe_initiate_handoff().await?,
            "stale older instances must not trigger startup handoff"
        );
        Ok(())
    }

    #[sinex_test]
    async fn maybe_initiate_handoff_ignores_older_standby_when_self_is_leader(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-leader-only-handoff").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let fresh = coordination.current_metadata();
        coordination.kv_client.register_instance(&fresh).await?;
        coordination
            .kv_client
            .register_instance(&InstanceMetadata {
                instance_id: "older-standby".to_string(),
                hostname: fresh.hostname.clone(),
                version: "0.0.0".to_string(),
                started_at: fresh.started_at,
                last_heartbeat: fresh.last_heartbeat,
            })
            .await?;
        coordination
            .kv_client
            .acquire_leadership(&fresh.instance_id)
            .await?;

        assert!(
            !coordination.maybe_initiate_handoff().await?,
            "only the current leader should be considered for startup handoff"
        );
        Ok(())
    }

    #[sinex_test]
    async fn send_handoff_request_publishes_explicit_requester_and_target(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-payload").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let subject = format!(
            "sinex.coordination.{}.handoff",
            coordination.instance.service_name
        );
        let mut sub = coordination.nats_client.subscribe(subject).await?;

        coordination
            .send_handoff_request("older-leader", "0.0.0".parse()?)
            .await?;

        let message = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await?
            .ok_or_else(|| SinexError::processing("handoff request not published"))?;
        let request: HandoffRequest = serde_json::from_slice(&message.payload)?;
        assert_eq!(
            request.requester_instance_id,
            coordination.instance.instance_id
        );
        assert_eq!(request.requester_version, coordination.instance.version);
        assert_eq!(request.target_instance_id, "older-leader");
        assert_eq!(request.target_version, "0.0.0".parse()?);
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_handoff_ready_ignores_unrelated_messages(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-ready-filter").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let service = coordination.instance.service_name.clone();
        let requester = coordination.instance.instance_id.clone();
        let requester_version = coordination.instance.version.clone();
        let target_version = "0.0.1".parse::<RuntimeVersion>().expect("valid version");
        let ready_target_version = target_version.clone();
        let nats = coordination.nats_client.clone();
        let mut sub = coordination.subscribe_handoff_ready().await?;

        let publisher = tokio::spawn(async move {
            let ready_subject = format!("sinex.coordination.{service}.handoff_ready");

            nats.publish(ready_subject.clone(), "not-json".into())
                .await
                .expect("publish malformed ready");

            let unrelated = HandoffRequest {
                requester_instance_id: "other-requester".to_string(),
                requester_version: "9.9.9".parse().expect("valid version"),
                target_instance_id: "other-target".to_string(),
                target_version: "0.0.1".parse().expect("valid version"),
                requested_at: SystemTime::now(),
                timeout_seconds: Seconds::from_secs(30),
            };
            nats.publish(
                ready_subject.clone(),
                serde_json::to_vec(&unrelated)
                    .expect("serialize unrelated")
                    .into(),
            )
            .await
            .expect("publish unrelated ready");

            tokio::time::sleep(Duration::from_millis(50)).await;

            let matching = HandoffRequest {
                requester_instance_id: requester,
                requester_version,
                target_instance_id: "older-leader".to_string(),
                target_version: ready_target_version,
                requested_at: SystemTime::now(),
                timeout_seconds: Seconds::from_secs(30),
            };
            nats.publish(
                ready_subject,
                serde_json::to_vec(&matching)
                    .expect("serialize matching")
                    .into(),
            )
            .await
            .expect("publish matching ready");
        });

        coordination
            .wait_for_handoff_ready_with_subscription(
                &mut sub,
                "older-leader",
                &target_version,
                Duration::from_secs(5),
            )
            .await?;
        publisher.await?;
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_handoff_ready_times_out_honestly(ctx: TestContext) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-timeout").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let mut sub = coordination.subscribe_handoff_ready().await?;
        let target_version = "0.0.1".parse().expect("valid version");

        let err = coordination
            .wait_for_handoff_ready_with_subscription(
                &mut sub,
                "older-leader",
                &target_version,
                Duration::from_millis(50),
            )
            .await
            .expect_err("missing handoff_ready should surface as an error");
        assert!(
            err.to_string()
                .contains("Timed out waiting for handoff_ready"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn maybe_initiate_handoff_targets_current_leader_and_waits_for_ready(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-handoff-roundtrip").await?;
        let coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let self_metadata = coordination.current_metadata();
        coordination
            .kv_client
            .register_instance(&self_metadata)
            .await?;

        let older_leader = InstanceMetadata {
            instance_id: "older-leader".to_string(),
            hostname: self_metadata.hostname.clone(),
            version: "0.0.0".to_string(),
            started_at: self_metadata.started_at,
            last_heartbeat: self_metadata.last_heartbeat,
        };
        coordination
            .kv_client
            .register_instance(&older_leader)
            .await?;
        coordination
            .kv_client
            .acquire_leadership(&older_leader.instance_id)
            .await?;

        let requester = coordination.instance.instance_id.clone();
        let service = coordination.instance.service_name.clone();
        let nats = coordination.nats_client.clone();
        let responder = tokio::spawn(async move {
            let handoff_subject = format!("sinex.coordination.{service}.handoff");
            let ready_subject = format!("sinex.coordination.{service}.handoff_ready");
            let mut sub = nats
                .subscribe(handoff_subject)
                .await
                .expect("subscribe handoff");
            let message = tokio::time::timeout(Duration::from_secs(5), sub.next())
                .await
                .expect("handoff timeout")
                .expect("handoff message missing");
            let request: HandoffRequest =
                serde_json::from_slice(&message.payload).expect("decode handoff request");
            assert_eq!(request.requester_instance_id, requester);
            assert_eq!(request.target_instance_id, "older-leader");
            nats.publish(
                ready_subject,
                serde_json::to_vec(&request)
                    .expect("serialize ready")
                    .into(),
            )
            .await
            .expect("publish handoff ready");
        });

        assert!(
            coordination.maybe_initiate_handoff().await?,
            "older current leader should trigger startup handoff"
        );
        responder.await?;
        Ok(())
    }

    #[sinex_test]
    async fn leader_maintenance_refreshes_metadata_without_restarting_process_future(
        ctx: TestContext,
    ) -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_COORDINATION_HEARTBEAT", "1");
        let harness = build_runtime(&ctx, "coordination-leader-heartbeat").await?;
        let mut coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let initial_last_heartbeat = coordination.current_metadata().last_heartbeat;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();
        let starts = Arc::new(AtomicUsize::new(0));
        let starts_for_task = starts.clone();

        let run_handle = tokio::spawn(async move {
            let _ = coordination
                .run_coordination_loop(move || {
                    let starts = starts_for_task.clone();
                    async move {
                        starts.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<Result<()>>().await
                    }
                })
                .await;
        });

        WaitHelpers::wait_for_condition(
            || {
                let kv_client = kv_client.clone();
                let instance_id = instance_id.clone();
                let starts = starts.clone();
                async move {
                    let metadata =
                        kv_client.get_instance(&instance_id).await?.ok_or_else(|| {
                            SinexError::processing("instance metadata missing from KV")
                        })?;
                    Ok::<bool, SinexError>(
                        starts.load(Ordering::SeqCst) == 1
                            && metadata.last_heartbeat > initial_last_heartbeat,
                    )
                }
            },
            3,
        )
        .await?;

        let metadata = kv_client
            .get_instance(&instance_id)
            .await?
            .ok_or_else(|| SinexError::processing("instance metadata missing from KV"))?;
        assert!(
            metadata.last_heartbeat > initial_last_heartbeat,
            "leader maintenance should keep refreshing last_heartbeat beyond startup registration"
        );
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "maintenance ticks must not recreate the leader process future"
        );

        run_handle.abort();
        let _ = run_handle.await;
        Ok(())
    }

    #[sinex_test]
    async fn leadership_loss_drains_and_exits_without_restarting_process_future(
        ctx: TestContext,
    ) -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_COORDINATION_HEARTBEAT", "1");
        env.set("SINEX_COORDINATION_HANDOFF", "2");
        let harness = build_runtime(&ctx, "coordination-leader-loss-drain").await?;
        let runtime_drain = harness.runtime.runtime_drain();
        let mut coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();
        let starts = Arc::new(AtomicUsize::new(0));
        let starts_for_task = starts.clone();
        let runtime_drain_for_task = runtime_drain.clone();

        let run_handle = tokio::spawn(async move {
            coordination
                .run_coordination_loop(move || {
                    let starts = starts_for_task.clone();
                    let runtime_drain = runtime_drain_for_task.clone();
                    async move {
                        starts.fetch_add(1, Ordering::SeqCst);
                        let mut drain_rx = runtime_drain.subscribe();
                        loop {
                            drain_rx.changed().await.map_err(|error| {
                                SinexError::channel_receive(format!(
                                    "runtime drain channel closed before leadership loss: {error}"
                                ))
                            })?;
                            if *drain_rx.borrow() {
                                return Ok(());
                            }
                        }
                    }
                })
                .await
        });

        WaitHelpers::wait_for_condition(
            || {
                let kv_client = kv_client.clone();
                let instance_id = instance_id.clone();
                let starts = starts.clone();
                async move {
                    Ok::<bool, SinexError>(
                        starts.load(Ordering::SeqCst) == 1
                            && kv_client.get_leader().await?.as_deref()
                                == Some(instance_id.as_str()),
                    )
                }
            },
            5,
        )
        .await?;

        kv_client.release_leadership(&instance_id).await?;
        assert!(
            kv_client.acquire_leadership("replacement-leader").await?,
            "replacement leader should take over before the old leader's next maintenance tick"
        );

        WaitHelpers::wait_for_condition(
            || {
                let runtime_drain = runtime_drain.clone();
                async move { Ok::<bool, SinexError>(runtime_drain.is_requested()) }
            },
            5,
        )
        .await?;

        let result = tokio::time::timeout(Duration::from_secs(5), run_handle).await??;
        assert!(
            result.is_ok(),
            "coordination loop should exit cleanly after draining leadership loss: {result:?}"
        );
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "leadership loss must not re-enter run_service on the same runner"
        );
        Ok(())
    }

    #[sinex_test]
    async fn handoff_waits_for_runtime_drain_before_ready(ctx: TestContext) -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_COORDINATION_HEARTBEAT", "1");
        env.set("SINEX_COORDINATION_HANDOFF", "2");

        let harness = build_runtime(&ctx, "coordination-runtime-drain").await?;
        let runtime_drain = harness.runtime.runtime_drain();
        let mut drain_rx = runtime_drain.subscribe();
        let mut coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "leader-instance".to_string())?;
        let kv_client = coordination.kv_client.clone();
        let instance_id = coordination.instance.instance_id.clone();
        let service = coordination.instance.service_name.clone();
        let target_version = coordination.instance.version.clone();
        let nats = coordination.nats_client.clone();
        let process_exited = Arc::new(AtomicUsize::new(0));
        let process_exited_for_task = process_exited.clone();
        let runtime_drain_for_task = runtime_drain.clone();

        let run_handle = tokio::spawn(async move {
            coordination
                .run_coordination_loop(move || {
                    let mut rx = runtime_drain_for_task.subscribe();
                    let process_exited = process_exited_for_task.clone();
                    async move {
                        rx.changed().await.map_err(|error| {
                            SinexError::channel_receive(format!(
                                "runtime drain channel closed before handoff: {error}"
                            ))
                        })?;
                        if *rx.borrow() {
                            process_exited.fetch_add(1, Ordering::SeqCst);
                            return Ok(());
                        }
                        std::future::pending::<Result<()>>().await
                    }
                })
                .await
        });

        WaitHelpers::wait_for_condition(
            || {
                let kv_client = kv_client.clone();
                let instance_id = instance_id.clone();
                async move {
                    Ok::<bool, SinexError>(
                        kv_client.get_leader().await?.as_deref() == Some(instance_id.as_str()),
                    )
                }
            },
            3,
        )
        .await?;

        let ready_subject = format!("sinex.coordination.{service}.handoff_ready");
        let mut ready_sub = nats.subscribe(ready_subject).await?;
        let request = HandoffRequest {
            requester_instance_id: "newer-instance".to_string(),
            requester_version: RuntimeVersion::current()?,
            target_instance_id: instance_id,
            target_version,
            requested_at: SystemTime::now(),
            timeout_seconds: Seconds::from_secs(2),
        };
        let handoff_subject = format!("sinex.coordination.{service}.handoff");
        nats.publish(handoff_subject, serde_json::to_vec(&request)?.into())
            .await?;

        tokio::time::timeout(Duration::from_secs(2), drain_rx.changed()).await??;
        assert!(
            *drain_rx.borrow(),
            "handoff must request runtime drain before readiness"
        );

        let ready = tokio::time::timeout(Duration::from_secs(3), ready_sub.next())
            .await?
            .ok_or_else(|| SinexError::processing("handoff_ready not published"))?;
        let ready_request: HandoffRequest = serde_json::from_slice(&ready.payload)?;
        assert_eq!(ready_request.requester_instance_id, "newer-instance");
        assert_eq!(
            process_exited.load(Ordering::SeqCst),
            1,
            "leader service future must exit before handoff_ready is published"
        );

        run_handle.await??;
        Ok(())
    }

    #[sinex_test]
    async fn run_coordination_loop_unregisters_instance_after_clean_exit(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-clean-exit").await?;
        let mut coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();

        coordination
            .run_coordination_loop(|| async { Ok::<(), SinexError>(()) })
            .await?;

        assert!(
            kv_client.get_instance(&instance_id).await?.is_none(),
            "clean loop exit must remove the instance registration"
        );
        Ok(())
    }

    #[sinex_test]
    async fn run_coordination_loop_propagates_leader_failures_and_unregisters(
        ctx: TestContext,
    ) -> TestResult<()> {
        let harness = build_runtime(&ctx, "coordination-fatal-exit").await?;
        let mut coordination =
            RuntimeCoordination::from_runtime(&harness.runtime, "coord-test".to_string())?;
        let instance_id = coordination.instance.instance_id.clone();
        let kv_client = coordination.kv_client.clone();

        let error = coordination
            .run_coordination_loop(|| async {
                Err::<(), _>(SinexError::service("fatal leader failure"))
            })
            .await
            .expect_err("fatal leader failure must terminate the coordination loop");
        assert!(
            error.to_string().contains("fatal leader failure"),
            "unexpected error: {error}"
        );
        assert!(
            kv_client.get_instance(&instance_id).await?.is_none(),
            "fatal loop exit must remove the instance registration"
        );
        Ok(())
    }
