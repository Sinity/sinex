//! Replay-worker dispatch helpers for `NodeRunner<T>`.
//!
//! These helpers run an isolated replay worker for a `NodeScanCommand`
//! received over the command listener: spawn a fresh node instance with
//! its own handles, drive its scan, forward emitted events, and finalize
//! the worker on completion.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    #[cfg(feature = "messaging")]
    pub(super) async fn execute_dispatched_scan(
        node_factory: NodeFactory<T>,
        base_handles: NodeHandles,
        base_service_info: ServiceInfo,
        raw_config: HashMap<String, serde_json::Value>,
        work_dir_utf8: Utf8PathBuf,
        command: NodeScanCommand,
    ) -> Result<DispatchedScanOutcome, FailedDispatchedScanOutcome> {
        let replay_service_name = format!(
            "{}.replay.{}",
            base_service_info.service_name(),
            command.operation_id.simple()
        );
        let replay_service_info = ServiceInfo::new_with_runtime_identity(
            replay_service_name.clone(),
            base_service_info.node_name().to_string(),
            base_service_info.source_unit_id().map(ToOwned::to_owned),
            base_service_info.runner_pack().map(ToOwned::to_owned),
            base_service_info.host().clone(),
            base_service_info.work_dir().clone(),
            base_service_info.dry_run(),
            base_service_info.instance_id().to_string(),
            base_service_info.version().to_string(),
            base_service_info.node_run_id(),
        );

        let (replay_handles, emitted_counter, forwarder_handle) =
            Self::build_replay_worker_handles(
                &base_handles,
                &replay_service_name,
                command.operation_id,
            )
            .await
            .map_err(|error| FailedDispatchedScanOutcome {
                error,
                events_emitted: 0,
            })?;

        let typed_config = if raw_config.is_empty() {
            T::Config::default()
        } else {
            let config_value =
                serde_json::to_value(&raw_config).map_err(|error| FailedDispatchedScanOutcome {
                    error: SinexError::configuration(format!(
                        "Failed to serialize replay worker config: {error}"
                    )),
                    events_emitted: 0,
                })?;
            serde_json::from_value(config_value).map_err(|error| FailedDispatchedScanOutcome {
                error: SinexError::configuration(format!(
                    "Failed to parse replay worker config: {error}"
                )),
                events_emitted: 0,
            })?
        };

        let init_context = NodeInitContext::new(
            typed_config,
            raw_config,
            replay_service_info,
            replay_handles,
            work_dir_utf8,
        );

        let mut worker = node_factory();
        if let Err(error) = worker.initialize(init_context).await {
            return Err(FailedDispatchedScanOutcome {
                error,
                events_emitted: 0,
            });
        }

        let scan_result = worker
            .scan(command.from.clone(), command.until.clone(), command.args)
            .await;
        let shutdown_result = worker.shutdown().await;
        drop(worker);

        let forwarder_result =
            Self::finish_replay_forwarder(forwarder_handle, emitted_counter).await;

        match (scan_result, shutdown_result, forwarder_result) {
            (Ok(report), Ok(()), Ok(events_emitted)) => Ok(DispatchedScanOutcome {
                report,
                events_emitted,
            }),
            (Err(error), Ok(()), Ok(events_emitted)) => Err(FailedDispatchedScanOutcome {
                error,
                events_emitted,
            }),
            (Ok(_), Err(error), Ok(events_emitted)) => Err(FailedDispatchedScanOutcome {
                error,
                events_emitted,
            }),
            (Err(scan_error), Err(shutdown_error), Ok(events_emitted)) => {
                Err(FailedDispatchedScanOutcome {
                    error: scan_error.with_context("shutdown_error", shutdown_error.to_string()),
                    events_emitted,
                })
            }
            (Ok(_), Ok(()), Err(forwarder_error)) => Err(forwarder_error),
            (Err(scan_error), Ok(()), Err(forwarder_error)) => Err(FailedDispatchedScanOutcome {
                error: scan_error
                    .with_context("replay_forwarder_error", forwarder_error.error.to_string()),
                events_emitted: forwarder_error.events_emitted,
            }),
            (Ok(_), Err(shutdown_error), Err(forwarder_error)) => {
                Err(FailedDispatchedScanOutcome {
                    error: shutdown_error
                        .with_context("replay_forwarder_error", forwarder_error.error.to_string()),
                    events_emitted: forwarder_error.events_emitted,
                })
            }
            (Err(scan_error), Err(shutdown_error), Err(forwarder_error)) => {
                Err(FailedDispatchedScanOutcome {
                    error: scan_error
                        .with_context("shutdown_error", shutdown_error.to_string())
                        .with_context("replay_forwarder_error", forwarder_error.error.to_string()),
                    events_emitted: forwarder_error.events_emitted,
                })
            }
        }
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn build_replay_worker_handles(
        base_handles: &NodeHandles,
        replay_service_name: &str,
        operation_id: Uuid,
    ) -> NodeResult<(
        NodeHandles,
        Arc<AtomicU64>,
        tokio::task::JoinHandle<NodeResult<()>>,
    )> {
        let checkpoint_kv = create_checkpoint_kv(base_handles.transport()).await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            checkpoint_kv,
            replay_service_name.to_string(),
            format!("replay-{}", operation_id.simple()),
            format!("dispatch-{}", operation_id.simple()),
        ));

        let (replay_sender, mut replay_receiver) =
            mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
        let replay_emitter = base_handles
            .emitter()
            .clone_with_sender(replay_sender)
            .with_default_created_by_operation_id(operation_id);
        let target_sender = base_handles.emitter().sender();
        let emitted_counter = Arc::new(AtomicU64::new(0));
        let counter = emitted_counter.clone();
        let forwarder_handle = tokio::spawn(async move {
            while let Some(event) = replay_receiver.recv().await {
                target_sender.send(event).await.map_err(|_| {
                    SinexError::processing("Replay forwarder target channel closed".to_string())
                })?;
                counter.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        });

        let confirmation_buffer = base_handles.confirmation_buffer();
        let schema_cache = base_handles.schema_cache();
        #[cfg(feature = "db")]
        let replay_handles = match base_handles.db_pool().cloned() {
            Some(db_pool) => NodeHandles::new(
                db_pool,
                checkpoint_manager,
                replay_emitter,
                base_handles.transport().clone(),
                confirmation_buffer,
                schema_cache,
            ),
            None => NodeHandles::new_edge(
                checkpoint_manager,
                replay_emitter,
                base_handles.transport().clone(),
                confirmation_buffer,
                schema_cache,
            ),
        };
        #[cfg(not(feature = "db"))]
        let replay_handles = NodeHandles::new_edge(
            checkpoint_manager,
            replay_emitter,
            base_handles.transport().clone(),
            confirmation_buffer,
            schema_cache,
        );

        Ok((replay_handles, emitted_counter, forwarder_handle))
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn finish_replay_forwarder(
        forwarder_handle: tokio::task::JoinHandle<NodeResult<()>>,
        emitted_counter: Arc<AtomicU64>,
    ) -> Result<u64, FailedDispatchedScanOutcome> {
        let events_emitted = emitted_counter.load(Ordering::SeqCst);
        match forwarder_handle.await {
            Ok(Ok(())) => Ok(events_emitted),
            Ok(Err(error)) => Err(FailedDispatchedScanOutcome {
                error,
                events_emitted,
            }),
            Err(join_error) => Err(FailedDispatchedScanOutcome {
                error: SinexError::processing("Replay forwarder join failed".to_string())
                    .with_std_error(&join_error),
                events_emitted,
            }),
        }
    }

}
