//! `start_command_listener` for `NodeRunner<T>`.
//!
//! Subscribes to `sinex.control.nodes.<node_name>.scan` and dispatches
//! incoming `NodeScanCommand`s to isolated replay workers. Only compiled
//! with the `messaging` feature.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    /// Start the NATS command listener for node-dispatch replay.
    ///
    /// Subscribes to `sinex.control.nodes.<node_name>.scan` using NATS request-reply.
    /// When a `NodeScanCommand` arrives, the listener:
    /// 1. Replies with `NodeScanAck` (accepted or rejected)
    /// 2. If accepted, spawns an isolated replay worker for the same node type/config
    /// 3. Publishes `NodeScanProgress` updates to `sinex.control.replay.progress.<operation_id>`
    ///
    /// Only ingestor nodes accept scan commands; automata reject them (they receive
    /// re-derived events naturally via `JetStream`).
    #[cfg(feature = "messaging")]
    pub(super) fn start_command_listener(&mut self) {
        let handles = if let Some(h) = &self.handles {
            h.clone()
        } else {
            warn!("Cannot start command listener: handles not initialized");
            return;
        };
        let service_info = if let Some(service_info) = &self.service_info {
            service_info.clone()
        } else {
            warn!("Cannot start command listener: service info not initialized");
            return;
        };
        let work_dir_utf8 = if let Some(work_dir_utf8) = &self.work_dir_utf8 {
            work_dir_utf8.clone()
        } else {
            warn!("Cannot start command listener: work dir not initialized");
            return;
        };

        let nats_client = match handles.transport() {
            EventTransport::Nats(publisher) => publisher.nats_client().clone(),
        };

        let node_name = service_info.control_identity().to_string();
        let node_type = self.node.node_type();
        let supports_historical = self.node.capabilities().supports_historical;
        let env = sinex_primitives::environment::environment().clone();
        let raw_config = self.raw_config.clone().unwrap_or_default();
        let dry_run = service_info.dry_run();
        let node_factory = self.node_factory.clone();
        let drain_controller = handles.runtime_drain();
        #[cfg(feature = "db")]
        let db_pool = handles.db_pool().cloned();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.*"));
            let active_scan = Arc::new(AtomicBool::new(false));
            let subscribe_client = nats_client.clone();
            let subscribe_subject = subject.clone();
            let helper_shutdown_rx = shutdown_rx.clone();
            let subscription_shutdown_rx = shutdown_rx.clone();

            run_resubscribing_listener(
                "command listener",
                &subject,
                LISTENER_RETRY_DELAY,
                helper_shutdown_rx,
                move || {
                    let client = subscribe_client.clone();
                    let subject = subscribe_subject.clone();
                    async move { client.subscribe(subject).await }
                },
                move |mut sub| {
                    let loop_client = nats_client.clone();
                    let loop_env = env.clone();
                    let loop_node_name = node_name.clone();
                    let loop_handles = handles.clone();
                    let loop_service_info = service_info.clone();
                    let loop_raw_config = raw_config.clone();
                    let loop_work_dir_utf8 = work_dir_utf8.clone();
                    let loop_node_factory = node_factory.clone();
                    let loop_active_scan = active_scan.clone();
                    let loop_drain_controller = drain_controller.clone();
                    #[cfg(feature = "db")]
                    let loop_db_pool = db_pool.clone();
                    let mut shutdown_rx = subscription_shutdown_rx.clone();
                    async move {
                        loop {
                            let msg = tokio::select! {
                                maybe_msg = sub.next() => {
                                    let Some(msg) = maybe_msg else {
                                        return true;
                                    };
                                    msg
                                }
                                changed = shutdown_rx.changed() => {
                                    if changed.is_err() || *shutdown_rx.borrow() {
                                        debug!(node = %loop_node_name, "Command listener subscription received shutdown");
                                        return false;
                                    }
                                    continue;
                                }
                            };
                            match control_command_kind(msg.subject.as_ref()) {
                                Some(ControlCommandKind::Drain) => {
                                    Self::handle_drain_command(
                                        &loop_node_name,
                                        &msg.payload,
                                        &loop_drain_controller,
                                        #[cfg(feature = "db")]
                                        loop_db_pool.clone(),
                                        &loop_service_info,
                                    )
                                    .await;
                                }
                                Some(ControlCommandKind::Resume) => {
                                    warn!(
                                        node = %loop_node_name,
                                        "Resume command received, but runtime drain is currently a one-way shutdown signal"
                                    );
                                }
                                Some(ControlCommandKind::Scan) => {
                                    let command: NodeScanCommand = match serde_json::from_slice(&msg.payload) {
                                        Ok(cmd) => cmd,
                                        Err(err) => {
                                            warn!(error = %err, "Failed to deserialize NodeScanCommand");
                                            if let Some(reply) = msg.reply {
                                                let nack = NodeScanAck {
                                                    operation_id: Uuid::now_v7(),
                                                    node_name: loop_node_name.clone(),
                                                    accepted: false,
                                                    error: Some(format!("Failed to deserialize command: {err}")),
                                                };
                                                if let Err(error) =
                                                    Self::publish_scan_ack(&loop_client, Some(reply), &nack).await
                                                {
                                                    warn!(
                                                        node = %loop_node_name,
                                                        error = %error,
                                                        "Failed to publish malformed-command rejection"
                                                    );
                                                }
                                            }
                                            continue;
                                        }
                                    };

                                    let operation_id = command.operation_id;
                                    let Some(reply) = msg.reply.clone() else {
                                        warn!(
                                            operation_id = %operation_id,
                                            node = %loop_node_name,
                                            "Ignoring scan command without reply subject"
                                        );
                                        continue;
                                    };

                                    if loop_drain_controller.is_requested() {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some("Node is draining and cannot accept replay scans".to_string()),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    if node_type != NodeType::Ingestor {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some(format!(
                                                "Node '{loop_node_name}' is a {node_type:?}, not an Ingestor. Automata receive replay events via JetStream."
                                            )),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    if !supports_historical {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some(format!(
                                                "Node '{loop_node_name}' does not support historical scans (supports_historical = false)"
                                            )),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    if dry_run {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some(
                                                "Node is running in dry-run mode and cannot execute replay scans"
                                                    .to_string(),
                                            ),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    let Some(factory) = loop_node_factory.clone() else {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some("Node was started without a replay worker factory".to_string()),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    };

                                    if loop_active_scan.swap(true, Ordering::SeqCst) {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some("A scan is already in progress on this node".to_string()),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    let ack = NodeScanAck {
                                        operation_id,
                                        node_name: loop_node_name.clone(),
                                        accepted: true,
                                        error: None,
                                    };
                                    if let Err(error) =
                                        Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                    {
                                        error!(
                                            operation_id = %operation_id,
                                            node = %loop_node_name,
                                            error = %error,
                                            "Failed to publish scan acceptance; aborting dispatched scan"
                                        );
                                        loop_active_scan.store(false, Ordering::SeqCst);
                                        continue;
                                    }

                                    info!(
                                        operation_id = %operation_id,
                                        node = %loop_node_name,
                                        "Accepted scan command, spawning historical scan task"
                                    );

                                    let scan_client = loop_client.clone();
                                    let scan_env = loop_env.clone();
                                    let scan_node_name = loop_node_name.clone();
                                    let scan_active = loop_active_scan.clone();
                                    let scan_handles = loop_handles.clone();
                                    let scan_service_info = loop_service_info.clone();
                                    let scan_raw_config = loop_raw_config.clone();
                                    let scan_work_dir_utf8 = loop_work_dir_utf8.clone();
                                    let scan_command = command.clone();

                                    tokio::spawn(async move {
                                        struct ActiveScanGuard(Arc<AtomicBool>);

                                        impl Drop for ActiveScanGuard {
                                            fn drop(&mut self) {
                                                self.0.store(false, Ordering::SeqCst);
                                            }
                                        }

                                        let _active_scan_guard = ActiveScanGuard(scan_active.clone());
                                        let progress_subject = scan_env
                                            .nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

                                        let start_progress = NodeScanProgress {
                                            operation_id,
                                            node_name: scan_node_name.clone(),
                                            events_processed: 0,
                                            events_emitted: 0,
                                            final_report: None,
                                            error: None,
                                        };
                                        if let Err(error) = Self::publish_scan_progress(
                                            &scan_client,
                                            progress_subject.clone(),
                                            &start_progress,
                                        )
                                        .await
                                        {
                                            error!(
                                                operation_id = %operation_id,
                                                node = %scan_node_name,
                                                error = %error,
                                                "Failed to publish initial scan progress; aborting dispatched scan"
                                            );
                                            return;
                                        }

                                        let scan_outcome = Self::execute_dispatched_scan(
                                            factory,
                                            scan_handles,
                                            scan_service_info,
                                            scan_raw_config,
                                            scan_work_dir_utf8,
                                            scan_command,
                                        )
                                        .await;

                                        let final_progress = match scan_outcome {
                                            Ok(outcome) => {
                                                let mut report = outcome.report;
                                                report
                                                    .node_stats
                                                    .entry("events_emitted".to_string())
                                                    .or_insert(outcome.events_emitted);
                                                NodeScanProgress {
                                                    operation_id,
                                                    node_name: scan_node_name.clone(),
                                                    events_processed: report.events_processed,
                                                    events_emitted: outcome.events_emitted,
                                                    final_report: Some(report),
                                                    error: None,
                                                }
                                            }
                                            Err(outcome) => {
                                                warn!(
                                                    operation_id = %operation_id,
                                                    node = %scan_node_name,
                                                    error = %outcome.error,
                                                    events_emitted = outcome.events_emitted,
                                                    "Dispatched scan failed"
                                                );
                                                NodeScanProgress {
                                                    operation_id,
                                                    node_name: scan_node_name.clone(),
                                                    events_processed: outcome.events_emitted,
                                                    events_emitted: outcome.events_emitted,
                                                    final_report: None,
                                                    error: Some(outcome.error.to_string()),
                                                }
                                            }
                                        };

                                        if let Err(error) =
                                            Self::publish_scan_progress(&scan_client, progress_subject, &final_progress)
                                                .await
                                        {
                                            error!(
                                                operation_id = %operation_id,
                                                node = %scan_node_name,
                                                error = %error,
                                                "Failed to publish final scan progress"
                                            );
                                        }
                                    });
                                }
                                None => {
                                    warn!(
                                        node = %loop_node_name,
                                        subject = %msg.subject,
                                        "Ignoring unsupported node control subject"
                                    );
                                }
                            }
                        }
                    }
                },
            )
            .await;
        });

        self.command_listener_shutdown = Some(shutdown_tx);
        self.command_listener_handle = Some(handle);
    }

}
