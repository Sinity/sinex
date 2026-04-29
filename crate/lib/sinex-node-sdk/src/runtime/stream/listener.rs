//! Long-lived listener helpers used by the runner: resubscribing NATS listener
//! loop, schema-broadcast cache hookup, and checkpoint KV bootstrap.

use super::{SchemaBroadcastCache, SchemaBroadcastEntry};
use crate::confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent};
use crate::event_node::EventTransport;
use crate::{NodeResult, SinexError};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use sinex_primitives::nats::create_or_open_kv_store;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

pub(super) const CONFIRMED_EVENT_CHANNEL_CAPACITY: usize = 1024;
pub(super) const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(super) const LISTENER_STARTUP_GRACE_PERIOD: Duration = Duration::from_secs(2);
pub(super) const TASK_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_millis(250);

pub(super) async fn run_resubscribing_listener<S, E, Subscribe, SubscribeFut, Handle, HandleFut>(
    listener: &'static str,
    subject: &str,
    retry_delay: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
    mut subscribe: Subscribe,
    mut handle_subscription: Handle,
) where
    Subscribe: FnMut() -> SubscribeFut,
    SubscribeFut: Future<Output = Result<S, E>>,
    E: std::fmt::Display,
    Handle: FnMut(S) -> HandleFut,
    HandleFut: Future<Output = bool>,
{
    loop {
        if *shutdown_rx.borrow() {
            debug!(
                listener,
                subject, "Listener shutdown requested before subscribe"
            );
            return;
        }

        let subscription = match tokio::select! {
            result = subscribe() => result,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    debug!(listener, subject, "Listener shutdown requested while waiting to subscribe");
                    return;
                }
                continue;
            }
        } {
            Ok(subscription) => subscription,
            Err(error) => {
                warn!(
                    listener,
                    subject,
                    error = %error,
                    retry_delay_ms = retry_delay.as_millis(),
                    "Listener subscribe failed; retrying"
                );
                tokio::select! {
                    () = tokio::time::sleep(retry_delay) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            debug!(listener, subject, "Listener shutdown requested during subscribe retry delay");
                            return;
                        }
                    }
                }
                continue;
            }
        };
        info!(listener, subject, "Listener subscribed");

        if handle_subscription(subscription).await {
            if *shutdown_rx.borrow() {
                debug!(
                    listener,
                    subject, "Listener shutdown requested after subscription exit"
                );
                return;
            }
            warn!(
                listener,
                subject,
                retry_delay_ms = retry_delay.as_millis(),
                "Listener subscription closed; reconnecting"
            );
            tokio::select! {
                () = tokio::time::sleep(retry_delay) => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        debug!(listener, subject, "Listener shutdown requested during retry delay");
                        return;
                    }
                }
            }
        } else {
            break;
        }
    }
}

pub(super) struct RunnerConfirmedEventHandler {
    sender: mpsc::Sender<ProvisionalEvent>,
}

impl RunnerConfirmedEventHandler {
    pub(super) fn new(sender: mpsc::Sender<ProvisionalEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl ConfirmedEventHandler for RunnerConfirmedEventHandler {
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> NodeResult<()> {
        self.sender.send(event.clone()).await.map_err(|_| {
            // Channel closed = receiver dropped = shutdown in progress.
            // Return a shutdown-specific error so callers can distinguish
            // normal shutdown from unexpected processing failures.
            SinexError::lifecycle(
                "Confirmed event channel closed (node is shutting down)".to_string(),
            )
        })
    }
}

pub(super) async fn create_checkpoint_kv(transport: &EventTransport) -> NodeResult<kv::Store> {
    // NATS KV is now mandatory
    let client = match transport {
        EventTransport::Nats(publisher) => publisher.nats_client().clone(),
    };

    let js = async_nats::jetstream::new(client);
    let env = sinex_primitives::environment::environment();
    // nats_kv_bucket_name() returns base_name (e.g. "dev_sinex_checkpoints")
    // We need to prepend "KV_" prefix for NATS bucket naming
    let bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_checkpoints"));
    let kv_store = create_or_open_kv_store(
        &js,
        kv::Config {
            bucket: bucket.clone(),
            ..Default::default()
        },
    )
    .await?;

    Ok(kv_store)
}

pub(super) async fn maybe_start_schema_listener(
    transport: &EventTransport,
) -> NodeResult<(
    Option<Arc<SchemaBroadcastCache>>,
    Option<Arc<crate::schema_validator::NodeSchemaValidator>>,
    Option<watch::Sender<bool>>,
    Option<tokio::task::JoinHandle<()>>,
)> {
    // Enable schema cache and validation when infrastructure is available.
    // Schemas are broadcast from ingestd and stored in NATS KV.
    // In edge mode (without full infrastructure), gracefully skip schema validation.

    let client = match transport {
        EventTransport::Nats(publisher) => publisher.nats_client().clone(),
    };
    let env = sinex_primitives::environment::environment();
    let subject = env.nats_subject("system.schemas.active");
    // Get KV bucket for fetching full schemas - if unavailable, skip schema validation
    let js = async_nats::jetstream::new(client.clone());
    let env = sinex_primitives::environment::environment();
    let schema_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_schemas"));
    let kv = match js.get_key_value(&schema_bucket).await {
        Ok(kv) => kv,
        Err(e) => {
            debug!("Schema KV bucket unavailable (edge mode): {e}");
            return Ok((None, None, None, None));
        }
    };

    // Create schema cache and validator
    let cache = Arc::new(SchemaBroadcastCache::default());
    let cache_clone = cache.clone();
    let validator = Arc::new(crate::schema_validator::NodeSchemaValidator::new());
    let validator_clone = validator.clone();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_ready_tx, listener_ready_rx) = oneshot::channel();
    let listener_ready_tx = Arc::new(Mutex::new(Some(listener_ready_tx)));

    // Background task to update cache and validator
    let listener_subject = subject.clone();
    let handle = tokio::spawn(async move {
        let subscribe_subject = listener_subject.clone();
        let subscribe_client = client.clone();
        let helper_shutdown_rx = shutdown_rx.clone();
        let subscription_shutdown_rx = shutdown_rx.clone();
        run_resubscribing_listener(
            "schema broadcast listener",
            &listener_subject,
            LISTENER_RETRY_DELAY,
            helper_shutdown_rx,
            move || {
                let client = subscribe_client.clone();
                let subject = subscribe_subject.clone();
                async move { client.subscribe(subject).await }
            },
            move |mut sub| {
                let cache = cache_clone.clone();
                let validator = validator_clone.clone();
                let kv = kv.clone();
                let listener_ready_tx = listener_ready_tx.clone();
                let mut shutdown_rx = subscription_shutdown_rx.clone();
                async move {
                    if let Some(listener_ready_tx) = listener_ready_tx
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                    {
                        let _ = listener_ready_tx.send(());
                    }
                    loop {
                        tokio::select! {
                            maybe_msg = sub.next() => {
                                let Some(msg) = maybe_msg else {
                                    return true;
                                };
                                match serde_json::from_slice::<Vec<SchemaBroadcastEntry>>(&msg.payload) {
                                    Ok(entries) => {
                                        cache.update(entries.clone()).await;
                                        match validator.update_from_broadcast(entries, &kv).await {
                                            Ok(count) => {
                                                debug!(count, "Updated schema validator from broadcast");
                                            }
                                            Err(err) => {
                                                warn!(error = %err, "Failed to update schema validator");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        warn!(error = %err, "Failed to decode schema broadcast payload");
                                    }
                                }
                            }
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    debug!("Schema broadcast listener subscription received shutdown");
                                    return false;
                                }
                            }
                        }
                    }
                }
            },
        )
        .await;
    });

    match tokio::time::timeout(LISTENER_STARTUP_GRACE_PERIOD, listener_ready_rx).await {
        Ok(Ok(())) => {
            debug!(
                subject,
                "Schema broadcast listener established before initialization completed"
            );
        }
        Ok(Err(_)) => {
            warn!(
                subject,
                "Schema broadcast listener ended before reporting initial readiness"
            );
        }
        Err(_) => {
            warn!(
                subject,
                startup_grace_ms = LISTENER_STARTUP_GRACE_PERIOD.as_millis(),
                "Schema broadcast listener did not report readiness before initialization completed"
            );
        }
    }

    info!("Started schema broadcast listener and validator for {subject}");

    Ok((
        Some(cache),
        Some(validator),
        Some(shutdown_tx),
        Some(handle),
    ))
}
