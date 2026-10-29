//! One fresh owner snapshot authorizes every contributor to one platform operation.
use super::*;
use ::signals::notifications::{Notice, SendDecision};
use ::signals::{Condition, EvaluationMode, FiredIntent};
use signal_delivery::Authorization;
use sqlx::{Postgres, Transaction};

// Diagnostic fields are identifiers, clocks and static categories only. Error text
// can contain private SQL, configuration or payloads and is never logged here.
#[derive(Debug)]
enum Refusal {
    HostConfigurationMissing,
    DesktopConfigurationMissing,
    PolicyNotReady,
    CheckpointMissing,
    SettingsChanged,
    ClockNotAfterCheckpoint,
    PendingMissing,
    PendingSettingsChanged,
    PrivateInterval,
    DeliveryUnavailable,
    DeliveryMalformed,
    DeliveryChanged,
    ForwardedUnavailable,
    ForwardedReadingUnavailable,
    ProducerGrantMissing,
    ForwardCapabilityMissing,
    IntentMalformed,
    IntentChanged,
    NoticeChanged,
    AuthorUnverified,
    EmergencyMalformed,
    EmergencyCapabilityMissing,
    InputMissing,
    InputEpochChanged,
    InputIncomplete,
    InputClassMalformed,
    InputClassRefused,
    InputPositionMissing,
    PeerConditionMissing,
    PeerNotMatching,
    PeerEnded,
    PeerIncomplete,
    PeerSnapshotChanged,
    LocalConditionEnded,
    LocalConditionIncomplete,
    QuietHold,
    Permit,
}
#[derive(Clone, Copy, Debug)]
enum Operation {
    PreflightConnection,
    PreflightCheckpoint,
    Clock,
    PeerCondition,
    SnapshotConnection,
    CalendarInitialization,
    SnapshotBegin,
    SnapshotIsolation,
    PolicyLock,
    CheckpointLoad,
    Context,
    Calendar,
    Position,
    Evaluation,
    EvaluatorAdvance,
    SetGrants,
    DeliveryRead,
    ForwardedRead,
    InputRead,
    InputPosition,
    SnapshotRollback,
}
#[derive(Clone, Copy, Debug)]
enum ErrorClass {
    Pool,
    Query,
    Schema,
    Blob,
    Invalid,
    StaleCursor,
    Unsealed,
    Clock,
    Context,
    Evaluation,
    Database,
    Transport,
}
struct Diagnostic<'a> {
    notices: &'a [Notice],
    preflight_revision: Option<u64>,
    preflight_at_ms: Option<i64>,
    preflight_evaluator_ms: Option<i64>,
    revision: Option<u64>,
    at_ms: Option<i64>,
    evaluator_ms: Option<i64>,
}
impl<'a> Diagnostic<'a> {
    fn new(notices: &'a [Notice]) -> Self {
        Self {
            notices,
            preflight_revision: None,
            preflight_at_ms: None,
            preflight_evaluator_ms: None,
            revision: None,
            at_ms: None,
            evaluator_ms: None,
        }
    }
    fn record(&self, id: Option<&str>, reason: Refusal) {
        for notice in self
            .notices
            .iter()
            .filter(|notice| id.is_none_or(|id| notice.delivery_id == id))
        {
            eprintln!(
                "signal_authorization reason={reason:?} delivery_id={} preflight_revision={:?} preflight_at_ms={:?} preflight_evaluator_ms={:?} revision={:?} at_ms={:?} evaluator_ms={:?}",
                notice.delivery_id,
                self.preflight_revision,
                self.preflight_at_ms,
                self.preflight_evaluator_ms,
                self.revision,
                self.at_ms,
                self.evaluator_ms
            );
        }
    }
    fn failure(&self, id: Option<&str>, operation: Operation, class: ErrorClass) {
        for notice in self
            .notices
            .iter()
            .filter(|notice| id.is_none_or(|id| notice.delivery_id == id))
        {
            eprintln!(
                "signal_authorization operation={operation:?} error_class={class:?} delivery_id={} preflight_revision={:?} preflight_at_ms={:?} preflight_evaluator_ms={:?} revision={:?} at_ms={:?} evaluator_ms={:?}",
                notice.delivery_id,
                self.preflight_revision,
                self.preflight_at_ms,
                self.preflight_evaluator_ms,
                self.revision,
                self.at_ms,
                self.evaluator_ms
            );
        }
    }
    fn error(
        &self,
        operation: Operation,
        class: ErrorClass,
        failure: impl std::fmt::Display,
    ) -> String {
        self.failure(None, operation, class);
        error(failure)
    }
    fn store_error(&self, operation: Operation, failure: sinex_store::Error) -> String {
        let class = match &failure {
            sinex_store::Error::Query(_) => ErrorClass::Query,
            sinex_store::Error::Schema(_) => ErrorClass::Schema,
            sinex_store::Error::Database(_) => ErrorClass::Database,
            sinex_store::Error::Blob(_) => ErrorClass::Blob,
            sinex_store::Error::Invalid(_) => ErrorClass::Invalid,
            sinex_store::Error::StaleCursor => ErrorClass::StaleCursor,
            sinex_store::Error::Unsealed(_) => ErrorClass::Unsealed,
        };
        self.error(operation, class, failure)
    }
}

pub(super) struct Authorizer {
    api: Api,
    client: async_nats::Client,
    subjects: Subjects,
    identity: String,
    grants: Arc<crate::ingress::fetch::ProducerGrants>,
}
impl Authorizer {
    pub(super) fn new(
        api: Api,
        client: async_nats::Client,
        subjects: Subjects,
        identity: String,
        grants: Arc<crate::ingress::fetch::ProducerGrants>,
    ) -> Self {
        Self {
            api,
            client,
            subjects,
            identity,
            grants,
        }
    }
    pub(super) fn desktop(self: &Arc<Self>) -> signal_delivery::PolicyCheck {
        let owner = self.clone();
        Arc::new(move |request| {
            let owner = owner.clone();
            Box::pin(async move {
                owner
                    .batch(&request.notices, true)
                    .await
                    .unwrap_or_else(|_| Authorization {
                        decisions: request
                            .notices
                            .iter()
                            .map(|notice| (notice.delivery_id.clone(), SendDecision::Cancel))
                            .collect(),
                    })
            })
        })
    }

    pub(super) async fn effect(&self, pending: &PendingDelivery) -> Result<bool, String> {
        let mut notice = outlets::notice(pending, 0);
        notice.interrupt = false;
        let authorization = self.batch(&[notice], false).await?;
        let permitted = matches!(
            authorization.decisions.get(&pending.delivery.id),
            Some(SendDecision::Permit)
        );
        // Admission is the final current-policy check. The lock ends before
        // platform I/O, so a slow outlet cannot block a policy update.
        drop(authorization);
        Ok(permitted)
    }
    async fn batch(&self, notices: &[Notice], desktop: bool) -> Result<Authorization, String> {
        let mut diagnostic = Diagnostic::new(notices);
        let mut decisions = notices
            .iter()
            .map(|notice| (notice.delivery_id.clone(), SendDecision::Cancel))
            .collect::<BTreeMap<_, _>>();
        let config = self.api.source_config.signals.as_ref().ok_or_else(|| {
            diagnostic.record(None, Refusal::HostConfigurationMissing);
            "host signals no longer configured"
        })?;
        if desktop && config.notifications.is_none() {
            diagnostic.record(None, Refusal::DesktopConfigurationMissing);
            return Ok(Authorization { decisions });
        }
        // Peer availability is a preflight, not work performed under a SQL
        // policy lock. The fresh admission snapshot checks the same frozen
        // intent and authenticated target again after every reply.
        let observed = {
            let mut connection = self.api.reads.acquire().await.map_err(|e| {
                diagnostic.error(Operation::PreflightConnection, ErrorClass::Pool, e)
            })?;
            sinex_store::signals::load(&mut connection, &self.api.host)
                .await
                .map_err(|e| diagnostic.store_error(Operation::PreflightCheckpoint, e))?
        };
        let mut conditions = BTreeMap::new();
        if let Some(saved) = observed {
            diagnostic.preflight_revision = Some(saved.revision);
            diagnostic.preflight_evaluator_ms = Some(saved.checkpoint.evaluator.now());
            let clock = millis(Timestamp::now())
                .map_err(|e| diagnostic.error(Operation::Clock, ErrorClass::Clock, e))?;
            diagnostic.preflight_at_ms = Some(clock);
            if clock <= saved.checkpoint.evaluator.now() {
                wait_until(Some(saved.checkpoint.evaluator.now().saturating_add(1))).await;
            }
            let at = millis(Timestamp::now())
                .map_err(|e| diagnostic.error(Operation::Clock, ErrorClass::Clock, e))?;
            let peer_diagnostic = &diagnostic;
            let checks = notices
                .iter()
                .filter_map(|notice| {
                    saved
                        .checkpoint
                        .forwarded
                        .get(&notice.delivery_id)
                        .filter(|forwarded| forwarded.available)
                        .cloned()
                })
                .map(|forwarded| async move {
                    let request = SignalCondition {
                        delivery_id: forwarded.intent.delivery.id.clone(),
                        at,
                    };
                    let reply = sinex_transport::replies::request_json::<SignalConditionReply>(
                        &self.client,
                        self.subjects.signal_condition(&forwarded.producer),
                        &forwarded.producer,
                        &request,
                        std::time::Duration::from_millis(config.outlet_timeout_ms),
                    )
                    .await;
                    let matching = match reply {
                        Ok(reply) => match reply.condition {
                            Condition::Matching => true,
                            Condition::Ended => {
                                peer_diagnostic.record(
                                    Some(&forwarded.intent.delivery.id),
                                    Refusal::PeerEnded,
                                );
                                false
                            }
                            Condition::Incomplete => {
                                peer_diagnostic.record(
                                    Some(&forwarded.intent.delivery.id),
                                    Refusal::PeerIncomplete,
                                );
                                false
                            }
                        },
                        Err(_) => {
                            peer_diagnostic.failure(
                                Some(&forwarded.intent.delivery.id),
                                Operation::PeerCondition,
                                ErrorClass::Transport,
                            );
                            false
                        }
                    };
                    (forwarded.intent.delivery.id.clone(), (forwarded, matching))
                });
            conditions.extend(futures::future::join_all(checks).await);
        }
        let mut connection =
            self.api.reads.acquire().await.map_err(|e| {
                diagnostic.error(Operation::SnapshotConnection, ErrorClass::Pool, e)
            })?;
        sinex_store::query::initialize_calendar(&mut connection)
            .await
            .map_err(|e| diagnostic.store_error(Operation::CalendarInitialization, e))?;
        let mut tx: Transaction<'static, Postgres> = Transaction::begin(connection, None)
            .await
            .map_err(|e| diagnostic.error(Operation::SnapshotBegin, ErrorClass::Database, e))?;
        sinex_store::signals::effect_snapshot(&mut tx)
            .await
            .map_err(|e| diagnostic.store_error(Operation::SnapshotIsolation, e))?;
        let decisions = async {
            let epoch = sinex_store::privacy::projection::lock_epoch(&mut tx)
                .await
                .map_err(|e| diagnostic.store_error(Operation::PolicyLock, e))?;
            if !epoch.ready() {
                diagnostic.record(None, Refusal::PolicyNotReady);
                return Ok(decisions);
            }
            let Some(saved) = sinex_store::signals::load(&mut tx, &self.api.host)
                .await
                .map_err(|e| diagnostic.store_error(Operation::CheckpointLoad, e))?
            else {
                diagnostic.record(None, Refusal::CheckpointMissing);
                return Ok(decisions);
            };
            let revision = saved.revision;
            diagnostic.revision = Some(revision);
            let mut state = saved.checkpoint;
            diagnostic.evaluator_ms = Some(state.evaluator.now());
            if state.settings_identity != self.identity {
                diagnostic.record(None, Refusal::SettingsChanged);
                return Ok(decisions);
            }
            let at = Timestamp::now();
            let current_ms =
                millis(at).map_err(|e| diagnostic.error(Operation::Clock, ErrorClass::Clock, e))?;
            diagnostic.at_ms = Some(current_ms);
            if current_ms <= state.evaluator.now() {
                diagnostic.record(None, Refusal::ClockNotAfterCheckpoint);
                return Ok(decisions);
            }
            let calendar = ::signals::DayCalendar::new(
                self.api
                    .context_at(at)
                    .map_err(|e| diagnostic.error(Operation::Context, ErrorClass::Context, e))?
                    .clock()
                    .map_err(|e| diagnostic.error(Operation::Calendar, ErrorClass::Context, e))?
                    .day,
            );
            let seq = sinex_store::query::current_seq(&mut tx)
                .await
                .map_err(|e| diagnostic.store_error(Operation::Position, e))?;
            if notices
                .iter()
                .any(|notice| !state.forwarded.contains_key(&notice.delivery_id))
            {
                evaluate::evaluate(&self.api, &mut tx, &mut state, at, seq, &calendar)
                    .await
                    .map_err(|e| {
                        diagnostic.error(Operation::Evaluation, ErrorClass::Evaluation, e)
                    })?;
                state
                    .evaluator
                    .advance_in_mode(
                        millis(at).map_err(|e| {
                            diagnostic.error(Operation::Clock, ErrorClass::Clock, e)
                        })?,
                        &calendar,
                        EvaluationMode::Forward,
                    )
                    .map_err(|e| {
                        diagnostic.error(Operation::EvaluatorAdvance, ErrorClass::Evaluation, e)
                    })?;
            }
            let now =
                millis(at).map_err(|e| diagnostic.error(Operation::Clock, ErrorClass::Clock, e))?;
            for notice in notices {
                let Some(pending) = state.outbox.get(&notice.delivery_id) else {
                    diagnostic.record(Some(&notice.delivery_id), Refusal::PendingMissing);
                    continue;
                };
                if pending.settings_identity != self.identity {
                    diagnostic.record(Some(&notice.delivery_id), Refusal::PendingSettingsChanged);
                    continue;
                }
                if config
                    .private
                    .iter()
                    .any(|interval| interval.start <= now && now < interval.end)
                {
                    diagnostic.record(Some(&notice.delivery_id), Refusal::PrivateInterval);
                    continue;
                }
                if pending.import.is_none() {
                    sinex_store::query::set_grants(&mut tx, "guarded")
                        .await
                        .map_err(|e| {
                            diagnostic.error(Operation::SetGrants, ErrorClass::Database, e)
                        })?;
                    let Some((_, current)) = sinex_store::query::live_payload(
                        &mut tx,
                        DeliveryRecorded::EVENT_TYPE,
                        "delivery_id",
                        &pending.delivery.id,
                    )
                    .await
                    .map_err(|e| diagnostic.store_error(Operation::DeliveryRead, e))?
                    else {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::DeliveryUnavailable);
                        continue;
                    };
                    let Ok(reading) = serde_json::from_value::<DeliveryRecorded>(current) else {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::DeliveryMalformed);
                        continue;
                    };
                    if reading.delivery != pending.delivery {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::DeliveryChanged);
                        continue;
                    }
                }
                let matching = if let Some(forwarded) = state.forwarded.get(&notice.delivery_id) {
                    if !forwarded.available {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::ForwardedUnavailable);
                        continue;
                    }
                    let kind = match forwarded.kind {
                        ForwardedKind::Intent => {
                            sinex_model::event_types::signal::IntentRecorded::EVENT_TYPE
                        }
                        ForwardedKind::Emergency => {
                            sinex_model::event_types::signal::EmergencyCompleted::EVENT_TYPE
                        }
                    };
                    sinex_store::query::set_grants(&mut tx, "guarded")
                        .await
                        .map_err(|e| {
                            diagnostic.error(Operation::SetGrants, ErrorClass::Database, e)
                        })?;
                    let Some(visible) =
                        sinex_store::query::event_payload(&mut tx, kind, forwarded.event)
                            .await
                            .map_err(|e| diagnostic.store_error(Operation::ForwardedRead, e))?
                    else {
                        diagnostic.record(
                            Some(&notice.delivery_id),
                            Refusal::ForwardedReadingUnavailable,
                        );
                        continue;
                    };
                    let Some(grant) = self.grants.get(&forwarded.producer) else {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::ProducerGrantMissing);
                        continue;
                    };
                    let Some(write) = grant.write.as_ref().filter(|write| {
                        write.forward_signals
                            && !write.canonical_prefixes.is_empty()
                            && write.family == forwarded.intent.delivery.trigger.rule.product
                    }) else {
                        diagnostic
                            .record(Some(&notice.delivery_id), Refusal::ForwardCapabilityMissing);
                        continue;
                    };
                    let inputs = match forwarded.kind {
                        ForwardedKind::Intent => {
                            let Ok(reading) = serde_json::from_value::<
                                sinex_model::event_types::signal::IntentRecorded,
                            >(visible) else {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::IntentMalformed);
                                continue;
                            };
                            if reading.intent != forwarded.intent {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::IntentChanged);
                                continue;
                            }
                            if reading.notice != forwarded.notice {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::NoticeChanged);
                                continue;
                            }
                            if sinex_protocol::signals::authorize_author(
                                &reading.intent,
                                reading.active_rule.as_ref(),
                                &write.family,
                                write.author_attestation.as_ref(),
                            )
                            .is_err()
                            {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::AuthorUnverified);
                                continue;
                            }
                            reading.inputs
                        }
                        ForwardedKind::Emergency => {
                            let Ok(reading) = serde_json::from_value::<
                                sinex_model::event_types::signal::EmergencyCompleted,
                            >(visible) else {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::EmergencyMalformed);
                                continue;
                            };
                            if reading.intent != forwarded.intent {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::IntentChanged);
                                continue;
                            }
                            if reading.notice != forwarded.notice {
                                diagnostic
                                    .record(Some(&notice.delivery_id), Refusal::NoticeChanged);
                                continue;
                            }
                            reading.inputs
                        }
                    };
                    if matches!(forwarded.kind, ForwardedKind::Emergency)
                        && (write.emergency_sinex_blocked_host.as_deref()
                            != Some(self.api.host.as_str())
                            || write.host.as_deref() != Some(self.api.host.as_str())
                            || write.emergency_lease_seconds.is_none())
                    {
                        diagnostic.record(
                            Some(&notice.delivery_id),
                            Refusal::EmergencyCapabilityMissing,
                        );
                        continue;
                    }
                    let mut readable = true;
                    for input in inputs {
                        let Some(output) =
                            sinex_store::producer_reads::event_output(&mut tx, input.0)
                                .await
                                .map_err(|e| diagnostic.store_error(Operation::InputRead, e))?
                        else {
                            diagnostic.record(Some(&notice.delivery_id), Refusal::InputMissing);
                            readable = false;
                            break;
                        };
                        let class = output.class.and_then(|class| {
                            serde_json::from_value::<Class>(serde_json::Value::String(class)).ok()
                        });
                        let refusal = if output.epoch != Some(epoch.active) {
                            Some(Refusal::InputEpochChanged)
                        } else if output.state.as_deref() != Some("complete") {
                            Some(Refusal::InputIncomplete)
                        } else if class.is_none() {
                            Some(Refusal::InputClassMalformed)
                        } else if !class.is_some_and(|class| {
                            class != Class::Sealed && grant.allows(&output.kind, class)
                        }) {
                            Some(Refusal::InputClassRefused)
                        } else if sinex_store::signals::input_position(&mut tx, input)
                            .await
                            .map_err(|e| diagnostic.store_error(Operation::InputPosition, e))?
                            .is_none()
                        {
                            Some(Refusal::InputPositionMissing)
                        } else {
                            None
                        };
                        if let Some(reason) = refusal {
                            diagnostic.record(Some(&notice.delivery_id), reason);
                            readable = false;
                            break;
                        }
                    }
                    if !readable {
                        continue;
                    }
                    match conditions.get(&notice.delivery_id) {
                        None => {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::PeerConditionMissing);
                            false
                        }
                        Some((_, false)) => {
                            diagnostic.record(Some(&notice.delivery_id), Refusal::PeerNotMatching);
                            false
                        }
                        Some((observed, true)) => {
                            let matching = observed.event == forwarded.event
                                && observed.producer == forwarded.producer
                                && observed.intent == forwarded.intent
                                && observed.notice == forwarded.notice;
                            if !matching {
                                diagnostic.record(
                                    Some(&notice.delivery_id),
                                    Refusal::PeerSnapshotChanged,
                                );
                            }
                            matching
                        }
                    }
                } else {
                    let intent = FiredIntent {
                        delivery: pending.delivery.clone(),
                        author: pending
                            .rule
                            .as_ref()
                            .map_or(::signals::Author::Shipped, |rule| rule.author.clone()),
                        quiet_hours: None,
                        rate_limit: None,
                        current: pending
                            .rule
                            .as_ref()
                            .is_some_and(::signals::ActiveRule::current),
                    };
                    match state.evaluator.condition(&intent, now) {
                        Condition::Matching => true,
                        Condition::Ended => {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::LocalConditionEnded);
                            false
                        }
                        Condition::Incomplete => {
                            diagnostic.record(
                                Some(&notice.delivery_id),
                                Refusal::LocalConditionIncomplete,
                            );
                            false
                        }
                    }
                };
                if !matching {
                    continue;
                }
                let quiet = state
                    .forwarded
                    .get(&notice.delivery_id)
                    .and_then(|forwarded| forwarded.intent.quiet_hours)
                    .or_else(|| pending.rule.as_ref().and_then(|rule| rule.rule.quiet_hours))
                    .or(config.quiet_hours);
                let decision = if notice.interrupt
                    && let Some(until) = quiet
                        .and_then(|hours| ::signals::Calendar::quiet_until(&calendar, now, hours))
                {
                    diagnostic.record(Some(&notice.delivery_id), Refusal::QuietHold);
                    SendDecision::HoldUntil {
                        at: until,
                        policy: format!("{}:{revision}:{}:{seq}", self.identity, epoch.active),
                    }
                } else {
                    diagnostic.record(Some(&notice.delivery_id), Refusal::Permit);
                    SendDecision::Permit
                };
                decisions.insert(notice.delivery_id.clone(), decision);
            }
            Ok::<_, String>(decisions)
        }
        .await;
        tx.rollback()
            .await
            .map_err(|e| diagnostic.error(Operation::SnapshotRollback, ErrorClass::Database, e))?;
        Ok(Authorization {
            decisions: decisions?,
        })
    }
}
