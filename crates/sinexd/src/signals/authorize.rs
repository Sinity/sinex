//! One fresh owner snapshot authorizes every contributor to one platform operation.
use super::*;
use ::signals::notifications::{Notice, SendDecision};
use ::signals::{Condition, EvaluationMode, FiredIntent};
use signal_delivery::{AttemptContext, Authorization, PolicyFailure};
mod snapshot;

// Diagnostic fields are identifiers, clocks and static categories only. Error text
// can contain private SQL, configuration or payloads and is never logged here.
#[derive(Debug)]
enum Refusal {
    AuthoritySettingsChanged,
    AuthorityBasisMissing,
    AuthorityEpochUnknown,
    AuthorityEpochDisagrees,
    AuthorityPendingMissing,
    AuthorityRevisionRegressed,
    AuthorityInputsChanged,
    AuthorityIntentChanged,
    AuthorityInputUnavailable,
    AuthorityPolicyEpochChanged,
    PolicyNotReady,
    HostConfigurationMissing,
    DesktopConfigurationMissing,
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
        _failure: impl std::fmt::Display,
    ) -> PolicyFailure {
        self.failure(None, operation, class);
        PolicyFailure::Unavailable
    }
    fn store_error(&self, operation: Operation, failure: sinex_store::Error) -> PolicyFailure {
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

fn frozen_notice_matches(
    offered: &Notice,
    pending: &PendingDelivery,
    historical: Option<&Notice>,
) -> bool {
    let mut pending = pending.clone();
    // An inherited held attempt already owns its exact words even if the old
    // outbox predates the explicit frozen Notice field.
    if pending.notice.is_none() {
        pending.notice = historical.cloned();
    }
    let mut offered = offered.clone();
    // The count is presentation progress; its presence still identifies the
    // passive-budget group and must match the frozen handoff decision.
    offered.budget_passive_count = offered.budget_passive_count.map(|_| 0);
    offered == outlets::notice(&pending, 0)
}

fn same_input_basis(
    original: &sinex_store::signals::MatchBasis,
    current: &sinex_store::signals::MatchBasis,
) -> bool {
    // Global archive/excision watermarks are progress, not input identity.
    // Fresh parent reads still verify the actual liveness and policy below.
    original.read.policy_epoch.is_some()
        && current.read.policy_epoch == original.read.policy_epoch
        && current.parents == original.parents
        && current.settings_identity == original.settings_identity
}

struct AuthorityBasis {
    epoch: i64,
    revision: u64,
    settings: String,
    inputs: BTreeMap<String, sinex_store::signals::MatchBasis>,
    pending: BTreeMap<String, PendingDelivery>,
}
impl AuthorityBasis {
    fn capture(
        state: &Checkpoint,
        revision: u64,
        notices: &[Notice],
        identity: &str,
        diagnostic: &Diagnostic<'_>,
    ) -> Result<Self, PolicyFailure> {
        if state.settings_identity != identity {
            diagnostic.record(None, Refusal::AuthoritySettingsChanged);
            return Err(PolicyFailure::Unavailable);
        }
        let mut epoch = None;
        let mut inputs = BTreeMap::new();
        let mut pending = BTreeMap::new();
        for notice in notices {
            let basis = state.match_basis.get(&notice.delivery_id).ok_or_else(|| {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthorityBasisMissing);
                PolicyFailure::Unavailable
            })?;
            let known = basis.read.policy_epoch.ok_or_else(|| {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthorityEpochUnknown);
                PolicyFailure::Unavailable
            })?;
            if basis.settings_identity != identity {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthoritySettingsChanged);
                return Err(PolicyFailure::Unavailable);
            }
            if epoch.is_some_and(|epoch| epoch != known) {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthorityEpochDisagrees);
                return Err(PolicyFailure::Unavailable);
            }
            epoch = Some(known);
            let outbox = state.outbox.get(&notice.delivery_id).ok_or_else(|| {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthorityPendingMissing);
                PolicyFailure::Unavailable
            })?;
            if outbox.settings_identity != identity {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthoritySettingsChanged);
                return Err(PolicyFailure::Unavailable);
            }
            let historical = state
                .notifications
                .attempt(&notice.delivery_id)
                .map(|attempt| &attempt.notice);
            if !frozen_notice_matches(notice, outbox, historical) {
                diagnostic.record(Some(&notice.delivery_id), Refusal::AuthorityIntentChanged);
                return Err(PolicyFailure::Unavailable);
            }
            inputs.insert(notice.delivery_id.clone(), basis.clone());
            pending.insert(notice.delivery_id.clone(), outbox.clone());
        }
        Ok(Self {
            epoch: epoch.ok_or(PolicyFailure::Unavailable)?,
            revision,
            settings: identity.into(),
            inputs,
            pending,
        })
    }
    fn validate(
        &self,
        state: &Checkpoint,
        revision: u64,
        diagnostic: &Diagnostic<'_>,
    ) -> Result<(), PolicyFailure> {
        if revision < self.revision {
            diagnostic.record(None, Refusal::AuthorityRevisionRegressed);
            return Err(PolicyFailure::Unavailable);
        }
        if state.settings_identity != self.settings {
            diagnostic.record(None, Refusal::AuthoritySettingsChanged);
            return Err(PolicyFailure::Unavailable);
        }
        for (id, original) in &self.inputs {
            let current = state.match_basis.get(id).ok_or_else(|| {
                diagnostic.record(Some(id), Refusal::AuthorityBasisMissing);
                PolicyFailure::Unavailable
            })?;
            if !same_input_basis(original, current) {
                diagnostic.record(Some(id), Refusal::AuthorityInputsChanged);
                return Err(PolicyFailure::Unavailable);
            }
            let frozen = self.pending.get(id).ok_or_else(|| {
                diagnostic.record(Some(id), Refusal::AuthorityPendingMissing);
                PolicyFailure::Unavailable
            })?;
            let pending = state.outbox.get(id).ok_or_else(|| {
                diagnostic.record(Some(id), Refusal::AuthorityPendingMissing);
                PolicyFailure::Unavailable
            })?;
            if pending.delivery != frozen.delivery
                || pending.rule != frozen.rule
                || pending.notice != frozen.notice
                || pending.producer != frozen.producer
                || pending.settings_identity != frozen.settings_identity
                || pending.handed_at != frozen.handed_at
                || pending.parents != frozen.parents
                || pending.import != frozen.import
            {
                diagnostic.record(Some(id), Refusal::AuthorityIntentChanged);
                return Err(PolicyFailure::Unavailable);
            }
        }
        Ok(())
    }
}

fn peer_matches(condition: Result<Condition, PolicyFailure>) -> Result<bool, PolicyFailure> {
    match condition {
        Ok(Condition::Matching) => Ok(true),
        Ok(Condition::Ended) => Ok(false),
        Ok(Condition::Incomplete) => Err(PolicyFailure::Unavailable),
        Err(failure) => Err(failure),
    }
}

type Conditions = BTreeMap<String, (Forwarded, Result<Condition, PolicyFailure>)>;

struct PeerConditions<'a> {
    client: &'a async_nats::Client,
    subjects: &'a Subjects,
    evaluator_ms: i64,
    forwarded: Vec<Forwarded>,
}
impl PeerConditions<'_> {
    async fn read(
        &self,
        attempt: &AttemptContext,
        diagnostic: &mut Diagnostic<'_>,
    ) -> Result<Conditions, PolicyFailure> {
        let clock = millis(Timestamp::now())
            .map_err(|error| diagnostic.error(Operation::Clock, ErrorClass::Clock, error))?;
        diagnostic.preflight_at_ms = Some(clock);
        if clock <= self.evaluator_ms {
            attempt
                .run(wait_until(Some(self.evaluator_ms.saturating_add(1))))
                .await
                .map_err(PolicyFailure::Interrupted)?;
        }
        let at = millis(Timestamp::now())
            .map_err(|error| diagnostic.error(Operation::Clock, ErrorClass::Clock, error))?;
        let diagnostic = &*diagnostic;
        let checks = self.forwarded.iter().cloned().map(|forwarded| async move {
            let request = SignalCondition {
                delivery_id: forwarded.intent.delivery.id.clone(),
                at,
            };
            let reply = attempt
                .run(
                    sinex_transport::replies::request_json::<SignalConditionReply>(
                        self.client,
                        self.subjects.signal_condition(&forwarded.producer),
                        &forwarded.producer,
                        &request,
                        attempt
                            .deadline()
                            .saturating_duration_since(tokio::time::Instant::now()),
                    ),
                )
                .await;
            let matching = match reply {
                Ok(Ok(reply)) => {
                    match reply.condition {
                        Condition::Ended => diagnostic
                            .record(Some(&forwarded.intent.delivery.id), Refusal::PeerEnded),
                        Condition::Incomplete => diagnostic
                            .record(Some(&forwarded.intent.delivery.id), Refusal::PeerIncomplete),
                        Condition::Matching => {}
                    }
                    Ok(reply.condition)
                }
                Ok(Err(_)) => {
                    diagnostic.failure(
                        Some(&forwarded.intent.delivery.id),
                        Operation::PeerCondition,
                        ErrorClass::Transport,
                    );
                    Err(PolicyFailure::Unavailable)
                }
                Err(interrupted) => Err(PolicyFailure::Interrupted(interrupted)),
            };
            (forwarded.intent.delivery.id.clone(), (forwarded, matching))
        });
        Ok(attempt
            .run(futures::future::join_all(checks))
            .await
            .map_err(PolicyFailure::Interrupted)?
            .into_iter()
            .collect())
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
            Box::pin(async move { owner.batch(&request.notices, true, &request.attempt).await })
        })
    }

    pub(super) async fn effect(
        &self,
        pending: &PendingDelivery,
        attempt: &AttemptContext,
    ) -> Result<Option<SendDecision>, PolicyFailure> {
        let mut notice = outlets::notice(pending, 0);
        notice.interrupt = false;
        let authorization = self.batch(&[notice], false, attempt).await?;
        Ok(authorization.decisions.get(&pending.delivery.id).cloned())
    }
    async fn batch(
        &self,
        notices: &[Notice],
        desktop: bool,
        attempt: &AttemptContext,
    ) -> Result<Authorization, PolicyFailure> {
        attempt.check().map_err(PolicyFailure::Interrupted)?;
        let mut diagnostic = Diagnostic::new(notices);
        let mut decisions = BTreeMap::new();
        let config = self.api.source_config.signals.as_ref().ok_or_else(|| {
            diagnostic.record(None, Refusal::HostConfigurationMissing);
            PolicyFailure::Unavailable
        })?;
        if desktop && config.notifications.is_none() {
            diagnostic.record(None, Refusal::DesktopConfigurationMissing);
            return Err(PolicyFailure::Unavailable);
        }
        // Peer availability is a preflight, not work performed under a SQL
        // policy lock. The fresh admission snapshot checks the same frozen
        // intent and authenticated target again after every reply.
        let observed = attempt
            .run(async {
                let mut connection = self.api.reads.acquire().await.map_err(|e| {
                    diagnostic.error(Operation::PreflightConnection, ErrorClass::Pool, e)
                })?;
                sinex_store::signals::load(&mut connection, &self.api.host)
                    .await
                    .map_err(|e| diagnostic.store_error(Operation::PreflightCheckpoint, e))
            })
            .await
            .map_err(PolicyFailure::Interrupted)??;
        let original = observed.as_ref().ok_or(PolicyFailure::Unavailable)?;
        let basis = AuthorityBasis::capture(
            &original.checkpoint,
            original.revision,
            notices,
            &self.identity,
            &diagnostic,
        )?;
        diagnostic.preflight_revision = Some(original.revision);
        diagnostic.preflight_evaluator_ms = Some(original.checkpoint.evaluator.now());
        let peers = PeerConditions {
            client: &self.client,
            subjects: &self.subjects,
            evaluator_ms: original.checkpoint.evaluator.now(),
            forwarded: notices
                .iter()
                .filter_map(|notice| original.checkpoint.forwarded.get(&notice.delivery_id))
                .filter(|forwarded| forwarded.available)
                .cloned()
                .collect(),
        };
        let (mut tx, epoch, conditions) = snapshot::open(
            &self.api.reads,
            attempt,
            basis.epoch,
            &mut diagnostic,
            Some(&peers),
        )
        .await?;
        let decisions = attempt
            .run(async {
                let Some(saved) = sinex_store::signals::load(&mut tx, &self.api.host)
                    .await
                    .map_err(|e| diagnostic.store_error(Operation::CheckpointLoad, e))?
                else {
                    diagnostic.record(None, Refusal::CheckpointMissing);
                    return Err(PolicyFailure::Unavailable);
                };
                let revision = saved.revision;
                diagnostic.revision = Some(revision);
                let mut state = saved.checkpoint;
                basis.validate(&state, revision, &diagnostic)?;
                diagnostic.evaluator_ms = Some(state.evaluator.now());
                if state.settings_identity != self.identity {
                    diagnostic.record(None, Refusal::SettingsChanged);
                    return Err(PolicyFailure::Unavailable);
                }
                let at = Timestamp::now();
                let current_ms = millis(at)
                    .map_err(|e| diagnostic.error(Operation::Clock, ErrorClass::Clock, e))?;
                diagnostic.at_ms = Some(current_ms);
                if current_ms <= state.evaluator.now() {
                    diagnostic.record(None, Refusal::ClockNotAfterCheckpoint);
                    return Err(PolicyFailure::Unavailable);
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
                let now = millis(at)
                    .map_err(|e| diagnostic.error(Operation::Clock, ErrorClass::Clock, e))?;
                for notice in notices {
                    let original = basis
                        .inputs
                        .get(&notice.delivery_id)
                        .ok_or(PolicyFailure::Unavailable)?;
                    for parent in &original.parents {
                        let output = sinex_store::producer_reads::event_output(&mut tx, parent.0)
                            .await
                            .map_err(|error| diagnostic.store_error(Operation::InputRead, error))?
                            .ok_or(PolicyFailure::Unavailable)?;
                        let class = output.class.and_then(|class| {
                            serde_json::from_value::<Class>(serde_json::Value::String(class)).ok()
                        });
                        if output.epoch != Some(basis.epoch)
                            || output.state.as_deref() != Some("complete")
                            || !class.is_some_and(|class| class != Class::Sealed)
                            || sinex_store::signals::input_position(&mut tx, *parent)
                                .await
                                .map_err(|error| {
                                    diagnostic.store_error(Operation::InputPosition, error)
                                })?
                                .is_none()
                        {
                            diagnostic.record(
                                Some(&notice.delivery_id),
                                Refusal::AuthorityInputUnavailable,
                            );
                            return Err(PolicyFailure::Unavailable);
                        }
                    }
                    let Some(pending) = state.outbox.get(&notice.delivery_id) else {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::PendingMissing);
                        return Err(PolicyFailure::Unavailable);
                    };
                    if pending.settings_identity != self.identity {
                        diagnostic
                            .record(Some(&notice.delivery_id), Refusal::PendingSettingsChanged);
                        return Err(PolicyFailure::Unavailable);
                    }
                    if config
                        .private
                        .iter()
                        .any(|interval| interval.start <= now && now < interval.end)
                    {
                        diagnostic.record(Some(&notice.delivery_id), Refusal::PrivateInterval);
                        decisions.insert(notice.delivery_id.clone(), SendDecision::Cancel);
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
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::DeliveryUnavailable);
                            return Err(PolicyFailure::Unavailable);
                        };
                        let Ok(reading) = serde_json::from_value::<DeliveryRecorded>(current)
                        else {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::DeliveryMalformed);
                            return Err(PolicyFailure::Unavailable);
                        };
                        if reading.delivery != pending.delivery {
                            diagnostic.record(Some(&notice.delivery_id), Refusal::DeliveryChanged);
                            return Err(PolicyFailure::Unavailable);
                        }
                    }
                    let matching = if let Some(forwarded) = state.forwarded.get(&notice.delivery_id)
                    {
                        if !forwarded.available {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::ForwardedUnavailable);
                            return Err(PolicyFailure::Unavailable);
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
                            return Err(PolicyFailure::Unavailable);
                        };
                        let Some(grant) = self.grants.get(&forwarded.producer) else {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::ProducerGrantMissing);
                            decisions.insert(notice.delivery_id.clone(), SendDecision::Cancel);
                            continue;
                        };
                        let Some(write) = grant.write.as_ref().filter(|write| {
                            write.forward_signals
                                && !write.canonical_prefixes.is_empty()
                                && write.family == forwarded.intent.delivery.trigger.rule.product
                        }) else {
                            diagnostic.record(
                                Some(&notice.delivery_id),
                                Refusal::ForwardCapabilityMissing,
                            );
                            decisions.insert(notice.delivery_id.clone(), SendDecision::Cancel);
                            continue;
                        };
                        let inputs = match forwarded.kind {
                            ForwardedKind::Intent => {
                                let Ok(reading) = serde_json::from_value::<
                                    sinex_model::event_types::signal::IntentRecorded,
                                >(visible) else {
                                    diagnostic.record(
                                        Some(&notice.delivery_id),
                                        Refusal::IntentMalformed,
                                    );
                                    return Err(PolicyFailure::Unavailable);
                                };
                                if reading.intent != forwarded.intent {
                                    diagnostic
                                        .record(Some(&notice.delivery_id), Refusal::IntentChanged);
                                    return Err(PolicyFailure::Unavailable);
                                }
                                if reading.notice != forwarded.notice {
                                    diagnostic
                                        .record(Some(&notice.delivery_id), Refusal::NoticeChanged);
                                    return Err(PolicyFailure::Unavailable);
                                }
                                if sinex_protocol::signals::authorize_author(
                                    &reading.intent,
                                    reading.active_rule.as_ref(),
                                    &write.family,
                                    write.author_attestation.as_ref(),
                                )
                                .is_err()
                                {
                                    diagnostic.record(
                                        Some(&notice.delivery_id),
                                        Refusal::AuthorUnverified,
                                    );
                                    decisions
                                        .insert(notice.delivery_id.clone(), SendDecision::Cancel);
                                    continue;
                                }
                                reading.inputs
                            }
                            ForwardedKind::Emergency => {
                                let Ok(reading) = serde_json::from_value::<
                                    sinex_model::event_types::signal::EmergencyCompleted,
                                >(visible) else {
                                    diagnostic.record(
                                        Some(&notice.delivery_id),
                                        Refusal::EmergencyMalformed,
                                    );
                                    return Err(PolicyFailure::Unavailable);
                                };
                                if reading.intent != forwarded.intent {
                                    diagnostic
                                        .record(Some(&notice.delivery_id), Refusal::IntentChanged);
                                    return Err(PolicyFailure::Unavailable);
                                }
                                if reading.notice != forwarded.notice {
                                    diagnostic
                                        .record(Some(&notice.delivery_id), Refusal::NoticeChanged);
                                    return Err(PolicyFailure::Unavailable);
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
                            decisions.insert(notice.delivery_id.clone(), SendDecision::Cancel);
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
                                return Err(PolicyFailure::Unavailable);
                            };
                            let class = output.class.and_then(|class| {
                                serde_json::from_value::<Class>(serde_json::Value::String(class))
                                    .ok()
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
                                let denied = matches!(reason, Refusal::InputClassRefused);
                                diagnostic.record(Some(&notice.delivery_id), reason);
                                if denied {
                                    decisions
                                        .insert(notice.delivery_id.clone(), SendDecision::Cancel);
                                    readable = false;
                                    break;
                                }
                                return Err(PolicyFailure::Unavailable);
                            }
                        }
                        if !readable {
                            continue;
                        }
                        let Some((observed, matching)) = conditions.get(&notice.delivery_id) else {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::PeerConditionMissing);
                            return Err(PolicyFailure::Unavailable);
                        };
                        if observed.event != forwarded.event
                            || observed.producer != forwarded.producer
                            || observed.intent != forwarded.intent
                            || observed.notice != forwarded.notice
                        {
                            diagnostic
                                .record(Some(&notice.delivery_id), Refusal::PeerSnapshotChanged);
                            return Err(PolicyFailure::Unavailable);
                        }
                        peer_matches(*matching)?
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
                                diagnostic.record(
                                    Some(&notice.delivery_id),
                                    Refusal::LocalConditionEnded,
                                );
                                false
                            }
                            Condition::Incomplete => {
                                diagnostic.record(
                                    Some(&notice.delivery_id),
                                    Refusal::LocalConditionIncomplete,
                                );
                                return Err(PolicyFailure::Unavailable);
                            }
                        }
                    };
                    if !matching {
                        decisions.insert(notice.delivery_id.clone(), SendDecision::Cancel);
                        continue;
                    }
                    let quiet = state
                        .forwarded
                        .get(&notice.delivery_id)
                        .and_then(|forwarded| forwarded.intent.quiet_hours)
                        .or_else(|| pending.rule.as_ref().and_then(|rule| rule.rule.quiet_hours))
                        .or(config.quiet_hours);
                    let decision = if notice.interrupt
                        && let Some(until) = quiet.and_then(|hours| {
                            ::signals::Calendar::quiet_until(&calendar, now, hours)
                        }) {
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
                Ok::<_, PolicyFailure>(decisions)
            })
            .await;
        tx.rollback()
            .await
            .map_err(|e| diagnostic.error(Operation::SnapshotRollback, ErrorClass::Database, e))?;
        Ok(Authorization {
            decisions: decisions.map_err(PolicyFailure::Interrupted)??,
        })
    }
}
