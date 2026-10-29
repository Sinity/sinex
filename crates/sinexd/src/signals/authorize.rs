//! One fresh owner snapshot authorizes every contributor to one platform operation.
use super::*;
use ::signals::notifications::{Notice, SendDecision};
use ::signals::{Condition, EvaluationMode, FiredIntent};
use signal_delivery::Authorization;
use sqlx::{Postgres, Transaction};

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
        let mut decisions = notices
            .iter()
            .map(|notice| (notice.delivery_id.clone(), SendDecision::Cancel))
            .collect::<BTreeMap<_, _>>();
        let config = self
            .api
            .source_config
            .signals
            .as_ref()
            .ok_or("host signals no longer configured")?;
        if desktop && config.notifications.is_none() {
            return Ok(Authorization { decisions });
        }
        // Peer availability is a preflight, not work performed under a SQL
        // policy lock. The fresh admission snapshot checks the same frozen
        // intent and authenticated target again after every reply.
        let observed = {
            let mut connection = self.api.reads.acquire().await.map_err(error)?;
            sinex_store::signals::load(&mut connection, &self.api.host)
                .await
                .map_err(error)?
        };
        let mut conditions = BTreeMap::new();
        if let Some(saved) = observed {
            if millis(Timestamp::now())? <= saved.checkpoint.evaluator.now() {
                wait_until(Some(saved.checkpoint.evaluator.now().saturating_add(1))).await;
            }
            let at = millis(Timestamp::now())?;
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
                    let matching = sinex_transport::replies::request_json::<SignalConditionReply>(
                        &self.client,
                        self.subjects.signal_condition(&forwarded.producer),
                        &forwarded.producer,
                        &request,
                        std::time::Duration::from_millis(config.outlet_timeout_ms),
                    )
                    .await
                    .is_ok_and(|reply| reply.condition == Condition::Matching);
                    (forwarded.intent.delivery.id.clone(), (forwarded, matching))
                });
            conditions.extend(futures::future::join_all(checks).await);
        }
        let mut connection = self.api.reads.acquire().await.map_err(error)?;
        sinex_store::query::initialize_calendar(&mut connection)
            .await
            .map_err(error)?;
        let mut tx: Transaction<'static, Postgres> =
            Transaction::begin(connection, None).await.map_err(error)?;
        sinex_store::signals::effect_snapshot(&mut tx)
            .await
            .map_err(error)?;
        let decisions = async {
            let epoch = sinex_store::privacy::projection::lock_epoch(&mut tx)
                .await
                .map_err(error)?;
            if !epoch.ready() {
                return Ok(decisions);
            }
            let Some(saved) = sinex_store::signals::load(&mut tx, &self.api.host)
                .await
                .map_err(error)?
            else {
                return Ok(decisions);
            };
            let revision = saved.revision;
            let mut state = saved.checkpoint;
            if state.settings_identity != self.identity {
                return Ok(decisions);
            }
            let at = Timestamp::now();
            if millis(at)? <= state.evaluator.now() {
                return Ok(decisions);
            }
            let calendar =
                ::signals::DayCalendar::new(self.api.context_at(at)?.clock().map_err(error)?.day);
            let seq = sinex_store::query::current_seq(&mut tx)
                .await
                .map_err(error)?;
            if notices
                .iter()
                .any(|notice| !state.forwarded.contains_key(&notice.delivery_id))
            {
                evaluate::evaluate(&self.api, &mut tx, &mut state, at, seq, &calendar).await?;
                state
                    .evaluator
                    .advance_in_mode(millis(at)?, &calendar, EvaluationMode::Forward)
                    .map_err(error)?;
            }
            let now = millis(at)?;
            for notice in notices {
                let Some(pending) = state.outbox.get(&notice.delivery_id) else {
                    continue;
                };
                if pending.settings_identity != self.identity
                    || config
                        .private
                        .iter()
                        .any(|interval| interval.start <= now && now < interval.end)
                {
                    continue;
                }
                if pending.import.is_none() {
                    sinex_store::query::set_grants(&mut tx, "guarded")
                        .await
                        .map_err(error)?;
                    let Some((_, current)) = sinex_store::query::live_payload(
                        &mut tx,
                        DeliveryRecorded::EVENT_TYPE,
                        "delivery_id",
                        &pending.delivery.id,
                    )
                    .await
                    .map_err(error)?
                    else {
                        continue;
                    };
                    if !serde_json::from_value::<DeliveryRecorded>(current)
                        .ok()
                        .is_some_and(|reading| reading.delivery == pending.delivery)
                    {
                        continue;
                    }
                }
                let matching = if let Some(forwarded) = state.forwarded.get(&notice.delivery_id) {
                    if !forwarded.available {
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
                        .map_err(error)?;
                    let Some(visible) =
                        sinex_store::query::event_payload(&mut tx, kind, forwarded.event)
                            .await
                            .map_err(error)?
                    else {
                        continue;
                    };
                    let Some(grant) = self.grants.get(&forwarded.producer) else {
                        continue;
                    };
                    let Some(write) = grant.write.as_ref().filter(|write| {
                        write.forward_signals
                            && !write.canonical_prefixes.is_empty()
                            && write.family == forwarded.intent.delivery.trigger.rule.product
                    }) else {
                        continue;
                    };
                    let inputs = match forwarded.kind {
                        ForwardedKind::Intent => serde_json::from_value::<
                            sinex_model::event_types::signal::IntentRecorded,
                        >(visible)
                        .ok()
                        .filter(|reading| {
                            reading.intent == forwarded.intent
                                && reading.notice == forwarded.notice
                                && sinex_protocol::signals::authorize_author(
                                    &reading.intent,
                                    reading.active_rule.as_ref(),
                                    &write.family,
                                    write.author_attestation.as_ref(),
                                )
                                .is_ok()
                        })
                        .map(|reading| reading.inputs),
                        ForwardedKind::Emergency => serde_json::from_value::<
                            sinex_model::event_types::signal::EmergencyCompleted,
                        >(visible)
                        .ok()
                        .filter(|reading| {
                            reading.intent == forwarded.intent && reading.notice == forwarded.notice
                        })
                        .map(|reading| reading.inputs),
                    };
                    let Some(inputs) = inputs else {
                        continue;
                    };
                    if matches!(forwarded.kind, ForwardedKind::Emergency)
                        && (write.emergency_sinex_blocked_host.as_deref()
                            != Some(self.api.host.as_str())
                            || write.host.as_deref() != Some(self.api.host.as_str())
                            || write.emergency_lease_seconds.is_none())
                    {
                        continue;
                    }
                    let mut readable = true;
                    for input in inputs {
                        let Some(output) =
                            sinex_store::producer_reads::event_output(&mut tx, input.0)
                                .await
                                .map_err(error)?
                        else {
                            readable = false;
                            break;
                        };
                        let class = output.class.and_then(|class| {
                            serde_json::from_value::<Class>(serde_json::Value::String(class)).ok()
                        });
                        if output.epoch != Some(epoch.active)
                            || output.state.as_deref() != Some("complete")
                            || !class.is_some_and(|class| {
                                class != Class::Sealed && grant.allows(&output.kind, class)
                            })
                            || sinex_store::signals::input_position(&mut tx, input)
                                .await
                                .map_err(error)?
                                .is_none()
                        {
                            readable = false;
                            break;
                        }
                    }
                    if !readable {
                        continue;
                    }
                    conditions
                        .get(&notice.delivery_id)
                        .is_some_and(|(observed, matching)| {
                            *matching
                                && observed.event == forwarded.event
                                && observed.producer == forwarded.producer
                                && observed.intent == forwarded.intent
                                && observed.notice == forwarded.notice
                        })
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
                    state.evaluator.condition(&intent, now) == Condition::Matching
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
                    SendDecision::HoldUntil {
                        at: until,
                        policy: format!("{}:{revision}:{}:{seq}", self.identity, epoch.active),
                    }
                } else {
                    SendDecision::Permit
                };
                decisions.insert(notice.delivery_id.clone(), decision);
            }
            Ok::<_, String>(decisions)
        }
        .await;
        tx.rollback().await.map_err(error)?;
        Ok(Authorization {
            decisions: decisions?,
        })
    }
}
