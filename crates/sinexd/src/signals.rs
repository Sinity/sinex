//! Canonical signal evaluation and the host's one delivery worker.
mod authorize;
mod config;
mod evaluate;
mod outlets;
mod provenance;
use crate::api::Api;
pub(crate) use config::Config;
use sinex_model::event_types::signal::{
    CheckpointRecorded, DeliveryRecorded, DesktopReceiptRecorded, HandoffRecorded, ReceiptRecorded,
    SignalRecord,
};
use sinex_model::{Class, EventPayload, Timestamp};
use sinex_protocol::{SignalCondition, SignalConditionReply, Subjects};
use sinex_store::signals::{Checkpoint, Commit, Forwarded, ForwardedKind, PendingDelivery, Saved};
use sqlx::Connection;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn millis(at: Timestamp) -> Result<i64, String> {
    i64::try_from(at.unix_ns() / 1_000_000).map_err(error)
}

pub(crate) async fn query(
    conn: &mut sqlx::PgConnection,
    text: &str,
    ctx: &sinex_model::query::Context,
    seq: i64,
) -> Result<sinex_store::query::signals::SignalAnswer, String> {
    sinex_store::query::signals::standing_query(
        conn,
        text,
        ctx,
        seq,
        sinex_sources::sources::signals::SPEC.name,
    )
    .await
    .map_err(error)
}

pub(crate) async fn run(
    api: Api,
    client: async_nats::Client,
    subjects: Subjects,
    grants: Arc<crate::ingress::fetch::ProducerGrants>,
    settings_identity: String,
    mut commits: watch::Receiver<u64>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let config = api
        .source_config
        .signals
        .clone()
        .ok_or("signals worker requires configured host policy")?;
    let calendar = ::signals::DayCalendar::new(api.context().clock().map_err(error)?.day);
    let authorizer = Arc::new(authorize::Authorizer::new(
        api.clone(),
        client.clone(),
        subjects.clone(),
        settings_identity.clone(),
        grants.clone(),
    ));
    let (notices, mut updates, worker) =
        outlets::notifications(&api, &config, authorizer.desktop()).await?;
    let mut resumed = false;
    loop {
        if *stop.borrow() {
            break;
        }
        if worker.is_finished() {
            return worker.await.map_err(error)?.map_err(error);
        }
        let _serial = api
            .writer
            .as_ref()
            .ok_or("signals require the writer")?
            .lock_signals()
            .await;
        let mut conn = api.reads.acquire().await.map_err(error)?;
        let saved = sinex_store::signals::load(&mut conn, &api.host)
            .await
            .map_err(error)?;
        let now = Timestamp::now();
        let at = millis(now)?;
        let (revision, mut state) = match saved {
            Some(Saved {
                revision,
                checkpoint,
            }) => (Some(revision), checkpoint),
            None => (
                None,
                Checkpoint {
                    settings_identity: settings_identity.clone(),
                    evaluator: ::signals::Evaluator::new(
                        at.saturating_sub(1),
                        vec![],
                        ::signals::Policy {
                            private: config.private.clone(),
                        },
                        &calendar,
                    )
                    .map_err(error)?,
                    host_policy: ::signals::HostPolicy::new(
                        ::signals::InterruptBudget {
                            per_hour: config.interrupt_budget_per_hour,
                            handoffs: vec![],
                        },
                        config.quiet_hours,
                    ),
                    notifications: Default::default(),
                    forwarded: BTreeMap::new(),
                    outbox: BTreeMap::new(),
                    rule_events: BTreeMap::new(),
                    rules: BTreeMap::new(),
                    match_basis: BTreeMap::new(),
                    current_inputs: BTreeMap::new(),
                    rows: BTreeMap::new(),
                    attempts: BTreeMap::new(),
                    observed_seq: -1,
                    passive_count: 0,
                },
            ),
        };
        let config_changed = state.settings_identity != settings_identity;
        if config_changed {
            state.settings_identity = settings_identity.clone();
            state.host_policy.budget.per_hour = config.interrupt_budget_per_hour;
            state.host_policy.quiet_hours = config.quiet_hours;
            state
                .evaluator
                .set_policy(::signals::Policy {
                    private: config.private.clone(),
                })
                .map_err(error)?;
            state.host_policy.fold(|intent| {
                if intent.delivery.trigger.rule.product == "sinex" {
                    ::signals::Condition::Ended
                } else {
                    ::signals::Condition::Incomplete
                }
            });
            for active in state.rules.values().cloned().collect::<Vec<_>>() {
                state.evaluator.withdraw(&active.version);
                state
                    .evaluator
                    .replace_rule(active, &calendar)
                    .map_err(error)?;
            }
            state.rows.clear();
        }
        // Worker updates are acknowledged only after their exact unknown/returned
        // receipt and the private notification state commit together.
        if let Ok(update) = updates.try_recv() {
            merge_notifications(&mut state.notifications, update.state)?;
            let records = update
                .receipts
                .into_iter()
                .map(|receipt| {
                    let route = match receipt.route {
                        ::signals::notifications::Route::Dbus => "dbus",
                        ::signals::notifications::Route::Command => "command",
                        ::signals::notifications::Route::Both => "both",
                    };
                    SignalRecord::DesktopReceipt(DesktopReceiptRecorded {
                        delivery_id: receipt.delivery_id.clone(),
                        route: route.into(),
                        receipt,
                        recorded_at: now,
                    })
                })
                .collect();
            persist(
                &api,
                &mut conn,
                revision,
                None,
                state,
                records,
                vec![],
                vec![],
                None,
                Class::Guarded,
                now,
            )
            .await?;
            let _ = update.persisted.send(());
            drop(_serial);
            continue;
        }
        sinex_store::query::initialize_calendar(&mut conn)
            .await
            .map_err(error)?;
        let mut tx = conn.begin().await.map_err(error)?;
        sinex_store::query::read_one_snapshot(&mut tx)
            .await
            .map_err(error)?;
        let seq = sinex_store::query::current_seq(&mut tx)
            .await
            .map_err(error)?;
        let due = state
            .evaluator
            .next_due()
            .into_iter()
            .chain(state.host_policy.next_due())
            .filter(|due| *due > state.evaluator.now())
            .min();
        if revision.is_some() && state.observed_seq == seq && due.is_none_or(|due| due > at) {
            tx.rollback().await.map_err(error)?;
            drop(conn);
            drop(_serial);
            tokio::select! {
                _=commits.changed()=>{},
                _=stop.changed()=>{},
                update=updates.recv()=>{if let Some(update)=update {persist_update(&api,update).await?;}},
                _=wait_until(due)=>{},
            }
            continue;
        }
        if at <= state.evaluator.now() {
            tx.rollback().await.map_err(error)?;
            drop(conn);
            drop(_serial);
            wait_until(Some(state.evaluator.now().saturating_add(1))).await;
            continue;
        }
        if !sinex_store::privacy::projection::epoch(&mut tx)
            .await
            .map_err(error)?
            .ready()
        {
            tx.rollback().await.map_err(error)?;
            drop(conn);
            drop(_serial);
            tokio::select! {
                _=commits.changed()=>{},_=stop.changed()=>{},
                update=updates.recv()=>if let Some(update)=update {persist_update(&api,update).await?;},
            }
            continue;
        }
        let read_mark = sinex_store::ReadMark::now(&mut tx).await.map_err(error)?;
        let (products, class) =
            evaluate::evaluate(&api, &mut tx, &mut state, now, seq, &calendar).await?;
        let mut refreshed = provenance::refresh(&mut tx, &mut state, read_mark).await?;
        refreshed.extend(load_forwarded(&mut tx, &mut state, &grants, at).await?);
        tx.commit().await.map_err(error)?;
        let mut conditions = BTreeMap::new();
        for forwarded in state.forwarded.values() {
            if config
                .private
                .iter()
                .any(|interval| interval.start <= at && at < interval.end)
            {
                conditions.insert(
                    forwarded.intent.delivery.id.clone(),
                    ::signals::Condition::Ended,
                );
                continue;
            }
            if !forwarded.available {
                conditions.insert(
                    forwarded.intent.delivery.id.clone(),
                    ::signals::Condition::Incomplete,
                );
                continue;
            }
            let request = SignalCondition {
                delivery_id: forwarded.intent.delivery.id.clone(),
                at,
            };
            let reply = sinex_transport::replies::request_json::<SignalConditionReply>(
                &client,
                subjects.signal_condition(&forwarded.producer),
                &forwarded.producer,
                &request,
                std::time::Duration::from_millis(config.outlet_timeout_ms),
            )
            .await;
            conditions.insert(
                forwarded.intent.delivery.id.clone(),
                reply.map_or(::signals::Condition::Incomplete, |reply| reply.condition),
            );
        }
        let check = |intent: &::signals::FiredIntent| {
            conditions
                .get(&intent.delivery.id)
                .copied()
                .unwrap_or(::signals::Condition::Incomplete)
        };
        let output = if resumed {
            state.evaluator.advance_in_mode(
                at,
                &calendar,
                ::signals::EvaluationMode::Local {
                    host: &mut state.host_policy,
                    forwarded: &check,
                },
            )
        } else {
            state.evaluator.resume_in_mode(
                at,
                &calendar,
                ::signals::EvaluationMode::Local {
                    host: &mut state.host_policy,
                    forwarded: &check,
                },
            )
        }
        .map_err(error)?;
        resumed = true;
        let intents = state
            .host_policy
            .pending_intents()
            .map(|intent| {
                (
                    intent.delivery.id.clone(),
                    intent.delivery.trigger.rule.id.clone(),
                )
            })
            .chain(
                output
                    .deliveries
                    .iter()
                    .map(|delivery| (delivery.id.clone(), delivery.trigger.rule.id.clone())),
            )
            .collect::<Vec<_>>();
        for (id, rule) in intents {
            if !state.match_basis.contains_key(&id) {
                let parents = if let Some(forwarded) = state.forwarded.get(&id) {
                    vec![forwarded.event]
                } else {
                    state
                        .current_inputs
                        .get(&rule)
                        .cloned()
                        .ok_or("signal intent lacks its exact canonical input selection")?
                };
                state.match_basis.insert(
                    id,
                    sinex_store::signals::MatchBasis {
                        parents,
                        read: read_mark,
                        settings_identity: state.settings_identity.clone(),
                    },
                );
            }
        }
        let mut records = Vec::new();
        let mut decisions = Vec::new();
        for delivery in output.deliveries {
            if state.outbox.contains_key(&delivery.id) {
                continue;
            }
            let forwarded = state.forwarded.get(&delivery.id);
            let rule = state.rules.get(&delivery.trigger.rule.id).cloned();
            let notice = forwarded.map(|forwarded| forwarded.notice.clone());
            let basis = state
                .match_basis
                .get(&delivery.id)
                .ok_or("signal handoff lacks its frozen match basis")?;
            if basis.settings_identity != state.settings_identity {
                return Err("signal handoff settings differ from frozen match".into());
            }
            let mut parents = basis.parents.clone();
            parents.sort();
            parents.dedup();
            let decision = DeliveryRecorded {
                delivery_id: delivery.id.clone(),
                delivery: delivery.clone(),
                handed_at: now,
            };
            decisions.push(
                sinex_model::Event::build(
                    &decision,
                    sinex_sources::sources::signals::SPEC.name,
                    sinex_model::Origin::Derived {
                        parents: parents.clone(),
                    },
                    sinex_model::ReadingOrder(i64::try_from(now.unix_ns()).map_err(error)?),
                    *blake3::hash(&serde_json::to_vec(&decision).map_err(error)?).as_bytes(),
                    class,
                )
                .map_err(error)?,
            );
            if delivery.downgrade == Some(::signals::Downgrade::InterruptBudgetSpent) {
                state.passive_count = state
                    .passive_count
                    .checked_add(1)
                    .ok_or("passive summary count exceeds u64")?;
            }
            state.outbox.insert(
                delivery.id.clone(),
                PendingDelivery {
                    handed_at: now,
                    delivery: delivery.clone(),
                    rule,
                    notice,
                    producer: forwarded.map(|forwarded| forwarded.producer.clone()),
                    import: None,
                    settings_identity: state.settings_identity.clone(),
                    parents,
                },
            );
            records.push(SignalRecord::Handoff(HandoffRecorded {
                delivery_id: delivery.id.clone(),
                channel: delivery.channel,
                handed_at: now,
            }));
        }
        for id in refreshed {
            let pending = state
                .outbox
                .get(&id)
                .ok_or("refreshed signal lost its retained physical handoff")?;
            let decision = DeliveryRecorded {
                delivery_id: id.clone(),
                delivery: pending.delivery.clone(),
                handed_at: pending.handed_at,
            };
            decisions.push(
                sinex_model::Event::build(
                    &decision,
                    sinex_sources::sources::signals::SPEC.name,
                    sinex_model::Origin::Derived {
                        parents: pending.parents.clone(),
                    },
                    sinex_model::ReadingOrder(i64::try_from(now.unix_ns()).map_err(error)?),
                    *blake3::hash(&serde_json::to_vec(&decision).map_err(error)?).as_bytes(),
                    class,
                )
                .map_err(error)?,
            );
            records.push(SignalRecord::Handoff(HandoffRecorded {
                delivery_id: id,
                channel: pending.delivery.channel.clone(),
                handed_at: pending.handed_at,
            }));
        }
        let decision_state = state.clone();
        let result = persist(
            &api,
            &mut conn,
            revision,
            Some(seq),
            state,
            records,
            products,
            decisions,
            Some(read_mark),
            class,
            now,
        )
        .await;
        if let Err(reason) = result {
            if reason.contains("signal evaluation input snapshot changed")
                || reason.contains("signal checkpoint changed")
                || reason.contains("derivation_policy_epoch_changed")
            {
                drop(_serial);
                continue;
            }
            return Err(reason);
        }
        dispatch(&api, &config, &decision_state, &notices, &authorizer).await?;
        drop(conn);
        drop(_serial);
    }
    drop(notices);
    let mut worker = worker;
    loop {
        tokio::select! {
            result=&mut worker=>return result.map_err(error)?.map_err(error),
            update=updates.recv()=>match update {Some(update)=>persist_update(&api,update).await?,None=>return worker.await.map_err(error)?.map_err(error)},
        }
    }
}

async fn wait_until(due: Option<i64>) {
    match due {
        Some(due) => {
            let now = millis(Timestamp::now()).unwrap_or(due);
            tokio::time::sleep(std::time::Duration::from_millis(
                u64::try_from(due.saturating_sub(now)).unwrap_or(0),
            ))
            .await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn persist(
    api: &Api,
    conn: &mut sqlx::PgConnection,
    revision: Option<u64>,
    observed_seq: Option<i64>,
    state: Checkpoint,
    mut records: Vec<SignalRecord>,
    evaluations: Vec<sinex_model::Event>,
    decisions: Vec<sinex_model::Event>,
    read_mark: Option<sinex_store::ReadMark>,
    class: Class,
    at: Timestamp,
) -> Result<(), String> {
    let next = revision
        .unwrap_or(0)
        .checked_add(1)
        .ok_or("signal checkpoint revision exceeds u64")?;
    records.push(SignalRecord::Checkpoint(CheckpointRecorded {
        host: api.host.clone(),
        revision: next,
        settings_identity: state.settings_identity.clone(),
        read_as: observed_seq.unwrap_or(state.observed_seq),
        written_at: at,
    }));
    let decision_marks = decisions
        .iter()
        .map(|event| {
            let id = event.payload["delivery_id"]
                .as_str()
                .ok_or("signal decision lacks its delivery identity")?;
            let basis = state
                .match_basis
                .get(id)
                .ok_or("signal decision lacks its original input mark")?;
            Ok((id.to_owned(), basis.read))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    crate::intake::request_checkpoint(
        conn,
        api.writer.as_ref().ok_or("signals require the writer")?,
        &api.blobs,
        Arc::new(sinex_sources::sources::signals::source()),
        &format!("signals.host:{}", api.host),
        &api.host,
        sinex_sources::sources::signals::material(&records, "sinexd", "host", &api.host, at),
        Commit {
            host: api.host.clone(),
            class,
            evaluations,
            decisions,
            decision_marks,
            read_mark,
            expected_revision: revision,
            observed_seq,
            checkpoint: state,
        },
    )
    .await?;
    Ok(())
}

fn merge_notifications(
    current: &mut ::signals::notifications::NotificationState,
    mut update: ::signals::notifications::NotificationState,
) -> Result<(), String> {
    for (id, attempt) in current.attempts() {
        if update.attempt(id).is_none() {
            update
                .import_attempt(attempt.clone())
                .map_err(|error| format!("notification attempt transfer: {error:?}"))?;
        }
    }
    *current = update;
    Ok(())
}

async fn persist_update(
    api: &Api,
    update: signal_delivery::NotificationUpdate,
) -> Result<(), String> {
    let _serial = api
        .writer
        .as_ref()
        .ok_or("signals require the writer")?
        .lock_signals()
        .await;
    let mut conn = api.reads.acquire().await.map_err(error)?;
    let saved = sinex_store::signals::load(&mut conn, &api.host)
        .await
        .map_err(error)?
        .ok_or("notification worker has no committed host checkpoint")?;
    let mut state = saved.checkpoint;
    merge_notifications(&mut state.notifications, update.state)?;
    let at = Timestamp::now();
    let records = update
        .receipts
        .into_iter()
        .map(|receipt| {
            SignalRecord::DesktopReceipt(DesktopReceiptRecorded {
                delivery_id: receipt.delivery_id.clone(),
                route: format!("{:?}", receipt.route).to_lowercase(),
                receipt,
                recorded_at: at,
            })
        })
        .collect();
    persist(
        api,
        &mut conn,
        Some(saved.revision),
        None,
        state,
        records,
        vec![],
        vec![],
        None,
        Class::Guarded,
        at,
    )
    .await?;
    let _ = update.persisted.send(());
    Ok(())
}

async fn load_forwarded(
    conn: &mut sqlx::PgConnection,
    state: &mut Checkpoint,
    grants: &crate::ingress::fetch::ProducerGrants,
    at: i64,
) -> Result<Vec<String>, String> {
    let mut refreshed = Vec::new();
    let read = sinex_store::ReadMark::now(conn).await.map_err(error)?;
    for forwarded in state.forwarded.values_mut() {
        forwarded.available = false;
    }
    sinex_store::query::set_grants(conn, "guarded")
        .await
        .map_err(error)?;
    let completed = sinex_store::signals::producer_records(
        conn,
        sinex_model::event_types::signal::EmergencyCompleted::EVENT_TYPE,
        sinex_model::event_types::signal::ReceivedRecorded::EVENT_TYPE,
    )
    .await
    .map_err(error)?;
    for (event, payload, producer, family) in completed {
        let Ok(completion) =
            serde_json::from_value::<sinex_protocol::signals::EmergencyCompletion>(payload)
        else {
            continue;
        };
        let Some(family) = family else {
            continue;
        };
        let Some(write) = grants.get(&producer).and_then(|grant| grant.write.as_ref()) else {
            continue;
        };
        if !write.forward_signals
            || write.family != family
            || write.emergency_sinex_blocked_host.as_deref() != Some(completion.host.as_str())
            || write.host.as_deref() != Some(completion.host.as_str())
            || write.emergency_lease_seconds.is_none()
        {
            continue;
        }
        sinex_protocol::signals::validate_emergency(&completion, &family)?;
        let id = completion.delivery_id.clone();
        state.host_policy.submit(completion.intent.clone(), at);
        state.host_policy.fold(|intent| {
            if intent.delivery.id == id {
                ::signals::Condition::Ended
            } else {
                ::signals::Condition::Incomplete
            }
        });
        state.forwarded.entry(id.clone()).or_insert(Forwarded {
            kind: ForwardedKind::Emergency,
            available: true,
            producer,
            event,
            intent: completion.intent.clone(),
            notice: completion.notice.clone(),
        });
        state.outbox.entry(id).or_insert(PendingDelivery {
            handed_at: completion.invoked_at,
            delivery: completion.intent.delivery,
            rule: None,
            notice: Some(completion.notice),
            producer: None,
            import: Some(completion.attempt),
            settings_identity: state.settings_identity.clone(),
            parents: vec![event],
        });
    }
    let rows = sinex_store::signals::producer_records(
        conn,
        sinex_model::event_types::signal::IntentRecorded::EVENT_TYPE,
        sinex_model::event_types::signal::ReceivedRecorded::EVENT_TYPE,
    )
    .await
    .map_err(error)?;
    for (event, payload, producer, family) in rows {
        let Ok(intent) =
            serde_json::from_value::<sinex_model::event_types::signal::IntentRecorded>(payload)
        else {
            continue;
        };
        let Some(family) = family else {
            continue;
        };
        let Some((target, _)) = grants.iter().find(|(_, grant)| {
            grant.write.as_ref().is_some_and(|write| {
                write.family == family
                    && write.forward_signals
                    && !write.canonical_prefixes.is_empty()
            })
        }) else {
            continue;
        };
        if intent.intent.delivery.trigger.rule.product != family || intent.producer != producer {
            continue;
        }
        let attestation=grants[target].write.as_ref().and_then(|write|write.author_attestation.as_ref());
        if sinex_protocol::signals::authorize_author(&intent.intent,intent.active_rule.as_ref(),&family,attestation).is_err(){continue;}
        let id = intent.delivery_id;
        if let Some(previous) = state.forwarded.get_mut(&id) {
            if previous.intent == intent.intent && previous.notice == intent.notice {
                previous.producer = target.clone();
                previous.available = true;
                if previous.event != event {
                    previous.event = event;
                    state.match_basis.insert(
                        id.clone(),
                        sinex_store::signals::MatchBasis {
                            parents: vec![event],
                            read,
                            settings_identity: state.settings_identity.clone(),
                        },
                    );
                    if let Some(pending) = state.outbox.get_mut(&id) {
                        pending.parents = vec![event];
                        refreshed.push(id.clone());
                    }
                }
            }
            continue;
        }
        state.host_policy.submit(intent.intent.clone(), at);
        state.forwarded.insert(
            id,
            Forwarded {
                kind: ForwardedKind::Intent,
                available: true,
                producer: target.clone(),
                event,
                intent: intent.intent,
                notice: intent.notice,
            },
        );
    }
    Ok(refreshed)
}

async fn dispatch(
    api: &Api,
    config: &Config,
    state: &Checkpoint,
    notices: &mpsc::UnboundedSender<signal_delivery::NotificationRequest>,
    authorizer: &authorize::Authorizer,
) -> Result<(), String> {
    for pending in state.outbox.values() {
        if let Some(attempt) = &pending.import {
            if state.notifications.attempt(&pending.delivery.id).is_none() {
                let (imported, _result) = tokio::sync::oneshot::channel();
                notices
                    .send(signal_delivery::NotificationRequest::Import {
                        attempt: Box::new(attempt.clone()),
                        imported,
                    })
                    .map_err(error)?;
            }
            continue;
        }
        if pending.producer.is_some()
            && matches!(
                pending.delivery.channel,
                ::signals::Channel::Value | ::signals::Channel::Action
            )
        {
            continue;
        }
        match pending.delivery.channel {
            ::signals::Channel::Interrupt if config.notifications.is_some() => {
                if state.notifications.attempt(&pending.delivery.id).is_none() {
                    notices
                        .send(signal_delivery::NotificationRequest::Deliver(Box::new(
                            outlets::notice(pending, state.passive_count),
                        )))
                        .map_err(error)?;
                }
            }
            ::signals::Channel::Notice
                if pending.delivery.downgrade
                    == Some(::signals::Downgrade::InterruptBudgetSpent)
                    && config.notifications.is_some() =>
            {
                if state.notifications.attempt(&pending.delivery.id).is_none() {
                    notices
                        .send(signal_delivery::NotificationRequest::Deliver(Box::new(
                            outlets::notice(pending, state.passive_count),
                        )))
                        .map_err(error)?;
                }
            }
            ::signals::Channel::Value | ::signals::Channel::Action
                if !state.attempts.contains_key(&pending.delivery.id) =>
            {
                let mut conn = api.reads.acquire().await.map_err(error)?;
                let saved = sinex_store::signals::load(&mut conn, &api.host)
                    .await
                    .map_err(error)?
                    .ok_or("outlet has no committed host checkpoint")?;
                let mut latest = saved.checkpoint;
                if latest.attempts.contains_key(&pending.delivery.id) {
                    continue;
                }
                let at = Timestamp::now();
                let receipt = ::signals::DeliveryReceipt {
                    delivery_id: pending.delivery.id.clone(),
                    at: millis(at)?,
                    route: match pending.delivery.channel {
                        ::signals::Channel::Action => "action",
                        _ => "value",
                    }
                    .into(),
                    outcome: ::signals::DeliveryOutcome::Unknown,
                };
                latest
                    .attempts
                    .insert(pending.delivery.id.clone(), receipt.clone());
                persist(
                    api,
                    &mut conn,
                    Some(saved.revision),
                    None,
                    latest,
                    vec![SignalRecord::Receipt(ReceiptRecorded {
                        delivery_id: receipt.delivery_id.clone(),
                        route: receipt.route.clone(),
                        receipt: receipt.clone(),
                        recorded_at: at,
                    })],
                    vec![],
                    vec![],
                    None,
                    Class::Guarded,
                    at,
                )
                .await?;
                let mut receipt = receipt;
                receipt.outcome = if authorizer.effect(pending).await? {
                    outlets::effect(config, pending).await
                } else {
                    ::signals::DeliveryOutcome::Cancelled
                };
                let at = Timestamp::now();
                receipt.at = millis(at)?;
                let saved = sinex_store::signals::load(&mut conn, &api.host)
                    .await
                    .map_err(error)?
                    .ok_or("outlet lost its committed host checkpoint")?;
                let mut latest = saved.checkpoint;
                latest
                    .attempts
                    .insert(pending.delivery.id.clone(), receipt.clone());
                persist(
                    api,
                    &mut conn,
                    Some(saved.revision),
                    None,
                    latest,
                    vec![SignalRecord::Receipt(ReceiptRecorded {
                        delivery_id: receipt.delivery_id.clone(),
                        route: receipt.route.clone(),
                        receipt,
                        recorded_at: at,
                    })],
                    vec![],
                    vec![],
                    None,
                    Class::Guarded,
                    at,
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}
