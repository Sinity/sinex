//! The host's signal deliverer. Sinex's rules are read through its own query
//! engine and evaluated by the shared evaluator; evaluator reports other
//! products forward are taken in beside them; the one host policy routes
//! every firing; the outlets show, send away, write or run what it hands off.
mod config;
mod evaluate;
mod outlets;
use crate::api::Api;
pub(crate) use config::Config;
use sinex_model::event_types::signal::{
    DeliveryRecorded, OutcomeRecorded, ReportReceived, SignalRecord,
};
use sinex_model::{Class, EventPayload, Timestamp};
use sinex_store::signals::{Checkpoint, Commit, Deliveries, Forwarded, Pending};
use sqlx::Connection;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

/// The product that owns Sinex's own rules and the deliverer's summaries.
const PRODUCT: &str = "sinex";
/// The derivation that writes deliveries.
pub(crate) const DELIVERIES: &str = "sinex.signals.deliveries";
/// Why a withdrawn handoff was not sent: the readings it cited no longer
/// stand under the current policy and lifecycle.
const BASIS_CHANGED: &str = "basis_changed";

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn millis(at: Timestamp) -> Result<i64, String> {
    i64::try_from(at.unix_ns() / 1_000_000).map_err(error)
}
fn timestamp(at: i64) -> Result<Timestamp, String> {
    Timestamp::from_unix_ns(i128::from(at) * 1_000_000).map_err(error)
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

enum Step {
    /// A step committed; its outlets may run.
    Committed,
    /// Another step committed first; start again from the saved state.
    Again,
    /// Nothing to do until the store changes, the instant passes, or the
    /// desktop deliverer reports.
    Wait(Option<i64>),
}

/// What every part of the deliverer reads: the daemon, the deployment's
/// signal settings and their public identity, the logical day and the
/// derivation run that writes deliveries.
struct Host<'a> {
    api: &'a Api,
    config: &'a Config,
    calendar: &'a ::signals::DayCalendar,
    settings_identity: &'a str,
    run: sinex_model::RunId,
}

pub(crate) async fn run(
    api: Api,
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
    let run = {
        let mut conn = api.reads.acquire().await.map_err(error)?;
        sinex_store::runs::start(
            &mut conn,
            &sinex_store::runs::Manifest {
                name: DELIVERIES,
                kind: "derivation",
                version: "1",
                build: crate::build_identity::current()?,
                emits: &[DeliveryRecorded::EVENT_TYPE],
            },
            &api.host,
        )
        .await
        .map_err(error)?
    };
    let host = Host {
        api: &api,
        config: &config,
        calendar: &calendar,
        settings_identity: &settings_identity,
        run,
    };
    let (notices, mut updates, worker) = outlets::notifications(&api, &config).await?;
    let mut resumed = false;
    // Requests sent to the desktop deliverer by this process, so a request
    // whose result is not persisted yet is not sent again.
    let mut requested = BTreeSet::new();
    let mut outcome = async {
        loop {
            if *stop.borrow() || worker.is_finished() {
                break;
            }
            // Desktop state commits before the deliverer goes on.
            if let Ok(update) = updates.try_recv() {
                persist_update(&host, update).await?;
                continue;
            }
            match step(&host, resumed).await? {
                Step::Committed => {
                    resumed = true;
                    dispatch(&host, &notices, &mut requested).await?;
                }
                Step::Again => {}
                Step::Wait(due) => tokio::select! {
                    _ = commits.changed() => {}
                    _ = stop.changed() => {}
                    update = updates.recv() => if let Some(update) = update {
                        persist_update(&host, update).await?;
                    },
                    _ = wait_until(due) => {}
                },
            }
        }
        Ok::<_, String>(())
    }
    .await;
    // Closing the channel stops the deliverer; what it reports until then is
    // still persisted.
    drop(notices);
    let mut worker = worker;
    loop {
        tokio::select! {
            result = &mut worker => return outcome.and(result.map_err(error)?),
            update = updates.recv() => match update {
                Some(update) => if let Err(failure) = persist_update(&host, update).await {
                    outcome = Err(failure);
                },
                None => return outcome.and(worker.await.map_err(error)?),
            },
        }
    }
}

/// The saved checkpoint and its revision, or a fresh one before the first
/// step commits.
async fn load(
    host: &Host<'_>,
    conn: &mut sqlx::PgConnection,
) -> Result<(Option<u64>, Checkpoint), String> {
    if let Some(saved) = sinex_store::signals::load(conn, &host.api.host)
        .await
        .map_err(error)?
    {
        return Ok((Some(saved.revision), saved.checkpoint));
    }
    let start = millis(Timestamp::now())?.saturating_sub(1);
    Ok((
        None,
        Checkpoint {
            settings_identity: host.settings_identity.into(),
            evaluator: ::signals::Evaluator::new(start, vec![], host.calendar).map_err(error)?,
            host: ::signals::HostPolicy::new(
                host.config.host_settings(),
                ::signals::InterruptBudget {
                    per_hour: host.config.interrupt_budget_per_hour,
                    handoffs: vec![],
                },
            ),
            notifications: Default::default(),
            rules: Default::default(),
            rows: Default::default(),
            inputs: Default::default(),
            open: Default::default(),
            outbox: Default::default(),
            forwarded: Default::default(),
            reports_seq: -1,
            observed_seq: -1,
        },
    ))
}

/// One step of the deliverer: evaluate the rules if the store changed, take
/// in forwarded reports, advance the evaluator and the host policy to now,
/// and commit what was handed off together with the new checkpoint.
async fn step(host: &Host<'_>, resumed: bool) -> Result<Step, String> {
    let (api, config, calendar) = (host.api, host.config, host.calendar);
    let _serial = api
        .writer
        .as_ref()
        .ok_or("signals require the writer")?
        .lock_signals()
        .await;
    let mut conn = api.reads.acquire().await.map_err(error)?;
    let (revision, mut state) = load(host, &mut conn).await?;
    let now = Timestamp::now();
    let at = millis(now)?;
    let settings_changed = state.settings_identity != host.settings_identity;
    if settings_changed {
        state.settings_identity = host.settings_identity.into();
        state.host.settings = config.host_settings();
        state.host.budget.per_hour = config.interrupt_budget_per_hour;
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
        .chain(state.host.next_due())
        .filter(|due| *due > state.evaluator.now())
        .min();
    if revision.is_some()
        && resumed
        && !settings_changed
        && state.observed_seq == seq
        && due.is_none_or(|due| due > at)
    {
        tx.rollback().await.map_err(error)?;
        return Ok(Step::Wait(due));
    }
    if at <= state.evaluator.now() {
        tx.rollback().await.map_err(error)?;
        return Ok(Step::Wait(Some(state.evaluator.now().saturating_add(1))));
    }
    if !sinex_store::privacy::projection::epoch(&mut tx)
        .await
        .map_err(error)?
        .ready()
    {
        tx.rollback().await.map_err(error)?;
        return Ok(Step::Wait(None));
    }
    let read_mark = sinex_store::ReadMark::now(&mut tx).await.map_err(error)?;
    if revision.is_none() || state.observed_seq != seq {
        evaluate::evaluate(api, &mut tx, &mut state, now, seq, read_mark, calendar).await?;
    }
    absorb_reports(&mut tx, &mut state, seq).await?;
    tx.commit().await.map_err(error)?;
    // After a restart, what was observed while down is taken in without
    // handing anything off for past instants.
    let handoffs = if resumed {
        state.evaluator.advance(at, calendar, &mut state.host)
    } else {
        state.evaluator.resume(at, calendar, &mut state.host)
    }
    .map_err(error)?;
    let (deliveries, mut records) = hand_off(config, &mut state, handoffs, read_mark, now)?;
    let changed = changed_basis(&mut conn, &deliveries).await?;
    let deliveries = withdraw(&mut state, deliveries, &mut records, &changed, now);
    let commit = Commit {
        host: api.host.clone(),
        class: Class::Guarded,
        deliveries: (!deliveries.is_empty()).then_some(Deliveries {
            run: host.run,
            events: deliveries,
        }),
        expected_revision: revision,
        observed_seq: Some(seq),
        checkpoint: state,
    };
    // A conflicting commit retries the step. A basis that changed after the
    // check above is found by the next step's check and withdrawn there, so
    // the same frozen inputs are never retried twice.
    match persist(api, &mut conn, records, commit, now).await {
        Ok(()) => Ok(Step::Committed),
        Err(reason)
            if reason.contains("signal evaluation input snapshot changed")
                || reason.contains("signal checkpoint changed")
                || reason.contains("derivation_policy_epoch_changed")
                || reason.contains("signal delivery inputs stopped being available") =>
        {
            Ok(Step::Again)
        }
        Err(reason) => Err(reason),
    }
}

/// Take in the evaluator reports other products forwarded since the last
/// step. A firing remembers the reading that carried it, which becomes its
/// delivery's parent.
async fn absorb_reports(
    conn: &mut sqlx::PgConnection,
    state: &mut Checkpoint,
    seq: i64,
) -> Result<(), String> {
    sinex_store::query::set_grants(conn, "guarded")
        .await
        .map_err(error)?;
    let reports =
        sinex_store::signals::reports(conn, ReportReceived::EVENT_TYPE, state.reports_seq, seq)
            .await
            .map_err(error)?;
    for report in reports {
        if report.payload.get("withheld").is_some() {
            continue;
        }
        let Ok(received) = serde_json::from_value::<ReportReceived>(report.payload) else {
            continue;
        };
        // A producer clears only its own product's deliveries.
        if let ::signals::Report::Cleared { delivery, .. } = &received.report {
            let product = state
                .open
                .get(delivery)
                .map(|open| &open.fired.trigger.rule.product)
                .or_else(|| {
                    state
                        .host
                        .held()
                        .find(|held| held.fired.id == *delivery)
                        .map(|held| &held.fired.trigger.rule.product)
                });
            if product != Some(&received.family) {
                continue;
            }
        }
        if let ::signals::Report::Fired(fired) = &received.report {
            state.forwarded.insert(
                fired.id.clone(),
                Forwarded {
                    report: report.event,
                    class: report.class,
                },
            );
        }
        state.host.absorb([received.report]);
    }
    state.reports_seq = seq;
    Ok(())
}

/// Turn one advance's handoffs into events and outlet work: a derived
/// delivery event per handoff, an outcome record per outcome, and the
/// notices, clears, values and actions the outlets still owe.
#[allow(clippy::type_complexity)]
fn hand_off(
    config: &Config,
    state: &mut Checkpoint,
    handoffs: ::signals::Handoffs,
    read_mark: sinex_store::ReadMark,
    now: Timestamp,
) -> Result<
    (
        Vec<(sinex_model::Event, sinex_store::ReadMark)>,
        Vec<SignalRecord>,
    ),
    String,
> {
    let mut deliveries = Vec::new();
    let mut records = Vec::new();
    let desktop = config.notifications.is_some();
    for delivery in handoffs.deliveries {
        let id = delivery.id().to_owned();
        let rule = &delivery.fired.trigger.rule;
        let own = (rule.product == PRODUCT)
            .then(|| state.rules.get(&rule.id))
            .flatten()
            .cloned();
        let (mut parents, class) = match (&own, state.forwarded.remove(&id)) {
            (Some(own), _) => (vec![own.event], own.class),
            (None, Some(forwarded)) => (vec![forwarded.report], forwarded.class),
            (None, None) => {
                return Err(format!(
                    "signal delivery {id} names neither a rule this host runs nor a forwarded report"
                ));
            }
        };
        // The readings each matched row was computed from, under the read
        // mark they were selected with.
        let mut read = read_mark;
        if let Some(inputs) = state.inputs.get(&rule.id).filter(|_| own.is_some()) {
            let matched: Vec<_> = delivery
                .fired
                .basis
                .iter()
                .filter_map(|row| inputs.get(&row.occurrence.id))
                .collect();
            // The oldest mark, so admission checks every cited reading.
            if let Some(oldest) = matched
                .iter()
                .map(|inputs| inputs.read)
                .min_by_key(|read| (read.changes, read.tombstoned))
            {
                read = oldest;
            }
            parents.extend(
                matched
                    .iter()
                    .flat_map(|inputs| inputs.parents.iter().copied()),
            );
        }
        parents.sort_unstable();
        parents.dedup();
        let recorded = DeliveryRecorded {
            delivery_id: id.clone(),
            delivery: delivery.clone(),
            handed_at: timestamp(delivery.at)?,
        };
        deliveries.push((
            sinex_model::Event::build(
                &recorded,
                sinex_sources::sources::signals::SPEC.name,
                sinex_model::Origin::Derived { parents },
                sinex_model::ReadingOrder(i64::try_from(now.unix_ns()).map_err(error)?),
                *blake3::hash(&serde_json::to_vec(&recorded).map_err(error)?).as_bytes(),
                class,
            )
            .map_err(error)?,
            read,
        ));
        match &delivery.decision {
            decision if decision.interrupts() => {
                if desktop {
                    state.outbox.insert(
                        id.clone(),
                        Pending::Notice {
                            notice: outlets::notice(&delivery, 0),
                        },
                    );
                }
            }
            ::signals::Decision::Run => {
                let reference = outlets::reference(&delivery);
                let rule = own.as_ref().map(|own| &own.active.rule);
                let pending = match delivery.fired.channel {
                    ::signals::Channel::Value => {
                        rule.and_then(|rule| rule.target.clone())
                            .map(|target| Pending::Value {
                                target,
                                delivery: Box::new(delivery.clone()),
                            })
                    }
                    ::signals::Channel::Action => rule
                        .and_then(|rule| rule.command.clone())
                        .map(|command| Pending::Action { command, reference }),
                    _ => None,
                };
                match pending {
                    Some(pending) => {
                        state.outbox.insert(id.clone(), pending);
                    }
                    // A forwarded value or action names its own product's
                    // configured outlet, which this host does not hold.
                    None => records.push(outcome(
                        &id,
                        ::signals::Outcome::Attempt {
                            result: ::signals::DeliveryOutcome::NotSent(
                                "outlet_not_configured_on_this_host".into(),
                            ),
                        },
                        now,
                    )),
                }
            }
            _ => {}
        }
        if delivery.fired.current {
            state.open.insert(id, delivery);
        }
    }
    for record in handoffs.outcomes {
        let id = record.delivery.clone();
        match &record.outcome {
            ::signals::Outcome::Cleared => {
                state.open.remove(&id);
                // A notice not yet shown is never shown; one shown is closed.
                // A notice already handed to the desktop deliverer but not yet
                // begun is closed too: requests reach it in order.
                let owed = matches!(state.outbox.get(&id), Some(Pending::Notice { .. }));
                if owed {
                    state.outbox.remove(&id);
                }
                if owed || state.notifications.attempt(&id).is_some() {
                    state.outbox.insert(id.clone(), Pending::Clear);
                }
            }
            ::signals::Outcome::Released { decision } if decision.interrupts() && desktop => {
                if let Some(delivery) = state.open.get(&id) {
                    let showing = state
                        .notifications
                        .attempt(&id)
                        .map_or(0, |attempt| attempt.notice.showing.saturating_add(1));
                    let mut released = delivery.clone();
                    released.decision = decision.clone();
                    state.outbox.insert(
                        id.clone(),
                        Pending::Notice {
                            notice: outlets::notice(&released, showing),
                        },
                    );
                }
            }
            _ => {}
        }
        records.push(outcome(&id, record.outcome, timestamp(record.at)?));
    }
    if desktop {
        for summary in handoffs.summaries {
            state.outbox.insert(
                summary.id.clone(),
                Pending::Notice {
                    notice: ::signals::notifications::Notice::for_summary(
                        &summary,
                        PRODUCT,
                        "dialog-information",
                        "status",
                    ),
                },
            );
        }
    }
    // Inputs stay while their row is answered or a held firing cites it; a
    // forwarded firing the host policy dropped needs no parent any more.
    let held: BTreeSet<&str> = state
        .host
        .held()
        .map(|held| held.fired.id.as_str())
        .collect();
    state.forwarded.retain(|id, _| held.contains(id.as_str()));
    let cited: BTreeSet<(&str, &str)> = state
        .host
        .held()
        .flat_map(|held| {
            held.fired.basis.iter().map(|row| {
                (
                    held.fired.trigger.rule.id.as_str(),
                    row.occurrence.id.as_str(),
                )
            })
        })
        .collect();
    let rows = &state.rows;
    for (rule, inputs) in &mut state.inputs {
        inputs.retain(|occurrence, _| {
            rows.get(rule)
                .is_some_and(|rows| rows.contains_key(occurrence))
                || cited.contains(&(rule.as_str(), occurrence.as_str()))
        });
    }
    state.inputs.retain(|_, inputs| !inputs.is_empty());
    Ok((deliveries, records))
}

/// The deliveries whose cited readings no longer stand: one was archived (by
/// a replay or a lifecycle step) or the privacy policy changed since they
/// were selected.
async fn changed_basis(
    conn: &mut sqlx::PgConnection,
    deliveries: &[(sinex_model::Event, sinex_store::ReadMark)],
) -> Result<BTreeSet<String>, String> {
    let mut changed = BTreeSet::new();
    for (event, read) in deliveries {
        let parents: Vec<uuid::Uuid> = match &event.origin {
            sinex_model::Origin::Derived { parents } => parents.iter().map(|id| id.0).collect(),
            sinex_model::Origin::Record { .. } => Vec::new(),
        };
        if !read.holds(conn, &parents).await.map_err(error)? {
            changed.insert(delivery_id(event)?);
        }
    }
    Ok(changed)
}

fn delivery_id(event: &sinex_model::Event) -> Result<String, String> {
    event.payload["delivery_id"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "a signal delivery lacks its identity".into())
}

/// Withdraw handoffs whose basis changed: no delivery event and no outlet
/// work, only an outcome saying why it was not sent. Admitting one would
/// certify evidence the current policy or lifecycle no longer holds, and
/// retrying it would meet the same refusal every time.
fn withdraw(
    state: &mut Checkpoint,
    deliveries: Vec<(sinex_model::Event, sinex_store::ReadMark)>,
    records: &mut Vec<SignalRecord>,
    changed: &BTreeSet<String>,
    now: Timestamp,
) -> Vec<(sinex_model::Event, sinex_store::ReadMark)> {
    if changed.is_empty() {
        return deliveries;
    }
    for id in changed {
        state.outbox.remove(id);
        state.open.remove(id);
    }
    records.retain(|record| {
        !matches!(record, SignalRecord::Outcome(recorded) if changed.contains(&recorded.delivery_id))
    });
    records.extend(changed.iter().map(|id| {
        outcome(
            id,
            ::signals::Outcome::Attempt {
                result: ::signals::DeliveryOutcome::NotSent(BASIS_CHANGED.into()),
            },
            now,
        )
    }));
    deliveries
        .into_iter()
        .filter(|(event, _)| delivery_id(event).is_ok_and(|id| !changed.contains(&id)))
        .collect()
}

fn outcome(delivery: &str, outcome: ::signals::Outcome, at: Timestamp) -> SignalRecord {
    SignalRecord::Outcome(Box::new(OutcomeRecorded {
        delivery_id: delivery.into(),
        outcome,
        recorded_at: at,
    }))
}

/// Notices already begun by the desktop deliverer leave the outbox: their
/// attempt is in its persisted state from then on.
fn settle(state: &mut Checkpoint) {
    let notifications = &state.notifications;
    state.outbox.retain(|id, pending| match pending {
        Pending::Notice { notice } => notifications
            .attempt(id)
            .is_none_or(|attempt| attempt.notice.showing < notice.showing),
        // Kept until the deliverer has closed it, or while its notice may
        // still be on the way to the deliverer.
        Pending::Clear => notifications
            .attempt(id)
            .is_none_or(|attempt| attempt.active),
        Pending::Value { .. } | Pending::Action { .. } => true,
    });
}

async fn persist(
    api: &Api,
    conn: &mut sqlx::PgConnection,
    records: Vec<SignalRecord>,
    mut commit: Commit,
    at: Timestamp,
) -> Result<(), String> {
    settle(&mut commit.checkpoint);
    let writer = api.writer.as_ref().ok_or("signals require the writer")?;
    if records.is_empty() {
        writer.submit_signals(None, commit).await.map_err(error)?;
        return Ok(());
    }
    crate::intake::request_checkpoint(
        conn,
        writer,
        &api.blobs,
        Arc::new(sinex_sources::sources::signals::source()),
        &format!("signals.host:{}", api.host),
        &api.host,
        sinex_sources::sources::signals::material(&records, "sinexd", "host", &api.host, at),
        commit,
    )
    .await?;
    Ok(())
}

/// Persist one desktop deliverer update: its state, and the outcomes its
/// receipts append to their deliveries.
async fn persist_update(
    host: &Host<'_>,
    update: signal_delivery::NotificationUpdate,
) -> Result<(), String> {
    let api = host.api;
    let _serial = api
        .writer
        .as_ref()
        .ok_or("signals require the writer")?
        .lock_signals()
        .await;
    let mut conn = api.reads.acquire().await.map_err(error)?;
    let (revision, mut state) = load(host, &mut conn).await?;
    state.notifications = update.state;
    let at = Timestamp::now();
    let now = millis(at)?;
    let records = update
        .receipts
        .iter()
        .flat_map(|receipt| receipt.outcomes(now))
        .map(|record| outcome(&record.delivery, record.outcome, at))
        .collect();
    persist(
        api,
        &mut conn,
        records,
        Commit {
            host: api.host.clone(),
            class: Class::Guarded,
            deliveries: None,
            expected_revision: revision,
            observed_seq: None,
            checkpoint: state,
        },
        at,
    )
    .await?;
    let _ = update.persisted.send(());
    Ok(())
}

/// Run the outlets the committed outbox still owes. Notices and clears go to
/// the desktop deliverer; a value or an action is recorded unknown, committed,
/// and only then run, so a restart never runs it twice.
async fn dispatch(
    host: &Host<'_>,
    notices: &mpsc::UnboundedSender<signal_delivery::NotificationRequest>,
    requested: &mut BTreeSet<String>,
) -> Result<(), String> {
    let state = {
        let mut conn = host.api.reads.acquire().await.map_err(error)?;
        load(host, &mut conn).await?.1
    };
    requested.retain(|request| {
        request
            .split(' ')
            .nth(1)
            .is_some_and(|id| state.outbox.contains_key(id))
    });
    for (id, pending) in state.outbox {
        match pending {
            Pending::Notice { notice } => {
                if requested.insert(format!("show {id} {}", notice.showing)) {
                    notices
                        .send(signal_delivery::NotificationRequest::Deliver(Box::new(
                            notice,
                        )))
                        .map_err(error)?;
                }
            }
            Pending::Clear => {
                // A clear for a notice never sent from this process, and never
                // begun, has nothing to close.
                if state.notifications.attempt(&id).is_none()
                    && !requested.iter().any(|request| {
                        request.starts_with("show ") && request.split(' ').nth(1) == Some(&id)
                    })
                {
                    forget(host, &id).await?;
                    continue;
                }
                if requested.insert(format!("clear {id}")) {
                    notices
                        .send(signal_delivery::NotificationRequest::Clear(id))
                        .map_err(error)?;
                }
            }
            effect @ (Pending::Value { .. } | Pending::Action { .. }) => {
                if !record_effect(host, &id, ::signals::DeliveryOutcome::Unknown).await? {
                    continue;
                }
                let result = outlets::effect(host.config, &effect).await;
                // An uncertain effect keeps its committed unknown.
                if result != ::signals::DeliveryOutcome::Unknown {
                    record_effect(host, &id, result).await?;
                }
            }
        }
    }
    Ok(())
}

/// Drop an outbox entry that owes nothing any more.
async fn forget(host: &Host<'_>, id: &str) -> Result<(), String> {
    let api = host.api;
    let _serial = api
        .writer
        .as_ref()
        .ok_or("signals require the writer")?
        .lock_signals()
        .await;
    let mut conn = api.reads.acquire().await.map_err(error)?;
    let (revision, mut state) = load(host, &mut conn).await?;
    if state.outbox.remove(id).is_none() {
        return Ok(());
    }
    persist(
        api,
        &mut conn,
        vec![],
        Commit {
            host: api.host.clone(),
            class: Class::Guarded,
            deliveries: None,
            expected_revision: revision,
            observed_seq: None,
            checkpoint: state,
        },
        Timestamp::now(),
    )
    .await
}

/// Record one outlet attempt for a value or action. The first record, the
/// unknown before the effect, also removes it from the outbox; it returns
/// false when another step already took it.
async fn record_effect(
    host: &Host<'_>,
    id: &str,
    result: ::signals::DeliveryOutcome,
) -> Result<bool, String> {
    let api = host.api;
    let _serial = api
        .writer
        .as_ref()
        .ok_or("signals require the writer")?
        .lock_signals()
        .await;
    let mut conn = api.reads.acquire().await.map_err(error)?;
    let (revision, mut state) = load(host, &mut conn).await?;
    if result == ::signals::DeliveryOutcome::Unknown && state.outbox.remove(id).is_none() {
        return Ok(false);
    }
    let at = Timestamp::now();
    persist(
        api,
        &mut conn,
        vec![outcome(id, ::signals::Outcome::Attempt { result }, at)],
        Commit {
            host: api.host.clone(),
            class: Class::Guarded,
            deliveries: None,
            expected_revision: revision,
            observed_seq: None,
            checkpoint: state,
        },
        at,
    )
    .await?;
    Ok(true)
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

#[cfg(test)]
mod tests;
