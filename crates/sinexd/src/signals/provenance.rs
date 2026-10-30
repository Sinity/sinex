//! Fresh compiler selections can prove another reading of a held native fact.
use super::*;

pub(super) async fn refresh(
    conn: &mut sqlx::PgConnection,
    state: &mut Checkpoint,
    read: sinex_store::ReadMark,
) -> Result<Vec<String>, String> {
    let mut candidates = state
        .host_policy
        .pending_intents()
        .filter(|intent| intent.delivery.trigger.rule.product == "sinex")
        .map(|intent| (intent.delivery.id.clone(), intent.delivery.clone()))
        .collect::<BTreeMap<_, _>>();
    for (id, pending) in &state.outbox {
        if pending.producer.is_some() || pending.import.is_some() {
            continue;
        }
        let retry = state.notifications.attempt(id).is_none_or(|attempt| {
            (!attempt.retried
                && attempt.dbus
                    == Some(::signals::DeliveryOutcome::NotSent(
                        "no_notification_server".into(),
                    )))
                || state
                    .notifications
                    .held_until(id, ::signals::notifications::Route::Dbus)
                    .is_some()
                || state
                    .notifications
                    .held_until(id, ::signals::notifications::Route::Command)
                    .is_some()
        });
        if retry && !state.attempts.contains_key(id) {
            candidates.insert(id.clone(), pending.delivery.clone());
        }
    }
    let mut refreshed = Vec::new();
    for (id, delivery) in candidates {
        let Some(basis) = state.match_basis.get(&id) else {
            continue;
        };
        if basis.settings_identity != state.settings_identity {
            continue;
        }
        let rule = &delivery.trigger.rule;
        let Some(active) = state
            .rules
            .get(&rule.id)
            .filter(|active| active.version == *rule)
        else {
            continue;
        };
        if state
            .outbox
            .get(&id)
            .and_then(|pending| pending.rule.as_ref())
            .is_some_and(|original| original != active)
        {
            continue;
        }
        let Some(inputs) = state.current_inputs.get(&rule.id) else {
            continue;
        };
        let Some(rows) = state.rows.get(&rule.id) else {
            continue;
        };
        if !delivery.basis.iter().all(|frozen| {
            rows.get(&frozen.occurrence.id).is_some_and(|current| {
                current.occurrence == frozen.occurrence
                    && current.evidence == frozen.evidence
                    && current.start == frozen.start
                    && current.end == frozen.end
            })
        }) {
            continue;
        }
        let Some(parents) = sinex_store::signals::rebind_inputs(conn, &basis.parents, inputs)
            .await
            .map_err(error)?
        else {
            continue;
        };
        if parents == basis.parents && read.policy_epoch == basis.read.policy_epoch {
            continue;
        }
        state.match_basis.insert(
            id.clone(),
            sinex_store::signals::MatchBasis {
                parents: parents.clone(),
                read,
                settings_identity: state.settings_identity.clone(),
            },
        );
        if let Some(pending) = state.outbox.get_mut(&id) {
            pending.parents = parents;
            refreshed.push(id);
        }
    }
    Ok(refreshed)
}
