//! Acquisition facts precede copied evidence in one ordinary writer transaction.
use super::*;
use sinex_model::{BatchKind, EventId, Origin, RecordOutcome};
use std::collections::BTreeMap;

pub(super) async fn admit(
    tx: &mut sinex_store::AdmissionTransaction<'_>,
    batch: &Batch,
    keys: &dyn DirtyKeys,
) -> sinex_store::Result<Outcome> {
    let is_forwarded = batch.events().any(|event| {
        event.source == sinex_sources::sources::signals::SPEC.name
            && matches!(event.origin, Origin::Derived { .. })
    }) && !matches!(batch.kind, BatchKind::Derived { .. });
    if !is_forwarded {
        return tx.admit(batch, keys).await;
    }
    if let BatchKind::Ingress(frame) = &batch.kind
        && let Some(recorded) = sinex_store::signals::recorded_frame(tx, frame).await?
    {
        return Ok(recorded);
    }

    let mut source = batch.clone();
    let mut derived = Vec::new();
    for record in &mut source.records {
        if let RecordOutcome::Events { events, .. } = record {
            let mut facts = Vec::new();
            for event in std::mem::take(events) {
                if matches!(event.origin, Origin::Derived { .. }) {
                    derived.push(event);
                } else {
                    facts.push(event);
                }
            }
            *events = facts;
        }
    }
    let mut outcome = tx.admit(&source, keys).await?;
    let mut acquisitions = BTreeMap::<EventId, (EventId, sinex_model::MaterialId)>::new();
    for event in source.events() {
        if let Some((location, _)) = event.location()
            && let Some(live) = sinex_store::signals::live_reading(tx, event).await?
        {
            acquisitions.insert(event.id, (live, location.material));
        }
    }
    let current = match sinex_store::ReadMark::now(tx).await {
        Ok(read) => Some(read),
        Err(sinex_store::Error::Invalid(reason)) if reason == "privacy_projection_stale" => None,
        Err(error) => return Err(error),
    };
    for mut event in derived {
        let Origin::Derived { parents } = &mut event.origin else {
            unreachable!()
        };
        let material = parents
            .iter()
            .find_map(|id| acquisitions.get(id).map(|(_, material)| *material));
        if let Some(material) = material
            && let Some(order) = sinex_store::signals::material_order(tx, material).await?
        {
            event.reading_order = sinex_model::ReadingOrder(order);
        }
        let read = match material {
            Some(material) => sinex_store::signals::material_read_mark(tx, material,event.payload["delivery_id"].as_str().ok_or_else(||sinex_store::Error::Invalid("copied signal lacks delivery identity".into()))?).await?,
            None => None,
        };
        let Some(read) = read.filter(|read| {
            read.policy_epoch.is_some()
                && current.is_some_and(|current| current.policy_epoch == read.policy_epoch)
        }) else {
            outcome.superseded_input += 1;
            continue;
        };
        if let BatchKind::Ingress(frame) = &batch.kind {
            let inputs = parents
                .iter()
                .filter(|id| !acquisitions.contains_key(id))
                .copied()
                .collect::<Vec<_>>();
            sinex_store::read_coverage::validate_inputs(tx, frame, &inputs).await?;
        }
        for parent in parents.iter_mut() {
            if let Some((resolved, _)) = acquisitions.get(parent) {
                *parent = *resolved;
            }
        }
        parents.sort_unstable();
        parents.dedup();
        let scope = sinex_store::DerivedScope::Keys {
            keys: vec![sinex_store::ComputedKey {
                key: serde_json::json!([event.event_type, event.payload["delivery_id"]]),
                dirty_seq: 0,
                waiting: false,
                refresh_at: None,
            }],
            read,
        };
        let product = Batch {
            run: batch.run,
            kind: BatchKind::Derived {
                derivation: "sinex.signals.forwarded".into(),
                scope: scope.to_value(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: vec![event],
            }],
            archive: vec![],
        };
        outcome += tx.admit(&product, keys).await?;
    }
    if let BatchKind::Ingress(frame) = &batch.kind {
        sinex_store::signals::finish_frame(tx, frame, &outcome).await?;
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests;
