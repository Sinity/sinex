//! Producer versions and read positions stamped on analysis outputs.
use super::*;
use crate::Result;

pub(super) async fn stamp(
    conn: &mut PgConnection,
    derivation: &dyn AnyKeyed,
    key: &Value,
    position: i64,
    computed: &mut sinex_derivations::Computed,
) -> Result<()> {
    if !matches!(derivation.name(), "analysis.products" | "context.packs") {
        return Ok(());
    }
    let mut versions_by_parents = BTreeMap::<Vec<sinex_model::EventId>, Value>::new();
    let prior = derivations::outputs(conn, derivation.name(), key).await?;
    let prior: BTreeMap<_, _> = prior
        .iter()
        .map(|event| (record_key(event), event))
        .collect();
    for output in &mut computed.outputs {
        let parents = parent_ids(output);
        let versions = if let Some(versions) = versions_by_parents.get(&parents) {
            versions.clone()
        } else {
            let versions = derivations::producer_versions(conn, &parents).await?;
            versions_by_parents.insert(parents, versions.clone());
            versions
        };
        output.payload["read_as"] = position.into();
        // Unchanged inputs retain their exact read context across a recompute.
        if let Some(previous) = prior.get(&record_key(output))
            && output.class == previous.class
            && parent_ids(output) == parent_ids(previous)
            && semantic_payload(&output.payload) == semantic_payload(&previous.payload)
            && let Some(position) = previous.payload["read_as"].as_i64()
        {
            output.payload["read_as"] = position.into();
            if let Some(evaluated_at) = previous.payload.get("evaluated_at") {
                output.payload["evaluated_at"] = evaluated_at.clone();
            }
        }
        output.payload["producer_versions"] = versions;
        if let sinex_model::Origin::Derived { parents } = &output.origin {
            output.record_hash =
                sinex_derivations::derived_event::payload_hash(&output.payload, parents);
        }
    }
    Ok(())
}

fn record_key(event: &Event) -> (String, String) {
    (
        event.event_type.clone(),
        event
            .equivalence_key
            .as_ref()
            .map(|key| key.0.clone())
            .unwrap_or_default(),
    )
}
fn parent_ids(event: &Event) -> Vec<sinex_model::EventId> {
    let mut parents = match &event.origin {
        sinex_model::Origin::Derived { parents } => parents.clone(),
        _ => vec![],
    };
    parents.sort_unstable();
    parents.dedup();
    parents
}
fn semantic_payload(payload: &Value) -> Value {
    let mut payload = payload.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("read_as");
        object.remove("producer_versions");
        object.remove("evaluated_at");
    }
    payload
}
