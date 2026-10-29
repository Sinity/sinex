//! The shared query compiler supplies query-backed keyed derivations.
use super::*;
use sinex_derivations::selected::SelectedInputs;

pub(super) async fn read(
    conn: &mut PgConnection,
    derivation: &dyn AnyKeyed,
    key: &Value,
    env: &Env,
    in_snapshot: bool,
) -> Result<Option<SelectedInputs>, String> {
    let Some(selection) = derivation.selected_query(key, env) else {return Ok(None)};
    let selected = if in_snapshot { sinex_store::query::selected_query_in_snapshot(conn,&selection).await }
        else { sinex_store::query::selected_query(conn,&selection).await }.map_err(|error| error.to_string())?;
    Ok(Some(SelectedInputs {
        read_as:selected.answer.resolved.commit_seq,
        value:serde_json::to_value(selected.answer).map_err(|error| error.to_string())?,
        parents:selected.parents,class:selected.class,signature:selected.signature,
    }))
}

pub(super) async fn stamp(
    conn: &mut PgConnection,
    derivation: &dyn AnyKeyed,
    key: &Value,
    _inputs: &[Event],
    selected: Option<&SelectedInputs>,
    position: i64,
    computed: &mut sinex_derivations::Computed,
) -> Result<(), String> {
    if !matches!(derivation.name(), "analysis.products" | "recall.products" | "context.packs") {return Ok(())}
    let mut versions_by_parents = BTreeMap::<Vec<sinex_model::EventId>, Value>::new();
    let prior = derivations::outputs(conn,derivation.name(),key).await.map_err(|error| error.to_string())?;
    let prior: BTreeMap<_,_> = prior.iter().map(|event| (record_key(event),event)).collect();
    let position = selected.map_or(position,|selected| selected.read_as);
    for output in &mut computed.outputs {
        let parents = parent_ids(output);
        let versions = if let Some(versions) = versions_by_parents.get(&parents) {
            versions.clone()
        } else {
            let versions = derivations::producer_versions(conn, &parents).await.map_err(|error| error.to_string())?;
            versions_by_parents.insert(parents, versions.clone());
            versions
        };
        output.payload["read_as"] = position.into();
        // Unchanged inputs retain their exact read context across a recompute.
        if let Some(previous) = prior.get(&record_key(output))
            && output.class==previous.class && parent_ids(output)==parent_ids(previous)
            && semantic_payload(&output.payload)==semantic_payload(&previous.payload)
            && let Some(position) = previous.payload["read_as"].as_i64() {
            output.payload["read_as"] = position.into();
            if let Some(evaluated_at) = previous.payload.get("evaluated_at") {
                output.payload["evaluated_at"] = evaluated_at.clone();
            }
            if let Some(previous) = previous.payload.pointer("/analysis/answer/resolved/commit_seq")
                && let Some(resolved) = output.payload.pointer_mut("/analysis/answer/resolved").and_then(Value::as_object_mut) {
                resolved.insert("commit_seq".into(),previous.clone());
            }
        }
        output.payload["producer_versions"] = versions;
        if let sinex_model::Origin::Derived {parents} = &output.origin {
            output.record_hash = sinex_derivations::derived_event::payload_hash(&output.payload,parents);
        }
    }
    Ok(())
}


fn record_key(event:&Event) -> (String,String) {
    (event.event_type.clone(),event.equivalence_key.as_ref().map(|key| key.0.clone()).unwrap_or_default())
}
fn parent_ids(event:&Event) -> Vec<sinex_model::EventId> {
    let mut parents = match &event.origin {sinex_model::Origin::Derived {parents}=>parents.clone(),_=>vec![]};
    parents.sort_unstable(); parents.dedup(); parents
}
fn semantic_payload(payload:&Value) -> Value {
    let mut payload = payload.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("read_as");
        object.remove("producer_versions");
        object.remove("evaluated_at");
    }
    if let Some(resolved) = payload.pointer_mut("/analysis/answer/resolved").and_then(Value::as_object_mut) {resolved.remove("commit_seq");}
    payload
}
