//! Synthetic complete work snapshots use stored producer declarations and
//! normal admission validation. No bundled outside payload types are restored.
use super::*;

pub(super) async fn register(url:&str){
    let mut declaration=sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef{name:"polylogue.work_snapshot".into(),version:3},"polylogue".into());
    declaration.key_rule=Some(sinex_model::kinds::KeyRule::Fields{fields:vec!["session".into(),"reducer_version".into()]});
    declaration.replace_rule=Some(sinex_model::ReplaceRule::ReplaceWhenNewer);
    let mut conn=PgConnection::connect(url).await.unwrap();
    sinex_schema::kinds::register(&mut conn,"polylogue",&[declaration]).await.unwrap();
}

pub(super) fn snapshot(session:&str,branch:&str,prompts:&[Timestamp],position:i64)->Value{
    json!({"session":session,"reducer_version":1,"source_position":position,"complete":true,
        "repo":"synthetic/repo","branch":branch,"kind":"main","mode":null,"machine":"box","cwd":["/synthetic/repo"],"objectives":[],
        "record":{"product":"work_snapshot","snapshot":{"reducer_version":1,"position":position,"session_id":session,"session":{"id":session},"retired":false,"unavailable":[],
            "messages":prompts.iter().map(|at|json!({"message":format!("{session}/{at}"),"role":"user","written_by":"human","prompt_kind":"prose","at":at,"at_evidence":{"quality":"intrinsic","precision":"instant","until":null},"source_event":"synthetic-source"})).collect::<Vec<_>>(),
            "calls":[],"turns":[],"sources":[],"native":[]}}})
}

pub(super) fn record(kind:&str,payload:Value,origin:Origin)->Event{
    assert_eq!(kind,"polylogue.work_snapshot");
    let hash=*blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes();
    let order=payload["source_position"].as_i64().unwrap();
    Event::submitted(sinex_model::KindRef{name:kind.into(),version:3},payload,"polylogue".into(),origin,ReadingOrder(order),hash,Class::Open)
}
