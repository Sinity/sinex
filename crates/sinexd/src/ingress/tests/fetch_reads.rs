use super::*;
use sinex_protocol::{FetchEvent,FetchEventReply,FetchMaterial,FetchMaterialReply,FetchRefusalKind};

#[devkit::test]
async fn authenticated_fetch_uses_configured_kind_and_class_grants(db:devkit::pg::TestDatabase) {
    let grants=std::collections::BTreeMap::from([
        ("kitty".into(),crate::ingress::fetch::Grants{kind_prefixes:vec!["command".into()],classes:vec![sinex_model::Class::Open,sinex_model::Class::Guarded]}),
        ("replica".into(),crate::ingress::fetch::Grants{kind_prefixes:vec!["command.".into()],classes:vec![sinex_model::Class::Open,sinex_model::Class::Guarded]}),
        ("near-prefix".into(),crate::ingress::fetch::Grants{kind_prefixes:vec!["command.import".into()],classes:vec![sinex_model::Class::Guarded]}),
        ("open-only".into(),crate::ingress::fetch::Grants{kind_prefixes:vec!["command".into()],classes:vec![sinex_model::Class::Open]}),
    ]);
    let h=Harness::start_with_grants(db,grants).await;
    let (material,frame)=h.material_frame("fetch-material",b"retained command\n");
    assert_eq!(h.send("kitty",&frame).await.refusal,None);
    let mut event=imported(material,0,"retained command");event.class=sinex_model::Class::Guarded;
    let ack=h.send("kitty",&Ingress::Events{frame:"fetch-event".into(),events:vec![event],failures:vec![]}).await;
    assert_eq!(ack.refusal,None);let id=ack.admitted[0];
    for producer in ["replica","near-prefix","open-only","unconfigured"] {
        let bytes=serde_json::to_vec(&FetchEvent{event:id}).unwrap();
        let message=h.client.request(h.subjects.fetch_event(producer),bytes.into()).await.unwrap();
        let reply:FetchEventReply=serde_json::from_slice(&message.payload).unwrap();
        if producer=="replica" {assert_eq!(reply.refusal,None);let event=reply.event.unwrap();assert_eq!(event.payload.unwrap()["command"],"retained command");assert_eq!(Some(event.commit_seq),ack.commit_seq);}
        else {assert_eq!(reply.refusal_kind,Some(FetchRefusalKind::Denied));assert!(reply.event.is_none());}
    }
    let forged=h.client.request(h.subjects.fetch_event("unconfigured"),serde_json::to_vec(&json!({"event":id,"producer":"replica"})).unwrap().into()).await.unwrap();
    let forged:FetchEventReply=serde_json::from_slice(&forged.payload).unwrap();assert_eq!(forged.refusal_kind,Some(FetchRefusalKind::Invalid));assert!(forged.event.is_none());
    let message=h.client.request(h.subjects.fetch_material("replica"),serde_json::to_vec(&FetchMaterial{material}).unwrap().into()).await.unwrap();
    let reply:FetchMaterialReply=serde_json::from_slice(&message.payload).unwrap();assert_eq!(reply.refusal,None);assert_eq!(reply.material.unwrap().id,material);assert!(!reply.extents.is_empty());
}
