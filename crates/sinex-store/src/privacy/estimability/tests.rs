use super::*;
use crate::privacy::{Action,Matcher,Rule,RuleSet};
use serde_json::json;
use sinex_model::{Class,Origin,ReadingOrder};
use std::collections::HashMap;

fn event()->Event {
    Event::submitted(KindRef{name:"fixture.input".into(),version:2},
        json!({"reference":"task:17","number":42,"text":"[masked]","unrelated":"prose","kind":"screen","width":100}),
        "fixture".into(),Origin::Derived{parents:vec![]},ReadingOrder(0),[0;32],Class::Open)
}
fn apply(event:&mut Event,path:&str,action:Action)->Vec<AffectedPath>{
    let rules=RuleSet::new(vec![Rule{id:uuid::Uuid::nil(),name:"configured".into(),source:"fixture".into(),event_type:Some("fixture.input".into()),
        field:Some(path.into()),matcher:Matcher::All,class:Class::Open,action}],HashMap::new(),[0;32]);
    let result=rules.apply(event).unwrap();
    assert_eq!(result.changed,action.transforms());result.trace
}
fn contract(paths:&[&str])->Vec<InputRequirement>{vec![InputRequirement::new(KindRef{name:"fixture.input".into(),version:2},paths.iter().copied())]}

#[devkit::test]
fn action_trace_distinguishes_required_reference_numeric_and_same_value_masks(){
    for path in ["/reference","/number","/text"] {
        let mut input=event();let before=input.payload.clone();let trace=apply(&mut input,path,Action::Mask);
        if path=="/text"{assert_eq!(input.payload,before,"same-value mask still has transformation provenance")}
        assert_eq!(trace[0].path,path);
        assert_eq!(reason(&input,&trace,Some(&contract(&[path]))),Some(InputUnavailableReason::RequiredPathTransformed));
    }
}
#[devkit::test]
fn unrelated_fields_and_classification_remain_estimable_but_unknown_contract_does_not(){
    let mut input=event();let trace=apply(&mut input,"/unrelated",Action::Mask);
    assert_eq!(reason(&input,&trace,Some(&contract(&["/number"]))),None);
    assert_eq!(reason(&input,&trace,None),Some(InputUnavailableReason::UndeclaredAffectedInput));
    let wrong=vec![InputRequirement::new(KindRef{name:input.event_type.clone(),version:1},["/number"])];
    assert_eq!(reason(&input,&trace,Some(&wrong)),Some(InputUnavailableReason::UndeclaredAffectedInput));
    let trace=apply(&mut input,"/number",Action::Classify);
    assert_eq!(reason(&input,&trace,None),None);
}
#[devkit::test]
fn required_paths_cover_ancestors_arrays_escaped_names_and_branch_selectors(){
    assert!(overlaps("/calls/*/at","/calls/12/at"));
    assert!(overlaps("/calls/*/at","/calls"));
    assert!(overlaps("/calls","/calls/12/at"));
    assert!(!overlaps("/calls/*/at","/calls/12/text"));
    assert!(overlaps("/a~1b/~0","/a~1b/~0"));
    let mut requirements=contract(&["/kind"]);
    let mut geometry=InputRequirement::new(requirements[0].kind.clone(),["/width"]);
    geometry.when.push(("/kind".into(),json!("screen")));requirements.push(geometry);
    let mut input=event();input.payload["kind"]=json!("audio");
    let trace=apply(&mut input,"/width",Action::Mask);
    assert_eq!(reason(&input,&trace,Some(&requirements)),None,"audio does not use OCR geometry");
    input.payload["kind"]=json!("screen");
    assert_eq!(reason(&input,&trace,Some(&requirements)),Some(InputUnavailableReason::RequiredPathTransformed));
    let trace=apply(&mut input,"/kind",Action::Mask);
    assert_eq!(reason(&input,&trace,Some(&requirements)),Some(InputUnavailableReason::RequiredPathTransformed),"masked selector cannot select a harmless branch");
}

#[devkit::test]
async fn projection_trace_survives_same_value_mask_and_rule_loosening(db:devkit::pg::TestDatabase){
    use crate::{AdmissionContext,NoDerivations};
    use sinex_model::{Batch,BatchKind,RecordOutcome,RunId,MaterialId,RecordLocation,event_types::operator::LogEntry};
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();
    let raw=Event::build(&LogEntry{written_at:None,label:Some("unrelated".into()),text:"[masked]".into()},"fixture",
        Origin::Record{location:RecordLocation{material:MaterialId::new(),anchor:0},output_index:0},ReadingOrder(1),[0;32],Class::Open).unwrap();
    let mut tx=conn.begin().await.unwrap();tx.admit(&Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![RecordOutcome::Events{location:None,events:vec![raw.clone()]}],archive:vec![]},&NoDerivations).await.unwrap();tx.commit().await.unwrap();
    let required=vec![InputRequirement::new(KindRef{name:raw.event_type.clone(),version:raw.kind_version},["/text"])];
    crate::privacy::add_rule(&mut conn,&crate::privacy::NewRule{name:"mask".into(),source:"fixture".into(),event_type:Some(raw.event_type.clone()),field:Some("/text".into()),matcher:"all".into(),matcher_arg:None,class:Class::Open,action:Action::Mask,author:"fixture".into()}).await.unwrap();
    assert!(unavailable(&mut conn,std::slice::from_ref(&raw),Some(&required)).await.is_err(),"stale projection is unavailable before build");
    projection::ensure_current(&mut conn).await.unwrap();
    let visible:serde_json::Value=sqlx::query_scalar("SELECT payload FROM core.derivation_events WHERE id=$1").bind(raw.id.0).fetch_one(&mut *conn).await.unwrap();
    assert_eq!(visible,raw.payload);
    let issues=unavailable(&mut conn,std::slice::from_ref(&raw),Some(&required)).await.unwrap();
    assert_eq!(issues[0].reason,InputUnavailableReason::RequiredPathTransformed);
    register(&mut conn,"fixture.consumer",2,"build",Some(&required)).await.unwrap();
    assert!(register(&mut conn,"fixture.consumer",2,"build",None).await.is_err(),"a build's input footprint cannot change");
    crate::privacy::end_rule(&mut conn,"mask","fixture").await.unwrap();projection::ensure_current(&mut conn).await.unwrap();
    assert!(unavailable(&mut conn,std::slice::from_ref(&raw),Some(&required)).await.unwrap().is_empty());
}

fn selector_contract(kind:&KindRef)->Vec<InputRequirement>{vec![InputRequirement::new(kind.clone(),["/targets","/request/operation","/recorded_response"])]}
async fn selector_kind(conn:&mut PgConnection,version:u64,validated:bool)->KindRef{
    let kind=KindRef{name:"fixture.selector".into(),version};
    let mut declaration=sinex_model::KindDeclaration::schemaless(kind.clone(),"fixture".into());
    if validated{declaration.payload_schema=Some(json!({"type":"object","required":["targets","request","recorded_response"],"properties":{
        "targets":{"type":"array","items":{"type":"string"}},"request":{"type":"object","required":["operation"],"properties":{"operation":{"type":"string"}}},
        "recorded_response":{"type":["string","null"]}}}));}
    sinex_schema::kinds::register(conn,"fixture",&[declaration]).await.unwrap();kind
}
fn selector_event(kind:&KindRef,target:&str,operation:&str)->Event{
    Event::submitted(kind.clone(),json!({"targets":[target],"request":{"operation":operation,"instructions":{"version":"expected"}},"recorded_response":null,"requested_by":"operator"}),
        "fixture.selector_source".into(),Origin::Record{location:sinex_model::RecordLocation{material:sinex_model::MaterialId::new(),anchor:0},output_index:0},ReadingOrder(1),[7;32],Class::Open)
}
async fn selector_admit(conn:&mut crate::AdmissionContext,events:Vec<Event>){
    use sinex_model::{Batch,BatchKind,RecordOutcome,RunId};
    let mut tx=conn.begin().await.unwrap();tx.admit(&Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![RecordOutcome::Events{location:None,events}],archive:vec![]},&crate::NoDerivations).await.unwrap();tx.commit().await.unwrap();
}
async fn selector_rule(conn:&mut PgConnection,name:&str,path:&str,action:Action,class:Class){
    crate::privacy::add_rule(conn,&crate::privacy::NewRule{name:name.into(),source:"fixture.selector_source".into(),event_type:Some("fixture.selector".into()),field:Some(path.into()),matcher:"all".into(),matcher_arg:None,class,action,author:"fixture".into()}).await.unwrap();
    projection::ensure_current(conn).await.unwrap();
}
async fn coverage(conn:&mut PgConnection,kind:&KindRef,requirements:Option<&[InputRequirement]>)->SelectorCoverage{
    use sqlx::Connection;
    let mut tx=conn.begin().await.unwrap();crate::query::read_one_snapshot(&mut tx).await.unwrap();
    let read=crate::ReadMark::now(&mut tx).await.unwrap();let through=crate::query::current_seq(&mut tx).await.unwrap();
    let result=selector_coverage(&mut tx,read,through,std::slice::from_ref(kind),requirements,1,1).await.unwrap();
    tx.rollback().await.unwrap();result
}

#[devkit::test]
async fn selector_coverage_finds_masked_membership_after_multiple_transfer_pages(db:devkit::pg::TestDatabase){
    let mut conn=crate::AdmissionContext::connect(&db.url(None)).await.unwrap();let kind=selector_kind(&mut conn,2,true).await;let requirements=selector_contract(&kind);
    let mut events:Vec<_>=(0..5).map(|i|selector_event(&kind,&format!("other-{i}"),"screen-ocr")).collect();let wanted=selector_event(&kind,"wanted","screen-ocr");let wanted_id=wanted.id;events.push(wanted);selector_admit(&mut conn,events).await;
    assert!(matches!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Complete{..}));
    // Only the last candidate is changed. A first-page-only guard would
    // incorrectly certify the subsequent empty reverse query.
    crate::privacy::add_rule(&mut conn,&crate::privacy::NewRule{name:"target".into(),source:"fixture.selector_source".into(),event_type:Some(kind.name.clone()),field:Some("/targets".into()),matcher:"pattern".into(),matcher_arg:Some("wanted".into()),class:Class::Open,action:Action::Mask,author:"fixture".into()}).await.unwrap();projection::ensure_current(&mut conn).await.unwrap();
    let filtered:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_events WHERE event_type=$1 AND payload->'targets' @> '[\"wanted\"]'::jsonb AND payload#>>'{request,operation}'='screen-ocr'").bind(&kind.name).fetch_one(&mut *conn).await.unwrap();assert_eq!(filtered,0);
    assert_eq!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::RequiredPathTransformed});
    crate::privacy::end_rule(&mut conn,"target","fixture").await.unwrap();projection::ensure_current(&mut conn).await.unwrap();
    assert!(matches!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Complete{..}));
    let filtered:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_events WHERE event_type=$1 AND payload->'targets' @> '[\"wanted\"]'::jsonb").bind(&kind.name).fetch_one(&mut *conn).await.unwrap();assert_eq!(filtered,1);
    let mut tx=conn.begin().await.unwrap();tx.admit(&sinex_model::Batch{run:sinex_model::RunId::new(),kind:sinex_model::BatchKind::Api{actor:"fixture".into()},records:vec![],archive:vec![sinex_model::ArchiveRequest::Hide{target:wanted_id,operation:sinex_model::OperationId::new()}]},&crate::NoDerivations).await.unwrap();tx.commit().await.unwrap();
    assert!(matches!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Complete{..}),"explicitly archived rows are outside the all-current live candidate scope");
}

#[devkit::test]
async fn selector_coverage_tracks_same_value_conditions_unrelated_fields_and_grants(db:devkit::pg::TestDatabase){
    let mut conn=crate::AdmissionContext::connect(&db.url(None)).await.unwrap();let kind=selector_kind(&mut conn,2,true).await;
    let mut requirements=selector_contract(&kind);requirements[0].when.push(("/request/instructions/version".into(),json!("expected")));
    selector_admit(&mut conn,vec![selector_event(&kind,"wanted","[masked]")]).await;
    selector_rule(&mut conn,"audit","/requested_by",Action::Mask,Class::Open).await;
    assert!(matches!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Complete{..}),"unrelated audit field does not invalidate membership or known response=null");
    let before:serde_json::Value=sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE event_type=$1").bind(&kind.name).fetch_one(&mut *conn).await.unwrap();
    selector_rule(&mut conn,"operation","/request/operation",Action::Mask,Class::Open).await;
    let after:serde_json::Value=sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE event_type=$1").bind(&kind.name).fetch_one(&mut *conn).await.unwrap();assert_eq!(before,after);
    assert_eq!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::RequiredPathTransformed});
    crate::privacy::end_rule(&mut conn,"operation","fixture").await.unwrap();projection::ensure_current(&mut conn).await.unwrap();
    selector_rule(&mut conn,"condition","/request/instructions/version",Action::DropField,Class::Open).await;
    assert_eq!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::RequiredPathTransformed});
    crate::privacy::end_rule(&mut conn,"condition","fixture").await.unwrap();projection::ensure_current(&mut conn).await.unwrap();
    selector_rule(&mut conn,"class","/targets",Action::Classify,Class::Guarded).await;
    assert_eq!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::Withheld});
    crate::query::set_grants(&mut conn,"guarded").await.unwrap();
    assert!(matches!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Complete{..}),"current connection grants, rather than request data, permit selection");
}

#[devkit::test]
async fn selector_coverage_refuses_missing_contract_required_path_projection_and_unvalidated_shape(db:devkit::pg::TestDatabase){
    let mut conn=crate::AdmissionContext::connect(&db.url(None)).await.unwrap();let kind=selector_kind(&mut conn,2,true).await;let requirements=selector_contract(&kind);
    let input=selector_event(&kind,"wanted","screen-ocr");let id=input.id.0;selector_admit(&mut conn,vec![input]).await;
    assert_eq!(coverage(&mut conn,&kind,None).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::UndeclaredSelector});
    let configured=vec![InputRequirement::new(kind.clone(),["/targets","/policy"])];
    assert_eq!(coverage(&mut conn,&kind,Some(&configured)).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::RequiredPathMissing});
    sqlx::query("DELETE FROM privacy.event_outputs WHERE event_id=$1").bind(id).execute(&mut *conn).await.unwrap();
    assert_eq!(coverage(&mut conn,&kind,Some(&requirements)).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::ProjectionUnavailable});
    let unknown=selector_kind(&mut conn,3,false).await;selector_admit(&mut conn,vec![selector_event(&unknown,"wanted","screen-ocr")]).await;
    assert_eq!(coverage(&mut conn,&unknown,Some(&selector_contract(&unknown))).await,SelectorCoverage::Unavailable{reason:SelectorUnavailable::UnvalidatedSelector});
}

#[devkit::test]
async fn selector_coverage_requires_the_existing_snapshot_and_current_read_mark(db:devkit::pg::TestDatabase){
    use sqlx::Connection;
    let mut conn=crate::AdmissionContext::connect(&db.url(None)).await.unwrap();let kind=selector_kind(&mut conn,2,true).await;let requirements=selector_contract(&kind);
    let read=crate::ReadMark::now(&mut conn).await.unwrap();let through=crate::query::current_seq(&mut conn).await.unwrap();
    assert_eq!(selector_coverage(&mut conn,read,through,std::slice::from_ref(&kind),Some(&requirements),1,1).await.unwrap(),SelectorCoverage::Unavailable{reason:SelectorUnavailable::SnapshotRequired});
    selector_admit(&mut conn,vec![selector_event(&kind,"wanted","screen-ocr")]).await;
    let mut tx=conn.begin().await.unwrap();crate::query::read_one_snapshot(&mut tx).await.unwrap();
    assert_eq!(selector_coverage(&mut tx,read,through,std::slice::from_ref(&kind),Some(&requirements),1,1).await.unwrap(),SelectorCoverage::Unavailable{reason:SelectorUnavailable::ReadScopeChanged});
    tx.rollback().await.unwrap();
}

#[devkit::test]
fn selector_presence_obeys_json_pointer_array_indices_and_explicit_null(){
    let payload=json!({"targets":[{"event":"a"},{"event":"b"}],"recorded_response":null,"01":"object key"});
    for path in ["/targets/0/event","/targets/*/event","/recorded_response","/01"]{assert!(present(&payload,&components(path).unwrap()),"{path}");}
    for path in ["/targets/01/event","/targets/+1/event","/missing"]{assert!(!present(&payload,&components(path).unwrap()),"{path}");}
    assert!(present(&json!({"targets":[]}),&components("/targets/*/event").unwrap()),"an explicit empty collection has known membership");
}
